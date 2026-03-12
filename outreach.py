"""
Outreach Pipeline — generates personalized DM drafts and tracks
outreach status for qualified leads.

Features:
  - Auto-generate personalized DM drafts using Claude
  - Track outreach status (not_contacted → dm_sent → responded → converted)
  - Follow-up reminders
  - Export outreach report
"""

import json
import time
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import anthropic
import pandas as pd

import config

logger = logging.getLogger(__name__)

OUTREACH_FILE = config.DATA_DIR / "outreach.json"

# ── Outreach statuses ────────────────────────────────────────────────
STATUS_NOT_CONTACTED = "not_contacted"
STATUS_DM_DRAFTED = "dm_drafted"
STATUS_DM_SENT = "dm_sent"
STATUS_RESPONDED = "responded"
STATUS_MEETING_BOOKED = "meeting_booked"
STATUS_CONVERTED = "converted"
STATUS_NOT_INTERESTED = "not_interested"

ALL_STATUSES = [
    STATUS_NOT_CONTACTED, STATUS_DM_DRAFTED, STATUS_DM_SENT,
    STATUS_RESPONDED, STATUS_MEETING_BOOKED, STATUS_CONVERTED,
    STATUS_NOT_INTERESTED,
]

# ── DM Generation Prompt ────────────────────────────────────────────
DM_DRAFT_PROMPT = """\
You are writing a personalized Instagram DM for Alex, a sales rep at \
Whop (whop.com). Whop is a platform for selling digital products, \
courses, memberships, and communities. Alex's job is to help Whop \
expand into Latin America and Spain — markets currently dominated \
by Hotmart.

The approach that works: a VERY short, casual DM that teases an idea \
and asks if they're open to a quick chat. NOT a pitch. NOT a wall of \
text. The goal is to get a reply, then set up a call.

## About the lead
Username: @{username}
Name: {full_name}
Bio: {bio}
Lead type: {lead_type}
Niche: {niche}
Bio analysis: {bio_reasoning}
{social_proof_line}

## Deal structure (for YOUR context, do NOT include exact numbers in DM)
- Referral partners earn 30% of gross profit for every seller they bring
- For very big partners, there may be equity on the table
- This is a generous, long-term partnership, not a one-time thing

## DM Guidelines
- Write in the SAME LANGUAGE as their bio ({language})
- MAXIMUM 2–3 sentences. Shorter = better. Under 200 characters ideal.
- Reference ONE specific thing from their bio or business so it \
  doesn't feel mass-sent
- Core message: "I have some ideas on how you/your agency could help \
  Whop expand into [their region]. Interested in a quick call?"
- Vary the phrasing — don't use the exact same template every time
- For AGENCIES: frame it as a referral partnership opportunity
- For BIG SELLERS: frame it as "we think Whop could be a game-changer \
  for your business" or "I have ideas on how we could work together"
- For AFFILIATES: frame it as a business opportunity with great upside
- Be CASUAL. Write like a real person, not a corporation.
- One emoji max. Zero is fine.
- Do NOT trash-talk Hotmart or any competitor
- End with a question to invite a reply

Respond with ONLY the DM text, nothing else. No quotes, no labels."""


class OutreachManager:

    def __init__(self):
        self._data: dict = self._load()
        self._claude = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)

    def _load(self) -> dict:
        if OUTREACH_FILE.exists():
            try:
                with open(OUTREACH_FILE) as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return {}

    def _save(self):
        with open(OUTREACH_FILE, "w") as f:
            json.dump(self._data, f, indent=2, ensure_ascii=False, default=str)

    # ── DM Draft Generation ──────────────────────────────────────────

    def generate_dm_draft(self, lead: dict, whop_sellers: list[str] | None = None) -> str:
        """Generate a personalized DM draft for a lead using Claude."""
        # Build social proof line
        social_proof_line = ""
        if whop_sellers:
            social_proof_line = (
                f"Social proof (sellers who already switched to Whop): "
                f"{', '.join('@' + s for s in whop_sellers[:5])}"
            )

        prompt = DM_DRAFT_PROMPT.format(
            username=lead.get("username", ""),
            full_name=lead.get("full_name", ""),
            bio=lead.get("bio", ""),
            lead_type=lead.get("lead_type", lead.get("classification", "")),
            niche=lead.get("niche", "unknown"),
            bio_reasoning=lead.get("bio_reasoning", ""),
            language=lead.get("language", "portuguese"),
            social_proof_line=social_proof_line,
        )

        try:
            message = self._claude.messages.create(
                model=config.CLAUDE_MODEL,
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}],
            )
            return message.content[0].text.strip()
        except Exception as e:
            logger.error(f"Failed to generate DM draft: {e}")
            return ""

    def generate_drafts_for_leads(
        self, leads: list[dict], overwrite: bool = False,
        whop_sellers: list[str] | None = None,
    ) -> int:
        """Generate DM drafts for all provided leads."""
        count = 0
        for lead in leads:
            username = lead["username"].lower()
            if not overwrite and username in self._data:
                if self._data[username].get("dm_draft"):
                    continue

            draft = self.generate_dm_draft(lead, whop_sellers=whop_sellers)
            if not draft:
                continue

            now = datetime.now(timezone.utc).isoformat()
            if username not in self._data:
                self._data[username] = {
                    "username": lead["username"],
                    "full_name": lead.get("full_name", ""),
                    "status": STATUS_DM_DRAFTED,
                    "score": lead.get("overall_score", 0),
                    "dm_draft": draft,
                    "created_at": now,
                    "updated_at": now,
                    "notes": "",
                    "follow_up_date": "",
                }
            else:
                self._data[username]["dm_draft"] = draft
                self._data[username]["status"] = STATUS_DM_DRAFTED
                self._data[username]["updated_at"] = now

            count += 1
            time.sleep(config.ANTHROPIC_DELAY_SECONDS)

        self._save()
        return count

    # ── Status Tracking ──────────────────────────────────────────────

    def update_status(self, username: str, status: str, notes: str = ""):
        username = username.lower()
        if username not in self._data:
            self._data[username] = {
                "username": username,
                "status": status,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        self._data[username]["status"] = status
        self._data[username]["updated_at"] = datetime.now(timezone.utc).isoformat()
        if notes:
            self._data[username]["notes"] = notes
        self._save()

    def set_follow_up(self, username: str, days_from_now: int = 3):
        username = username.lower()
        if username in self._data:
            follow_up = datetime.now(timezone.utc) + timedelta(days=days_from_now)
            self._data[username]["follow_up_date"] = follow_up.isoformat()
            self._save()

    def get_follow_ups_due(self) -> list[dict]:
        """Get leads with follow-ups due today or earlier."""
        now = datetime.now(timezone.utc).isoformat()
        due = []
        for entry in self._data.values():
            fu = entry.get("follow_up_date", "")
            if fu and fu <= now and entry.get("status") not in (
                STATUS_CONVERTED, STATUS_NOT_INTERESTED
            ):
                due.append(entry)
        return due

    # ── Reporting ────────────────────────────────────────────────────

    def get_all(self) -> list[dict]:
        return sorted(
            self._data.values(),
            key=lambda x: x.get("score", 0),
            reverse=True,
        )

    def get_by_status(self, status: str) -> list[dict]:
        return [
            e for e in self._data.values()
            if e.get("status") == status
        ]

    def funnel_stats(self) -> dict:
        stats = {s: 0 for s in ALL_STATUSES}
        for entry in self._data.values():
            status = entry.get("status", STATUS_NOT_CONTACTED)
            if status in stats:
                stats[status] += 1
        stats["total"] = len(self._data)
        return stats

    def export_csv(self, path: Optional[Path] = None) -> Path:
        path = path or config.DATA_DIR / "outreach.csv"
        data = self.get_all()
        if data:
            df = pd.DataFrame(data)
            cols = [
                "username", "full_name", "status", "score",
                "dm_draft", "notes", "follow_up_date",
                "created_at", "updated_at",
            ]
            existing = [c for c in cols if c in df.columns]
            df = df[existing]
        else:
            df = pd.DataFrame()
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return path
