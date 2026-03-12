"""
Persistent storage for Hotmart Lead Finder.

Tracks:
  - Processed accounts (so we never re-process)
  - Qualified leads (with full analysis data)
  - Seed accounts
  - Appearance counts (how many seeds follow the same account)
  - Outreach status

All data is stored as JSON files in the data/ directory, with CSV export
for the final leads spreadsheet.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

import config
from models import Lead, LeadAnalysis, LeadClassification, LeadTier, InstagramProfile

logger = logging.getLogger(__name__)


class Storage:

    def __init__(self):
        config.DATA_DIR.mkdir(exist_ok=True)
        self._processed: dict = self._load_json(config.PROCESSED_ACCOUNTS_FILE, {})
        self._seeds: list = self._load_json(config.SEED_ACCOUNTS_FILE, [])
        self._leads: dict = self._load_json(config.DATA_DIR / "leads.json", {})
        self._whop_sellers: list = self._load_json(config.WHOP_SELLERS_FILE, [])

    # ── JSON helpers ─────────────────────────────────────────────────

    @staticmethod
    def _load_json(path: Path, default):
        if path.exists():
            try:
                with open(path) as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Could not load {path}: {e}")
        return default

    @staticmethod
    def _save_json(path: Path, data):
        with open(path, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)

    def _save_processed(self):
        self._save_json(config.PROCESSED_ACCOUNTS_FILE, self._processed)

    def _save_leads(self):
        self._save_json(config.DATA_DIR / "leads.json", self._leads)

    def _save_seeds(self):
        self._save_json(config.SEED_ACCOUNTS_FILE, self._seeds)

    # ── Seed accounts ────────────────────────────────────────────────

    def get_seeds(self) -> list[str]:
        return list(self._seeds)

    def add_seed(self, username: str) -> bool:
        username = username.lower().strip().lstrip("@")
        if username not in self._seeds:
            self._seeds.append(username)
            self._save_seeds()
            logger.info(f"Added seed account: @{username}")
            return True
        return False

    def remove_seed(self, username: str) -> bool:
        username = username.lower().strip().lstrip("@")
        if username in self._seeds:
            self._seeds.remove(username)
            self._save_seeds()
            return True
        return False

    # ── Processed accounts ───────────────────────────────────────────

    def is_processed(self, username: str) -> bool:
        return username.lower() in self._processed

    def mark_processed(self, username: str, seed: str, score: float = 0.0):
        key = username.lower()
        now = datetime.now(timezone.utc).isoformat()
        if key in self._processed:
            entry = self._processed[key]
            entry["last_seen"] = now
            if seed not in entry["seen_via_seeds"]:
                entry["seen_via_seeds"].append(seed)
            entry["appearance_count"] = len(entry["seen_via_seeds"])
            entry["score"] = max(entry.get("score", 0), score)
        else:
            self._processed[key] = {
                "first_seen": now,
                "last_seen": now,
                "seen_via_seeds": [seed],
                "appearance_count": 1,
                "score": score,
            }
        self._save_processed()

    def get_appearance_count(self, username: str) -> int:
        entry = self._processed.get(username.lower(), {})
        return entry.get("appearance_count", 0)

    def get_seen_via_seeds(self, username: str) -> list[str]:
        entry = self._processed.get(username.lower(), {})
        return entry.get("seen_via_seeds", [])

    def increment_appearance(self, username: str, seed: str):
        """Record that we saw this username via a new seed, without re-analysing."""
        key = username.lower()
        if key in self._processed:
            if seed not in self._processed[key]["seen_via_seeds"]:
                self._processed[key]["seen_via_seeds"].append(seed)
                self._processed[key]["appearance_count"] = len(
                    self._processed[key]["seen_via_seeds"]
                )
                self._processed[key]["last_seen"] = (
                    datetime.now(timezone.utc).isoformat()
                )
                self._save_processed()

    def clear_processed(self):
        """Reset processed accounts (useful for re-running with new prompts)."""
        self._processed = {}
        self._save_processed()

    # ── Known Whop sellers ───────────────────────────────────────────
    # Sellers who already switched to Whop — skip contacting them
    # and use their names as social proof in outreach DMs.

    def get_whop_sellers(self) -> list[str]:
        return list(self._whop_sellers)

    def add_whop_seller(self, username: str) -> bool:
        username = username.lower().strip().lstrip("@")
        if username not in self._whop_sellers:
            self._whop_sellers.append(username)
            self._save_json(config.WHOP_SELLERS_FILE, self._whop_sellers)
            return True
        return False

    def is_whop_seller(self, username: str) -> bool:
        return username.lower().strip().lstrip("@") in self._whop_sellers

    def remove_whop_seller(self, username: str) -> bool:
        username = username.lower().strip().lstrip("@")
        if username in self._whop_sellers:
            self._whop_sellers.remove(username)
            self._save_json(config.WHOP_SELLERS_FILE, self._whop_sellers)
            return True
        return False

    # ── Leads ────────────────────────────────────────────────────────

    def save_lead(self, lead: Lead):
        key = lead.profile.username.lower()
        now = datetime.now(timezone.utc).isoformat()
        self._leads[key] = {
            "username": lead.profile.username,
            "full_name": lead.profile.full_name,
            "bio": lead.profile.bio,
            "bio_link": lead.profile.bio_link,
            "follower_count": lead.profile.follower_count,
            "following_count": lead.profile.following_count,
            "is_verified": lead.profile.is_verified,
            "overall_score": lead.analysis.overall_score,
            "classification": lead.analysis.classification.value,
            "tier": lead.analysis.tier.value,
            "lead_type": lead.analysis.lead_type,
            "niche": lead.analysis.bio_result.details.get("niche", "unknown") if lead.analysis.bio_result else "unknown",
            "summary": lead.analysis.summary,
            "bio_score": lead.analysis.bio_result.score if lead.analysis.bio_result else 0,
            "bio_reasoning": lead.analysis.bio_result.reasoning if lead.analysis.bio_result else "",
            "website_score": lead.analysis.website_result.score if lead.analysis.website_result else 0,
            "website_reasoning": lead.analysis.website_result.reasoning if lead.analysis.website_result else "",
            "caption_score": lead.analysis.caption_result.score if lead.analysis.caption_result else 0,
            "caption_reasoning": lead.analysis.caption_result.reasoning if lead.analysis.caption_result else "",
            "event_score": lead.analysis.event_result.score if lead.analysis.event_result else 0,
            "event_reasoning": lead.analysis.event_result.reasoning if lead.analysis.event_result else "",
            "appearance_count": lead.analysis.appearance_count,
            "boosted": lead.analysis.boosted,
            "found_via_seeds": lead.found_via_seeds,
            "seed_depth": lead.seed_depth,
            "first_seen": lead.first_seen or now,
            "last_updated": now,
            "instagram_url": f"https://instagram.com/{lead.profile.username}",
        }
        self._save_leads()
        logger.info(
            f"Saved lead: @{lead.profile.username} "
            f"(score={lead.analysis.overall_score:.2f}, "
            f"{lead.analysis.tier.value}, "
            f"{lead.analysis.classification.value})"
        )

    def get_lead(self, username: str) -> Optional[dict]:
        return self._leads.get(username.lower())

    def get_all_leads(self) -> list[dict]:
        return sorted(
            self._leads.values(),
            key=lambda x: x.get("overall_score", 0),
            reverse=True,
        )

    def get_leads_by_tier(self, tier: str) -> list[dict]:
        return sorted(
            [l for l in self._leads.values() if l.get("tier") == tier],
            key=lambda x: x.get("overall_score", 0),
            reverse=True,
        )

    def get_new_leads_since(self, since_iso: str) -> list[dict]:
        return [
            lead for lead in self._leads.values()
            if lead.get("first_seen", "") >= since_iso
        ]

    # ── Export ────────────────────────────────────────────────────────

    def export_csv(self, path: Optional[Path] = None) -> Path:
        """Export all leads to a CSV file, sorted by score descending."""
        path = path or config.LEADS_CSV
        leads = self.get_all_leads()

        if not leads:
            logger.warning("No leads to export")
            columns = [
                "username", "full_name", "bio", "bio_link", "instagram_url",
                "overall_score", "classification", "tier", "lead_type", "summary",
                "bio_score", "website_score", "caption_score", "event_score",
                "appearance_count", "boosted", "follower_count",
                "found_via_seeds", "seed_depth", "first_seen", "last_updated",
            ]
            df = pd.DataFrame(columns=columns)
        else:
            df = pd.DataFrame(leads)
            priority_cols = [
                "username", "full_name", "overall_score", "tier", "lead_type",
                "classification",
                "bio", "bio_link", "instagram_url", "summary",
                "bio_score", "bio_reasoning",
                "website_score", "website_reasoning",
                "caption_score", "caption_reasoning",
                "event_score", "event_reasoning",
                "appearance_count", "boosted",
                "follower_count", "following_count",
                "found_via_seeds", "seed_depth",
                "first_seen", "last_updated",
            ]
            existing_cols = [c for c in priority_cols if c in df.columns]
            extra_cols = [c for c in df.columns if c not in priority_cols]
            df = df[existing_cols + extra_cols]

        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info(f"Exported {len(leads)} leads to {path}")
        return path

    # ── Stats ────────────────────────────────────────────────────────

    def stats(self) -> dict:
        leads = self.get_all_leads()
        tier_counts = {}
        for t in LeadTier:
            tier_counts[t.value] = sum(
                1 for l in leads if l.get("tier") == t.value
            )
        return {
            "seed_accounts": len(self._seeds),
            "total_processed": len(self._processed),
            "total_leads": len(leads),
            "high_value": sum(1 for l in leads if l["classification"] == "high_value"),
            "potential_value": sum(
                1 for l in leads if l["classification"] == "potential_value"
            ),
            "boosted_leads": sum(1 for l in leads if l.get("boosted")),
            "avg_score": (
                sum(l["overall_score"] for l in leads) / len(leads)
                if leads
                else 0
            ),
            "tiers": tier_counts,
        }
