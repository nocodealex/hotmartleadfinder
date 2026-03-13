"""
Pipedrive CRM client — search for persons, fetch deals, enrich prospects.

Uses the Pipedrive REST API v1 with an API token for authentication.
Caches results locally to avoid hammering the API on repeated dashboard loads.
"""

import json
import logging
import time
from pathlib import Path
from typing import Optional

import requests

import config

logger = logging.getLogger(__name__)

CACHE_FILE = config.DATA_DIR / "pipedrive_cache.json"
STAGES_CACHE_FILE = config.DATA_DIR / "pipedrive_stages.json"


class PipedriveClient:

    def __init__(
        self,
        api_token: str = "",
        domain: str = "",
    ):
        self.api_token = api_token or config.PIPEDRIVE_API_TOKEN
        self.domain = domain or config.PIPEDRIVE_DOMAIN
        if not self.api_token or not self.domain:
            raise ValueError(
                "PIPEDRIVE_API_TOKEN and PIPEDRIVE_DOMAIN must be set. "
                "Add them to your .env file."
            )
        self.base_url = f"https://{self.domain}.pipedrive.com"
        self._cache: dict = self._load_cache(CACHE_FILE)
        self._stages: dict = self._load_cache(STAGES_CACHE_FILE)
        self._call_count = 0

    # ── Helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _load_cache(path: Path) -> dict:
        if path.exists():
            try:
                return json.loads(path.read_text())
            except (json.JSONDecodeError, IOError):
                pass
        return {}

    @staticmethod
    def _save_cache(path: Path, data: dict):
        path.write_text(json.dumps(data, indent=2, default=str, ensure_ascii=False))

    def _get(self, endpoint: str, params: dict | None = None) -> dict:
        """Make an authenticated GET request to the Pipedrive API."""
        if self._call_count > 0:
            time.sleep(0.25)
        self._call_count += 1

        url = f"{self.base_url}{endpoint}"
        params = params or {}
        params["api_token"] = self.api_token

        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.warning(f"Pipedrive API error: {e}")
            return {}

    # ── Stages ────────────────────────────────────────────────────────

    def get_stages(self) -> dict[int, dict]:
        """
        Fetch all pipeline stages and cache them.
        Returns {stage_id: {"name": ..., "pipeline_name": ...}}.
        """
        if self._stages:
            return self._stages

        result = self._get("/v1/stages")
        if not result.get("success"):
            return {}

        # Also fetch pipelines for names
        pipelines_result = self._get("/v1/pipelines")
        pipeline_map = {}
        if pipelines_result.get("success"):
            for p in pipelines_result.get("data", []) or []:
                pipeline_map[p["id"]] = p.get("name", "")

        stages = {}
        for stage in result.get("data", []) or []:
            stages[stage["id"]] = {
                "name": stage.get("name", ""),
                "pipeline_id": stage.get("pipeline_id"),
                "pipeline_name": pipeline_map.get(stage.get("pipeline_id"), ""),
                "order_nr": stage.get("order_nr", 0),
            }

        self._stages = stages
        self._save_cache(STAGES_CACHE_FILE, stages)
        return stages

    # ── Person Search ─────────────────────────────────────────────────

    def search_person(self, name: str) -> Optional[dict]:
        """
        Search Pipedrive for a person by name.
        Returns the best match or None.
        """
        cache_key = name.lower().strip()
        if cache_key in self._cache:
            cached = self._cache[cache_key]
            if cached is not None:
                return cached
            return None

        if not name or len(name.strip()) < 2:
            self._cache[cache_key] = None
            return None

        result = self._get("/v1/persons/search", {
            "term": name,
            "fields": "name",
            "limit": 5,
        })

        if not result.get("success"):
            return None

        items = result.get("data", {}).get("items", [])
        if not items:
            self._cache[cache_key] = None
            self._save_cache(CACHE_FILE, self._cache)
            return None

        best = items[0].get("item", {})

        raw_emails = best.get("emails") or []
        emails = []
        for e in raw_emails:
            if isinstance(e, dict):
                val = e.get("value", "")
            elif isinstance(e, str):
                val = e
            else:
                continue
            if val:
                emails.append(val)

        raw_phones = best.get("phones") or []
        phones = []
        for p in raw_phones:
            if isinstance(p, dict):
                val = p.get("value", "")
            elif isinstance(p, str):
                val = p
            else:
                continue
            if val:
                phones.append(val)

        org = best.get("organization")
        org_name = ""
        if isinstance(org, dict):
            org_name = org.get("name", "")
        elif isinstance(org, str):
            org_name = org

        owner = best.get("owner")
        owner_name = ""
        if isinstance(owner, dict):
            owner_name = owner.get("name", "")

        person_data = {
            "id": best.get("id"),
            "name": best.get("name", ""),
            "emails": emails,
            "phones": phones,
            "org_name": org_name,
            "owner_name": owner_name,
        }

        self._cache[cache_key] = person_data
        self._save_cache(CACHE_FILE, self._cache)
        return person_data

    # ── Deal Fetching ─────────────────────────────────────────────────

    def get_person_deals(self, person_id: int) -> list[dict]:
        """Fetch all deals associated with a person."""
        cache_key = f"deals_{person_id}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        result = self._get(f"/v1/persons/{person_id}/deals", {
            "status": "all_not_deleted",
        })

        if not result.get("success") or not result.get("data"):
            self._cache[cache_key] = []
            self._save_cache(CACHE_FILE, self._cache)
            return []

        stages = self.get_stages()
        deals = []
        for deal in result["data"]:
            stage_id = deal.get("stage_id")
            stage_info = stages.get(stage_id) or stages.get(str(stage_id)) or {}
            deals.append({
                "id": deal.get("id"),
                "title": deal.get("title", ""),
                "status": deal.get("status", ""),  # open, won, lost, deleted
                "stage_name": stage_info.get("name", ""),
                "pipeline_name": stage_info.get("pipeline_name", ""),
                "value": deal.get("value", 0),
                "currency": deal.get("currency", ""),
                "add_time": deal.get("add_time", ""),
                "won_time": deal.get("won_time", ""),
                "lost_time": deal.get("lost_time", ""),
            })

        self._cache[cache_key] = deals
        self._save_cache(CACHE_FILE, self._cache)
        return deals

    # ── Prospect Enrichment ──────────────────────────────────────────

    def enrich_prospects(self, prospects: list[dict]) -> list[dict]:
        """
        Enrich a list of prospect dicts with Pipedrive CRM data.
        Adds keys: crm_status, crm_person_id, crm_deal_stage, crm_deal_status,
        crm_pipeline, crm_person_name.
        """
        enriched = []
        for prospect in prospects:
            try:
                enriched.append(self.enrich_single(prospect))
            except Exception as e:
                logger.warning(f"Pipedrive enrich failed for @{prospect.get('username', '?')}: {e}")
                result = dict(prospect)
                result.update({
                    "crm_status": "Error",
                    "crm_person_id": None,
                    "crm_person_name": "",
                    "crm_deal_stage": "",
                    "crm_deal_status": "",
                    "crm_pipeline": "",
                })
                enriched.append(result)
        return enriched

    def enrich_single(self, prospect: dict) -> dict:
        """Enrich a single prospect with Pipedrive data."""
        result = dict(prospect)
        name = prospect.get("full_name", "").strip()

        result["crm_status"] = "Not in CRM"
        result["crm_tag"] = "new"
        result["crm_person_id"] = None
        result["crm_person_name"] = ""
        result["crm_deal_stage"] = ""
        result["crm_deal_status"] = ""
        result["crm_pipeline"] = ""

        if not name:
            return result

        person = self.search_person(name)
        if not person:
            return result

        result["crm_status"] = "In CRM"
        result["crm_tag"] = "no_deal"
        result["crm_person_id"] = person["id"]
        result["crm_person_name"] = person.get("name", "")

        deals = self.get_person_deals(person["id"])
        if deals:
            active_deals = [d for d in deals if d["status"] == "open"]
            won_deals = [d for d in deals if d["status"] == "won"]

            if won_deals:
                best = won_deals[0]
                result["crm_status"] = "Won"
                result["crm_tag"] = "won_deal"
                result["crm_deal_stage"] = "Won"
                result["crm_deal_status"] = "won"
                result["crm_pipeline"] = best.get("pipeline_name", "")
            elif active_deals:
                best = active_deals[0]
                result["crm_status"] = best.get("stage_name", "Active Deal")
                result["crm_tag"] = "active_deal"
                result["crm_deal_stage"] = best.get("stage_name", "")
                result["crm_deal_status"] = "open"
                result["crm_pipeline"] = best.get("pipeline_name", "")
            else:
                lost = [d for d in deals if d["status"] == "lost"]
                if lost:
                    result["crm_status"] = "Lost"
                    result["crm_tag"] = "lost_deal"
                    result["crm_deal_stage"] = "Lost"
                    result["crm_deal_status"] = "lost"
                    result["crm_pipeline"] = lost[0].get("pipeline_name", "")

        return result

    def clear_cache(self):
        """Clear all cached data to force fresh API calls."""
        self._cache = {}
        self._save_cache(CACHE_FILE, self._cache)
        self._stages = {}
        if STAGES_CACHE_FILE.exists():
            STAGES_CACHE_FILE.unlink()

    def test_connection(self) -> bool:
        """Verify API token and domain are valid."""
        result = self._get("/v1/users/me")
        if result.get("success"):
            user = result.get("data", {})
            logger.info(
                f"Pipedrive connected as: {user.get('name', '?')} "
                f"({user.get('email', '?')})"
            )
            return True
        return False
