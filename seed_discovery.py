"""
Seed Discovery — automatically finds new seed accounts for the pipeline.

Methods:
  1. Search Instagram for accounts with "Hotmart" in their bio
  2. Promote high-scoring leads to seeds (recursive expansion)
  3. Known Hotmart ecosystem accounts (manually curated)
"""

import logging
from typing import Optional

from instagram_client import InstagramClient, InstagramAPIError
from storage import Storage

logger = logging.getLogger(__name__)

# ── Known Hotmart ecosystem accounts ─────────────────────────────────
# Manually curated list of accounts deeply embedded in the Hotmart
# ecosystem. These are excellent seeds for network crawling.
KNOWN_HOTMART_SEEDS = [
    # Hotmart official
    "hotmart",
    "hotmart.es",
    # Top Brazilian digital product creators
    "wendellcarvalho",
    "paborges",
    "joaopedrocarvalho",
    # Top Spanish-speaking Hotmart affiliates
    "davidmarchante",
    "ialbertous",
    # Hotmart co-production / agency ecosystem
    "calixtofabio",
]


class SeedDiscovery:

    def __init__(
        self,
        ig_client: Optional[InstagramClient] = None,
        storage: Optional[Storage] = None,
    ):
        self.ig = ig_client or InstagramClient()
        self.storage = storage or Storage()

    def add_known_seeds(self) -> list[str]:
        """Add the curated list of known Hotmart ecosystem accounts."""
        added = []
        for username in KNOWN_HOTMART_SEEDS:
            if self.storage.add_seed(username):
                added.append(username)
        return added

    def promote_leads_to_seeds(
        self, min_score: float = 0.70, max_to_add: int = 20
    ) -> list[str]:
        """
        Promote high-scoring leads to seed accounts.
        This creates recursive expansion — leads become seeds whose
        following lists are crawled for more leads.
        """
        leads = self.storage.get_all_leads()
        added = []
        for lead in leads:
            if len(added) >= max_to_add:
                break
            if lead.get("overall_score", 0) >= min_score:
                username = lead["username"]
                if self.storage.add_seed(username):
                    added.append(username)
                    logger.info(
                        f"Promoted lead @{username} to seed "
                        f"(score={lead['overall_score']:.2f})"
                    )
        return added

    def search_instagram_bios(
        self, query: str = "hotmart", max_results: int = 50
    ) -> list[str]:
        """
        Search Instagram for accounts matching a query.
        Uses the Instagram API search endpoint if available.
        """
        # Try the search endpoint
        try:
            data = self.ig._get("/search", {"query": query, "type": "user"})
            users = []
            if isinstance(data, dict):
                users = data.get("users", data.get("items", data.get("data", [])))
            elif isinstance(data, list):
                users = data

            added = []
            for user in users[:max_results]:
                username = ""
                if isinstance(user, dict):
                    username = user.get("username", "")
                if username and self.storage.add_seed(username):
                    added.append(username)
                    logger.info(f"Found seed via search: @{username}")

            return added

        except InstagramAPIError as e:
            logger.warning(f"Instagram search not available: {e}")
            return []

    def discover_all(self) -> dict:
        """
        Run all seed discovery methods and return a summary.
        """
        results = {
            "known_seeds_added": [],
            "leads_promoted": [],
            "search_results": [],
        }

        # 1. Add known ecosystem accounts
        results["known_seeds_added"] = self.add_known_seeds()
        logger.info(f"Added {len(results['known_seeds_added'])} known seeds")

        # 2. Promote high-scoring leads
        results["leads_promoted"] = self.promote_leads_to_seeds()
        logger.info(f"Promoted {len(results['leads_promoted'])} leads to seeds")

        # 3. Search Instagram for "hotmart"
        for query in ["hotmart", "lançamentos digitais", "coprodutor digital"]:
            found = self.search_instagram_bios(query, max_results=20)
            results["search_results"].extend(found)

        logger.info(
            f"Seed discovery complete: "
            f"{len(results['known_seeds_added'])} known, "
            f"{len(results['leads_promoted'])} promoted, "
            f"{len(results['search_results'])} from search. "
            f"Total seeds: {len(self.storage.get_seeds())}"
        )

        return results
