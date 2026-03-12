"""
Network Graph — builds a follow-graph across all seeds to identify
high-signal accounts via cross-seed appearances.

Strategy:
  Phase 1: Fetch following lists for ALL seeds (graph building)
  Phase 2: Count how many seeds follow each account (appearance score)
  Phase 3: Prioritize accounts by appearance count for analysis

If 8 different Hotmart sellers all follow @someagency, that's a
stronger signal than any bio analysis.
"""

import json
import logging
from pathlib import Path
from collections import defaultdict
from typing import Optional

import config
from instagram_client import InstagramClient, InstagramAPIError
from storage import Storage

logger = logging.getLogger(__name__)

GRAPH_FILE = config.DATA_DIR / "network_graph.json"


class NetworkGraph:

    def __init__(
        self,
        ig_client: Optional[InstagramClient] = None,
        storage: Optional[Storage] = None,
    ):
        self.ig = ig_client or InstagramClient()
        self.storage = storage or Storage()
        # {username: set_of_seeds_that_follow_them}
        self._graph: dict[str, list[str]] = {}
        self._load()

    # ── Persistence ──────────────────────────────────────────────────

    def _load(self):
        if GRAPH_FILE.exists():
            try:
                with open(GRAPH_FILE) as f:
                    self._graph = json.load(f)
            except (json.JSONDecodeError, IOError):
                self._graph = {}

    def _save(self):
        with open(GRAPH_FILE, "w") as f:
            json.dump(self._graph, f, indent=2, ensure_ascii=False)

    # ── Graph building ───────────────────────────────────────────────

    def has_seed(self, seed: str) -> bool:
        """Check if a seed's following list is already in the graph."""
        return any(seed in seeds for seeds in self._graph.values())

    def build_for_seed(self, seed: str, force: bool = False) -> int:
        """
        Fetch the following list for a seed and add all edges to the graph.
        Returns the number of accounts added.
        Skips if seed is already in the graph (use force=True to re-scrape).
        """
        if not force and self.has_seed(seed):
            count = sum(1 for seeds in self._graph.values() if seed in seeds)
            logger.info(f"Graph: @{seed} already has {count} edges — skipping (use force=True to re-scrape)")
            return 0

        try:
            following_raw = self.ig.get_all_following(seed)
        except Exception as e:
            logger.warning(f"Error fetching following for @{seed}: {e}")
            return 0

        count = 0
        for user_data in following_raw:
            username = user_data.get("username", "").lower()
            if not username:
                continue

            if username not in self._graph:
                self._graph[username] = []

            if seed not in self._graph[username]:
                self._graph[username].append(seed)
                count += 1

        self._save()
        logger.info(
            f"Graph: added {count} edges from @{seed} "
            f"(following {len(following_raw)} accounts)"
        )
        return count

    def build_for_all_seeds(self, seeds: list[str] | None = None) -> dict:
        """
        Build the graph for all seed accounts.
        Returns summary stats.
        """
        seeds = seeds or self.storage.get_seeds()
        stats = {"seeds_processed": 0, "total_edges": 0, "errors": []}

        for seed in seeds:
            logger.info(f"Building graph for @{seed} ...")
            try:
                edges = self.build_for_seed(seed)
                stats["total_edges"] += edges
                stats["seeds_processed"] += 1
            except Exception as e:
                stats["errors"].append(f"@{seed}: {e}")
                logger.warning(f"Failed to build graph for @{seed}: {e}")

        stats["unique_accounts"] = len(self._graph)
        stats["multi_seed_accounts"] = sum(
            1 for v in self._graph.values() if len(v) >= 2
        )
        return stats

    # ── Querying ─────────────────────────────────────────────────────

    def get_appearance_count(self, username: str) -> int:
        return len(self._graph.get(username.lower(), []))

    def get_followed_by_seeds(self, username: str) -> list[str]:
        return self._graph.get(username.lower(), [])

    def get_prioritized_accounts(
        self, min_appearances: int = 1, exclude_processed: bool = True
    ) -> list[tuple[str, int, list[str]]]:
        """
        Get accounts sorted by appearance count (highest first).
        Returns list of (username, appearance_count, list_of_seeds).

        Set min_appearances=2 to only get accounts followed by 2+ seeds
        (dramatically reduces Claude analysis volume).
        """
        results = []
        for username, seeds in self._graph.items():
            if len(seeds) < min_appearances:
                continue
            if exclude_processed and self.storage.is_processed(username):
                continue
            results.append((username, len(seeds), seeds))

        # Sort by appearance count descending
        results.sort(key=lambda x: x[1], reverse=True)
        return results

    def stats(self) -> dict:
        appearance_dist = defaultdict(int)
        for seeds in self._graph.values():
            appearance_dist[len(seeds)] += 1

        return {
            "total_accounts": len(self._graph),
            "appearance_distribution": dict(sorted(appearance_dist.items())),
            "accounts_2plus": sum(
                1 for v in self._graph.values() if len(v) >= 2
            ),
            "accounts_3plus": sum(
                1 for v in self._graph.values() if len(v) >= 3
            ),
            "accounts_5plus": sum(
                1 for v in self._graph.values() if len(v) >= 5
            ),
        }
