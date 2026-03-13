"""
Apify integration for Instagram following list scraping.

Strategy:
  1. Try "thenetaji/instagram-following-scraper" via synchronous endpoint
     (bypasses empty-dataset bug by returning items in HTTP response).
  2. Fall back to "data-slayer/instagram-following" if thenetaji fails.
  3. Pick whichever returns more results.
"""

import logging
import math
import time
import requests

import config

logger = logging.getLogger(__name__)

THENETAJI_ACTOR = "thenetaji~instagram-following-scraper"
DATASLAYER_ACTOR = "data-slayer~instagram-following"
APIFY_BASE = "https://api.apify.com/v2"
FOLLOWINGS_PER_PAGE = 50


class ApifyFollowingError(Exception):
    """Raised when the Apify following scraper fails."""
    pass


class ApifyFollowingScraper:

    def __init__(self, api_token: str = ""):
        self.api_token = api_token or config.APIFY_API_TOKEN
        if not self.api_token:
            raise ApifyFollowingError(
                "APIFY_API_TOKEN not set. "
                "Sign up at https://apify.com, get your token from "
                "Settings > Integrations, and add it to .env"
            )

    def _api_url(self, path: str) -> str:
        return f"{APIFY_BASE}{path}?token={self.api_token}"

    def get_following(
        self,
        username: str,
        limit: int = 0,
    ) -> list[dict]:
        """
        Fetch the following list for an Instagram user.

        Tries thenetaji (sync endpoint) first, then data-slayer as fallback.
        Returns whichever gives more results.

        Returns:
            List of dicts with keys: username, full_name, pk,
            is_private, is_verified, profile_pic_url.
        """
        limit = limit or config.MAX_FOLLOWING_TO_FETCH

        # Try primary (thenetaji) via sync endpoint
        primary_result = []
        try:
            primary_result = self._run_thenetaji_sync(username, limit)
            logger.info(
                f"[Apify] thenetaji returned {len(primary_result)} for @{username}"
            )
            if len(primary_result) >= 20:
                return primary_result
        except ApifyFollowingError as e:
            logger.warning(f"[Apify] thenetaji failed for @{username}: {e}")

        # Try fallback (data-slayer) via async polling
        fallback_result = []
        try:
            fallback_result = self._run_dataslayer(username, limit)
            logger.info(
                f"[Apify] data-slayer returned {len(fallback_result)} for @{username}"
            )
        except ApifyFollowingError as e:
            logger.warning(f"[Apify] data-slayer failed for @{username}: {e}")

        # Return whichever got more results
        if len(primary_result) >= len(fallback_result):
            best = primary_result
            source = "thenetaji"
        else:
            best = fallback_result
            source = "data-slayer"

        if not best:
            raise ApifyFollowingError(
                f"Both Apify actors failed or returned 0 items for @{username}."
            )

        logger.info(f"[Apify] Using {source} ({len(best)} items) for @{username}")
        return best

    # ── thenetaji: synchronous endpoint (items returned in response) ────

    def _run_thenetaji_sync(self, username: str, limit: int) -> list[dict]:
        """
        Run thenetaji actor using the sync endpoint that returns dataset
        items directly in the HTTP response body, bypassing the separate
        dataset-fetch step that was returning empty.
        """
        logger.info(f"[Apify/thenetaji] @{username} limit={limit} (sync mode)")

        run_input = {
            "username": [username],
            "type": "followings",
            "maxItem": limit,
            "profileEnriched": False,
        }

        url = (
            f"{APIFY_BASE}/acts/{THENETAJI_ACTOR}/run-sync-get-dataset-items"
            f"?token={self.api_token}&timeout=600&format=json"
        )

        try:
            resp = requests.post(
                url,
                json=run_input,
                headers={"Content-Type": "application/json"},
                timeout=660,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            raise ApifyFollowingError(f"thenetaji sync call failed: {e}")

        raw = resp.json()

        if isinstance(raw, list):
            items = raw
        elif isinstance(raw, dict):
            items = raw.get("items", raw.get("data", []))
            if not items and "id" in raw:
                raise ApifyFollowingError(
                    "thenetaji sync returned run metadata instead of items. "
                    "Actor may have timed out."
                )
        else:
            items = []

        if not items:
            raise ApifyFollowingError(
                f"thenetaji sync returned 0 items for @{username}"
            )

        logger.info(
            f"[Apify/thenetaji] Got {len(items)} raw items. "
            f"Sample keys: {list(items[0].keys())}"
        )

        return self._normalize(items, username)

    # ── data-slayer: async polling ──────────────────────────────────────

    def _run_dataslayer(self, username: str, limit: int) -> list[dict]:
        max_pages = min(100, max(1, math.ceil(limit / FOLLOWINGS_PER_PAGE)))
        logger.info(
            f"[Apify/data-slayer] @{username} limit={limit} maxPages={max_pages}"
        )

        run_input = {
            "username": username,
            "maxPages": max_pages,
        }

        items = self._start_and_poll(DATASLAYER_ACTOR, run_input, username)
        return self._normalize(items, username)

    # ── Shared async polling logic ──────────────────────────────────────

    def _start_and_poll(
        self, actor_id: str, run_input: dict, username: str
    ) -> list[dict]:
        actor_label = actor_id.split("~")[0]

        try:
            start_url = self._api_url(f"/acts/{actor_id}/runs")
            resp = requests.post(
                start_url,
                json=run_input,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            run_data = resp.json().get("data", {})
            run_id = run_data.get("id")
            if not run_id:
                raise ApifyFollowingError(
                    f"[{actor_label}] No run ID: {resp.text[:300]}"
                )
        except requests.RequestException as e:
            raise ApifyFollowingError(f"[{actor_label}] Failed to start: {e}")

        logger.info(f"[Apify/{actor_label}] Run started: {run_id}")

        max_wait = 1800
        poll_interval = 5
        waited = 0
        status = "UNKNOWN"
        run_info = {}

        while waited < max_wait:
            time.sleep(poll_interval)
            waited += poll_interval

            try:
                status_url = self._api_url(f"/acts/{actor_id}/runs/{run_id}")
                resp = requests.get(status_url, timeout=15)
                resp.raise_for_status()
                run_info = resp.json().get("data", {})
                status = run_info.get("status", "UNKNOWN")
            except requests.RequestException as e:
                logger.warning(f"[Apify/{actor_label}] Poll error: {e}")
                continue

            if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                logger.info(
                    f"[Apify/{actor_label}] Finished: {status} ({waited}s)"
                )
                break

            if waited % 30 == 0:
                logger.info(
                    f"[Apify/{actor_label}] Still running... ({waited}s)"
                )

        if status != "SUCCEEDED":
            raise ApifyFollowingError(
                f"[{actor_label}] Finished with status: {status}"
            )

        dataset_id = run_info.get("defaultDatasetId")
        if not dataset_id:
            raise ApifyFollowingError(
                f"[{actor_label}] No dataset ID in run result"
            )

        try:
            items_url = (
                self._api_url(f"/datasets/{dataset_id}/items") + "&format=json"
            )
            resp = requests.get(items_url, timeout=120)
            resp.raise_for_status()
            raw = resp.json()
        except requests.RequestException as e:
            raise ApifyFollowingError(
                f"[{actor_label}] Failed to fetch dataset: {e}"
            )

        if isinstance(raw, list):
            items = raw
        elif isinstance(raw, dict):
            items = raw.get("items", raw.get("data", []))
        else:
            items = []

        # Fallback: key-value store
        if not items:
            kv_id = run_info.get("defaultKeyValueStoreId")
            if kv_id:
                logger.info(f"[Apify/{actor_label}] Dataset empty, trying KV store")
                try:
                    kv_url = self._api_url(
                        f"/key-value-stores/{kv_id}/records/OUTPUT"
                    )
                    kv_resp = requests.get(kv_url, timeout=60)
                    if kv_resp.status_code == 200:
                        kv_data = kv_resp.json()
                        if isinstance(kv_data, list):
                            items = kv_data
                        elif isinstance(kv_data, dict):
                            items = kv_data.get(
                                "items",
                                kv_data.get("data", kv_data.get("following", [])),
                            )
                        logger.info(
                            f"[Apify/{actor_label}] Got {len(items)} from KV store"
                        )
                except Exception as e:
                    logger.warning(f"[Apify/{actor_label}] KV fetch failed: {e}")

        logger.info(
            f"[Apify/{actor_label}] Got {len(items)} raw items for @{username}"
        )

        if not items:
            raise ApifyFollowingError(
                f"[{actor_label}] Succeeded but 0 items for @{username}. "
                f"Dataset: {dataset_id}."
            )

        if items:
            logger.info(
                f"[Apify/{actor_label}] Sample keys: {list(items[0].keys())}"
            )

        return items

    # ── Normalize to common format ──────────────────────────────────────

    @staticmethod
    def _normalize(items: list[dict], username: str) -> list[dict]:
        following = []
        for item in items:
            if "message" in item and "username" not in item and "userName" not in item:
                logger.warning(
                    f"[Apify] Actor message: {item.get('message', '')[:200]}"
                )
                continue

            uname = (
                item.get("username")
                or item.get("user_name")
                or item.get("userName")
                or ""
            )
            full_name = (
                item.get("full_name")
                or item.get("fullName")
                or item.get("name")
                or ""
            )
            pk = str(
                item.get("id")
                or item.get("userId")
                or item.get("user_id")
                or item.get("pk")
                or ""
            )

            normalized = {
                "username": uname,
                "full_name": full_name,
                "pk": pk,
                "is_private": item.get("is_private", item.get("isPrivate", False)),
                "is_verified": item.get("is_verified", item.get("isVerified", False)),
                "profile_pic_url": item.get(
                    "profile_pic_url", item.get("profilePicUrl", "")
                ),
            }
            if normalized["username"]:
                following.append(normalized)

        logger.info(
            f"[Apify] Normalized {len(following)} accounts for @{username}"
        )

        if len(items) > 0 and len(following) == 0:
            raise ApifyFollowingError(
                f"Apify returned {len(items)} items but none had a username. "
                f"Keys found: {list(items[0].keys())}"
            )

        return following

    def test_connection(self) -> bool:
        """Test that the Apify token is valid."""
        try:
            url = self._api_url("/users/me")
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                data = resp.json().get("data", {})
                uname = data.get("username", "?")
                logger.info(f"[Apify] Connected as: {uname}")
                return True
        except Exception as e:
            logger.error(f"[Apify] Connection test failed: {e}")
        return False
