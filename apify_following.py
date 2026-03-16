"""
Apify integration for Instagram following list scraping.

Uses "thenetaji/instagram-following-scraper" actor (82.5% success rate).
The critical fix: input MUST include type="followings" — without it the
actor "succeeds" but returns 0 items.
"""

import logging
import time
import requests

import config

logger = logging.getLogger(__name__)

ACTOR_ID = "thenetaji~instagram-following-scraper"
APIFY_BASE = "https://api.apify.com/v2"


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

        Returns:
            List of dicts with keys: username, full_name, pk,
            is_private, is_verified, profile_pic_url.
        """
        limit = limit or config.MAX_FOLLOWING_TO_FETCH

        logger.info(f"[Apify] Fetching following for @{username} (limit={limit})")

        run_input = {
            "username": [username],
            "type": "followings",
            "maxItem": limit,
            "profileEnriched": False,
        }

        # Start the actor run
        try:
            start_url = self._api_url(f"/acts/{ACTOR_ID}/runs")
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
                raise ApifyFollowingError(f"No run ID returned: {resp.text[:300]}")
        except requests.RequestException as e:
            raise ApifyFollowingError(f"Failed to start Apify actor: {e}")

        logger.info(f"[Apify] Run started: {run_id}")

        # Poll for completion (up to 30 minutes)
        max_wait = 1800
        poll_interval = 5
        waited = 0
        status = "UNKNOWN"
        run_info = {}

        while waited < max_wait:
            time.sleep(poll_interval)
            waited += poll_interval

            try:
                status_url = self._api_url(f"/acts/{ACTOR_ID}/runs/{run_id}")
                resp = requests.get(status_url, timeout=15)
                resp.raise_for_status()
                run_info = resp.json().get("data", {})
                status = run_info.get("status", "UNKNOWN")
            except requests.RequestException as e:
                logger.warning(f"[Apify] Poll error: {e}")
                continue

            if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                logger.info(f"[Apify] Run finished: {status} (waited {waited}s)")
                break

            if waited % 30 == 0:
                logger.info(f"[Apify] Still running... ({waited}s)")

        if status != "SUCCEEDED":
            raise ApifyFollowingError(
                f"Apify actor finished with status: {status}"
            )

        # Collect items from dataset
        items = self._collect_items(run_info, username)

        # Normalize
        return self._normalize(items, username)

    def _collect_items(self, run_info: dict, username: str) -> list[dict]:
        """Try dataset first, then key-value store, then logs."""
        dataset_id = run_info.get("defaultDatasetId")
        items = []

        # Attempt 1: Default dataset
        if dataset_id:
            items = self._fetch_dataset(dataset_id)
            if items:
                logger.info(f"[Apify] Got {len(items)} items from dataset")
                return items

        # Attempt 2: Key-value store OUTPUT
        kv_id = run_info.get("defaultKeyValueStoreId")
        if kv_id:
            items = self._fetch_kv_store(kv_id)
            if items:
                logger.info(f"[Apify] Got {len(items)} items from KV store")
                return items

        # Attempt 3: Try dataset with different parameters
        if dataset_id:
            items = self._fetch_dataset_raw(dataset_id)
            if items:
                logger.info(f"[Apify] Got {len(items)} items from raw dataset")
                return items

        raise ApifyFollowingError(
            f"Apify actor succeeded but returned 0 items for @{username}. "
            f"Dataset ID: {dataset_id}."
        )

    def _fetch_dataset(self, dataset_id: str) -> list[dict]:
        """Fetch items from a dataset using the standard endpoint."""
        try:
            url = self._api_url(f"/datasets/{dataset_id}/items") + "&format=json"
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            raw = resp.json()

            if isinstance(raw, list):
                return raw
            if isinstance(raw, dict):
                return raw.get("items", raw.get("data", []))
        except Exception as e:
            logger.warning(f"[Apify] Dataset fetch failed: {e}")
        return []

    def _fetch_dataset_raw(self, dataset_id: str) -> list[dict]:
        """Fetch items without format parameter (some actors need this)."""
        try:
            url = self._api_url(f"/datasets/{dataset_id}/items")
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            raw = resp.json()

            if isinstance(raw, list):
                return raw
            if isinstance(raw, dict):
                return raw.get("items", raw.get("data", []))
        except Exception as e:
            logger.warning(f"[Apify] Raw dataset fetch failed: {e}")
        return []

    def _fetch_kv_store(self, kv_id: str) -> list[dict]:
        """Fetch items from the key-value store OUTPUT key."""
        try:
            url = self._api_url(f"/key-value-stores/{kv_id}/records/OUTPUT")
            resp = requests.get(url, timeout=60)
            if resp.status_code != 200:
                return []
            kv_data = resp.json()

            if isinstance(kv_data, list):
                return kv_data
            if isinstance(kv_data, dict):
                return kv_data.get("items", kv_data.get("data", kv_data.get("following", [])))
        except Exception as e:
            logger.warning(f"[Apify] KV store fetch failed: {e}")
        return []

    @staticmethod
    def _normalize(items: list[dict], username: str) -> list[dict]:
        """Normalize raw Apify items to a common format."""
        following = []
        for item in items:
            if "message" in item and "username" not in item:
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
            sample_keys = list(items[0].keys()) if items else []
            raise ApifyFollowingError(
                f"Apify returned {len(items)} items but none had a username. "
                f"Keys: {sample_keys}"
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
