"""
Apify integration for Instagram following list scraping.

Uses the "instaprism/instagram-following-scraper" actor on Apify via REST API.
No Instagram login required. Pay-per-event: ~$2 per 1,000 results.
"""

import logging
import time
import requests
from typing import Optional

import config

logger = logging.getLogger(__name__)

ACTOR_ID = "instaprism~instagram-following-scraper"
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

        Args:
            username: Instagram username (without @)
            limit: Max followings to fetch (0 = all / up to plan limit)

        Returns:
            List of dicts with keys: username, full_name, pk,
            is_private, is_verified, profile_pic_url.
        """
        limit = limit or config.MAX_FOLLOWING_TO_FETCH

        logger.info(f"[Apify] Fetching following list for @{username} (limit={limit})")

        run_input = {
            "username": username,
            "limit": limit,
            "extractEmails": False,
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
                raise ApifyFollowingError(f"No run ID returned: {resp.text[:200]}")
        except requests.RequestException as e:
            raise ApifyFollowingError(f"Failed to start Apify actor: {e}")

        logger.info(f"[Apify] Run started: {run_id}")

        # Poll for completion (up to 30 minutes for large lists)
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

        # Fetch results from the default dataset
        dataset_id = run_info.get("defaultDatasetId")
        if not dataset_id:
            raise ApifyFollowingError("No dataset ID in Apify run result")

        try:
            items_url = self._api_url(f"/datasets/{dataset_id}/items") + "&format=json"
            resp = requests.get(items_url, timeout=120)
            resp.raise_for_status()
            raw = resp.json()
        except requests.RequestException as e:
            raise ApifyFollowingError(f"Failed to fetch dataset: {e}")

        if isinstance(raw, list):
            items = raw
        elif isinstance(raw, dict):
            items = raw.get("items", raw.get("data", []))
        else:
            items = []

        # If dataset empty, try key-value store OUTPUT
        if not items:
            kv_store_id = run_info.get("defaultKeyValueStoreId")
            if kv_store_id:
                logger.info(f"[Apify] Dataset empty, trying key-value store")
                try:
                    kv_url = self._api_url(f"/key-value-stores/{kv_store_id}/records/OUTPUT")
                    kv_resp = requests.get(kv_url, timeout=60)
                    if kv_resp.status_code == 200:
                        kv_data = kv_resp.json()
                        if isinstance(kv_data, list):
                            items = kv_data
                        elif isinstance(kv_data, dict):
                            items = kv_data.get("items", kv_data.get("data", kv_data.get("following", [])))
                        logger.info(f"[Apify] Got {len(items)} items from key-value store")
                except Exception as e:
                    logger.warning(f"[Apify] Key-value store fetch failed: {e}")

        logger.info(f"[Apify] Got {len(items)} raw items for @{username}")

        if not items:
            raise ApifyFollowingError(
                f"Apify actor succeeded but returned 0 items for @{username}. "
                f"Dataset ID: {dataset_id}."
            )

        if items:
            sample = items[0]
            logger.info(f"[Apify] Sample item keys: {list(sample.keys())}")

        # Normalize — handle both snake_case and camelCase field names
        following = []
        for item in items:
            if "message" in item and "username" not in item and "userName" not in item:
                logger.warning(f"[Apify] Message from actor: {item.get('message', '')[:200]}")
                continue

            uname = (
                item.get("username")
                or item.get("user_name")
                or item.get("userName")
                or ""
            )
            full_name = (
                item.get("fullName")
                or item.get("full_name")
                or item.get("name")
                or ""
            )
            pk = str(
                item.get("userId")
                or item.get("user_id")
                or item.get("id")
                or item.get("pk")
                or ""
            )

            normalized = {
                "username": uname,
                "full_name": full_name,
                "pk": pk,
                "is_private": item.get("is_private", item.get("isPrivate", False)),
                "is_verified": item.get("is_verified", item.get("isVerified", False)),
                "profile_pic_url": item.get("profile_pic_url", item.get("profilePicUrl", "")),
            }
            if normalized["username"]:
                following.append(normalized)

        logger.info(
            f"[Apify] Normalized {len(following)} following accounts for @{username}"
        )

        if len(items) > 0 and len(following) == 0:
            raise ApifyFollowingError(
                f"Apify returned {len(items)} items but none had a username field. "
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
