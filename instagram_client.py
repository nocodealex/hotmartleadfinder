"""
Instagram API client — hybrid approach.

Profile / posts / bio: RapidAPI "Instagram API – Fast & Reliable Data Scraper"
Following lists:       Apify "datavoyantlab/instagram-following-scraper"

The RapidAPI following endpoint has broken pagination (stuck at 25 results),
so we delegate following-list scraping to Apify which handles pagination
natively and can fetch thousands of accounts.
"""

import time
import logging
import requests
from typing import Optional

import config
from models import InstagramProfile, PostData

logger = logging.getLogger(__name__)

# Lazy import to avoid requiring Apify token when not scraping following lists
_apify_scraper = None


class InstagramAPIError(Exception):
    """Raised when the Instagram API returns an error."""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(f"Instagram API error {status_code}: {message}")


class InstagramClient:

    def __init__(self, api_key: str = "", api_host: str = ""):
        self.api_key = api_key or config.RAPIDAPI_KEY
        self.api_host = api_host or config.INSTAGRAM_API_HOST
        self.base_url = f"https://{self.api_host}"
        self.headers = {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": self.api_host,
        }
        self.delay = config.REQUEST_DELAY_SECONDS
        self._call_count = 0

    # ── Low-level request ────────────────────────────────────────────

    def _get(self, endpoint: str, params: dict | None = None, _retry: int = 0) -> dict:
        """Make a GET request with rate limiting and error handling."""
        MAX_RETRIES = 3

        if self._call_count > 0:
            time.sleep(self.delay)
        self._call_count += 1

        url = f"{self.base_url}{endpoint}"
        logger.debug(f"GET {url} params={params}")

        try:
            resp = requests.get(url, headers=self.headers, params=params, timeout=30)
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise InstagramAPIError(0, str(e))

        if resp.status_code == 429:
            if _retry >= MAX_RETRIES:
                raise InstagramAPIError(429, "Rate limited after max retries")
            wait = 30 * (2 ** _retry)  # Exponential backoff: 30s, 60s, 120s
            logger.warning(f"Rate limited — waiting {wait}s (retry {_retry + 1}/{MAX_RETRIES})")
            time.sleep(wait)
            return self._get(endpoint, params, _retry=_retry + 1)

        if resp.status_code not in (200, 201):
            raise InstagramAPIError(resp.status_code, resp.text[:500])

        return resp.json()

    # ── Public methods ───────────────────────────────────────────────

    def get_user_id(self, username: str) -> Optional[str]:
        """Resolve a username to a numeric user_id."""
        # The /profile endpoint returns pk directly
        endpoint = config.ENDPOINTS["user_id_by_username"]
        data = self._get(endpoint, {"username": username})

        if isinstance(data, dict):
            for key in ("pk", "user_id", "id"):
                if key in data:
                    return str(data[key])
            inner = data.get("data", data.get("user", {}))
            if isinstance(inner, dict):
                for key in ("pk", "user_id", "id"):
                    if key in inner:
                        return str(inner[key])
        return None

    def get_profile(self, username: str) -> Optional[InstagramProfile]:
        """Fetch full profile data for a username."""
        endpoint = config.ENDPOINTS["user_profile"]
        data = self._get(endpoint, {"username": username})

        try:
            return InstagramProfile.from_api_response(data)
        except Exception as e:
            logger.error(f"Failed to parse profile for @{username}: {e}")
            return None

    def get_all_following(
        self, username_or_id: str, limit: int = 0, username: str = ""
    ) -> list[dict]:
        """
        Fetch the complete following list for a user via Apify.

        Args:
            username_or_id: Username (preferred) or user ID.
            limit: Max followings to fetch (0 = use config default).
            username: Explicit username if username_or_id is numeric.

        Returns:
            List of dicts with at least a "username" key per followed account.
        """
        global _apify_scraper

        # Resolve username — Apify needs a username, not a numeric ID
        uname = username or username_or_id
        if uname.isdigit():
            logger.warning(
                f"get_all_following called with numeric ID '{uname}'. "
                f"Apify needs a username — attempting profile lookup..."
            )
            # Fallback: we can't reverse-lookup, so caller should pass username
            raise InstagramAPIError(
                0,
                f"Apify requires a username, not a numeric ID. "
                f"Pass username= explicitly."
            )

        # Lazy-init Apify scraper
        if _apify_scraper is None:
            from apify_following import ApifyFollowingScraper
            _apify_scraper = ApifyFollowingScraper()

        return _apify_scraper.get_following(uname, limit=limit)

    def get_posts(
        self, user_id: str, count: int = 0
    ) -> list[PostData]:
        """Fetch recent posts for a user."""
        count = count or config.POSTS_TO_ANALYZE
        endpoint = config.ENDPOINTS["user_posts"]
        data = self._get(endpoint, {"user_id": user_id, "batch_size": count})

        # Extract items list
        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = (
                data.get("items", [])
                or data.get("posts", [])
                or data.get("data", [])
                or data.get("edge_owner_to_timeline_media", {}).get("edges", [])
            )

        posts = []
        for item in items[:count]:
            # Handle GraphQL edge format
            if "node" in item:
                item = item["node"]
            try:
                posts.append(PostData.from_api_response(item))
            except Exception as e:
                logger.warning(f"Skipping unparseable post: {e}")

        return posts

    def test_connection(self) -> dict:
        """
        Test the API connection by fetching a known public profile.
        Returns a dict with status info.
        """
        result = {"connected": False, "endpoints_working": {}, "errors": []}

        try:
            # Test user_id lookup
            uid = self.get_user_id("instagram")
            result["endpoints_working"]["user_id_by_username"] = uid is not None
            if not uid:
                result["errors"].append("user_id_by_username returned None")
        except Exception as e:
            result["endpoints_working"]["user_id_by_username"] = False
            result["errors"].append(f"user_id_by_username: {e}")

        try:
            # Test profile fetch
            profile = self.get_profile("instagram")
            result["endpoints_working"]["user_profile"] = profile is not None
            if profile:
                result["sample_profile"] = {
                    "username": profile.username,
                    "bio": profile.bio[:100],
                    "followers": profile.follower_count,
                }
        except Exception as e:
            result["endpoints_working"]["user_profile"] = False
            result["errors"].append(f"user_profile: {e}")

        # Test Apify connection (for following lists)
        try:
            if config.APIFY_API_TOKEN:
                from apify_following import ApifyFollowingScraper
                scraper = ApifyFollowingScraper()
                apify_ok = scraper.test_connection()
                result["endpoints_working"]["apify_following"] = apify_ok
                if not apify_ok:
                    result["errors"].append("Apify connection test failed")
            else:
                result["endpoints_working"]["apify_following"] = False
                result["errors"].append(
                    "APIFY_API_TOKEN not set — following lists won't work"
                )
        except Exception as e:
            result["endpoints_working"]["apify_following"] = False
            result["errors"].append(f"apify_following: {e}")

        result["connected"] = any(result["endpoints_working"].values())
        result["api_calls_made"] = self._call_count
        return result
