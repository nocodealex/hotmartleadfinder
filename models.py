"""
Data models for Hotmart Lead Finder.
"""

from dataclasses import dataclass, field
from typing import Optional
from enum import Enum


class LeadClassification(str, Enum):
    HIGH_VALUE = "high_value"
    POTENTIAL_VALUE = "potential_value"
    NOT_VALUABLE = "not_valuable"
    UNKNOWN = "unknown"


class LeadTier(str, Enum):
    """Lead priority tiers based on estimated business impact."""
    TIER1_WHALE = "tier1_whale"          # 100K+ followers, 7+ fig revenue, top affiliates
    TIER2_AGENCY = "tier2_agency"        # Agency owners with multiple clients
    TIER3_AFFILIATE = "tier3_affiliate"  # Platform affiliates, co-producers
    TIER4_SELLER = "tier4_seller"        # Individual sellers
    UNTIERED = "untiered"


@dataclass
class InstagramProfile:
    username: str
    user_id: str
    full_name: str
    bio: str
    bio_link: Optional[str] = None
    follower_count: int = 0
    following_count: int = 0
    post_count: int = 0
    is_private: bool = False
    is_verified: bool = False
    profile_pic_url: Optional[str] = None

    @classmethod
    def from_api_response(cls, data: dict) -> "InstagramProfile":
        user = data.get("user", data)

        bio_link = None
        external_url = user.get("external_url") or user.get("bio_link")
        if external_url:
            bio_link = external_url
        elif "bio_links" in user and user["bio_links"]:
            bio_link = user["bio_links"][0].get("url", "")
        elif "external_url_linkshimmed" in user:
            bio_link = user["external_url_linkshimmed"]

        return cls(
            username=user.get("username", ""),
            user_id=str(user.get("pk", user.get("id", user.get("user_id", "")))),
            full_name=user.get("full_name", ""),
            bio=user.get("biography", user.get("bio", "")),
            bio_link=bio_link,
            follower_count=user.get("follower_count", user.get("edge_followed_by", {}).get("count", 0)),
            following_count=user.get("following_count", user.get("edge_follow", {}).get("count", 0)),
            post_count=user.get("media_count", user.get("edge_owner_to_timeline_media", {}).get("count", 0)),
            is_private=user.get("is_private", False),
            is_verified=user.get("is_verified", False),
            profile_pic_url=user.get("profile_pic_url", user.get("profile_pic_url_hd", "")),
        )


@dataclass
class PostData:
    post_id: str
    caption: Optional[str] = None
    image_urls: list = field(default_factory=list)
    timestamp: Optional[str] = None
    like_count: int = 0
    comment_count: int = 0
    shortcode: Optional[str] = None

    @classmethod
    def from_api_response(cls, data: dict) -> "PostData":
        caption = ""
        if "caption" in data:
            cap = data["caption"]
            if isinstance(cap, dict):
                caption = cap.get("text", "")
            elif isinstance(cap, str):
                caption = cap
        elif "edge_media_to_caption" in data:
            edges = data["edge_media_to_caption"].get("edges", [])
            if edges:
                caption = edges[0].get("node", {}).get("text", "")

        image_urls = []
        if "image_versions2" in data:
            candidates = data["image_versions2"].get("candidates", [])
            if candidates:
                image_urls.append(candidates[0].get("url", ""))
        elif "display_url" in data:
            image_urls.append(data["display_url"])
        elif "thumbnail_url" in data:
            image_urls.append(data["thumbnail_url"])

        if "carousel_media" in data:
            for item in data["carousel_media"]:
                if "image_versions2" in item:
                    cands = item["image_versions2"].get("candidates", [])
                    if cands:
                        image_urls.append(cands[0].get("url", ""))
        elif "edge_sidecar_to_children" in data:
            for edge in data["edge_sidecar_to_children"].get("edges", []):
                node = edge.get("node", {})
                if "display_url" in node:
                    image_urls.append(node["display_url"])

        return cls(
            post_id=str(data.get("pk", data.get("id", ""))),
            caption=caption,
            image_urls=image_urls,
            timestamp=data.get("taken_at", data.get("timestamp", "")),
            like_count=data.get("like_count", data.get("edge_media_preview_like", {}).get("count", 0)),
            comment_count=data.get("comment_count", data.get("edge_media_to_comment", {}).get("count", 0)),
            shortcode=data.get("code", data.get("shortcode", "")),
        )


@dataclass
class SignalResult:
    """Result from a single analysis signal."""
    score: float
    classification: str
    reasoning: str
    details: dict = field(default_factory=dict)


@dataclass
class LeadAnalysis:
    """Aggregated analysis result for a potential lead."""
    bio_result: Optional[SignalResult] = None
    website_result: Optional[SignalResult] = None
    caption_result: Optional[SignalResult] = None
    event_result: Optional[SignalResult] = None
    overall_score: float = 0.0
    classification: LeadClassification = LeadClassification.UNKNOWN
    tier: LeadTier = LeadTier.UNTIERED
    lead_type: str = ""  # agency, big_seller, platform_affiliate, mixed
    summary: str = ""
    appearance_count: int = 1
    boosted: bool = False


@dataclass
class Lead:
    """A qualified lead with all associated data."""
    profile: InstagramProfile
    analysis: LeadAnalysis
    found_via_seeds: list = field(default_factory=list)
    seed_depth: int = 0
    first_seen: str = ""
    last_updated: str = ""
