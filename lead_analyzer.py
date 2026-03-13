"""
Lead Analyzer — uses Claude to score Instagram profiles as potential
referral partners for Whop.

Runs four analysis signals:
  1. Bio text analysis
  2. Bio-link website analysis
  3. Post-caption analysis
  4. Post-image analysis (Hotmart event detection)

Then aggregates into a composite score (fit x size x warmth) with
revenue-based tier classification.
"""

import json
import time
import base64
import logging
import requests
from typing import Optional

import anthropic

import config
from models import (
    InstagramProfile,
    PostData,
    SignalResult,
    LeadAnalysis,
    LeadClassification,
    LeadTier,
)
from prompts import (
    BIO_ANALYSIS_PROMPT,
    WEBSITE_ANALYSIS_PROMPT,
    CAPTION_ANALYSIS_PROMPT,
    EVENT_IMAGE_ANALYSIS_PROMPT,
)
from website_scraper import fetch_website_text

logger = logging.getLogger(__name__)

BUSINESS_SIZE_RANK = {
    "whale": 5,
    "large": 4,
    "medium": 3,
    "small": 2,
    "micro": 1,
    "unknown": 0,
}


class CreditExhaustedError(Exception):
    """Raised when Anthropic API credits are depleted."""
    pass


class LeadAnalyzer:

    def __init__(self, api_key: str = ""):
        self.client = anthropic.Anthropic(api_key=api_key or config.ANTHROPIC_API_KEY)
        self.model = config.CLAUDE_MODEL
        self._call_count = 0

    # ── LLM helpers ──────────────────────────────────────────────────

    def _ask_claude(self, prompt: str, max_tokens: int = 0) -> dict:
        """Send a text prompt to Claude and parse JSON response."""
        max_tokens = max_tokens or config.CLAUDE_MAX_TOKENS
        if self._call_count > 0:
            time.sleep(config.ANTHROPIC_DELAY_SECONDS)
        self._call_count += 1

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
            text = message.content[0].text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
            return json.loads(text)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Claude JSON: {e}\nRaw: {text[:500]}")
            return {}
        except anthropic.APIError as e:
            if "credit balance is too low" in str(e):
                raise CreditExhaustedError(
                    "Anthropic API credits depleted. "
                    "Top up at https://console.anthropic.com/settings/billing "
                    "then re-run the pipeline."
                )
            logger.error(f"Claude API error: {e}")
            return {}

    def _ask_claude_vision(self, prompt: str, image_url: str) -> dict:
        """Send an image + prompt to Claude for vision analysis."""
        if self._call_count > 0:
            time.sleep(config.ANTHROPIC_DELAY_SECONDS)
        self._call_count += 1

        try:
            img_resp = requests.get(image_url, timeout=15)
            img_resp.raise_for_status()
            img_data = base64.standard_b64encode(img_resp.content).decode("utf-8")

            content_type = img_resp.headers.get("content-type", "image/jpeg")
            if "png" in content_type:
                media_type = "image/png"
            elif "webp" in content_type:
                media_type = "image/webp"
            elif "gif" in content_type:
                media_type = "image/gif"
            else:
                media_type = "image/jpeg"

            message = self.client.messages.create(
                model=self.model,
                max_tokens=config.CLAUDE_MAX_TOKENS,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": media_type,
                                    "data": img_data,
                                },
                            },
                            {"type": "text", "text": prompt},
                        ],
                    }
                ],
            )
            text = message.content[0].text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
            return json.loads(text)

        except anthropic.APIError as e:
            if "credit balance is too low" in str(e):
                raise CreditExhaustedError(
                    "Anthropic API credits depleted. "
                    "Top up at https://console.anthropic.com/settings/billing "
                    "then re-run the pipeline."
                )
            logger.warning(f"Vision analysis failed for {image_url[:80]}: {e}")
            return {}
        except Exception as e:
            logger.warning(f"Vision analysis failed for {image_url[:80]}: {e}")
            return {}

    # ── Signal 1: Bio ────────────────────────────────────────────────

    def analyze_bio(self, profile: InstagramProfile) -> SignalResult:
        """Analyze the Instagram bio for referral-partner signals."""
        prompt = BIO_ANALYSIS_PROMPT.format(
            username=profile.username,
            full_name=profile.full_name,
            bio=profile.bio,
            follower_count=profile.follower_count,
            following_count=profile.following_count,
            is_verified=profile.is_verified,
            is_business_account=profile.is_business_account,
            category=profile.category or "None",
        )
        result = self._ask_claude(prompt)

        return SignalResult(
            score=result.get("score", 0.0),
            classification=result.get("classification", "not_valuable"),
            reasoning=result.get("reasoning", "Analysis failed"),
            details={
                "key_signals": result.get("key_signals", []),
                "language": result.get("language", "unknown"),
                "lead_type": result.get("lead_type", "none"),
                "niche": result.get("niche", "unknown"),
                "business_size_tier": result.get("business_size_tier", "unknown"),
                "revenue_confidence": result.get("revenue_confidence", "low"),
                "size_signals": result.get("size_signals", []),
            },
        )

    # ── Signal 2: Website ────────────────────────────────────────────

    def analyze_website(
        self, profile: InstagramProfile
    ) -> Optional[SignalResult]:
        """Fetch and analyze the website linked in the bio."""
        if not profile.bio_link:
            return None

        website_text = fetch_website_text(profile.bio_link)
        if not website_text:
            return None

        prompt = WEBSITE_ANALYSIS_PROMPT.format(
            website_text=website_text,
            username=profile.username,
            full_name=profile.full_name,
        )
        result = self._ask_claude(prompt)

        return SignalResult(
            score=result.get("score", 0.0),
            classification=result.get("classification", "inconclusive"),
            reasoning=result.get("reasoning", "Analysis failed"),
            details={
                "services_found": result.get("services_or_products_found", []),
                "mentions_hotmart": result.get("mentions_hotmart", False),
                "url": profile.bio_link,
                "business_size_tier": result.get("business_size_tier", "unknown"),
                "pricing_found": result.get("pricing_found", []),
                "student_or_client_count": result.get("student_or_client_count"),
                "product_count": result.get("product_count", 0),
            },
        )

    # ── Signal 3: Post Captions ──────────────────────────────────────

    def analyze_captions(
        self, posts: list[PostData], profile: InstagramProfile
    ) -> Optional[SignalResult]:
        """Analyze recent post captions for referral-partner signals."""
        captions = [p.caption for p in posts if p.caption and p.caption.strip()]
        if not captions:
            return None

        captions_text = "\n---\n".join(
            f"Post {i+1}: {cap[:500]}" for i, cap in enumerate(captions)
        )

        prompt = CAPTION_ANALYSIS_PROMPT.format(
            num_posts=len(captions),
            username=profile.username,
            captions=captions_text,
        )
        result = self._ask_claude(prompt)

        return SignalResult(
            score=result.get("score", 0.0),
            classification=result.get("classification", "not_valuable"),
            reasoning=result.get("reasoning", "Analysis failed"),
            details={
                "key_signals": result.get("key_signals", []),
                "mentions_hotmart": result.get("mentions_hotmart", False),
                "is_digital_product_seller": result.get("is_digital_product_seller", False),
                "serves_clients": result.get("serves_clients", False),
                "business_size_tier": result.get("business_size_tier", "unknown"),
                "revenue_claims": result.get("revenue_claims", []),
            },
        )

    # ── Signal 4: Event Images ───────────────────────────────────────

    def analyze_post_images(
        self, posts: list[PostData], profile: InstagramProfile
    ) -> Optional[SignalResult]:
        """Scan post images for Hotmart event attendance."""
        candidates = []
        for post in posts:
            if not post.image_urls:
                continue
            caption_lower = (post.caption or "").lower()
            is_event_hint = any(
                kw in caption_lower
                for kw in [
                    "hotmart", "fire", "evento", "event", "congres",
                    "summit", "palestra", "conferencia", "conferência",
                ]
            )
            candidates.append((post.image_urls[0], post.caption or "", is_event_hint))

        if not candidates:
            return None

        event_hinted = [c for c in candidates if c[2]]
        non_hinted = [c for c in candidates if not c[2]]
        to_analyse = event_hinted + non_hinted[:max(0, 3 - len(event_hinted))]

        best_score = 0.0
        best_result = {}
        for img_url, caption, _ in to_analyse:
            prompt = EVENT_IMAGE_ANALYSIS_PROMPT.format(caption=caption[:300])
            result = self._ask_claude_vision(prompt, img_url)
            score = result.get("confidence", 0.0)
            if result.get("is_hotmart_event"):
                score = max(score, 0.8)
            elif result.get("is_marketing_event"):
                score = max(score, 0.5)
            if score > best_score:
                best_score = score
                best_result = result

        if not best_result:
            return None

        return SignalResult(
            score=best_score,
            classification="event_detected" if best_score >= 0.5 else "no_event",
            reasoning=best_result.get("reasoning", ""),
            details={
                "is_hotmart_event": best_result.get("is_hotmart_event", False),
                "is_marketing_event": best_result.get("is_marketing_event", False),
                "event_details": best_result.get("event_details", ""),
            },
        )

    # ── Revenue Estimation ───────────────────────────────────────────

    @staticmethod
    def synthesize_revenue(
        bio_result: SignalResult,
        website_result: Optional[SignalResult],
        caption_result: Optional[SignalResult],
        engagement_rate: float = 0.0,
        follower_count: int = 0,
    ) -> dict:
        """
        Synthesize a revenue estimate from all signal results and
        engagement metrics. Returns dict with business_size_tier,
        estimated_annual_revenue_low, estimated_annual_revenue_high,
        estimated_deal_value, revenue_confidence, and revenue_signals.
        """
        tier_votes: list[tuple[str, float]] = []
        all_size_signals: list[str] = []

        confidence_weights = {"high": 3.0, "medium": 2.0, "low": 1.0}

        bio_tier = bio_result.details.get("business_size_tier", "unknown")
        bio_conf = bio_result.details.get("revenue_confidence", "low")
        if bio_tier != "unknown":
            tier_votes.append((bio_tier, confidence_weights.get(bio_conf, 1.0)))
        all_size_signals.extend(bio_result.details.get("size_signals", []))

        if website_result:
            web_tier = website_result.details.get("business_size_tier", "unknown")
            if web_tier != "unknown":
                tier_votes.append((web_tier, 2.5))
            pricing = website_result.details.get("pricing_found", [])
            if pricing:
                all_size_signals.extend(f"pricing: {p}" for p in pricing)
            student_count = website_result.details.get("student_or_client_count")
            if student_count and student_count > 0:
                all_size_signals.append(f"{student_count} students/clients (website)")
            product_count = website_result.details.get("product_count", 0)
            if product_count and product_count > 1:
                all_size_signals.append(f"{product_count} products (website)")

        if caption_result:
            cap_tier = caption_result.details.get("business_size_tier", "unknown")
            if cap_tier != "unknown":
                tier_votes.append((cap_tier, 2.0))
            revenue_claims = caption_result.details.get("revenue_claims", [])
            if revenue_claims:
                all_size_signals.extend(f"caption: {c}" for c in revenue_claims)

        if follower_count > 0 and engagement_rate > 0:
            eng_tier = _engagement_to_tier(follower_count, engagement_rate)
            if eng_tier != "unknown":
                tier_votes.append((eng_tier, 1.0))
                all_size_signals.append(
                    f"engagement: {follower_count} followers, {engagement_rate:.2%} rate"
                )
        elif follower_count > 0:
            foll_tier = _followers_to_tier(follower_count)
            if foll_tier != "unknown":
                tier_votes.append((foll_tier, 0.5))
                all_size_signals.append(f"followers: {follower_count}")

        if not tier_votes:
            return {
                "business_size_tier": "unknown",
                "estimated_annual_revenue_low": 0,
                "estimated_annual_revenue_high": 0,
                "estimated_deal_value": 0,
                "revenue_confidence": "none",
                "revenue_signals": all_size_signals,
            }

        weighted_score = 0.0
        total_weight = 0.0
        for tier, weight in tier_votes:
            weighted_score += BUSINESS_SIZE_RANK.get(tier, 0) * weight
            total_weight += weight

        avg_rank = weighted_score / total_weight if total_weight > 0 else 0

        if avg_rank >= 4.5:
            final_tier = "whale"
        elif avg_rank >= 3.5:
            final_tier = "large"
        elif avg_rank >= 2.5:
            final_tier = "medium"
        elif avg_rank >= 1.5:
            final_tier = "small"
        elif avg_rank >= 0.5:
            final_tier = "micro"
        else:
            final_tier = "unknown"

        rev_ranges = {
            "whale": (1_000_000, 5_000_000),
            "large": (200_000, 1_000_000),
            "medium": (50_000, 200_000),
            "small": (10_000, 50_000),
            "micro": (1_000, 10_000),
            "unknown": (0, 0),
        }

        low, high = rev_ranges.get(final_tier, (0, 0))
        midpoint = config.REVENUE_MIDPOINTS.get(final_tier, 0)
        deal_value = midpoint * config.TAKE_RATE

        if len(tier_votes) >= 3:
            confidence = "high"
        elif len(tier_votes) >= 2:
            confidence = "medium"
        else:
            confidence = "low"

        return {
            "business_size_tier": final_tier,
            "estimated_annual_revenue_low": low,
            "estimated_annual_revenue_high": high,
            "estimated_deal_value": deal_value,
            "revenue_confidence": confidence,
            "revenue_signals": all_size_signals,
        }

    # ── Tier Classification ──────────────────────────────────────────

    @staticmethod
    def classify_tier(
        overall_score: float,
        lead_type: str,
        follower_count: int = 0,
        appearance_count: int = 1,
        bio_details: dict | None = None,
        business_size_tier: str = "unknown",
    ) -> LeadTier:
        """
        Assign a lead tier based on score, type, revenue estimate,
        and context signals. Revenue-based sizing takes priority.
        """
        # Revenue-first classification
        if business_size_tier == "whale" and overall_score >= config.TIER4_MIN_SCORE:
            return LeadTier.TIER1_WHALE
        if business_size_tier == "large" and overall_score >= config.TIER4_MIN_SCORE:
            return LeadTier.TIER2_AGENCY

        # Score-based fallback (original logic enhanced)
        if overall_score >= config.TIER1_MIN_SCORE:
            return LeadTier.TIER1_WHALE
        if follower_count >= 500_000 and overall_score >= config.TIER2_MIN_SCORE:
            return LeadTier.TIER1_WHALE
        if appearance_count >= 5 and overall_score >= config.TIER2_MIN_SCORE:
            return LeadTier.TIER1_WHALE

        if business_size_tier == "medium" and overall_score >= config.TIER4_MIN_SCORE:
            return LeadTier.TIER3_AFFILIATE

        if lead_type in ("agency", "mixed") and overall_score >= config.TIER2_MIN_SCORE:
            return LeadTier.TIER2_AGENCY

        if lead_type == "platform_affiliate" and overall_score >= config.TIER3_MIN_SCORE:
            return LeadTier.TIER3_AFFILIATE
        if appearance_count >= 3 and overall_score >= config.TIER3_MIN_SCORE:
            return LeadTier.TIER3_AFFILIATE

        if lead_type == "big_seller" and overall_score >= config.TIER4_MIN_SCORE:
            return LeadTier.TIER4_SELLER
        if overall_score >= config.TIER3_MIN_SCORE:
            return LeadTier.TIER3_AFFILIATE
        if overall_score >= config.TIER4_MIN_SCORE:
            return LeadTier.TIER4_SELLER

        return LeadTier.UNTIERED

    # ── Aggregate Scoring ────────────────────────────────────────────

    def calculate_overall_score(
        self,
        bio_result: SignalResult,
        website_result: Optional[SignalResult],
        caption_result: Optional[SignalResult],
        event_result: Optional[SignalResult],
        appearance_count: int = 1,
        follower_count: int = 0,
        engagement_rate: float = 0.0,
        avg_likes: float = 0.0,
        avg_comments: float = 0.0,
    ) -> LeadAnalysis:
        """
        Combine all signal scores into a composite lead score:
          fit (40%) x size (35%) x warmth (25%)
        """
        # ── 1. FIT SCORE: weighted average of Claude signal scores ──
        fit_weight = 0.0
        fit_sum = 0.0

        fit_sum += bio_result.score * config.WEIGHT_BIO
        fit_weight += config.WEIGHT_BIO

        if website_result:
            fit_sum += website_result.score * config.WEIGHT_WEBSITE
            fit_weight += config.WEIGHT_WEBSITE

        if caption_result:
            fit_sum += caption_result.score * config.WEIGHT_CAPTIONS
            fit_weight += config.WEIGHT_CAPTIONS

        if event_result:
            fit_sum += event_result.score * config.WEIGHT_EVENTS
            fit_weight += config.WEIGHT_EVENTS

        fit_score = fit_sum / fit_weight if fit_weight > 0 else 0.0

        # Niche boost for high-value niches
        lead_type = bio_result.details.get("lead_type", "none")
        niche = bio_result.details.get("niche", "unknown")
        HIGH_VALUE_NICHES = {"business_coaching", "financial_education", "marketing"}
        if niche in HIGH_VALUE_NICHES:
            fit_score = min(1.0, fit_score + 0.05)

        # ── 2. SIZE SCORE: revenue estimation ───────────────────────
        revenue = self.synthesize_revenue(
            bio_result=bio_result,
            website_result=website_result,
            caption_result=caption_result,
            engagement_rate=engagement_rate,
            follower_count=follower_count,
        )

        size_tier = revenue["business_size_tier"]
        size_rank = BUSINESS_SIZE_RANK.get(size_tier, 0)
        size_score = min(1.0, size_rank / 5.0)

        # ── 3. WARMTH SCORE: partner connections ────────────────────
        if appearance_count >= 5:
            warmth_score = 1.0
        elif appearance_count > 1:
            warmth_score = min(1.0, appearance_count / 5.0)
        else:
            warmth_score = 0.2

        # ── COMPOSITE ───────────────────────────────────────────────
        overall_score = (
            fit_score * config.WEIGHT_FIT
            + size_score * config.WEIGHT_SIZE
            + warmth_score * config.WEIGHT_WARMTH
        )
        overall_score = min(1.0, overall_score)

        # Classify
        if overall_score >= 0.70:
            classification = LeadClassification.HIGH_VALUE
        elif overall_score >= config.LEAD_SCORE_THRESHOLD:
            classification = LeadClassification.POTENTIAL_VALUE
        else:
            classification = LeadClassification.NOT_VALUABLE

        # Assign tier (revenue-aware)
        tier = self.classify_tier(
            overall_score=overall_score,
            lead_type=lead_type,
            follower_count=follower_count,
            appearance_count=appearance_count,
            bio_details=bio_result.details,
            business_size_tier=size_tier,
        )

        # Build summary
        parts = [f"Fit: {fit_score:.2f}"]
        parts.append(f"Size: {size_tier}")
        parts.append(f"Warmth: {warmth_score:.2f}")
        if engagement_rate > 0:
            parts.append(f"Eng: {engagement_rate:.1%}")
        if appearance_count > 1:
            parts.append(f"x{appearance_count} partners")
        deal_val = revenue["estimated_deal_value"]
        if deal_val > 0:
            parts.append(f"~${deal_val:,.0f} deal")
        summary = f"Overall {overall_score:.2f} [{tier.value}] ({', '.join(parts)})"

        analysis = LeadAnalysis(
            bio_result=bio_result,
            website_result=website_result,
            caption_result=caption_result,
            event_result=event_result,
            overall_score=overall_score,
            classification=classification,
            tier=tier,
            lead_type=lead_type,
            summary=summary,
            appearance_count=appearance_count,
            boosted=appearance_count > 1,
        )

        analysis._revenue = revenue
        analysis._fit_score = fit_score
        analysis._size_score = size_score
        analysis._warmth_score = warmth_score
        analysis._engagement_rate = engagement_rate
        analysis._avg_likes = avg_likes
        analysis._avg_comments = avg_comments

        return analysis


# ── Helper functions ─────────────────────────────────────────────────

def _engagement_to_tier(follower_count: int, engagement_rate: float) -> str:
    """Estimate business size tier from engagement metrics."""
    if follower_count >= 500_000 and engagement_rate >= 0.02:
        return "whale"
    if follower_count >= 100_000 and engagement_rate >= 0.02:
        return "large"
    if follower_count >= 100_000 and engagement_rate >= 0.01:
        return "large"
    if follower_count >= 50_000 and engagement_rate >= 0.02:
        return "medium"
    if follower_count >= 20_000 and engagement_rate >= 0.02:
        return "medium"
    if follower_count >= 10_000:
        return "small"
    if follower_count >= 5_000:
        return "small"
    if follower_count >= 1_000:
        return "micro"
    return "unknown"


def _followers_to_tier(follower_count: int) -> str:
    """Rough tier estimate from follower count alone (low confidence)."""
    if follower_count >= 500_000:
        return "large"
    if follower_count >= 100_000:
        return "medium"
    if follower_count >= 20_000:
        return "small"
    if follower_count >= 5_000:
        return "micro"
    return "unknown"
