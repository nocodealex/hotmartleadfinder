"""
Lead Analyzer — uses Claude to score Instagram profiles as potential
referral partners for Whop.

Runs four analysis signals:
  1. Bio text analysis
  2. Bio-link website analysis
  3. Post-caption analysis
  4. Post-image analysis (Hotmart event detection)

Then aggregates into an overall score with tier classification.
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

    # ── Tier Classification ──────────────────────────────────────────

    @staticmethod
    def classify_tier(
        overall_score: float,
        lead_type: str,
        follower_count: int = 0,
        appearance_count: int = 1,
        bio_details: dict | None = None,
    ) -> LeadTier:
        """
        Assign a lead tier based on score, type, and context signals.

        Tier 1 (Whales): massive reach OR revenue, top affiliates
        Tier 2 (Agencies): agency owners with multiple clients
        Tier 3 (Affiliates): platform affiliates, co-producers
        Tier 4 (Sellers): individual sellers who could switch
        """
        # Tier 1 — Whales
        if overall_score >= config.TIER1_MIN_SCORE:
            return LeadTier.TIER1_WHALE
        if follower_count >= 100_000 and overall_score >= config.TIER2_MIN_SCORE:
            return LeadTier.TIER1_WHALE
        if appearance_count >= 5 and overall_score >= config.TIER2_MIN_SCORE:
            # Followed by 5+ seeds = deeply embedded in the ecosystem
            return LeadTier.TIER1_WHALE

        # Tier 2 — Agencies
        if lead_type in ("agency", "mixed") and overall_score >= config.TIER2_MIN_SCORE:
            return LeadTier.TIER2_AGENCY

        # Tier 3 — Affiliates / Co-producers
        if lead_type == "platform_affiliate" and overall_score >= config.TIER3_MIN_SCORE:
            return LeadTier.TIER3_AFFILIATE
        if appearance_count >= 3 and overall_score >= config.TIER3_MIN_SCORE:
            return LeadTier.TIER3_AFFILIATE

        # Tier 4 — Sellers
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
    ) -> LeadAnalysis:
        """
        Combine all signal scores into an overall lead score with tier.
        """
        total_weight = 0.0
        weighted_sum = 0.0

        # Bio (always present)
        weighted_sum += bio_result.score * config.WEIGHT_BIO
        total_weight += config.WEIGHT_BIO

        # Website (optional)
        if website_result:
            weighted_sum += website_result.score * config.WEIGHT_WEBSITE
            total_weight += config.WEIGHT_WEBSITE

        # Captions (optional)
        if caption_result:
            weighted_sum += caption_result.score * config.WEIGHT_CAPTIONS
            total_weight += config.WEIGHT_CAPTIONS

        # Events (optional)
        if event_result:
            weighted_sum += event_result.score * config.WEIGHT_EVENTS
            total_weight += config.WEIGHT_EVENTS

        # Appearance boost (much higher weight now)
        if appearance_count > 1:
            appearance_boost = min(appearance_count / 5, 1.0)
            weighted_sum += appearance_boost * config.WEIGHT_APPEARANCES
            total_weight += config.WEIGHT_APPEARANCES

        # Normalise
        overall_score = weighted_sum / total_weight if total_weight > 0 else 0.0

        # Extract lead type and niche from bio analysis
        lead_type = bio_result.details.get("lead_type", "none")
        niche = bio_result.details.get("niche", "unknown")

        # Niche boost: business coaching and financial education leads
        # generate more revenue, so give them a small scoring bump
        HIGH_VALUE_NICHES = {"business_coaching", "financial_education", "marketing"}
        if niche in HIGH_VALUE_NICHES:
            overall_score = min(1.0, overall_score + 0.05)

        # Classify
        if overall_score >= 0.70:
            classification = LeadClassification.HIGH_VALUE
        elif overall_score >= config.LEAD_SCORE_THRESHOLD:
            classification = LeadClassification.POTENTIAL_VALUE
        else:
            classification = LeadClassification.NOT_VALUABLE

        # Assign tier
        tier = self.classify_tier(
            overall_score=overall_score,
            lead_type=lead_type,
            follower_count=follower_count,
            appearance_count=appearance_count,
            bio_details=bio_result.details,
        )

        # Build summary
        parts = [f"Bio: {bio_result.score:.2f}"]
        if website_result:
            parts.append(f"Website: {website_result.score:.2f}")
        if caption_result:
            parts.append(f"Captions: {caption_result.score:.2f}")
        if event_result:
            parts.append(f"Events: {event_result.score:.2f}")
        if appearance_count > 1:
            parts.append(f"Appearances: {appearance_count}")
        summary = f"Overall {overall_score:.2f} [{tier.value}] ({', '.join(parts)})"

        return LeadAnalysis(
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
