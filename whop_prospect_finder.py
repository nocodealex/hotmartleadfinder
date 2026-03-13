"""
Whop Prospect Finder — discover prospects from referral partners' followings.

For each referral partner, scrapes who they FOLLOW on Instagram, then
qualifies those accounts using the existing prefilter + Claude analysis
pipeline. Outputs a per-partner intro list so each partner knows who
they can introduce you to.

Usage:
    python main.py whop-prospects
    python main.py whop-prospects --skip-new       # only check existing leads
    python main.py whop-prospects --min-score 0.6
    python main.py whop-prospects --partners fedmkt yanvispo
"""

import json
import logging
import time
from pathlib import Path

import pandas as pd
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.table import Table

import config
from apify_following import ApifyFollowingScraper
from instagram_client import InstagramClient, InstagramAPIError
from lead_analyzer import LeadAnalyzer, CreditExhaustedError
from models import Lead, LeadClassification, InstagramProfile
from prefilter import prefilter_bio
from storage import Storage

logger = logging.getLogger(__name__)
console = Console()

# ── Config ───────────────────────────────────────────────────────────────

REFERRAL_PARTNERS = [
    "alexis_bautista_sanchez",
    "fedmkt",
    "yanvispo",
    "diego.nxt",
    "rodrigomajarin",
    "sam.vilanova",
    "danielmarote",
    "soyjoellopezoficial",
]

EXCLUDE_USERNAMES = {
    "alexis_bautista_sanchez", "fedmkt", "yanvispo", "diego.nxt",
    "rodrigomajarin", "sam.vilanova", "danielmarote", "soyjoellopezoficial",
    "aymon_holth", "nocode.alex",
}

DEFAULT_FOLLOWINGS_CACHE_DIR = Path("data/partner_followings")
DEFAULT_OUTPUT_CSV = Path("data/whop_prospect_intros.csv")
DEFAULT_OUTPUT_JSON = Path("data/whop_prospect_intros.json")


# ── Following scraper ────────────────────────────────────────────────────

def scrape_partner_following(
    partner: str,
    scraper: ApifyFollowingScraper,
    cache_dir: Path | None = None,
    cache_load_fn=None,
    cache_save_fn=None,
    force_refresh: bool = False,
) -> list[dict]:
    """
    Fetch who a partner follows, with caching.

    If cache_load_fn/cache_save_fn are provided (e.g. Supabase callbacks),
    they are used instead of the local filesystem cache.
    """
    if not force_refresh:
        if cache_load_fn is not None:
            cached = cache_load_fn(partner)
            if cached is not None and len(cached) > 0:
                logger.info(f"[@{partner}] Loaded {len(cached)} followings from DB cache")
                return cached

        if cache_load_fn is None:
            cache_dir = cache_dir or DEFAULT_FOLLOWINGS_CACHE_DIR
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_file = cache_dir / f"{partner}.json"

            if cache_file.exists():
                cached = json.loads(cache_file.read_text())
                if cached:
                    logger.info(f"[@{partner}] Loaded {len(cached)} followings from file cache")
                    return cached

    logger.info(f"[@{partner}] Scraping following list via Apify...")
    try:
        following = scraper.get_following(partner, limit=0)
    except Exception as e:
        logger.error(f"[@{partner}] Failed to scrape followings: {e}")
        return []

    logger.info(f"[@{partner}] Got {len(following)} followings")

    if following:
        if cache_save_fn is not None:
            cache_save_fn(partner, following)
        else:
            cache_dir = cache_dir or DEFAULT_FOLLOWINGS_CACHE_DIR
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_file = cache_dir / f"{partner}.json"
            cache_file.write_text(json.dumps(following, default=str))

    return following


# ── Engagement metrics ───────────────────────────────────────────────────

def compute_engagement_metrics(posts, follower_count: int) -> dict:
    """Compute engagement analytics from a list of PostData objects."""
    if not posts:
        return {
            "avg_likes": 0.0,
            "avg_comments": 0.0,
            "engagement_rate": 0.0,
            "posting_frequency": 0.0,
        }

    total_likes = sum(p.like_count for p in posts)
    total_comments = sum(p.comment_count for p in posts)
    n = len(posts)

    avg_likes = total_likes / n
    avg_comments = total_comments / n
    avg_engagement = avg_likes + avg_comments
    engagement_rate = avg_engagement / follower_count if follower_count > 0 else 0.0

    timestamps = []
    for p in posts:
        ts = p.timestamp
        if isinstance(ts, (int, float)) and ts > 0:
            timestamps.append(ts)
        elif isinstance(ts, str) and ts.isdigit():
            timestamps.append(int(ts))

    posting_frequency = 0.0
    if len(timestamps) >= 2:
        timestamps.sort()
        span_days = (timestamps[-1] - timestamps[0]) / 86400
        if span_days > 0:
            posting_frequency = (len(timestamps) - 1) / (span_days / 7)

    return {
        "avg_likes": round(avg_likes, 1),
        "avg_comments": round(avg_comments, 1),
        "engagement_rate": round(engagement_rate, 4),
        "posting_frequency": round(posting_frequency, 1),
    }


# ── Main logic ───────────────────────────────────────────────────────────

def find_prospects(
    partners: list[str] | None = None,
    skip_new: bool = False,
    min_score: float = 0.0,
    output_dir: Path | None = None,
    cache_dir: Path | None = None,
    exclude_usernames: set[str] | None = None,
    api_keys: dict | None = None,
    save_callback=None,
    cache_load_fn=None,
    cache_save_fn=None,
    existing_prospects: list[dict] | None = None,
    progress_save_fn=None,
    force_refresh: bool = False,
):
    """
    Scan referral partners' followings for Whop prospects.

    Args:
        partners: Subset of partners to scan (default: all)
        skip_new: If True, only match against existing leads (no Claude calls)
        min_score: Minimum overall score to include (default: LEAD_SCORE_THRESHOLD)
        output_dir: Directory for output CSV/JSON (default: data/)
        cache_dir: Directory for following list cache (default: data/partner_followings/)
        exclude_usernames: Usernames to exclude from results (default: EXCLUDE_USERNAMES)
        api_keys: Optional dict with keys 'rapidapi_key', 'anthropic_api_key', 'apify_api_token'
        save_callback: Optional function(prospects) called to save results (e.g. to Supabase)
        cache_load_fn: Optional function(partner) -> list[dict] | None for loading following cache
        cache_save_fn: Optional function(partner, data) for saving following cache
        existing_prospects: Previously scanned prospects (from Supabase) to skip re-analysis
        progress_save_fn: Optional function(prospect_dict) to save each prospect immediately
        force_refresh: If True, re-scrape all following lists (ignore cache)
    """
    min_score = min_score or config.LEAD_SCORE_THRESHOLD
    partners = partners or REFERRAL_PARTNERS
    output_csv = (output_dir / "prospects.csv") if output_dir else DEFAULT_OUTPUT_CSV
    output_json = (output_dir / "prospects.json") if output_dir else DEFAULT_OUTPUT_JSON
    cache_dir = cache_dir or DEFAULT_FOLLOWINGS_CACHE_DIR
    exclude = exclude_usernames if exclude_usernames is not None else EXCLUDE_USERNAMES
    keys = api_keys or {}

    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)

    storage = Storage()
    scraper = ApifyFollowingScraper(api_token=keys.get("apify_api_token"))

    if not skip_new:
        ig = InstagramClient(api_key=keys.get("rapidapi_key"))
        analyzer = LeadAnalyzer(api_key=keys.get("anthropic_api_key"))

    # Phase 1: Scrape followings for each partner
    console.print(f"\n[bold cyan]Phase 1: Scraping followings for {len(partners)} partners[/]\n")

    partner_followings: dict[str, set[str]] = {}
    all_followed_usernames: dict[str, list[str]] = {}

    for partner in partners:
        console.print(f"  Fetching who @{partner} follows...")
        following = scrape_partner_following(
            partner, scraper,
            cache_dir=cache_dir,
            cache_load_fn=cache_load_fn,
            cache_save_fn=cache_save_fn,
            force_refresh=force_refresh,
        )

        usernames = set()
        for item in following:
            uname = item.get("username", "").lower().strip()
            if uname and uname not in exclude:
                usernames.add(uname)

        partner_followings[partner] = usernames
        console.print(f"    @{partner} follows {len(usernames)} accounts (excl. partners)\n")

        for uname in usernames:
            all_followed_usernames.setdefault(uname, []).append(partner)

    total_unique = len(all_followed_usernames)
    console.print(f"  [bold]Total unique accounts across all partners: {total_unique}[/]\n")

    # Phase 2: Qualify accounts
    console.print("[bold cyan]Phase 2: Qualifying prospects[/]\n")

    existing_leads = storage.get_all_leads()
    existing_lead_map = {l["username"].lower(): l for l in existing_leads}

    if existing_prospects:
        for p in existing_prospects:
            uname = p.get("username", "").lower()
            if uname and uname not in existing_lead_map:
                existing_lead_map[uname] = p

    prospects: list[dict] = []
    new_analyzed = 0
    skipped_prefilter = 0
    skipped_private = 0
    already_known = 0

    accounts_to_process = sorted(
        all_followed_usernames.items(),
        key=lambda x: -len(x[1]),
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    ) as progress:
        task = progress.add_task("Qualifying prospects", total=len(accounts_to_process))

        for username, followed_by_partners in accounts_to_process:
            progress.update(task, description=f"@{username} (x{len(followed_by_partners)})")

            if username in existing_lead_map:
                lead_data = existing_lead_map[username]
                if lead_data.get("overall_score", 0) >= min_score:
                    prospects.append(_build_prospect_entry(lead_data, followed_by_partners))
                    already_known += 1
                progress.advance(task)
                continue

            if skip_new:
                progress.advance(task)
                continue

            if storage.is_whop_seller(username):
                progress.advance(task)
                continue

            try:
                profile = ig.get_profile(username)
                if not profile:
                    progress.advance(task)
                    continue

                if profile.is_private:
                    skipped_private += 1
                    progress.advance(task)
                    continue

                pf_result = prefilter_bio(username, profile.bio, profile.follower_count)
                if pf_result == "skip":
                    skipped_prefilter += 1
                    progress.advance(task)
                    continue

                bio_result = analyzer.analyze_bio(profile)

                website_result = None
                caption_result = None
                event_result = None
                posts = []
                engagement = {"avg_likes": 0, "avg_comments": 0, "engagement_rate": 0, "posting_frequency": 0}

                if bio_result.score >= config.BIO_SCORE_THRESHOLD:
                    if profile.bio_link:
                        website_result = analyzer.analyze_website(profile)

                    try:
                        posts = ig.get_posts(profile.user_id)
                        if posts:
                            engagement = compute_engagement_metrics(posts, profile.follower_count)
                            caption_result = analyzer.analyze_captions(posts, profile)
                            event_result = analyzer.analyze_post_images(posts, profile)
                    except InstagramAPIError:
                        pass

                appearance_count = len(followed_by_partners)
                analysis = analyzer.calculate_overall_score(
                    bio_result=bio_result,
                    website_result=website_result,
                    caption_result=caption_result,
                    event_result=event_result,
                    appearance_count=appearance_count,
                    follower_count=profile.follower_count,
                    engagement_rate=engagement["engagement_rate"],
                    avg_likes=engagement["avg_likes"],
                    avg_comments=engagement["avg_comments"],
                )

                new_analyzed += 1

                if analysis.classification in (
                    LeadClassification.HIGH_VALUE,
                    LeadClassification.POTENTIAL_VALUE,
                ):
                    lead = Lead(
                        profile=profile,
                        analysis=analysis,
                        found_via_seeds=followed_by_partners,
                        seed_depth=0,
                    )
                    storage.save_lead(lead)

                    if analysis.overall_score >= min_score:
                        lead_data = storage.get_lead(username)
                        if lead_data:
                            revenue_data = getattr(analysis, "_revenue", {})
                            prospect_entry = _build_prospect_entry(
                                lead_data, followed_by_partners,
                                engagement=engagement,
                                revenue=revenue_data,
                                profile=profile,
                            )
                            prospects.append(prospect_entry)

                            if progress_save_fn is not None:
                                try:
                                    progress_save_fn(prospect_entry)
                                except Exception as e:
                                    logger.warning(f"Progress save failed for @{username}: {e}")

            except CreditExhaustedError:
                console.print(
                    "\n[bold red]Anthropic API credits depleted![/]\n"
                    "Top up at: https://console.anthropic.com/settings/billing\n"
                )
                break
            except InstagramAPIError as e:
                logger.warning(f"API error for @{username}: {e}")
            except Exception as e:
                logger.warning(f"Error analysing @{username}: {e}")

            progress.advance(task)

    # Phase 3: Build per-partner report
    console.print(f"\n[bold cyan]Phase 3: Building intro report[/]\n")

    prospects.sort(key=lambda x: (-x.get("estimated_deal_value", 0), -x["overall_score"]))

    _print_results(prospects, partners)

    if save_callback is not None:
        save_callback(prospects)
    else:
        _save_results(prospects, output_csv, output_json)

    # Stats
    console.print(f"\n[bold]Stats:[/]")
    console.print(f"  Already-known leads matched: {already_known}")
    if not skip_new:
        console.print(f"  New accounts analyzed:       {new_analyzed}")
        console.print(f"  Skipped (prefilter):         {skipped_prefilter}")
        console.print(f"  Skipped (private):           {skipped_private}")
    console.print(f"  Total prospects found:       {len(prospects)}")

    total_pipeline = sum(p.get("estimated_deal_value", 0) for p in prospects)
    if total_pipeline > 0:
        console.print(f"  [bold green]Estimated pipeline value:  ${total_pipeline:,.0f}[/]")

    return prospects


def _build_prospect_entry(
    lead_data: dict,
    followed_by_partners: list[str],
    engagement: dict | None = None,
    revenue: dict | None = None,
    profile: InstagramProfile | None = None,
) -> dict:
    """Build a standardized prospect dict with engagement and revenue data."""
    fc = lead_data.get("follower_count", 0)
    eng = engagement or {}
    rev = revenue or {}

    return {
        "username": lead_data["username"],
        "full_name": lead_data.get("full_name", ""),
        "bio": lead_data.get("bio", ""),
        "follower_count": fc,
        "overall_score": lead_data.get("overall_score", 0),
        "tier": lead_data.get("tier", ""),
        "classification": lead_data.get("classification", ""),
        "niche": lead_data.get("niche", ""),
        "lead_type": lead_data.get("lead_type", ""),
        "instagram_url": f"https://instagram.com/{lead_data['username']}",
        "followed_by_partners": followed_by_partners,
        "num_partners_connected": len(followed_by_partners),
        "partner_list": ", ".join(f"@{p}" for p in followed_by_partners),
        # Engagement metrics
        "avg_likes": eng.get("avg_likes", 0),
        "avg_comments": eng.get("avg_comments", 0),
        "engagement_rate": eng.get("engagement_rate", 0),
        "posting_frequency": eng.get("posting_frequency", 0),
        # Business account info
        "is_business_account": getattr(profile, "is_business_account", False) if profile else False,
        "ig_category": getattr(profile, "category", "") if profile else "",
        # Revenue estimation
        "business_size_tier": rev.get("business_size_tier", "unknown"),
        "estimated_annual_revenue_low": rev.get("estimated_annual_revenue_low", 0),
        "estimated_annual_revenue_high": rev.get("estimated_annual_revenue_high", 0),
        "estimated_deal_value": rev.get("estimated_deal_value", 0),
        "revenue_confidence": rev.get("revenue_confidence", "none"),
        "revenue_signals": rev.get("revenue_signals", []),
    }


def _format_followers(count) -> str:
    if not count or not isinstance(count, (int, float)) or count <= 0:
        return "-"
    if count >= 1_000_000:
        return f"{count / 1_000_000:.1f}M"
    if count >= 1_000:
        return f"{count / 1_000:.1f}K"
    return str(int(count))


def _format_revenue(low, high) -> str:
    if not low and not high:
        return "-"
    def _fmt(v):
        if v >= 1_000_000:
            return f"${v / 1_000_000:.1f}M"
        if v >= 1_000:
            return f"${v / 1_000:.0f}K"
        return f"${v:.0f}"
    return f"{_fmt(low)}–{_fmt(high)}"


TIER_SHORT = {
    "tier1_whale": "WHALE",
    "tier2_agency": "AGENCY",
    "tier3_affiliate": "AFFIL",
    "tier4_seller": "SELLER",
}

SIZE_SHORT = {
    "whale": "WHALE",
    "large": "LARGE",
    "medium": "MED",
    "small": "SMALL",
    "micro": "MICRO",
    "unknown": "?",
}

NICHE_SHORT = {
    "business_coaching": "BIZ",
    "marketing": "MKT",
    "financial_education": "FIN",
    "personal_development": "DEV",
    "health_fitness": "FIT",
    "education": "EDU",
}


def _print_results(prospects: list[dict], partners: list[str]):
    """Print per-partner intro report to console."""
    if not prospects:
        console.print("  [dim]No prospects found.[/]")
        return

    table = Table(title=f"Whop Prospects — {len(prospects)} Total")
    table.add_column("#", style="dim", width=3)
    table.add_column("Prospect", style="bold cyan")
    table.add_column("Score", justify="right")
    table.add_column("Tier", style="magenta")
    table.add_column("Size", style="yellow")
    table.add_column("Deal Value", justify="right", style="green")
    table.add_column("Niche", style="green")
    table.add_column("Followers", justify="right", style="dim")
    table.add_column("Eng Rate", justify="right", style="dim")
    table.add_column("Partners", style="yellow")

    for i, p in enumerate(prospects[:50], 1):
        score = p["overall_score"]
        score_style = "bold green" if score >= 0.7 else "yellow" if score >= 0.55 else "dim"
        deal_val = p.get("estimated_deal_value", 0)
        eng_rate = p.get("engagement_rate", 0)

        table.add_row(
            str(i),
            f"@{p['username']}",
            f"[{score_style}]{score:.2f}[/]",
            TIER_SHORT.get(p["tier"], p["tier"][:6] if p["tier"] else "?"),
            SIZE_SHORT.get(p.get("business_size_tier", "unknown"), "?"),
            f"${deal_val:,.0f}" if deal_val > 0 else "-",
            NICHE_SHORT.get(p["niche"], p["niche"][:5] if p["niche"] else "?"),
            _format_followers(p["follower_count"]),
            f"{eng_rate:.1%}" if eng_rate > 0 else "-",
            p["partner_list"],
        )

    console.print(table)

    if len(prospects) > 50:
        console.print(f"  [dim]... and {len(prospects) - 50} more (see CSV for full list)[/]")

    # Per-partner breakdown sorted by deal value
    console.print(f"\n[bold]Top Intros Each Partner Can Make:[/]\n")

    for partner in partners:
        partner_prospects = [
            p for p in prospects if partner in p["followed_by_partners"]
        ]
        if not partner_prospects:
            console.print(f"  @{partner}: [dim]no prospects found in their followings[/]")
            continue

        partner_prospects.sort(key=lambda x: -x.get("estimated_deal_value", 0))
        total_value = sum(p.get("estimated_deal_value", 0) for p in partner_prospects)
        console.print(
            f"  [bold]@{partner}[/] — {len(partner_prospects)} intros "
            f"(~${total_value:,.0f} total value):"
        )

        for p in partner_prospects[:5]:
            tier = TIER_SHORT.get(p["tier"], "?")
            size = SIZE_SHORT.get(p.get("business_size_tier", "unknown"), "?")
            deal = p.get("estimated_deal_value", 0)
            foll = _format_followers(p["follower_count"])
            console.print(
                f"    @{p['username']:<25} ${deal:>8,.0f}  "
                f"{tier:<7} {size:<6} {foll:>8}"
            )

        if len(partner_prospects) > 5:
            console.print(f"    [dim]... and {len(partner_prospects) - 5} more[/]")
        console.print()


def _save_results(
    prospects: list[dict],
    output_csv: Path | None = None,
    output_json: Path | None = None,
):
    """Save results to CSV and JSON."""
    output_csv = output_csv or DEFAULT_OUTPUT_CSV
    output_json = output_json or DEFAULT_OUTPUT_JSON

    if not prospects:
        return

    csv_rows = []
    for p in prospects:
        row = dict(p)
        row["followed_by_partners"] = ", ".join(row["followed_by_partners"])
        if isinstance(row.get("revenue_signals"), list):
            row["revenue_signals"] = "; ".join(row["revenue_signals"])
        csv_rows.append(row)

    df = pd.DataFrame(csv_rows)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    console.print(f"  Saved to {output_csv}")

    with open(output_json, "w") as f:
        json.dump(prospects, f, indent=2, default=str)
    console.print(f"  Saved to {output_json}")


# ── Partner brief generation ─────────────────────────────────────────────

def generate_partner_brief(partner: str, prospects: list[dict], max_intros: int = 5) -> str:
    """
    Generate a ready-to-send WhatsApp/DM message for a referral partner
    with their top prospects ranked by estimated deal value.
    """
    partner_prospects = [
        p for p in prospects if partner in p.get("followed_by_partners", [])
    ]
    if not partner_prospects:
        return ""

    partner_prospects.sort(key=lambda x: -x.get("estimated_deal_value", 0))
    top = partner_prospects[:max_intros]

    lines = [f"Hey @{partner}! I found {len(top)} people you follow who'd be great fits for Whop:\n"]

    for i, p in enumerate(top, 1):
        username = p["username"]
        niche = p.get("niche", "unknown")
        size = p.get("business_size_tier", "unknown")
        lead_type = p.get("lead_type", "")

        desc_parts = []
        if lead_type == "agency":
            desc_parts.append("runs a marketing agency")
        elif lead_type == "big_seller":
            desc_parts.append("digital product creator")
        elif lead_type == "mixed":
            desc_parts.append("agency + creator")
        elif lead_type == "platform_affiliate":
            desc_parts.append("ecosystem connector")
        else:
            desc_parts.append("digital business")

        if niche and niche != "unknown":
            nice_niche = niche.replace("_", " ")
            desc_parts.append(f"in {nice_niche}")

        size_labels = {
            "whale": "very large business",
            "large": "established business",
            "medium": "growing business",
            "small": "early-stage",
        }
        if size in size_labels:
            desc_parts.append(f"({size_labels[size]})")

        desc = ", ".join(desc_parts)
        lines.append(f"{i}. @{username} — {desc}")

    lines.append("\nWould you be open to making an intro to any of them?")

    return "\n".join(lines)


# ── Standalone entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )
    find_prospects()
