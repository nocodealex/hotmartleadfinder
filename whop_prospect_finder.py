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
) -> list[dict]:
    """
    Fetch who a partner follows, with local caching.
    Returns list of dicts with at least 'username' key.
    """
    cache_dir = cache_dir or DEFAULT_FOLLOWINGS_CACHE_DIR
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{partner}.json"

    if cache_file.exists():
        cached = json.loads(cache_file.read_text())
        logger.info(f"[@{partner}] Loaded {len(cached)} followings from cache")
        return cached

    logger.info(f"[@{partner}] Scraping following list via Apify...")
    try:
        following = scraper.get_following(partner, limit=0)
    except Exception as e:
        logger.error(f"[@{partner}] Failed to scrape followings: {e}")
        return []

    logger.info(f"[@{partner}] Got {len(following)} followings")
    cache_file.write_text(json.dumps(following, default=str))
    return following


# ── Main logic ───────────────────────────────────────────────────────────

def find_prospects(
    partners: list[str] | None = None,
    skip_new: bool = False,
    min_score: float = 0.0,
    output_dir: Path | None = None,
    cache_dir: Path | None = None,
    exclude_usernames: set[str] | None = None,
    api_keys: dict | None = None,
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
    all_followed_usernames: dict[str, list[str]] = {}  # username -> [partners who follow them]

    for partner in partners:
        console.print(f"  Fetching who @{partner} follows...")
        following = scrape_partner_following(partner, scraper, cache_dir=cache_dir)

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

    prospects: list[dict] = []
    new_analyzed = 0
    skipped_prefilter = 0
    skipped_private = 0
    already_known = 0

    accounts_to_process = sorted(
        all_followed_usernames.items(),
        key=lambda x: -len(x[1]),  # prioritize accounts followed by more partners
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

            # Check if already a known lead
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

            # Skip known Whop sellers
            if storage.is_whop_seller(username):
                progress.advance(task)
                continue

            # Fetch profile and analyze
            try:
                profile = ig.get_profile(username)
                if not profile:
                    progress.advance(task)
                    continue

                if profile.is_private:
                    skipped_private += 1
                    progress.advance(task)
                    continue

                # Prefilter
                pf_result = prefilter_bio(username, profile.bio, profile.follower_count)
                if pf_result == "skip":
                    skipped_prefilter += 1
                    progress.advance(task)
                    continue

                # Bio analysis
                bio_result = analyzer.analyze_bio(profile)

                website_result = None
                caption_result = None
                event_result = None

                if bio_result.score >= config.BIO_SCORE_THRESHOLD:
                    if profile.bio_link:
                        website_result = analyzer.analyze_website(profile)

                    try:
                        posts = ig.get_posts(profile.user_id)
                        if posts:
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
                            prospects.append(
                                _build_prospect_entry(lead_data, followed_by_partners)
                            )

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

    prospects.sort(key=lambda x: (-x["num_partners_connected"], -x["overall_score"]))

    _print_results(prospects, partners)
    _save_results(prospects, output_csv, output_json)

    # Stats
    console.print(f"\n[bold]Stats:[/]")
    console.print(f"  Already-known leads matched: {already_known}")
    if not skip_new:
        console.print(f"  New accounts analyzed:       {new_analyzed}")
        console.print(f"  Skipped (prefilter):         {skipped_prefilter}")
        console.print(f"  Skipped (private):           {skipped_private}")
    console.print(f"  Total prospects found:       {len(prospects)}")

    return prospects


def _build_prospect_entry(lead_data: dict, followed_by_partners: list[str]) -> dict:
    """Build a standardized prospect dict from lead data + partner connections."""
    fc = lead_data.get("follower_count", 0)
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
    }


def _format_followers(count) -> str:
    if not count or not isinstance(count, (int, float)) or count <= 0:
        return "-"
    if count >= 1_000_000:
        return f"{count / 1_000_000:.1f}M"
    if count >= 1_000:
        return f"{count / 1_000:.1f}K"
    return str(int(count))


TIER_SHORT = {
    "tier1_whale": "WHALE",
    "tier2_agency": "AGENCY",
    "tier3_affiliate": "AFFIL",
    "tier4_seller": "SELLER",
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

    # Overall summary table
    table = Table(title=f"Whop Prospects — {len(prospects)} Total")
    table.add_column("#", style="dim", width=3)
    table.add_column("Prospect", style="bold cyan")
    table.add_column("Score", justify="right")
    table.add_column("Tier", style="magenta")
    table.add_column("Niche", style="green")
    table.add_column("Followers", justify="right", style="dim")
    table.add_column("Partners Who Follow", style="yellow")

    for i, p in enumerate(prospects[:50], 1):
        score = p["overall_score"]
        score_style = "bold green" if score >= 0.7 else "yellow" if score >= 0.55 else "dim"
        table.add_row(
            str(i),
            f"@{p['username']}",
            f"[{score_style}]{score:.2f}[/]",
            TIER_SHORT.get(p["tier"], p["tier"][:6] if p["tier"] else "?"),
            NICHE_SHORT.get(p["niche"], p["niche"][:5] if p["niche"] else "?"),
            _format_followers(p["follower_count"]),
            p["partner_list"],
        )

    console.print(table)

    if len(prospects) > 50:
        console.print(f"  [dim]... and {len(prospects) - 50} more (see CSV for full list)[/]")

    # Per-partner breakdown
    console.print(f"\n[bold]Intros Each Partner Can Make:[/]\n")

    for partner in partners:
        partner_prospects = [
            p for p in prospects if partner in p["followed_by_partners"]
        ]
        if not partner_prospects:
            console.print(f"  @{partner}: [dim]no prospects found in their followings[/]")
            continue

        partner_prospects.sort(key=lambda x: -x["overall_score"])
        console.print(f"  [bold]@{partner}[/] can intro you to [bold green]{len(partner_prospects)}[/] prospects:")

        for p in partner_prospects[:10]:
            tier = TIER_SHORT.get(p["tier"], "?")
            foll = _format_followers(p["follower_count"])
            console.print(
                f"    @{p['username']:<25} score:{p['overall_score']:.2f}  "
                f"{tier:<7} {foll:>8}"
            )

        if len(partner_prospects) > 10:
            console.print(f"    [dim]... and {len(partner_prospects) - 10} more[/]")
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
        csv_rows.append(row)

    df = pd.DataFrame(csv_rows)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    console.print(f"  Saved to {output_csv}")

    with open(output_json, "w") as f:
        json.dump(prospects, f, indent=2, default=str)
    console.print(f"  Saved to {output_json}")


# ── Standalone entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )
    find_prospects()
