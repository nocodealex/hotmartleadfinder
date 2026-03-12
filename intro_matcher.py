"""
Intro Matcher — find which of your leads follow your referral partners.

For each referral partner, scrapes their FOLLOWER list, then checks which
of your 985 leads appear. If a lead follows a partner, you can ask that
partner for a warm introduction.

Usage:
    python intro_matcher.py
"""

import json
import time
import logging
import requests
import pandas as pd
from pathlib import Path

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────────────

REFERRAL_PARTNERS = [
    "alexis_bautista_sanchez",
    "fedmkt",
    "yanvispo",
    "diego.nxt",
    "rodrigomajarin",
    "sam.vilanova",
    "davituru",
]

# Exclude from results — partners themselves, Whop employees, etc.
EXCLUDE_USERNAMES = {
    "alexis_bautista_sanchez", "fedmkt", "yanvispo", "diego.nxt",
    "rodrigomajarin", "sam.vilanova", "davituru",
    "aymon_holth", "nocode.alex",
}

APIFY_BASE = "https://api.apify.com/v2"
LEADS_CSV = "data/leads.csv"
OUTPUT_CSV = "data/intro_matches.csv"
OUTPUT_JSON = "data/intro_matches.json"
CACHE_DIR = Path("data/partner_followers")

# Actor for follower lists
FOLLOWER_ACTOR = "thenetaji~instagram-followers-followings-scraper"


# ── Apify helpers ───────────────────────────────────────────────────────────

def _api_url(path: str) -> str:
    return f"{APIFY_BASE}{path}?token={config.APIFY_API_TOKEN}"


def scrape_followers(username: str) -> list[str]:
    """
    Scrape the follower list for a partner via Apify.
    Returns a list of usernames who follow this partner.
    """
    cache_file = CACHE_DIR / f"{username}_followers.json"
    if cache_file.exists():
        cached = json.loads(cache_file.read_text())
        logger.info(f"[@{username}] Loaded {len(cached)} followers from cache")
        return cached

    logger.info(f"[@{username}] Scraping follower list via Apify...")

    # Try multiple input formats to maximize compatibility
    run_input = {
        "username": [username],
        "scrape_type": "followers",
        "maxItem": 50000,
        "maxItems": 50000,
    }

    try:
        resp = requests.post(
            _api_url(f"/acts/{FOLLOWER_ACTOR}/runs"),
            json=run_input,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        run_id = resp.json().get("data", {}).get("id")
        if not run_id:
            logger.error(f"[@{username}] No run ID returned")
            return []
    except requests.RequestException as e:
        logger.error(f"[@{username}] Failed to start Apify: {e}")
        return []

    logger.info(f"[@{username}] Apify run started: {run_id}")

    # Poll for completion
    max_wait = 1800
    poll_interval = 5
    waited = 0
    status = "RUNNING"
    run_info = {}

    while waited < max_wait:
        time.sleep(poll_interval)
        waited += poll_interval
        try:
            resp = requests.get(
                _api_url(f"/acts/{FOLLOWER_ACTOR}/runs/{run_id}"),
                timeout=15,
            )
            resp.raise_for_status()
            run_info = resp.json().get("data", {})
            status = run_info.get("status", "UNKNOWN")
        except requests.RequestException:
            continue

        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            logger.info(f"[@{username}] Run finished: {status} ({waited}s)")
            break
        if waited % 30 == 0:
            logger.info(f"[@{username}] Still running... ({waited}s)")

    if status != "SUCCEEDED":
        logger.error(f"[@{username}] Apify run failed: {status}")
        return []

    # Fetch results
    dataset_id = run_info.get("defaultDatasetId")
    if not dataset_id:
        return []

    try:
        resp = requests.get(_api_url(f"/datasets/{dataset_id}/items"), timeout=120)
        resp.raise_for_status()
        items = resp.json()
    except requests.RequestException as e:
        logger.error(f"[@{username}] Failed to fetch results: {e}")
        return []

    # Extract usernames from multiple possible formats
    follower_usernames = []
    for item in items:
        fu = item.get("follower_user", {})
        if isinstance(fu, dict) and fu.get("username"):
            follower_usernames.append(fu["username"].lower())
            continue
        uname = item.get("username", "")
        if uname:
            follower_usernames.append(uname.lower())

    logger.info(f"[@{username}] Got {len(follower_usernames)} followers")

    # Cache
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(json.dumps(follower_usernames))
    return follower_usernames


# ── Main ────────────────────────────────────────────────────────────────────

def run():
    df = pd.read_csv(LEADS_CSV)
    lead_usernames = set(df["username"].str.lower())
    logger.info(f"Loaded {len(lead_usernames)} leads from {LEADS_CSV}")

    all_matches = {}  # lead_username -> list of partners they follow

    for partner in REFERRAL_PARTNERS:
        print(f"\n{'='*60}")
        print(f"  Scraping who follows @{partner}")
        print(f"{'='*60}")

        followers = scrape_followers(partner)
        if not followers:
            print(f"  No follower data for @{partner}, skipping")
            continue

        follower_set = set(followers)
        matches = lead_usernames & follower_set
        print(f"  @{partner} has {len(followers)} followers | {len(matches)} of your leads follow them")

        for match in matches:
            if match not in all_matches:
                all_matches[match] = []
            all_matches[match].append(partner)

    # Build results
    print(f"\n{'='*60}")
    print(f"  RESULTS — Leads who follow your referral partners")
    print(f"{'='*60}")

    if not all_matches:
        print("  No matches found.")
        return

    results = []
    for username, partners in all_matches.items():
        if username in EXCLUDE_USERNAMES:
            continue
        lead_row = df[df["username"].str.lower() == username]
        if lead_row.empty:
            continue
        lead = lead_row.iloc[0]
        results.append({
            "username": lead["username"],
            "full_name": lead.get("full_name", ""),
            "overall_score": lead.get("overall_score", 0),
            "tier": lead.get("tier", ""),
            "niche": lead.get("niche", ""),
            "classification": lead.get("classification", ""),
            "follower_count": lead.get("follower_count", 0),
            "instagram_url": f"https://instagram.com/{lead['username']}",
            "follows_partners": partners,
            "num_partner_connections": len(partners),
            "partner_list": ", ".join(f"@{p}" for p in partners),
            "bio": lead.get("bio", ""),
        })

    results.sort(key=lambda x: (-x["num_partner_connections"], -x["overall_score"]))

    print(f"\n  {len(results)} leads follow at least one of your partners:\n")
    header = f"  {'#':>3}  {'Lead':<24} {'Score':>5}  {'Tier':<10} {'Niche':<5} {'Followers':>9}  {'Follows Your Partner(s)'}"
    print(header)
    print(f"  {'-' * (len(header) + 20)}")

    for i, r in enumerate(results):
        followers = r["follower_count"]
        if pd.notna(followers) and followers > 0:
            if followers >= 1_000_000:
                foll = f"{followers/1_000_000:.1f}M"
            elif followers >= 1_000:
                foll = f"{followers/1_000:.1f}K"
            else:
                foll = str(int(followers))
        else:
            foll = "-"

        tier_map = {"tier1_whale": "WHALE", "tier2_agency": "AGENCY",
                     "tier3_affiliate": "AFFIL", "tier4_seller": "SELLER"}
        niche_map = {"business_coaching": "BIZ", "marketing": "MKT",
                      "financial_education": "FIN", "personal_development": "DEV",
                      "health_fitness": "FIT", "education": "EDU"}

        tier = tier_map.get(r["tier"], r["tier"][:6] if r["tier"] else "?")
        niche = niche_map.get(r["niche"], r["niche"][:5] if r["niche"] else "?")

        print(
            f"  {i+1:>3}  @{r['username']:<23} {r['overall_score']:>5.2f}  "
            f"{tier:<10} {niche:<5} {foll:>9}  {r['partner_list']}"
        )

    # Save
    results_df = pd.DataFrame(results)
    results_df.to_csv(OUTPUT_CSV, index=False)
    with open(OUTPUT_JSON, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n  Saved to {OUTPUT_CSV}")
    print(f"  Saved to {OUTPUT_JSON}")

    # Partner summary
    print(f"\n  PARTNER BREAKDOWN:")
    for partner in REFERRAL_PARTNERS:
        count = sum(1 for r in results if partner in r["follows_partners"])
        if count > 0:
            print(f"    @{partner:<28} {count} of your leads follow them")


if __name__ == "__main__":
    run()
