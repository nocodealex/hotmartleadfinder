"""
Hotmart Seller Instagram Finder

Takes top Hotmart sellers and finds their Instagram profiles using:
1. Instagram handles already extracted from product descriptions
2. Username pattern generation + profile API verification

Usage:
    python hotmart_ig_finder.py
"""

import csv
import json
import sys
import time
import re
import unicodedata
import logging
import requests
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────────────

RAPIDAPI_KEY = "59d28307f6msh6d99f19f97ea797p134770jsn4496a9342542"
API_HOST = "instagram-api-fast-reliable-data-scraper.p.rapidapi.com"

INPUT_CSV = "data/hotmart_unique_sellers.csv"
OUTPUT_CSV = "data/hotmart_instagram_leads.csv"
OUTPUT_JSON = "data/hotmart_instagram_leads.json"
CACHE_DIR = Path("data/hotmart_ig_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

MAX_SELLERS = 200

HEADERS = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": API_HOST,
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def strip_accents(s):
    """Remove accents from a string."""
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )


def generate_usernames(name):
    """Generate possible Instagram usernames from a person/brand name."""
    clean = strip_accents(name.lower().strip())
    # Remove common business suffixes
    for suffix in [' inc', ' inc.', ' s.a.s', ' s.a', ' sas', ' sa', ' llc',
                   ' ltd', ' s.l', ' s.l.']:
        clean = clean.replace(suffix, '')

    # Remove special chars, keep spaces
    clean = re.sub(r'[^a-z0-9\s]', '', clean).strip()
    parts = clean.split()

    if not parts:
        return []

    candidates = []

    # Exact joined
    joined = ''.join(parts)
    if len(joined) >= 3:
        candidates.append(joined)

    # Underscore joined
    if len(parts) > 1:
        candidates.append('_'.join(parts))

    # Dot joined
    if len(parts) > 1:
        candidates.append('.'.join(parts))

    # First + last only (for multi-word names)
    if len(parts) >= 3:
        candidates.append(parts[0] + parts[-1])
        candidates.append(parts[0] + '_' + parts[-1])
        candidates.append(parts[0] + '.' + parts[-1])
        # First initial + last
        candidates.append(parts[0][0] + parts[-1])

    # For 2-word names
    if len(parts) == 2:
        candidates.append(parts[0] + parts[1])
        candidates.append(parts[1] + parts[0])
        candidates.append(parts[0] + '_' + parts[1])
        candidates.append(parts[0] + '.' + parts[1])
        candidates.append(parts[0][0] + parts[1])

    # Brand-style (no spaces)
    if len(parts) == 1 and len(parts[0]) >= 3:
        candidates.append(parts[0])

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for c in candidates:
        if c not in seen and 3 <= len(c) <= 30:
            seen.add(c)
            unique.append(c)

    return unique[:8]  # Limit attempts per seller


def get_profile(username, retries=3):
    """Get full Instagram profile for a username. Returns dict or None."""
    safe = re.sub(r'[^a-zA-Z0-9._]', '_', username.lower())
    cache_file = CACHE_DIR / f"profile_{safe}.json"
    if cache_file.exists():
        data = json.loads(cache_file.read_text())
        return data if data else None

    for attempt in range(retries):
        try:
            resp = requests.get(
                f"https://{API_HOST}/profile",
                params={"username": username},
                headers=HEADERS,
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get('username'):
                    cache_file.write_text(json.dumps(data))
                    return data
                else:
                    # Account doesn't exist
                    cache_file.write_text(json.dumps({}))
                    return None
            elif resp.status_code == 429:
                wait = 8 * (attempt + 1)
                logger.warning(f"Rate limited, waiting {wait}s...")
                time.sleep(wait)
            elif resp.status_code == 404:
                cache_file.write_text(json.dumps({}))
                return None
            else:
                logger.warning(f"HTTP {resp.status_code} for @{username}")
                cache_file.write_text(json.dumps({}))
                return None
        except Exception as e:
            logger.warning(f"Profile error for {username}: {e}")
            time.sleep(2)

    return None


def name_match_score(seller_name, ig_full_name, ig_username):
    """Score how well an IG profile matches the seller name (0-10)."""
    seller_lower = strip_accents(seller_name.lower().strip())
    ig_name_lower = strip_accents((ig_full_name or '').lower().strip())
    ig_user_lower = strip_accents((ig_username or '').lower().strip())

    seller_words = set(re.findall(r'[a-z]+', seller_lower))
    name_words = set(re.findall(r'[a-z]+', ig_name_lower))

    if not seller_words:
        return 0

    # Word overlap in full name
    overlap = len(seller_words & name_words)
    total = len(seller_words)
    name_ratio = overlap / total if total else 0

    score = 0

    # Full name is very similar
    if name_ratio >= 0.8:
        score += 6
    elif name_ratio >= 0.5:
        score += 4
    elif name_ratio >= 0.3:
        score += 2

    # Username contains key words
    for w in seller_words:
        if len(w) > 3 and w in ig_user_lower:
            score += 1

    return min(score, 10)


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    def out(msg=""):
        print(msg, flush=True)

    out("=" * 70)
    out("  HOTMART SELLER → INSTAGRAM FINDER")
    out("  Finding Instagram profiles for top Hotmart sellers")
    out("=" * 70)

    # Load sellers
    with open(INPUT_CSV) as f:
        all_sellers = list(csv.DictReader(f))

    # Spanish-language sellers with reviews, sorted by review count
    sellers = [
        s for s in all_sellers
        if s.get('language') == 'ES' and int(s.get('total_reviews', 0) or 0) > 0
    ]
    sellers.sort(key=lambda x: -int(x.get('total_reviews', 0) or 0))
    sellers = sellers[:MAX_SELLERS]

    out(f"\n  Processing top {len(sellers)} Spanish-language Hotmart sellers")
    out(f"  Using profile verification API")
    out()

    results = []
    found = 0
    not_found = 0
    api_calls = 0

    for i, seller in enumerate(sellers):
        author = seller.get('author', '')
        title = seller.get('title', '')
        rating = seller.get('rating', '')
        reviews = seller.get('total_reviews', '')
        product_url = seller.get('product_url', '')
        existing_ig = seller.get('instagram', '')

        # Progress
        if (i + 1) % 5 == 0 or i == 0:
            out(f"  [{i+1}/{len(sellers)}] Found: {found} | Not found: {not_found} | API calls: {api_calls}")

        matched_profile = None
        match_method = 'not_found'

        # ── Strategy 1: Use existing IG handle from description ──
        if existing_ig and existing_ig != '-':
            handles = [h.strip() for h in existing_ig.split(',') if h.strip()]
            for handle in handles:
                # Skip junk
                if handle in ('hotmail.com', 'gmail.com', 'yahoo.com') or len(handle) < 3:
                    continue
                if handle.startswith('core__'):  # junk pattern
                    continue
                profile = get_profile(handle)
                api_calls += 1
                time.sleep(1.5)
                if profile:
                    matched_profile = profile
                    match_method = 'from_hotmart_description'
                    break

        # ── Strategy 2: Try username patterns ──
        if not matched_profile:
            candidates = generate_usernames(author)
            for candidate in candidates:
                profile = get_profile(candidate)
                api_calls += 1
                time.sleep(1.5)

                if profile:
                    # Verify it's actually the right person
                    score = name_match_score(
                        author,
                        profile.get('full_name', ''),
                        profile.get('username', '')
                    )
                    if score >= 3:
                        matched_profile = profile
                        match_method = 'username_pattern'
                        break

        # ── Build result ──
        if matched_profile:
            p = matched_profile
            bio = (p.get('biography', '') or '').replace('\n', ' | ')[:300]

            results.append({
                'hotmart_seller': author,
                'hotmart_product': title[:100],
                'hotmart_rating': rating,
                'hotmart_reviews': reviews,
                'hotmart_url': product_url,
                'ig_username': p.get('username', ''),
                'ig_full_name': p.get('full_name', ''),
                'ig_followers': p.get('follower_count', 0),
                'ig_following': p.get('following_count', 0),
                'ig_posts': p.get('media_count', 0),
                'ig_bio': bio,
                'ig_website': p.get('external_url', '') or '',
                'ig_verified': p.get('is_verified', False),
                'ig_is_business': p.get('is_business', False),
                'ig_url': f"https://instagram.com/{p.get('username', '')}",
                'match_method': match_method,
            })
            found += 1
        else:
            results.append({
                'hotmart_seller': author,
                'hotmart_product': title[:100],
                'hotmart_rating': rating,
                'hotmart_reviews': reviews,
                'hotmart_url': product_url,
                'ig_username': '',
                'ig_full_name': '',
                'ig_followers': '',
                'ig_following': '',
                'ig_posts': '',
                'ig_bio': '',
                'ig_website': '',
                'ig_verified': '',
                'ig_is_business': '',
                'ig_url': '',
                'match_method': 'not_found',
            })
            not_found += 1

    # ── Sort: found first (by followers desc), then not-found ──
    found_results = [r for r in results if r['ig_username']]
    notfound_results = [r for r in results if not r['ig_username']]
    found_results.sort(key=lambda x: -(x.get('ig_followers') or 0))
    results = found_results + notfound_results

    # ── Display ──
    out(f"\n{'='*70}")
    out(f"  RESULTS SUMMARY")
    out(f"{'='*70}")
    out(f"  Sellers processed:    {len(results)}")
    out(f"  Instagram found:      {found}")
    out(f"  Not found:            {not_found}")
    out(f"  Match rate:           {found/max(len(results),1)*100:.1f}%")
    out(f"  Total API calls:      {api_calls}")

    # Top found sellers
    out(f"\n  TOP HOTMART SELLERS WITH INSTAGRAM (showing {min(found, 60)}):")
    out(f"  {'#':>3}  {'Seller':<25} {'IG Handle':<22} {'Followers':>10} {'Reviews':>7}  {'Method':<12}  Bio")
    out(f"  {'-'*130}")

    for i, r in enumerate(found_results[:60]):
        fc = r.get('ig_followers', 0)
        if isinstance(fc, int) and fc > 0:
            if fc >= 1_000_000:
                foll = f"{fc/1_000_000:.1f}M"
            elif fc >= 1_000:
                foll = f"{fc/1_000:.1f}K"
            else:
                foll = str(fc)
        else:
            foll = "-"

        bio_preview = r['ig_bio'][:35] if r['ig_bio'] else r['hotmart_product'][:35]
        out(
            f"  {i+1:>3}  {r['hotmart_seller'][:24]:<25} @{r['ig_username'][:20]:<22} "
            f"{foll:>10} {r['hotmart_reviews']:>7}  {r['match_method']:<12}  {bio_preview}"
        )

    # ── Save ──
    if results:
        fields = list(results[0].keys())
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(results)

        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        # Also save just the found ones for easy import
        found_csv = "data/hotmart_ig_found.csv"
        with open(found_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(found_results)

        out(f"\n  FILES SAVED:")
        out(f"    All sellers:    {OUTPUT_CSV}")
        out(f"    Found only:     {found_csv}")
        out(f"    JSON:           {OUTPUT_JSON}")
        out(f"\n  Import to Google Sheets: File > Import > Upload the CSV")
    else:
        out("  No results to save!")

    out()


if __name__ == "__main__":
    main()
