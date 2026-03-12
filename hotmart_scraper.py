"""
Hotmart Marketplace Scraper v2 — extract top sellers with contact info.

Uses Hotmart's __NEXT_DATA__ JSON to get structured product data including
seller names, ratings, and full descriptions. Extracts Instagram handles,
emails, WhatsApp numbers, and websites from descriptions.

Usage:
    python hotmart_scraper.py
"""

import re
import csv
import json
import time
import logging
import requests
from pathlib import Path
from urllib.parse import quote
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────────────

OUTPUT_CSV = "data/hotmart_sellers.csv"
OUTPUT_JSON = "data/hotmart_sellers.json"
CONTACT_CSV = "data/hotmart_sellers_with_contact.csv"

BASE_URL = "https://hotmart.com"

# Spanish-language search queries across high-value niches
SEARCH_QUERIES = [
    # Business / MMO
    "marketing digital", "ganar dinero", "emprendimiento", "negocio online",
    "afiliados hotmart", "ventas online", "ecommerce", "dropshipping",
    "trading", "inversiones", "finanzas personales", "coaching",
    "marca personal", "redes sociales", "facebook ads", "instagram marketing",
    "copywriting", "embudos de venta", "educación financiera", "criptomonedas",
    "bienes raices", "amazon fba", "infoproductos", "whatsapp marketing",
    # Personal Development
    "desarrollo personal", "productividad", "liderazgo", "mentalidad",
    # Additional high-intent queries
    "curso online", "academia online", "masterclass", "certificación",
    "vender por internet", "dinero online", "libertad financiera",
    "tráfico pago", "publicidad digital", "google ads",
    "tienda online", "shopify", "inteligencia artificial",
]

MAX_PAGES_PER_QUERY = 5  # 24 results per page × 5 = up to 120 per query

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

# ── Contact extraction ──────────────────────────────────────────────────────

def extract_instagram(text):
    handles = set()
    for m in re.finditer(r'instagram\.com/([a-zA-Z0-9._]{2,30})', text, re.IGNORECASE):
        h = m.group(1).lower().rstrip('.')
        if h not in ('p', 'reel', 'reels', 'stories', 'explore', 'tv', 'accounts', 'about'):
            handles.add(h)
    for m in re.finditer(r'@([a-zA-Z0-9._]{3,30})', text):
        h = m.group(1).lower().rstrip('.')
        # Only add if in context of Instagram mention
        if 'instagram' in text.lower() or 'ig:' in text.lower():
            handles.add(h)
    return list(handles)


def extract_email(text):
    emails = set()
    for m in re.finditer(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text):
        email = m.group(0).lower()
        if not email.endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg')):
            emails.add(email)
    return list(emails)


def extract_whatsapp(text):
    numbers = set()
    for m in re.finditer(r'wa\.me/(\+?\d{10,15})', text):
        numbers.add(m.group(1))
    for m in re.finditer(r'whatsapp[:\s]*(\+?\d[\d\s\-]{8,18}\d)', text, re.IGNORECASE):
        num = re.sub(r'[\s\-]', '', m.group(1))
        if len(num) >= 10:
            numbers.add(num)
    return list(numbers)


def extract_website(text):
    urls = set()
    for m in re.finditer(r'https?://[^\s<>"\'\\]+', text):
        url = m.group(0).rstrip('.,;:!?)]}')
        skip = ['instagram.com', 'facebook.com', 'twitter.com', 'tiktok.com',
                'youtube.com', 'hotmart.com', 'wa.me', 'whatsapp.com',
                'hotmart.s3', 'scontent', 'cdninstagram', 'fbcdn', 'bit.ly']
        if not any(s in url.lower() for s in skip):
            urls.add(url)
    return list(urls)


def extract_youtube(text):
    channels = set()
    for m in re.finditer(r'youtube\.com/(?:c/|channel/|@)?([a-zA-Z0-9._-]{2,50})', text, re.IGNORECASE):
        ch = m.group(1)
        if ch not in ('watch', 'playlist', 'results', 'feed', 'channel'):
            channels.add(ch)
    return list(channels)


def extract_all_contact(text):
    """Extract all contact info from text."""
    return {
        'instagram': extract_instagram(text),
        'email': extract_email(text),
        'whatsapp': extract_whatsapp(text),
        'website': extract_website(text),
        'youtube': extract_youtube(text),
    }


# ── Scraping ────────────────────────────────────────────────────────────────

def fetch_search_results(query, page=1):
    """Fetch products from Hotmart marketplace via __NEXT_DATA__."""
    url = f"{BASE_URL}/es/marketplace/productos?q={quote(query)}&page={page}"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.text, 'html.parser')
        next_data = soup.find('script', id='__NEXT_DATA__')
        if not next_data:
            return []

        data = json.loads(next_data.string)
        results = (
            data.get('props', {})
            .get('pageProps', {})
            .get('resultsData', {})
            .get('requestData', {})
            .get('results', [])
        )
        return results

    except Exception as e:
        logger.warning(f"Error fetching {query} p{page}: {e}")
        return []


def main():
    print("=" * 70)
    print("  HOTMART MARKETPLACE SCRAPER v2")
    print("  Extracting top sellers with contact info from structured data")
    print("=" * 70)

    # Step 1: Collect all products
    all_products = {}  # productId -> product data

    for qi, query in enumerate(SEARCH_QUERIES):
        for page in range(1, MAX_PAGES_PER_QUERY + 1):
            logger.info(f"[{qi+1}/{len(SEARCH_QUERIES)}] {query} (page {page})")
            results = fetch_search_results(query, page)
            if not results:
                break

            new = 0
            for r in results:
                pid = r.get('productId')
                if pid and pid not in all_products:
                    r['_query'] = query
                    all_products[pid] = r
                    new += 1

            logger.info(f"  {len(results)} results, {new} new (total: {len(all_products)})")
            time.sleep(0.5)

            if len(results) < 24:
                break  # No more pages

    print(f"\n  Total unique products collected: {len(all_products)}")

    # Step 2: Process each product
    sellers = []
    seller_names = {}  # authorName -> best product

    for pid, product in all_products.items():
        title = product.get('title', '')
        author = product.get('authorName', product.get('ownerName', ''))
        description = product.get('description', '')
        rating = product.get('rating', 0)
        total_reviews = product.get('totalReviews', 0)
        locale = product.get('locale', '')
        slug = product.get('slug', '')
        pref = product.get('producerReferenceCode', '')

        # Build product URL
        if locale == 'PT_BR':
            product_url = f"{BASE_URL}/pt-br/marketplace/produtos/{slug}/{pref}"
        else:
            product_url = f"{BASE_URL}/es/marketplace/productos/{slug}/{pref}"

        # Extract contact from description
        contact = extract_all_contact(description)
        has_contact = any(contact[k] for k in ['instagram', 'email', 'whatsapp', 'website'])

        # Determine language
        is_spanish = locale == 'ES' or any(w in description.lower() for w in ['aprenderás', 'podrás', 'enseñar', 'negocio', 'dinero'])
        is_portuguese = locale == 'PT_BR' or any(w in description.lower() for w in ['você', 'aprenderá', 'ensinar', 'negócio'])

        record = {
            'product_id': pid,
            'title': title,
            'author': author,
            'rating': round(rating, 1) if rating else 0,
            'total_reviews': total_reviews or 0,
            'locale': locale,
            'language': 'ES' if is_spanish else ('PT' if is_portuguese else locale),
            'product_url': product_url,
            'query': product.get('_query', ''),
            'description': description[:500],
            'instagram': ', '.join(contact.get('instagram', [])),
            'email': ', '.join(contact.get('email', [])),
            'whatsapp': ', '.join(contact.get('whatsapp', [])),
            'website': ', '.join(contact.get('website', [])),
            'youtube': ', '.join(contact.get('youtube', [])),
            'has_contact': has_contact,
        }
        sellers.append(record)

        # Track best product per author
        if author:
            if author not in seller_names or (total_reviews or 0) > (seller_names[author].get('total_reviews', 0) or 0):
                seller_names[author] = record

    # Sort by reviews descending
    sellers.sort(key=lambda x: (-(x['total_reviews'] or 0), -(x['rating'] or 0)))

    # Stats
    spanish_products = [s for s in sellers if s['language'] == 'ES']
    with_contact = [s for s in sellers if s['has_contact']]
    spanish_with_contact = [s for s in sellers if s['has_contact'] and s['language'] == 'ES']

    print(f"\n{'='*70}")
    print(f"  RESULTS")
    print(f"{'='*70}")
    print(f"  Total products: {len(sellers)}")
    print(f"  Spanish products: {len(spanish_products)}")
    print(f"  With contact info: {len(with_contact)}")
    print(f"  Spanish with contact: {len(spanish_with_contact)}")
    print(f"  Unique sellers: {len(seller_names)}")

    # Show top Spanish sellers by reviews
    print(f"\n  TOP 50 SPANISH-LANGUAGE SELLERS (by reviews):")
    print(f"  {'#':>3}  {'Rating':>6} {'Reviews':>7}  {'Author':<30} {'Title':<40} {'IG/Contact'}")
    print(f"  {'-'*130}")

    shown = 0
    for s in sellers:
        if s['language'] != 'ES':
            continue
        shown += 1
        if shown > 50:
            break
        contact_str = s['instagram'] or s['email'] or s['whatsapp'] or s['website'] or '-'
        print(
            f"  {shown:>3}  {s['rating']:>6.1f} {s['total_reviews'] or 0:>7}  "
            f"{s['author'][:29]:<30} {s['title'][:39]:<40} {contact_str[:40]}"
        )

    # Show all with contact info
    if with_contact:
        print(f"\n  ALL PRODUCTS WITH CONTACT INFO ({len(with_contact)}):")
        print(f"  {'#':>3}  {'Lang':>4} {'Rating':>6} {'Author':<25} {'Instagram':<25} {'Email':<30} {'Title'}")
        print(f"  {'-'*130}")
        for i, s in enumerate(with_contact):
            print(
                f"  {i+1:>3}  {s['language']:>4} {s['rating']:>6.1f} "
                f"{s['author'][:24]:<25} {s['instagram'][:24] or '-':<25} "
                f"{s['email'][:29] or '-':<30} {s['title'][:40]}"
            )

    # Save
    Path("data").mkdir(exist_ok=True)

    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        fields = list(sellers[0].keys()) if sellers else []
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(sellers)

    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(sellers, f, indent=2, ensure_ascii=False)

    # Contact-only CSV
    with open(CONTACT_CSV, 'w', newline='', encoding='utf-8') as f:
        fields = [k for k in (sellers[0].keys() if sellers else []) if k != 'has_contact']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for s in with_contact:
            writer.writerow({k: v for k, v in s.items() if k != 'has_contact'})

    print(f"\n  Saved {len(sellers)} products to {OUTPUT_CSV}")
    print(f"  Saved {len(with_contact)} with contact to {CONTACT_CSV}")

    # Also save unique sellers list
    unique_sellers = sorted(seller_names.values(), key=lambda x: (-(x['total_reviews'] or 0), -(x['rating'] or 0)))
    with open("data/hotmart_unique_sellers.csv", 'w', newline='', encoding='utf-8') as f:
        fields = ['author', 'rating', 'total_reviews', 'language', 'title', 'product_url',
                  'instagram', 'email', 'whatsapp', 'website', 'youtube']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for s in unique_sellers:
            writer.writerow({k: s.get(k, '') for k in fields})

    print(f"  Saved {len(unique_sellers)} unique sellers to data/hotmart_unique_sellers.csv")


if __name__ == "__main__":
    main()
