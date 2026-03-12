"""
Website scraper for Instagram bio links.

Fetches the URL from a user's bio, follows redirects (handles Linktree,
Beacons, Stan Store, etc.), and extracts readable text for LLM analysis.
"""

import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Domains where scraping the page is unlikely to yield useful agency info
SKIP_DOMAINS = {
    "youtube.com", "youtu.be",
    "tiktok.com",
    "twitter.com", "x.com",
    "facebook.com", "fb.com",
    "wa.me", "api.whatsapp.com",
    "t.me",                        # Telegram
    "open.spotify.com",
    "music.apple.com",
    "pinterest.com",
}

# Link-in-bio aggregators — still worth scraping for link titles
LINKINBIO_DOMAINS = {
    "linktr.ee", "beacons.ai", "stan.store",
    "linkin.bio", "lnk.bio", "bio.link",
    "taplink.cc", "hoo.be", "campsite.bio",
    "carrd.co", "snipfeed.co",
}

MAX_TEXT_LENGTH = 3000  # Characters to send to LLM


def fetch_website_text(url: str, timeout: int = 15) -> str | None:
    """
    Fetch a URL and return cleaned text content.
    Returns None if the site can't be reached or parsed.
    """
    if not url:
        return None

    # Normalise URL
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    # Check if we should skip this domain
    try:
        domain = urlparse(url).netloc.lower().lstrip("www.")
        if any(domain.endswith(skip) for skip in SKIP_DOMAINS):
            logger.debug(f"Skipping social-media domain: {domain}")
            return None
    except Exception:
        pass

    try:
        resp = requests.get(
            url,
            timeout=timeout,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "pt-BR,pt;q=0.9,es;q=0.8,en;q=0.7",
            },
            allow_redirects=True,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return None

    # Detect content type
    content_type = resp.headers.get("content-type", "")
    if "text/html" not in content_type and "application/xhtml" not in content_type:
        logger.debug(f"Non-HTML content at {url}: {content_type}")
        return None

    try:
        soup = BeautifulSoup(resp.text, "html.parser")

        # Remove script, style, nav, footer elements
        for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
            tag.decompose()

        # Get text
        text = soup.get_text(separator=" ", strip=True)

        # Collapse whitespace
        text = " ".join(text.split())

        if len(text) < 20:
            logger.debug(f"Too little text extracted from {url}")
            return None

        # Check if it's a link-in-bio page — note that in the text
        domain = urlparse(resp.url).netloc.lower().lstrip("www.")
        is_linkinbio = any(domain.endswith(lib) for lib in LINKINBIO_DOMAINS)
        if is_linkinbio:
            text = f"[Link-in-bio page on {domain}] {text}"

        return text[:MAX_TEXT_LENGTH]

    except Exception as e:
        logger.warning(f"Failed to parse HTML from {url}: {e}")
        return None
