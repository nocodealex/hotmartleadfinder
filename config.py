"""
Configuration for Hotmart Lead Finder.

All settings are centralized here. API endpoint paths can be updated
if you switch to a different Instagram scraping API.
"""

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env", override=True)
except ImportError:
    pass

try:
    import streamlit as st
    for key, val in st.secrets.items():
        os.environ.setdefault(key, str(val))
except Exception:
    pass

# ── Directories ──────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# ── API Keys ─────────────────────────────────────────────────────────
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN", "")

# ── Supabase ─────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# ── Pipedrive CRM ────────────────────────────────────────────────────
PIPEDRIVE_API_TOKEN = os.getenv("PIPEDRIVE_API_TOKEN", "")
PIPEDRIVE_DOMAIN = os.getenv("PIPEDRIVE_DOMAIN", "")  # e.g. "yourcompany" for yourcompany.pipedrive.com

# ── Instagram API (RapidAPI) ─────────────────────────────────────────
INSTAGRAM_API_HOST = "instagram-api-fast-reliable-data-scraper.p.rapidapi.com"
INSTAGRAM_API_BASE = f"https://{INSTAGRAM_API_HOST}"

ENDPOINTS = {
    "user_id_by_username": "/profile",
    "user_profile":        "/profile",
    "user_following":      "/following",
    "user_posts":          "/feed",
}

# ── Rate Limiting ────────────────────────────────────────────────────
REQUEST_DELAY_SECONDS = 2.0
ANTHROPIC_DELAY_SECONDS = 0.5

# ── Pipeline Settings ────────────────────────────────────────────────
MAX_FOLLOWING_TO_FETCH = 3000
POSTS_TO_ANALYZE = 10
BIO_SCORE_THRESHOLD = 0.35
LEAD_SCORE_THRESHOLD = 0.55        # Min overall score to classify as lead
DEPTH_CRAWL_THRESHOLD = 0.70
MAX_CRAWL_DEPTH = 1

# ── Network Graph ────────────────────────────────────────────────────
# In graph mode, only Claude-analyze accounts appearing in 2+ seeds
GRAPH_MIN_APPEARANCES = 2
# Appearance count weight is much higher in graph mode
GRAPH_APPEARANCE_BOOST = True

# ── Scoring Weights ──────────────────────────────────────────────────
WEIGHT_BIO = 0.25
WEIGHT_WEBSITE = 0.20
WEIGHT_CAPTIONS = 0.15
WEIGHT_EVENTS = 0.10
WEIGHT_APPEARANCES = 0.30           # Boosted from 0.10 → 0.30

# ── Lead Tiers ───────────────────────────────────────────────────────
# Tier 1 (Whales): massive sellers, top affiliates, revenue 7+ figures
TIER1_MIN_SCORE = 0.80
# Tier 2 (Agencies): agency owners with multiple clients
TIER2_MIN_SCORE = 0.65
# Tier 3 (Affiliates): platform affiliates, co-producers
TIER3_MIN_SCORE = 0.55
# Tier 4 (Sellers): individual sellers who could switch
TIER4_MIN_SCORE = 0.40

# ── Claude Model ─────────────────────────────────────────────────────
CLAUDE_MODEL = "claude-sonnet-4-20250514"
CLAUDE_MAX_TOKENS = 1024

# ── Monitoring ───────────────────────────────────────────────────────
MONITOR_INTERVAL_HOURS = 6
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "")

# ── Storage Files ────────────────────────────────────────────────────
LEADS_CSV = DATA_DIR / "leads.csv"
PROCESSED_ACCOUNTS_FILE = DATA_DIR / "processed_accounts.json"
SEED_ACCOUNTS_FILE = DATA_DIR / "seed_accounts.json"
CALIBRATION_FILE = DATA_DIR / "calibration_log.json"
WHOP_SELLERS_FILE = DATA_DIR / "whop_sellers.json"
