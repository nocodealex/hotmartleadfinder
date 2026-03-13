"""
Supabase storage backend for the multi-user Whop Intro Dashboard.

Provides the same API as the old file-based load/save functions,
backed by Supabase Postgres for persistent storage.
"""

import os
import logging
from datetime import datetime, timezone

from supabase import create_client, Client

logger = logging.getLogger(__name__)

_client: Client | None = None


def _get_client() -> Client:
    global _client
    if _client is None:
        url = os.environ.get("SUPABASE_URL", "")
        key = os.environ.get("SUPABASE_KEY", "")

        if not url or not key:
            try:
                import streamlit as st
                url = url or st.secrets.get("SUPABASE_URL", "")
                key = key or st.secrets.get("SUPABASE_KEY", "")
            except Exception:
                pass

        if not url or not key:
            raise RuntimeError(
                "SUPABASE_URL and SUPABASE_KEY must be set. "
                "Add them to your .env or Streamlit secrets."
            )
        _client = create_client(url, key)
    return _client


# ── Users ────────────────────────────────────────────────────────────

def get_user_list() -> list[str]:
    db = _get_client()
    resp = db.table("users").select("name").order("name").execute()
    return [row["name"] for row in resp.data]


def create_user(name: str) -> None:
    db = _get_client()
    db.table("users").upsert(
        {"name": name, "created_at": datetime.now(timezone.utc).isoformat()},
        on_conflict="name",
    ).execute()


# ── Partners ─────────────────────────────────────────────────────────

def load_partners(user: str) -> list[str]:
    db = _get_client()
    resp = (
        db.table("partners")
        .select("ig_username")
        .eq("user_name", user)
        .order("created_at")
        .execute()
    )
    return [row["ig_username"] for row in resp.data]


def save_partners(user: str, partners: list[str]) -> None:
    db = _get_client()
    db.table("partners").delete().eq("user_name", user).execute()
    if partners:
        rows = [
            {
                "user_name": user,
                "ig_username": p,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            for p in partners
        ]
        db.table("partners").insert(rows).execute()


# ── Prospects ────────────────────────────────────────────────────────

PROSPECT_COLUMNS = [
    "username", "full_name", "bio", "follower_count", "overall_score",
    "tier", "classification", "niche", "lead_type", "instagram_url",
    "followed_by_partners", "num_partners_connected", "partner_list",
    # Engagement metrics
    "avg_likes", "avg_comments", "engagement_rate", "posting_frequency",
    # Business account info
    "is_business_account", "ig_category",
    # Revenue estimation
    "business_size_tier", "estimated_annual_revenue_low",
    "estimated_annual_revenue_high", "estimated_deal_value",
    "revenue_confidence", "revenue_signals",
    # CRM cache
    "crm_status", "crm_deal_stage",
]


def load_prospects(user: str) -> list[dict]:
    db = _get_client()
    resp = (
        db.table("prospects")
        .select("*")
        .eq("user_name", user)
        .order("overall_score", desc=True)
        .execute()
    )
    results = []
    for row in resp.data:
        prospect = {col: row.get(col) for col in PROSPECT_COLUMNS}
        results.append(prospect)
    return results


def save_prospects(user: str, prospects: list[dict]) -> None:
    db = _get_client()
    db.table("prospects").delete().eq("user_name", user).execute()
    if not prospects:
        return
    rows = []
    for p in prospects:
        row = {"user_name": user}
        for col in PROSPECT_COLUMNS:
            row[col] = p.get(col)
        rows.append(row)
    # Insert in batches of 500 to avoid payload limits
    for i in range(0, len(rows), 500):
        db.table("prospects").insert(rows[i : i + 500]).execute()


# ── Outreach ─────────────────────────────────────────────────────────

def load_outreach(user: str) -> dict:
    db = _get_client()
    resp = (
        db.table("outreach")
        .select("prospect_username, status, notes, updated_at")
        .eq("user_name", user)
        .execute()
    )
    result = {}
    for row in resp.data:
        result[row["prospect_username"]] = {
            "status": row.get("status", "Not Contacted"),
            "notes": row.get("notes", ""),
            "updated_at": row.get("updated_at", ""),
        }
    return result


def save_outreach_entry(user: str, prospect_username: str, status: str, notes: str) -> None:
    db = _get_client()
    db.table("outreach").upsert(
        {
            "user_name": user,
            "prospect_username": prospect_username,
            "status": status,
            "notes": notes,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
        on_conflict="user_name,prospect_username",
    ).execute()


# ── API Keys ─────────────────────────────────────────────────────────

def load_api_keys(user: str) -> dict:
    db = _get_client()
    resp = (
        db.table("api_keys")
        .select("rapidapi_key, anthropic_api_key, apify_api_token")
        .eq("user_name", user)
        .execute()
    )
    if resp.data:
        row = resp.data[0]
        return {k: v for k, v in row.items() if v}
    return {}


def save_api_keys(user: str, keys: dict) -> None:
    db = _get_client()
    db.table("api_keys").upsert(
        {
            "user_name": user,
            "rapidapi_key": keys.get("rapidapi_key", ""),
            "anthropic_api_key": keys.get("anthropic_api_key", ""),
            "apify_api_token": keys.get("apify_api_token", ""),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
        on_conflict="user_name",
    ).execute()


# ── Following Cache ──────────────────────────────────────────────────

def load_following_cache(user: str, partner: str) -> list[dict] | None:
    db = _get_client()
    resp = (
        db.table("following_cache")
        .select("following_data")
        .eq("user_name", user)
        .eq("partner_username", partner)
        .execute()
    )
    if resp.data:
        return resp.data[0]["following_data"]
    return None


def save_following_cache(user: str, partner: str, data: list[dict]) -> None:
    db = _get_client()
    db.table("following_cache").upsert(
        {
            "user_name": user,
            "partner_username": partner,
            "following_data": data,
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
        on_conflict="user_name,partner_username",
    ).execute()
