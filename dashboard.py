"""
Whop Prospect Intro Dashboard — Multi-user Streamlit app.

Each user manages their own referral partner list, runs scans,
and sees isolated prospect results. Data persists in Supabase.
Pipedrive CRM enrichment is shared.

Usage:
    streamlit run dashboard.py
"""

import json
from datetime import datetime, timezone

import streamlit as st
import pandas as pd

import config
import supabase_db as db

TIER_LABELS = {
    "tier1_whale": "Whale",
    "tier2_agency": "Agency",
    "tier3_affiliate": "Affiliate",
    "tier4_seller": "Seller",
    "untiered": "Untiered",
}

NICHE_LABELS = {
    "business_coaching": "Business Coaching",
    "marketing": "Marketing",
    "financial_education": "Financial Education",
    "personal_development": "Personal Development",
    "health_fitness": "Health & Fitness",
    "education": "Education",
    "unknown": "Unknown",
}

SIZE_LABELS = {
    "whale": "Whale ($1M+)",
    "large": "Large ($200K-$1M)",
    "medium": "Medium ($50K-$200K)",
    "small": "Small ($10K-$50K)",
    "micro": "Micro (<$10K)",
    "unknown": "Unknown",
}

TIER_ORDER = ["tier1_whale", "tier2_agency", "tier3_affiliate", "tier4_seller", "untiered"]
SIZE_ORDER = ["whale", "large", "medium", "small", "micro", "unknown"]

OUTREACH_STATUSES = [
    "Not Contacted",
    "DM Sent",
    "Responded",
    "Meeting Booked",
    "Converted",
    "Not Interested",
]


# ── Helpers ──────────────────────────────────────────────────────────

def get_effective_api_keys(user: str) -> dict:
    """Return user's API keys, falling back to global config for any missing ones."""
    user_keys = db.load_api_keys(user)
    return {
        "rapidapi_key": user_keys.get("rapidapi_key") or config.RAPIDAPI_KEY,
        "anthropic_api_key": user_keys.get("anthropic_api_key") or config.ANTHROPIC_API_KEY,
        "apify_api_token": user_keys.get("apify_api_token") or config.APIFY_API_TOKEN,
    }


def format_followers(count) -> str:
    if not count or count <= 0:
        return "-"
    if count >= 1_000_000:
        return f"{count / 1_000_000:.1f}M"
    if count >= 1_000:
        return f"{count / 1_000:.1f}K"
    return str(int(count))


def format_deal_value(value) -> str:
    if not value or value <= 0:
        return "-"
    if value >= 1_000:
        return f"${value / 1_000:.1f}K"
    return f"${value:,.0f}"


def format_revenue_range(low, high) -> str:
    if not low and not high:
        return "-"
    def _fmt(v):
        if v >= 1_000_000:
            return f"${v / 1_000_000:.1f}M"
        if v >= 1_000:
            return f"${v / 1_000:.0f}K"
        return f"${v:.0f}"
    return f"{_fmt(low)}-{_fmt(high)}/yr"


def pipedrive_available() -> bool:
    return bool(config.PIPEDRIVE_API_TOKEN and config.PIPEDRIVE_DOMAIN)


@st.cache_data(ttl=600, show_spinner="Checking Pipedrive CRM...")
def enrich_with_pipedrive(prospects_json: str) -> list[dict]:
    from pipedrive_client import PipedriveClient
    prospects = json.loads(prospects_json)
    client = PipedriveClient()
    return client.enrich_prospects(prospects)


# ── Main ─────────────────────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title="Whop Intro Dashboard",
        page_icon="🤝",
        layout="wide",
    )

    # ── User selector (sidebar top) ──────────────────────────────────
    st.sidebar.title("Whop Intro Dashboard")

    users = db.get_user_list()

    if "current_user" not in st.session_state:
        st.session_state.current_user = users[0] if users else ""

    st.sidebar.subheader("User")

    if users:
        selected_user = st.sidebar.selectbox(
            "Select user",
            options=users,
            index=users.index(st.session_state.current_user) if st.session_state.current_user in users else 0,
            format_func=lambda u: u.title(),
            label_visibility="collapsed",
        )
        st.session_state.current_user = selected_user
    else:
        selected_user = ""

    new_user = st.sidebar.text_input(
        "Add new user",
        placeholder="Enter name...",
        label_visibility="collapsed",
    )
    if st.sidebar.button("Create User") and new_user:
        clean_name = new_user.strip().lower().replace(" ", "_")
        if clean_name:
            db.create_user(clean_name)
            st.session_state.current_user = clean_name
            st.rerun()

    if not selected_user and not new_user:
        st.title("Whop Prospect Intro Dashboard")
        st.info("Create a user in the sidebar to get started.")
        return

    current_user = st.session_state.current_user

    st.title(f"Whop Intro Dashboard — {current_user.title()}")

    # ── Tabs ─────────────────────────────────────────────────────────
    tabs = st.tabs([
        "Manage Partners",
        "Partner Briefs",
        "All Prospects",
        "Grouped by Partner",
        "Overlap Matrix",
        "Outreach Tracker",
        "Settings",
    ])

    with tabs[0]:
        _render_manage_partners(current_user)

    with tabs[6]:
        _render_settings(current_user)

    # Load prospect data for remaining tabs
    prospects = db.load_prospects(current_user)

    if not prospects:
        for tab in tabs[1:6]:
            with tab:
                st.info("No prospect data yet. Go to **Manage Partners** to add partners and run a scan.")
        return

    outreach = db.load_outreach(current_user)

    # Pipedrive enrichment
    has_pipedrive = pipedrive_available()
    if has_pipedrive:
        try:
            prospects = enrich_with_pipedrive(json.dumps(prospects))
        except Exception as e:
            st.sidebar.warning(f"Pipedrive error: {e}")
            has_pipedrive = False

    # Merge outreach status
    for p in prospects:
        key = p["username"].lower()
        if key in outreach:
            p["outreach_status"] = outreach[key].get("status", "Not Contacted")
            p["outreach_notes"] = outreach[key].get("notes", "")
        else:
            p["outreach_status"] = "Not Contacted"
            p["outreach_notes"] = ""

    df = pd.DataFrame(prospects)

    # Ensure numeric columns exist with defaults
    for col, default in [
        ("estimated_deal_value", 0), ("engagement_rate", 0),
        ("avg_likes", 0), ("avg_comments", 0),
        ("estimated_annual_revenue_low", 0), ("estimated_annual_revenue_high", 0),
    ]:
        if col not in df.columns:
            df[col] = default
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(default)

    for col, default in [
        ("business_size_tier", "unknown"), ("revenue_confidence", "none"),
    ]:
        if col not in df.columns:
            df[col] = default
        df[col] = df[col].fillna(default)

    if "crm_tag" not in df.columns:
        df["crm_tag"] = "new"

    all_partners = sorted(set(
        p for row in prospects for p in row.get("followed_by_partners", [])
    ))

    # ── Revenue Impact Summary (top of page) ─────────────────────────
    total_pipeline = df["estimated_deal_value"].sum()
    col_rev1, col_rev2, col_rev3, col_rev4, col_rev5, col_rev6 = st.columns(6)
    col_rev1.metric("Total Prospects", len(df))
    col_rev2.metric("Pipeline Value", format_deal_value(total_pipeline))
    col_rev3.metric("Whales", len(df[df["business_size_tier"] == "whale"]))
    col_rev4.metric("Large", len(df[df["business_size_tier"] == "large"]))
    col_rev5.metric("Not Contacted", len(df[df["outreach_status"] == "Not Contacted"]))

    if has_pipedrive:
        active_deals = len(df[df.get("crm_tag", pd.Series(dtype=str)) == "active_deal"]) if "crm_tag" in df.columns else 0
        col_rev6.metric("Active in CRM", active_deals)
    else:
        avg_score = df["overall_score"].mean()
        col_rev6.metric("Avg Score", f"{avg_score:.2f}" if len(df) else "-")

    # ── Sidebar filters ──────────────────────────────────────────────
    st.sidebar.divider()
    st.sidebar.header("Filters")

    min_score = st.sidebar.slider("Minimum score", 0.0, 1.0, 0.40, 0.05)

    selected_tiers = st.sidebar.multiselect(
        "Tiers",
        options=TIER_ORDER,
        default=TIER_ORDER,
        format_func=lambda t: TIER_LABELS.get(t, t),
    )

    selected_sizes = st.sidebar.multiselect(
        "Business Size",
        options=SIZE_ORDER,
        default=SIZE_ORDER,
        format_func=lambda s: SIZE_LABELS.get(s, s),
    )

    all_niches = sorted(df["niche"].dropna().unique())
    selected_niches = st.sidebar.multiselect(
        "Niches",
        options=all_niches,
        default=all_niches,
        format_func=lambda n: NICHE_LABELS.get(n, n),
    )

    min_partners = st.sidebar.slider(
        "Min partner connections", 1, max(1, int(df["num_partners_connected"].max())), 1,
    )

    selected_partners = st.sidebar.multiselect(
        "Show only these partners",
        options=all_partners,
        default=all_partners,
        format_func=lambda p: f"@{p}",
    )

    if has_pipedrive:
        st.sidebar.divider()
        st.sidebar.header("CRM Filter")
        hide_active = st.sidebar.checkbox(
            "Hide prospects with active CRM deals",
            value=True,
            help="Excludes prospects already being worked in Pipedrive",
        )
        if "crm_status" in df.columns:
            crm_options = sorted(df["crm_status"].dropna().unique())
            selected_crm = st.sidebar.multiselect(
                "CRM status", options=crm_options, default=crm_options,
            )
        else:
            selected_crm = None
    else:
        hide_active = False
        selected_crm = None

    st.sidebar.divider()
    st.sidebar.header("Outreach")
    outreach_options = sorted(df["outreach_status"].unique())
    selected_outreach = st.sidebar.multiselect(
        "Outreach status", options=outreach_options, default=outreach_options,
    )

    # ── Apply filters ─────────────────────────────────────────────────
    filtered = df[
        (df["overall_score"] >= min_score)
        & (df["tier"].isin(selected_tiers))
        & (df["business_size_tier"].isin(selected_sizes))
        & (df["niche"].isin(selected_niches))
        & (df["num_partners_connected"] >= min_partners)
        & (df["outreach_status"].isin(selected_outreach))
    ].copy()

    if selected_crm is not None and "crm_status" in filtered.columns:
        filtered = filtered[filtered["crm_status"].isin(selected_crm)]

    if hide_active and "crm_tag" in filtered.columns:
        filtered = filtered[filtered["crm_tag"] != "active_deal"]

    filtered = filtered[
        filtered["followed_by_partners"].apply(
            lambda partners: any(p in selected_partners for p in partners) if isinstance(partners, list) else False
        )
    ]

    # ── Render tabs ───────────────────────────────────────────────────
    with tabs[1]:
        _render_partner_briefs(filtered, all_partners, has_pipedrive)
    with tabs[2]:
        _render_all_prospects(filtered, has_pipedrive)
    with tabs[3]:
        _render_partner_view(filtered, selected_partners, has_pipedrive)
    with tabs[4]:
        _render_overlap_matrix(filtered, selected_partners)
    with tabs[5]:
        _render_outreach_tracker(filtered, outreach, has_pipedrive, current_user)


# ── Manage Partners ──────────────────────────────────────────────────

def _render_manage_partners(user: str):
    st.subheader("Your Referral Partners")
    st.caption("Add Instagram accounts of partners who can make intros for you")

    partners = db.load_partners(user)

    if partners:
        st.markdown(f"**{len(partners)} partner(s) configured:**")
        for i, partner in enumerate(partners):
            col1, col2 = st.columns([4, 1])
            col1.markdown(f"[@{partner}](https://instagram.com/{partner})")
            if col2.button("Remove", key=f"remove_{i}_{partner}"):
                partners.remove(partner)
                db.save_partners(user, partners)
                st.rerun()
    else:
        st.info("No partners added yet. Add Instagram accounts below.")

    st.divider()

    st.markdown("#### Add Partner")
    with st.form("add_partner_form", clear_on_submit=True):
        new_partner = st.text_input(
            "Instagram username or URL",
            placeholder="e.g. fedmkt or https://instagram.com/fedmkt",
        )
        submitted = st.form_submit_button("Add Partner", type="primary")

    if submitted and new_partner:
        raw = new_partner.strip().lstrip("@").split("?")[0].rstrip("/")
        clean = raw.split("/")[-1].lower().strip()
        if clean and clean not in partners:
            partners.append(clean)
            db.save_partners(user, partners)
            st.success(f"Added @{clean}")
            st.rerun()
        elif clean in partners:
            st.warning(f"@{clean} is already in your list")
    elif submitted:
        st.warning("Please enter a username")

    st.divider()

    # Run scan
    st.markdown("#### Run Prospect Scan")
    if not partners:
        st.warning("Add at least one partner before running a scan.")
        return

    st.markdown(f"This will scan the followings of your {len(partners)} partner(s) and find qualified Whop prospects.")

    col_scan1, col_scan2 = st.columns(2)
    skip_new = col_scan1.checkbox(
        "Skip new analysis (match existing leads only — free & fast)",
        value=True,
    )

    if col_scan2.button("Run Scan", type="primary"):
        _run_scan(user, partners, skip_new)


def _run_scan(user: str, partners: list[str], skip_new: bool):
    """Run the prospect finder for this user's partners."""
    from whop_prospect_finder import find_prospects

    keys = get_effective_api_keys(user)

    missing = []
    if not keys.get("apify_api_token"):
        missing.append("Apify API Token")
    if not keys.get("rapidapi_key"):
        missing.append("RapidAPI Key")
    if not skip_new and not keys.get("anthropic_api_key"):
        missing.append("Anthropic API Key")
    if missing:
        st.error(f"Missing API keys: {', '.join(missing)}. Go to the **Settings** tab to add them.")
        return

    exclude = set(partners) | {"aymon_holth", "nocode.alex"}

    existing = db.load_prospects(user)

    def on_save(prospects):
        db.save_prospects(user, prospects)

    def on_progress(prospect):
        db.upsert_prospect(user, prospect)

    def cache_load(partner):
        return db.load_following_cache(user, partner)

    def cache_save(partner, data):
        db.save_following_cache(user, partner, data)

    with st.spinner(f"Scanning followings for {len(partners)} partner(s)... This may take a few minutes."):
        try:
            prospects = find_prospects(
                partners=partners,
                skip_new=skip_new,
                exclude_usernames=exclude,
                api_keys=keys,
                save_callback=on_save,
                cache_load_fn=cache_load,
                cache_save_fn=cache_save,
                existing_prospects=existing,
                progress_save_fn=on_progress,
            )
            if prospects:
                total_value = sum(p.get("estimated_deal_value", 0) for p in prospects)
                st.success(
                    f"Found {len(prospects)} prospects! "
                    f"Estimated pipeline value: {format_deal_value(total_value)}. "
                    f"Switch to the other tabs to view results."
                )
            else:
                st.warning("No prospects found. Try adding more partners or running without 'skip new analysis'.")
        except Exception as e:
            st.error(f"Scan failed: {e}")


# ── Partner Briefs ───────────────────────────────────────────────────

def _render_partner_briefs(df: pd.DataFrame, partners: list[str], has_pipedrive: bool):
    st.subheader("Partner Intro Briefs")
    st.caption(
        "Top prospects per partner, ranked by estimated deal value. "
        "Copy the brief and send it to your partner via WhatsApp or DM."
    )

    if df.empty:
        st.info("No prospects match your filters.")
        return

    # Partner leaderboard
    st.markdown("#### Partner Leaderboard")
    leaderboard_data = []
    for partner in partners:
        partner_df = df[
            df["followed_by_partners"].apply(
                lambda ps: partner in ps if isinstance(ps, list) else False
            )
        ]
        if partner_df.empty:
            continue
        total_value = partner_df["estimated_deal_value"].sum()
        whale_count = len(partner_df[partner_df["business_size_tier"] == "whale"])
        large_count = len(partner_df[partner_df["business_size_tier"] == "large"])
        leaderboard_data.append({
            "Partner": f"@{partner}",
            "Prospects": len(partner_df),
            "Pipeline Value": format_deal_value(total_value),
            "Whales": whale_count,
            "Large": large_count,
            "_sort_value": total_value,
        })

    if leaderboard_data:
        leaderboard_data.sort(key=lambda x: -x["_sort_value"])
        lb_df = pd.DataFrame(leaderboard_data).drop(columns=["_sort_value"])
        st.dataframe(lb_df, use_container_width=True, hide_index=True)

    st.divider()

    # Per-partner briefs
    st.markdown("#### Ready-to-Send Briefs")

    for partner in partners:
        partner_df = df[
            df["followed_by_partners"].apply(
                lambda ps: partner in ps if isinstance(ps, list) else False
            )
        ].sort_values("estimated_deal_value", ascending=False)

        if partner_df.empty:
            continue

        top5 = partner_df.head(5)
        total_value = partner_df["estimated_deal_value"].sum()

        with st.expander(
            f"@{partner} — {len(partner_df)} prospects ({format_deal_value(total_value)} value)",
            expanded=False,
        ):
            # Display top prospects
            display = top5[["username", "full_name", "overall_score", "business_size_tier",
                            "estimated_deal_value", "niche", "lead_type", "follower_count"]].copy()
            display["username"] = display["username"].apply(lambda u: f"@{u}")
            display["overall_score"] = display["overall_score"].apply(lambda s: f"{s:.2f}")
            display["business_size_tier"] = display["business_size_tier"].map(SIZE_LABELS).fillna("?")
            display["estimated_deal_value"] = display["estimated_deal_value"].apply(format_deal_value)
            display["niche"] = display["niche"].map(NICHE_LABELS).fillna("?")
            display["follower_count"] = display["follower_count"].apply(format_followers)

            display = display.rename(columns={
                "username": "Prospect",
                "full_name": "Name",
                "overall_score": "Score",
                "business_size_tier": "Business Size",
                "estimated_deal_value": "Deal Value",
                "niche": "Niche",
                "lead_type": "Type",
                "follower_count": "Followers",
            })

            st.dataframe(display, use_container_width=True, hide_index=True)

            # Generate copyable brief
            from whop_prospect_finder import generate_partner_brief
            brief = generate_partner_brief(partner, df.to_dict("records"))

            if brief:
                st.text_area(
                    "Copy this message:",
                    value=brief,
                    height=200,
                    key=f"brief_{partner}",
                )


# ── Views ────────────────────────────────────────────────────────────

def _crm_columns(has_pipedrive: bool) -> list[str]:
    if has_pipedrive:
        return ["CRM Status", "Deal Stage"]
    return []


def _prepare_display(df_slice: pd.DataFrame, has_pipedrive: bool) -> pd.DataFrame:
    cols = [
        "username", "full_name", "overall_score", "tier",
        "business_size_tier", "estimated_deal_value",
        "niche", "follower_count", "engagement_rate",
        "lead_type", "num_partners_connected",
        "outreach_status", "bio",
    ]
    if has_pipedrive:
        cols += ["crm_status", "crm_deal_stage"]

    available_cols = [c for c in cols if c in df_slice.columns]
    display = df_slice[available_cols].copy()

    display["tier"] = display["tier"].map(TIER_LABELS).fillna("?")
    display["business_size_tier"] = display["business_size_tier"].map(SIZE_LABELS).fillna("?")
    display["niche"] = display["niche"].map(NICHE_LABELS).fillna("?")
    display["followers"] = display["follower_count"].apply(format_followers)
    display["score"] = display["overall_score"].apply(lambda s: f"{s:.2f}")
    display["deal_value"] = display["estimated_deal_value"].apply(format_deal_value)
    display["username"] = display["username"].apply(lambda u: f"@{u}")
    display["bio"] = display["bio"].apply(
        lambda b: (b[:80] + "...") if isinstance(b, str) and len(b) > 80 else b
    )
    if "engagement_rate" in display.columns:
        display["eng_rate"] = display["engagement_rate"].apply(
            lambda r: f"{r:.1%}" if r and r > 0 else "-"
        )

    rename_map = {
        "username": "Prospect",
        "full_name": "Name",
        "score": "Score",
        "tier": "Tier",
        "business_size_tier": "Biz Size",
        "deal_value": "Deal Value",
        "niche": "Niche",
        "followers": "Followers",
        "eng_rate": "Eng Rate",
        "lead_type": "Type",
        "num_partners_connected": "# Partners",
        "outreach_status": "Outreach",
        "bio": "Bio",
    }
    if has_pipedrive:
        rename_map["crm_status"] = "CRM Status"
        rename_map["crm_deal_stage"] = "Deal Stage"

    display = display.rename(columns=rename_map)
    return display


def _render_partner_view(df: pd.DataFrame, partners: list[str], has_pipedrive: bool):
    for partner in partners:
        partner_prospects = df[
            df["followed_by_partners"].apply(
                lambda ps: partner in ps if isinstance(ps, list) else False
            )
        ].sort_values("estimated_deal_value", ascending=False)

        if partner_prospects.empty:
            continue

        total_value = partner_prospects["estimated_deal_value"].sum()

        with st.expander(
            f"@{partner} — {len(partner_prospects)} intros ({format_deal_value(total_value)} value)",
            expanded=True,
        ):
            display = _prepare_display(partner_prospects, has_pipedrive)
            display["Also Followed By"] = partner_prospects["followed_by_partners"].apply(
                lambda ps: ", ".join(f"@{p}" for p in ps if p != partner) or "-"
                if isinstance(ps, list) else "-"
            )

            show_cols = [
                "Prospect", "Name", "Score", "Biz Size", "Deal Value",
                "Tier", "Niche", "Followers", "Eng Rate", "Type", "Outreach",
            ] + _crm_columns(has_pipedrive) + [
                "# Partners", "Also Followed By", "Bio",
            ]
            show_cols = [c for c in show_cols if c in display.columns]

            st.dataframe(
                display[show_cols],
                use_container_width=True,
                hide_index=True,
                height=min(len(display) * 38 + 40, 600),
            )


def _render_all_prospects(df: pd.DataFrame, has_pipedrive: bool):
    sort_col = st.selectbox(
        "Sort by",
        [
            "Deal Value (high to low)",
            "Score (high to low)",
            "Followers (high to low)",
            "Engagement Rate (high to low)",
            "Partner connections (high to low)",
        ],
    )

    sort_map = {
        "Deal Value (high to low)": ("estimated_deal_value", False),
        "Score (high to low)": ("overall_score", False),
        "Followers (high to low)": ("follower_count", False),
        "Engagement Rate (high to low)": ("engagement_rate", False),
        "Partner connections (high to low)": ("num_partners_connected", False),
    }
    col, asc = sort_map[sort_col]
    sorted_df = df.sort_values(col, ascending=asc)

    display = _prepare_display(sorted_df, has_pipedrive)
    display["Intro Via"] = sorted_df["partner_list"]

    show_cols = [
        "Prospect", "Name", "Score", "Biz Size", "Deal Value",
        "Tier", "Niche", "Followers", "Eng Rate", "Type", "Outreach",
    ] + _crm_columns(has_pipedrive) + [
        "# Partners", "Intro Via", "Bio",
    ]
    show_cols = [c for c in show_cols if c in display.columns]

    st.dataframe(
        display[show_cols],
        use_container_width=True,
        hide_index=True,
        height=min(len(display) * 38 + 40, 800),
    )


def _render_outreach_tracker(df: pd.DataFrame, outreach: dict, has_pipedrive: bool, user: str):
    st.subheader("Outreach Tracker")
    st.caption("Update outreach status for prospects directly from here")

    status_counts = df["outreach_status"].value_counts()
    funnel_cols = st.columns(len(OUTREACH_STATUSES))
    for i, status in enumerate(OUTREACH_STATUSES):
        funnel_cols[i].metric(status, status_counts.get(status, 0))

    st.divider()

    col_left, col_right = st.columns([1, 2])

    with col_left:
        st.markdown("#### Update Status")
        prospect_options = sorted(df["username"].unique())
        selected_prospect = st.selectbox(
            "Prospect",
            options=prospect_options,
            format_func=lambda u: f"@{u}",
        )

        current_status = outreach.get(selected_prospect.lower(), {}).get("status", "Not Contacted")
        new_status = st.selectbox(
            "New status",
            options=OUTREACH_STATUSES,
            index=OUTREACH_STATUSES.index(current_status) if current_status in OUTREACH_STATUSES else 0,
        )

        notes = st.text_area(
            "Notes",
            value=outreach.get(selected_prospect.lower(), {}).get("notes", ""),
            placeholder="Add any notes about this contact...",
        )

        if st.button("Save", type="primary"):
            db.save_outreach_entry(user, selected_prospect.lower(), new_status, notes)
            st.success(f"Updated @{selected_prospect} -> {new_status}")
            st.rerun()

    with col_right:
        st.markdown("#### Prospect Details")
        if selected_prospect:
            prospect_row = df[df["username"] == selected_prospect]
            if not prospect_row.empty:
                p = prospect_row.iloc[0]
                detail_col1, detail_col2 = st.columns(2)
                with detail_col1:
                    st.markdown(f"**Name:** {p.get('full_name', '-')}")
                    st.markdown(f"**Score:** {p.get('overall_score', 0):.2f}")
                    st.markdown(f"**Tier:** {TIER_LABELS.get(p.get('tier', ''), '?')}")
                    st.markdown(f"**Business Size:** {SIZE_LABELS.get(p.get('business_size_tier', ''), '?')}")
                    deal_val = p.get("estimated_deal_value", 0)
                    st.markdown(f"**Est. Deal Value:** {format_deal_value(deal_val)}")
                    st.markdown(f"**Niche:** {NICHE_LABELS.get(p.get('niche', ''), '?')}")
                    st.markdown(f"**Followers:** {format_followers(p.get('follower_count', 0))}")
                    eng = p.get("engagement_rate", 0)
                    if eng and eng > 0:
                        st.markdown(f"**Engagement Rate:** {eng:.1%}")
                with detail_col2:
                    st.markdown(f"**Outreach:** {p.get('outreach_status', 'Not Contacted')}")
                    if has_pipedrive:
                        st.markdown(f"**CRM Status:** {p.get('crm_status', 'Not in CRM')}")
                        if p.get("crm_deal_stage"):
                            st.markdown(f"**Deal Stage:** {p.get('crm_deal_stage', '-')}")
                            st.markdown(f"**Pipeline:** {p.get('crm_pipeline', '-')}")
                    st.markdown(f"**Intro via:** {p.get('partner_list', '')}")
                    rev_low = p.get("estimated_annual_revenue_low", 0)
                    rev_high = p.get("estimated_annual_revenue_high", 0)
                    if rev_low or rev_high:
                        st.markdown(f"**Est. Revenue:** {format_revenue_range(rev_low, rev_high)}")
                    ig_url = p.get("instagram_url", "")
                    if ig_url:
                        st.markdown(f"[Instagram Profile]({ig_url})")

                bio = p.get("bio", "")
                if bio:
                    st.markdown(f"**Bio:** {bio}")

    st.divider()

    st.markdown("#### All Prospects — Outreach Status")
    display = _prepare_display(df.sort_values("outreach_status"), has_pipedrive)
    display["Intro Via"] = df.sort_values("outreach_status")["partner_list"]

    show_cols = [
        "Prospect", "Name", "Score", "Biz Size", "Deal Value",
        "Tier", "Outreach",
    ] + _crm_columns(has_pipedrive) + [
        "Followers", "Intro Via",
    ]
    show_cols = [c for c in show_cols if c in display.columns]

    st.dataframe(
        display[show_cols],
        use_container_width=True,
        hide_index=True,
        height=min(len(display) * 38 + 40, 600),
    )


def _render_overlap_matrix(df: pd.DataFrame, partners: list[str]):
    st.subheader("Partner Overlap — Shared Prospects")
    st.caption("Number of prospects that both partners follow (strongest intro signals)")

    matrix = {}
    for p1 in partners:
        matrix[p1] = {}
        p1_prospects = set(
            df[df["followed_by_partners"].apply(
                lambda ps: p1 in ps if isinstance(ps, list) else False
            )]["username"]
        )
        for p2 in partners:
            p2_prospects = set(
                df[df["followed_by_partners"].apply(
                    lambda ps: p2 in ps if isinstance(ps, list) else False
                )]["username"]
            )
            matrix[p1][p2] = len(p1_prospects & p2_prospects)

    matrix_df = pd.DataFrame(matrix, index=partners, columns=partners)
    matrix_df.index = [f"@{p}" for p in matrix_df.index]
    matrix_df.columns = [f"@{p}" for p in matrix_df.columns]

    st.dataframe(
        matrix_df.style.background_gradient(cmap="YlOrRd", axis=None),
        use_container_width=True,
        height=min(len(partners) * 45 + 50, 500),
    )

    st.subheader("Multi-Partner Prospects")
    st.caption("Prospects followed by 2+ of your partners — warmest intro opportunities")

    multi = df[df["num_partners_connected"] >= 2].sort_values(
        ["estimated_deal_value", "overall_score"], ascending=[False, False]
    )

    if multi.empty:
        st.info("No prospects are shared across multiple partners.")
        return

    display = multi[[
        "username", "full_name", "overall_score", "tier",
        "business_size_tier", "estimated_deal_value",
        "follower_count", "num_partners_connected", "partner_list",
    ]].copy()

    display["tier"] = display["tier"].map(TIER_LABELS).fillna("?")
    display["business_size_tier"] = display["business_size_tier"].map(SIZE_LABELS).fillna("?")
    display["followers"] = display["follower_count"].apply(format_followers)
    display["score"] = display["overall_score"].apply(lambda s: f"{s:.2f}")
    display["deal_value"] = display["estimated_deal_value"].apply(format_deal_value)
    display["username"] = display["username"].apply(lambda u: f"@{u}")

    display = display.rename(columns={
        "username": "Prospect",
        "full_name": "Name",
        "score": "Score",
        "tier": "Tier",
        "business_size_tier": "Biz Size",
        "deal_value": "Deal Value",
        "followers": "Followers",
        "num_partners_connected": "# Partners",
        "partner_list": "Followed By",
    })

    st.dataframe(
        display[[
            "Prospect", "Name", "Score", "Biz Size", "Deal Value",
            "Tier", "Followers", "# Partners", "Followed By",
        ]],
        use_container_width=True,
        hide_index=True,
        height=min(len(display) * 38 + 40, 600),
    )


# ── Settings ─────────────────────────────────────────────────────────

def _render_settings(user: str):
    st.subheader("API Keys")
    st.caption("Enter your own API keys so scans use your accounts, not the shared ones.")

    user_keys = db.load_api_keys(user)

    st.markdown("""
**Where to get your keys:**
1. **RapidAPI Key** — Sign up at [rapidapi.com](https://rapidapi.com), subscribe to the [Instagram API](https://rapidapi.com/social-starter-api-social-starter-api-default/api/instagram-api-fast-reliable-data-scraper) (free tier available), and copy your API key from the dashboard
2. **Anthropic API Key** — Sign up at [console.anthropic.com](https://console.anthropic.com), add credits under Billing, then create an API key under API Keys
3. **Apify API Token** — Sign up at [apify.com](https://apify.com) (free tier available), go to Settings > Integrations, and copy your API token
    """)

    st.divider()

    with st.form("api_keys_form"):
        rapidapi_key = st.text_input(
            "RapidAPI Key",
            value=user_keys.get("rapidapi_key", ""),
            type="password",
            help="Used to fetch Instagram profiles and posts",
        )
        anthropic_key = st.text_input(
            "Anthropic API Key",
            value=user_keys.get("anthropic_api_key", ""),
            type="password",
            help="Used for AI-powered lead qualification (only needed for full scans)",
        )
        apify_token = st.text_input(
            "Apify API Token",
            value=user_keys.get("apify_api_token", ""),
            type="password",
            help="Used to scrape partner following lists",
        )

        saved = st.form_submit_button("Save API Keys", type="primary")

    if saved:
        new_keys = {}
        if rapidapi_key.strip():
            new_keys["rapidapi_key"] = rapidapi_key.strip()
        if anthropic_key.strip():
            new_keys["anthropic_api_key"] = anthropic_key.strip()
        if apify_token.strip():
            new_keys["apify_api_token"] = apify_token.strip()

        db.save_api_keys(user, new_keys)
        st.success("API keys saved!")

    st.divider()
    st.markdown("#### Your Key Status")
    effective = get_effective_api_keys(user)

    for label, key_name in [
        ("RapidAPI", "rapidapi_key"),
        ("Anthropic", "anthropic_api_key"),
        ("Apify", "apify_api_token"),
    ]:
        has_own = bool(user_keys.get(key_name))
        has_any = bool(effective.get(key_name))
        if has_own:
            st.markdown(f"- **{label}:** Using your own key")
        elif has_any:
            st.markdown(f"- **{label}:** Using shared key (add your own to avoid using shared credits)")
        else:
            st.markdown(f"- **{label}:** Not configured")


if __name__ == "__main__":
    main()
