"""
Whop Prospect Intro Dashboard — Multi-user Streamlit app.

Each user manages their own referral partner list, runs scans,
and sees isolated prospect results. Pipedrive CRM is shared.

Usage:
    streamlit run dashboard.py
"""

import json
from pathlib import Path
from datetime import datetime, timezone

import streamlit as st
import pandas as pd

import config

USERS_DIR = config.DATA_DIR / "users"

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

TIER_ORDER = ["tier1_whale", "tier2_agency", "tier3_affiliate", "tier4_seller", "untiered"]

OUTREACH_STATUSES = [
    "Not Contacted",
    "DM Sent",
    "Responded",
    "Meeting Booked",
    "Converted",
    "Not Interested",
]


# ── User management ─────────────────────────────────────────────────

def get_user_list() -> list[str]:
    USERS_DIR.mkdir(parents=True, exist_ok=True)
    return sorted([
        d.name for d in USERS_DIR.iterdir()
        if d.is_dir() and not d.name.startswith(".")
    ])


def get_user_dir(username: str) -> Path:
    user_dir = USERS_DIR / username
    user_dir.mkdir(parents=True, exist_ok=True)
    return user_dir


def load_user_partners(user_dir: Path) -> list[str]:
    partners_file = user_dir / "partners.json"
    if partners_file.exists():
        try:
            return json.loads(partners_file.read_text())
        except (json.JSONDecodeError, IOError):
            pass
    return []


def save_user_partners(user_dir: Path, partners: list[str]):
    (user_dir / "partners.json").write_text(json.dumps(partners, indent=2))


def load_user_prospects(user_dir: Path) -> list[dict]:
    prospects_file = user_dir / "prospects.json"
    if prospects_file.exists():
        try:
            return json.loads(prospects_file.read_text())
        except (json.JSONDecodeError, IOError):
            pass
    return []


def load_user_outreach(user_dir: Path) -> dict:
    outreach_file = user_dir / "outreach.json"
    if outreach_file.exists():
        try:
            return json.loads(outreach_file.read_text())
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def save_user_outreach(user_dir: Path, data: dict):
    (user_dir / "outreach.json").write_text(
        json.dumps(data, indent=2, default=str, ensure_ascii=False)
    )


# ── Helpers ──────────────────────────────────────────────────────────

def format_followers(count) -> str:
    if not count or count <= 0:
        return "-"
    if count >= 1_000_000:
        return f"{count / 1_000_000:.1f}M"
    if count >= 1_000:
        return f"{count / 1_000:.1f}K"
    return str(int(count))


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

    users = get_user_list()

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
            get_user_dir(clean_name)
            st.session_state.current_user = clean_name
            st.rerun()

    if not selected_user and not new_user:
        st.title("Whop Prospect Intro Dashboard")
        st.info("Create a user in the sidebar to get started.")
        return

    current_user = st.session_state.current_user
    user_dir = get_user_dir(current_user)

    st.title(f"Whop Intro Dashboard — {current_user.title()}")

    # ── Tabs ─────────────────────────────────────────────────────────
    tabs = st.tabs([
        "Manage Partners",
        "Grouped by Partner",
        "All Prospects",
        "Overlap Matrix",
        "Outreach Tracker",
    ])

    with tabs[0]:
        _render_manage_partners(user_dir, current_user)

    # Load prospect data for remaining tabs
    prospects = load_user_prospects(user_dir)

    if not prospects:
        for tab in tabs[1:]:
            with tab:
                st.info("No prospect data yet. Go to **Manage Partners** to add partners and run a scan.")
        return

    outreach = load_user_outreach(user_dir)

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

    all_partners = sorted(set(
        p for row in prospects for p in row["followed_by_partners"]
    ))

    # ── Sidebar filters ──────────────────────────────────────────────
    st.sidebar.divider()
    st.sidebar.header("Filters")

    min_score = st.sidebar.slider("Minimum score", 0.0, 1.0, 0.55, 0.05)

    selected_tiers = st.sidebar.multiselect(
        "Tiers",
        options=TIER_ORDER,
        default=TIER_ORDER,
        format_func=lambda t: TIER_LABELS.get(t, t),
    )

    all_niches = sorted(df["niche"].unique())
    selected_niches = st.sidebar.multiselect(
        "Niches",
        options=all_niches,
        default=all_niches,
        format_func=lambda n: NICHE_LABELS.get(n, n),
    )

    min_partners = st.sidebar.slider(
        "Min partner connections", 1, int(df["num_partners_connected"].max()), 1,
    )

    selected_partners = st.sidebar.multiselect(
        "Show only these partners",
        options=all_partners,
        default=all_partners,
        format_func=lambda p: f"@{p}",
    )

    if has_pipedrive:
        st.sidebar.divider()
        st.sidebar.header("CRM Status")
        crm_options = sorted(df["crm_status"].unique())
        selected_crm = st.sidebar.multiselect(
            "CRM status", options=crm_options, default=crm_options,
        )
    else:
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
        & (df["niche"].isin(selected_niches))
        & (df["num_partners_connected"] >= min_partners)
        & (df["outreach_status"].isin(selected_outreach))
    ].copy()

    if selected_crm is not None:
        filtered = filtered[filtered["crm_status"].isin(selected_crm)]

    filtered = filtered[
        filtered["followed_by_partners"].apply(
            lambda partners: any(p in selected_partners for p in partners)
        )
    ]

    # ── Summary metrics (above tabs) ──────────────────────────────────
    if has_pipedrive:
        col1, col2, col3, col4, col5, col6 = st.columns(6)
    else:
        col1, col2, col3, col4, col5 = st.columns(5)
        col6 = None

    col1.metric("Total Prospects", len(filtered))
    col2.metric("Whales", len(filtered[filtered["tier"] == "tier1_whale"]))
    col3.metric("Agencies", len(filtered[filtered["tier"] == "tier2_agency"]))
    col4.metric("Avg Score", f"{filtered['overall_score'].mean():.2f}" if len(filtered) else "-")
    col5.metric("Not Contacted", len(filtered[filtered["outreach_status"] == "Not Contacted"]))
    if col6 is not None:
        col6.metric("In CRM", len(filtered[filtered["crm_status"] != "Not in CRM"]))

    # ── Render tabs ───────────────────────────────────────────────────
    with tabs[1]:
        _render_partner_view(filtered, selected_partners, has_pipedrive)
    with tabs[2]:
        _render_all_prospects(filtered, has_pipedrive)
    with tabs[3]:
        _render_overlap_matrix(filtered, selected_partners)
    with tabs[4]:
        _render_outreach_tracker(filtered, outreach, has_pipedrive, user_dir)


# ── Manage Partners ──────────────────────────────────────────────────

def _render_manage_partners(user_dir: Path, username: str):
    st.subheader("Your Referral Partners")
    st.caption("Add Instagram accounts of partners who can make intros for you")

    partners = load_user_partners(user_dir)

    # Current partner list
    if partners:
        st.markdown(f"**{len(partners)} partner(s) configured:**")
        for i, partner in enumerate(partners):
            col1, col2 = st.columns([4, 1])
            col1.markdown(f"[@{partner}](https://instagram.com/{partner})")
            if col2.button("Remove", key=f"remove_{i}_{partner}"):
                partners.remove(partner)
                save_user_partners(user_dir, partners)
                st.rerun()
    else:
        st.info("No partners added yet. Add Instagram accounts below.")

    st.divider()

    # Add partner (using a form so text input + button submit together)
    st.markdown("#### Add Partner")
    with st.form("add_partner_form", clear_on_submit=True):
        new_partner = st.text_input(
            "Instagram username or URL",
            placeholder="e.g. fedmkt or https://instagram.com/fedmkt",
        )
        submitted = st.form_submit_button("Add Partner", type="primary")

    if submitted and new_partner:
        clean = new_partner.strip().lstrip("@").split("?")[0].split("/")[-1].lower()
        if clean and clean not in partners:
            partners.append(clean)
            save_user_partners(user_dir, partners)
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
        _run_scan(user_dir, partners, skip_new)


def _run_scan(user_dir: Path, partners: list[str], skip_new: bool):
    """Run the prospect finder for this user's partners."""
    from whop_prospect_finder import find_prospects

    exclude = set(partners) | {"aymon_holth", "nocode.alex"}

    with st.spinner(f"Scanning followings for {len(partners)} partner(s)... This may take a few minutes."):
        try:
            prospects = find_prospects(
                partners=partners,
                skip_new=skip_new,
                output_dir=user_dir,
                cache_dir=user_dir / "partner_followings",
                exclude_usernames=exclude,
            )
            if prospects:
                st.success(f"Found {len(prospects)} prospects! Switch to the other tabs to view results.")
            else:
                st.warning("No prospects found. Try adding more partners or running without 'skip new analysis'.")
        except Exception as e:
            st.error(f"Scan failed: {e}")


# ── Views ────────────────────────────────────────────────────────────

def _crm_columns(has_pipedrive: bool) -> list[str]:
    if has_pipedrive:
        return ["CRM Status", "Deal Stage"]
    return []


def _prepare_display(df_slice: pd.DataFrame, has_pipedrive: bool) -> pd.DataFrame:
    cols = [
        "username", "full_name", "overall_score", "tier",
        "niche", "follower_count", "lead_type", "num_partners_connected",
        "outreach_status", "bio",
    ]
    if has_pipedrive:
        cols += ["crm_status", "crm_deal_stage"]

    available_cols = [c for c in cols if c in df_slice.columns]
    display = df_slice[available_cols].copy()

    display["tier"] = display["tier"].map(TIER_LABELS).fillna("?")
    display["niche"] = display["niche"].map(NICHE_LABELS).fillna("?")
    display["followers"] = display["follower_count"].apply(format_followers)
    display["score"] = display["overall_score"].apply(lambda s: f"{s:.2f}")
    display["username"] = display["username"].apply(lambda u: f"@{u}")
    display["bio"] = display["bio"].apply(
        lambda b: (b[:80] + "...") if isinstance(b, str) and len(b) > 80 else b
    )

    rename_map = {
        "username": "Prospect",
        "full_name": "Name",
        "score": "Score",
        "tier": "Tier",
        "niche": "Niche",
        "followers": "Followers",
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
            df["followed_by_partners"].apply(lambda ps: partner in ps)
        ].sort_values("overall_score", ascending=False)

        if partner_prospects.empty:
            continue

        with st.expander(
            f"@{partner} — {len(partner_prospects)} intros available",
            expanded=True,
        ):
            display = _prepare_display(partner_prospects, has_pipedrive)
            display["Also Followed By"] = partner_prospects["followed_by_partners"].apply(
                lambda ps: ", ".join(f"@{p}" for p in ps if p != partner) or "-"
            )

            show_cols = [
                "Prospect", "Name", "Score", "Tier", "Niche",
                "Followers", "Type", "Outreach",
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
        ["Score (high to low)", "Followers (high to low)", "Partner connections (high to low)"],
    )

    sort_map = {
        "Score (high to low)": ("overall_score", False),
        "Followers (high to low)": ("follower_count", False),
        "Partner connections (high to low)": ("num_partners_connected", False),
    }
    col, asc = sort_map[sort_col]
    sorted_df = df.sort_values(col, ascending=asc)

    display = _prepare_display(sorted_df, has_pipedrive)
    display["Intro Via"] = sorted_df["partner_list"]

    show_cols = [
        "Prospect", "Name", "Score", "Tier", "Niche",
        "Followers", "Type", "Outreach",
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


def _render_outreach_tracker(df: pd.DataFrame, outreach: dict, has_pipedrive: bool, user_dir: Path):
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
            key = selected_prospect.lower()
            outreach[key] = {
                "status": new_status,
                "notes": notes,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            save_user_outreach(user_dir, outreach)
            st.success(f"Updated @{selected_prospect} → {new_status}")
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
                    st.markdown(f"**Niche:** {NICHE_LABELS.get(p.get('niche', ''), '?')}")
                    st.markdown(f"**Followers:** {format_followers(p.get('follower_count', 0))}")
                with detail_col2:
                    st.markdown(f"**Outreach:** {p.get('outreach_status', 'Not Contacted')}")
                    if has_pipedrive:
                        st.markdown(f"**CRM Status:** {p.get('crm_status', 'Not in CRM')}")
                        if p.get("crm_deal_stage"):
                            st.markdown(f"**Deal Stage:** {p.get('crm_deal_stage', '-')}")
                            st.markdown(f"**Pipeline:** {p.get('crm_pipeline', '-')}")
                    st.markdown(f"**Intro via:** {p.get('partner_list', '')}")
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
        "Prospect", "Name", "Score", "Tier", "Outreach",
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
            df[df["followed_by_partners"].apply(lambda ps: p1 in ps)]["username"]
        )
        for p2 in partners:
            p2_prospects = set(
                df[df["followed_by_partners"].apply(lambda ps: p2 in ps)]["username"]
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
        ["num_partners_connected", "overall_score"], ascending=[False, False]
    )

    if multi.empty:
        st.info("No prospects are shared across multiple partners.")
        return

    display = multi[[
        "username", "full_name", "overall_score", "tier",
        "follower_count", "num_partners_connected", "partner_list",
    ]].copy()

    display["tier"] = display["tier"].map(TIER_LABELS).fillna("?")
    display["followers"] = display["follower_count"].apply(format_followers)
    display["score"] = display["overall_score"].apply(lambda s: f"{s:.2f}")
    display["username"] = display["username"].apply(lambda u: f"@{u}")

    display = display.rename(columns={
        "username": "Prospect",
        "full_name": "Name",
        "score": "Score",
        "tier": "Tier",
        "followers": "Followers",
        "num_partners_connected": "# Partners",
        "partner_list": "Followed By",
    })

    st.dataframe(
        display[[
            "Prospect", "Name", "Score", "Tier", "Followers",
            "# Partners", "Followed By",
        ]],
        use_container_width=True,
        hide_index=True,
        height=min(len(display) * 38 + 40, 600),
    )


if __name__ == "__main__":
    main()
