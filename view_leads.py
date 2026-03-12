"""
Leads Table Viewer — clean formatted tables for your lead data.

Usage:
    python view_leads.py                  # Show top 50 leads
    python view_leads.py --all            # Show ALL leads
    python view_leads.py --tier tier1_whale
    python view_leads.py --niche marketing
    python view_leads.py --min-score 0.90
    python view_leads.py --top 100
    python view_leads.py --search "hormozi"
"""

import argparse
import pandas as pd


TIER_LABELS = {
    "tier1_whale": "WHALE",
    "tier2_agency": "AGENCY",
    "tier3_affiliate": "AFFILIATE",
    "tier4_seller": "SELLER",
}

NICHE_LABELS = {
    "business_coaching": "BIZ",
    "marketing": "MKT",
    "financial_education": "FIN",
    "personal_development": "DEV",
    "health_fitness": "FIT",
    "education": "EDU",
    "other": "OTH",
    "unknown": "???",
}


def fmt_followers(n):
    if pd.isna(n) or n <= 0:
        return "-"
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(int(n))


def print_table(df: pd.DataFrame, title: str):
    header = (
        f"{'#':>4}  {'Username':<24} {'Name':<22} {'Score':>5}  "
        f"{'Tier':<10} {'Niche':<5} {'Followers':>9}  {'V':>1}  {'S':>1}"
    )
    sep = "-" * len(header)

    print(f"\n  {title}")
    print(f"  {sep}")
    print(f"  {header}")
    print(f"  {sep}")

    for i, (_, row) in enumerate(df.iterrows()):
        username = f"@{row['username']}"
        name = str(row.get("full_name", ""))[:22] if pd.notna(row.get("full_name")) else ""
        score = row.get("overall_score", 0)
        tier = TIER_LABELS.get(str(row.get("tier", "")), str(row.get("tier", "?")))
        niche = NICHE_LABELS.get(str(row.get("niche", "")), str(row.get("niche", "?"))[:5])
        followers = fmt_followers(row.get("follower_count", 0))
        verified = "Y" if row.get("is_verified") else ""
        seeds = str(int(row.get("appearance_count", 1)))

        line = (
            f"{i+1:>4}  {username:<24} {name:<22} {score:>5.2f}  "
            f"{tier:<10} {niche:<5} {followers:>9}  {verified:>1}  {seeds:>1}"
        )
        print(f"  {line}")

    print(f"  {sep}")


def main():
    parser = argparse.ArgumentParser(description="View leads in a formatted table")
    parser.add_argument("--all", action="store_true", help="Show all leads")
    parser.add_argument("--top", type=int, default=50, help="Number of leads to show (default: 50)")
    parser.add_argument("--tier", type=str, help="Filter by tier (tier1_whale, tier2_agency, etc.)")
    parser.add_argument("--niche", type=str, help="Filter by niche (marketing, business_coaching, etc.)")
    parser.add_argument("--min-score", type=float, default=0, help="Minimum score filter")
    parser.add_argument("--classification", type=str, help="Filter: high_value, potential_value")
    parser.add_argument("--search", type=str, help="Search username, name, or bio")
    parser.add_argument("--verified", action="store_true", help="Only verified accounts")
    parser.add_argument("--multi-seed", action="store_true", help="Only accounts from 2+ seeds")
    parser.add_argument("--sort", type=str, default="overall_score", help="Sort column")
    parser.add_argument("--asc", action="store_true", help="Sort ascending")
    parser.add_argument("--csv", type=str, default="data/leads.csv", help="Path to CSV")

    args = parser.parse_args()

    try:
        df = pd.read_csv(args.csv)
    except FileNotFoundError:
        print(f"File not found: {args.csv}")
        return

    total = len(df)

    # Filters
    if args.tier:
        df = df[df["tier"] == args.tier]
    if args.niche:
        df = df[df["niche"].str.contains(args.niche, case=False, na=False)]
    if args.min_score > 0:
        df = df[df["overall_score"] >= args.min_score]
    if args.classification:
        df = df[df["classification"] == args.classification]
    if args.search:
        mask = (
            df["username"].str.contains(args.search, case=False, na=False)
            | df["full_name"].str.contains(args.search, case=False, na=False)
            | df["bio"].str.contains(args.search, case=False, na=False)
        )
        df = df[mask]
    if args.verified:
        df = df[df["is_verified"] == True]
    if args.multi_seed:
        df = df[df["appearance_count"] >= 2]

    # Sort
    if args.sort in df.columns:
        df = df.sort_values(args.sort, ascending=args.asc)
    else:
        df = df.sort_values("overall_score", ascending=False)

    filtered = len(df)

    # Limit
    if not args.all:
        df = df.head(args.top)

    # Summary
    print(f"\n  LEADS SUMMARY: {total} total | {filtered} matched | showing {len(df)}")

    # Tier summary
    tier_counts = df["tier"].value_counts()
    niche_counts = df["niche"].value_counts()
    print(f"  Tiers:  {' | '.join(f'{TIER_LABELS.get(t,t)}: {c}' for t, c in tier_counts.items())}")
    print(f"  Niches: {' | '.join(f'{NICHE_LABELS.get(n,n)}: {c}' for n, c in niche_counts.head(6).items())}")

    # Build title
    title_parts = [f"Top {len(df)} Leads" if not args.all else f"All {len(df)} Leads"]
    if args.tier:
        title_parts.append(f"tier={args.tier}")
    if args.niche:
        title_parts.append(f"niche={args.niche}")
    if args.min_score > 0:
        title_parts.append(f"score>={args.min_score}")
    if args.search:
        title_parts.append(f'search="{args.search}"')

    print_table(df, " | ".join(title_parts))

    print(f"\n  Columns: # | Username | Name | Score | Tier | Niche | Followers | V=Verified | S=Seeds")
    print(f"  Niches:  BIZ=Business Coaching | MKT=Marketing | FIN=Financial Ed | DEV=Personal Dev | FIT=Health")
    print(f"  Filters: --tier --niche --min-score --search --verified --multi-seed --classification --all")
    print()


if __name__ == "__main__":
    main()
