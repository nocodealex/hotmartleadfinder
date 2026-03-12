#!/usr/bin/env python3
"""
Hotmart Lead Finder V2 — CLI entry point.

Usage:
  python main.py add-seed <username> [<username> ...]   Add seed accounts
  python main.py discover-seeds                          Auto-discover seeds
  python main.py run                                     Run full pipeline (graph mode)
  python main.py run --legacy                            Run legacy pipeline
  python main.py graph-stats                             Show network graph stats
  python main.py calibrate [--count N]                   Calibrate on N accounts
  python main.py leads [--top N]                         Show top leads
  python main.py leads --tier <tier>                     Show leads by tier
  python main.py export                                  Export leads to CSV
  python main.py status                                  Show pipeline stats
  python main.py test-api                                Test Instagram API connection
  python main.py monitor                                 Run continuous monitoring daemon
  python main.py outreach generate                       Generate DM drafts for leads
  python main.py outreach status                         Show outreach funnel stats
  python main.py outreach update <user> <status>         Update outreach status
  python main.py outreach export                         Export outreach CSV
  python main.py whop-prospects                          Find Whop prospects from partner followings
  python main.py whop-prospects --skip-new               Only match existing leads (no Claude)
  python main.py hotmart-scrape                          Scrape Hotmart marketplace
"""

import sys
import logging
import argparse
import time
import json
from datetime import datetime, timezone

from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

import config
from pipeline import Pipeline
from storage import Storage

console = Console()


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )


def check_keys():
    """Verify that required API keys are set."""
    missing = []
    if not config.RAPIDAPI_KEY or config.RAPIDAPI_KEY == "your_rapidapi_key_here":
        missing.append("RAPIDAPI_KEY")
    if not config.ANTHROPIC_API_KEY or config.ANTHROPIC_API_KEY == "your_anthropic_api_key_here":
        missing.append("ANTHROPIC_API_KEY")
    if not config.APIFY_API_TOKEN or config.APIFY_API_TOKEN == "your_apify_api_token_here":
        missing.append("APIFY_API_TOKEN")
    if missing:
        console.print(f"[bold red]Missing API keys: {', '.join(missing)}[/]")
        console.print(
            "\n[bold]Setup instructions:[/]\n"
            "  1. Copy .env.example to .env:\n"
            "       cp .env.example .env\n"
            "  2. Add your RapidAPI key:\n"
            "       Sign up at https://rapidapi.com\n"
            "       Subscribe to the Instagram API\n"
            "       Copy your key from the dashboard\n"
            "  3. Add your Anthropic API key:\n"
            "       Sign up at https://console.anthropic.com\n"
            "       Create an API key\n"
            "  4. Add your Apify API token:\n"
            "       Sign up at https://apify.com\n"
            "       Go to Settings > Integrations\n"
            "       Copy your API token\n"
        )
        return False
    return True


# ── Seed Management ──────────────────────────────────────────────────

def cmd_add_seed(args):
    storage = Storage()
    for username in args.usernames:
        username = username.strip().lstrip("@").split("?")[0].split("/")[-1]
        if storage.add_seed(username):
            console.print(f"  [green]Added seed: @{username}[/]")
        else:
            console.print(f"  [dim]Already exists: @{username}[/]")
    console.print(f"\nCurrent seeds: {', '.join('@' + s for s in storage.get_seeds())}")


def cmd_discover_seeds(args):
    if not check_keys():
        return
    from seed_discovery import SeedDiscovery

    console.print("\n[bold cyan]Discovering new seed accounts...[/]\n")
    discovery = SeedDiscovery()
    results = discovery.discover_all()

    if results["known_seeds_added"]:
        console.print(f"  [green]Added {len(results['known_seeds_added'])} known ecosystem seeds[/]")
        for s in results["known_seeds_added"]:
            console.print(f"    @{s}")

    if results["leads_promoted"]:
        console.print(f"  [green]Promoted {len(results['leads_promoted'])} high-scoring leads to seeds[/]")
        for s in results["leads_promoted"]:
            console.print(f"    @{s}")

    if results["search_results"]:
        console.print(f"  [green]Found {len(results['search_results'])} seeds via Instagram search[/]")
        for s in results["search_results"]:
            console.print(f"    @{s}")

    storage = Storage()
    seeds = storage.get_seeds()
    console.print(f"\n  [bold]Total seeds: {len(seeds)}[/]")


# ── Pipeline ─────────────────────────────────────────────────────────

def cmd_run(args):
    if not check_keys():
        return
    pipeline = Pipeline()
    if args.legacy:
        pipeline.run_legacy()
    else:
        pipeline.run()


def cmd_calibrate(args):
    if not check_keys():
        return
    pipeline = Pipeline()
    pipeline.calibrate(count=args.count)


# ── Network Graph ────────────────────────────────────────────────────

def cmd_graph_stats(args):
    from network_graph import NetworkGraph

    graph = NetworkGraph()
    stats = graph.stats()

    console.print("\n[bold]Network Graph Statistics[/]\n")
    console.print(f"  Total accounts in graph: {stats['total_accounts']:,}")
    console.print(f"  Accounts in 2+ seeds:    {stats['accounts_2plus']:,}")
    console.print(f"  Accounts in 3+ seeds:    {stats['accounts_3plus']:,}")
    console.print(f"  Accounts in 5+ seeds:    {stats['accounts_5plus']:,}")

    dist = stats.get("appearance_distribution", {})
    if dist:
        console.print("\n  [bold]Appearance distribution:[/]")
        for count, num in sorted(dist.items(), key=lambda x: int(x[0])):
            bar = "█" * min(int(num / max(dist.values()) * 30), 30)
            console.print(f"    {count}× appearances: {num:>5} accounts {bar}")


# ── Leads ────────────────────────────────────────────────────────────

def cmd_leads(args):
    pipeline = Pipeline()
    if args.tier:
        storage = Storage()
        leads = storage.get_leads_by_tier(args.tier)
        if not leads:
            console.print(f"[dim]No leads found for tier: {args.tier}[/]")
            return
        console.print(f"\n[bold]Leads for tier: {args.tier}[/]")

        table = Table()
        table.add_column("#", style="dim", width=3)
        table.add_column("Username", style="bold cyan")
        table.add_column("Score", justify="right")
        table.add_column("Type", style="green")
        table.add_column("Bio", max_width=40)

        for i, lead in enumerate(leads[:args.top], 1):
            score = lead["overall_score"]
            score_style = "bold green" if score >= 0.7 else "yellow"
            bio = lead.get("bio", "")
            bio_display = (bio[:40] + "...") if len(bio) > 40 else bio
            table.add_row(
                str(i),
                f"@{lead['username']}",
                f"[{score_style}]{score:.2f}[/]",
                lead.get("lead_type", ""),
                bio_display,
            )
        console.print(table)
    else:
        pipeline.show_leads(top_n=args.top)


def cmd_export(args):
    storage = Storage()
    path = storage.export_csv()
    console.print(f"[green]Exported to {path}[/]")


def cmd_status(args):
    storage = Storage()
    stats = storage.stats()

    console.print("\n[bold]Hotmart Lead Finder V2 — Status[/]\n")
    console.print(f"  Seeds:              {stats['seed_accounts']}")
    seeds = storage.get_seeds()
    if seeds:
        console.print(f"                      {', '.join('@' + s for s in seeds)}")
    console.print(f"  Processed accounts: {stats['total_processed']}")
    console.print(f"  Total leads:        {stats['total_leads']}")
    console.print(f"    High value:       {stats['high_value']}")
    console.print(f"    Potential value:   {stats['potential_value']}")
    console.print(f"    Boosted:          {stats['boosted_leads']}")
    console.print(f"  Avg lead score:     {stats['avg_score']:.2f}")

    tiers = stats.get("tiers", {})
    if tiers and any(v > 0 for v in tiers.values()):
        console.print("\n  [bold]Lead Tiers:[/]")
        tier_labels = {
            "tier1_whale": "Tier 1 (Whales)",
            "tier2_agency": "Tier 2 (Agencies)",
            "tier3_affiliate": "Tier 3 (Affiliates)",
            "tier4_seller": "Tier 4 (Sellers)",
            "untiered": "Untiered",
        }
        for tier_val, label in tier_labels.items():
            count = tiers.get(tier_val, 0)
            if count > 0:
                console.print(f"    {label}: {count}")
    console.print()


# ── API Test ─────────────────────────────────────────────────────────

def cmd_test_api(args):
    if not check_keys():
        return

    from instagram_client import InstagramClient

    console.print("\n[bold]Testing Instagram API connection ...[/]\n")
    client = InstagramClient()
    result = client.test_connection()

    if result["connected"]:
        console.print("[bold green]Connected![/]\n")
    else:
        console.print("[bold red]Connection failed[/]\n")

    for endpoint, working in result["endpoints_working"].items():
        status = "[green]OK[/]" if working else "[red]FAIL[/]"
        console.print(f"  {endpoint}: {status}")

    if result.get("sample_profile"):
        p = result["sample_profile"]
        console.print(f"\n  Sample profile: @{p['username']}")
        console.print(f"  Bio: {p['bio']}")
        console.print(f"  Followers: {p['followers']:,}")

    if result["errors"]:
        console.print("\n[yellow]Errors:[/]")
        for err in result["errors"]:
            console.print(f"  - {err}")

    console.print(f"\n  API calls made: {result['api_calls_made']}")

    if not result["connected"]:
        console.print(
            "\n[bold yellow]Troubleshooting:[/]\n"
            "  1. Check your RAPIDAPI_KEY in .env\n"
            "  2. Make sure you've subscribed to the API on RapidAPI\n"
            "  3. The API endpoint paths in config.py may need updating.\n"
        )


# ── Continuous Monitoring ────────────────────────────────────────────

def cmd_monitor(args):
    """Run the pipeline in a continuous loop with alerts."""
    if not check_keys():
        return

    interval = args.interval or config.MONITOR_INTERVAL_HOURS
    console.print(
        f"\n[bold cyan]Starting continuous monitoring[/]\n"
        f"  Interval: every {interval} hours\n"
        f"  Press Ctrl+C to stop\n"
    )

    try:
        while True:
            run_start = datetime.now(timezone.utc).isoformat()
            console.print(f"\n[bold]{'='*50}[/]")
            console.print(f"[bold]Monitor run at {run_start}[/]")
            console.print(f"[bold]{'='*50}[/]")

            try:
                pipeline = Pipeline()
                pipeline.run()

                # Check for new high-value leads
                storage = Storage()
                new_leads = storage.get_new_leads_since(run_start)
                high_value_new = [
                    l for l in new_leads
                    if l.get("classification") == "high_value"
                ]

                if high_value_new:
                    console.print(
                        f"\n[bold green]ALERT: {len(high_value_new)} new "
                        f"high-value leads found![/]"
                    )
                    for lead in high_value_new:
                        console.print(
                            f"  @{lead['username']} — "
                            f"score: {lead['overall_score']:.2f} — "
                            f"tier: {lead.get('tier', '?')} — "
                            f"type: {lead.get('lead_type', '?')}"
                        )

                    # Send Slack alert if configured
                    _send_slack_alert(high_value_new)

                console.print(
                    f"\n[dim]Next run in {interval} hours. "
                    f"Press Ctrl+C to stop.[/]"
                )

            except Exception as e:
                console.print(f"[red]Monitor run error: {e}[/]")
                logging.getLogger(__name__).exception("Monitor error")

            time.sleep(interval * 3600)

    except KeyboardInterrupt:
        console.print("\n[bold]Monitoring stopped.[/]")


def _send_slack_alert(leads: list[dict]):
    """Send a Slack webhook notification for new high-value leads."""
    webhook_url = config.SLACK_WEBHOOK_URL
    if not webhook_url:
        return

    import requests

    text_parts = ["*New High-Value Leads Found*\n"]
    for lead in leads[:10]:
        text_parts.append(
            f"• *@{lead['username']}* — "
            f"Score: {lead['overall_score']:.2f} | "
            f"Tier: {lead.get('tier', '?')} | "
            f"Type: {lead.get('lead_type', '?')}\n"
            f"  Bio: {lead.get('bio', '')[:100]}\n"
            f"  {lead.get('instagram_url', '')}"
        )

    try:
        requests.post(
            webhook_url,
            json={"text": "\n".join(text_parts)},
            timeout=10,
        )
    except Exception as e:
        logging.getLogger(__name__).warning(f"Slack alert failed: {e}")


# ── Outreach ─────────────────────────────────────────────────────────

def cmd_outreach(args):
    if not check_keys():
        return

    from outreach import OutreachManager, ALL_STATUSES

    manager = OutreachManager()

    if args.outreach_command == "generate":
        storage = Storage()
        leads = storage.get_all_leads()

        if args.tier:
            leads = [l for l in leads if l.get("tier") == args.tier]

        if not leads:
            console.print("[dim]No leads to generate DMs for.[/]")
            return

        count_limit = args.count or len(leads)
        leads = leads[:count_limit]

        # Load known Whop sellers for social proof in DMs
        whop_sellers = storage.get_whop_sellers()
        if whop_sellers:
            console.print(f"  Using {len(whop_sellers)} known Whop sellers for social proof\n")

        console.print(f"[bold cyan]Generating DM drafts for {len(leads)} leads...[/]\n")
        generated = manager.generate_drafts_for_leads(leads, whop_sellers=whop_sellers)
        console.print(f"[green]Generated {generated} DM drafts[/]")

        # Show a sample
        all_outreach = manager.get_all()
        for entry in all_outreach[:3]:
            console.print(f"\n  [bold]@{entry['username']}[/]")
            console.print(f"  [dim]{entry.get('dm_draft', '(none)')}[/]")

        path = manager.export_csv()
        console.print(f"\nExported to {path}")

    elif args.outreach_command == "status":
        stats = manager.funnel_stats()
        console.print("\n[bold]Outreach Funnel[/]\n")

        table = Table(title="Outreach Pipeline")
        table.add_column("Status", style="bold")
        table.add_column("Count", justify="right")

        status_labels = {
            "not_contacted": "Not Contacted",
            "dm_drafted": "DM Drafted",
            "dm_sent": "DM Sent",
            "responded": "Responded",
            "meeting_booked": "Meeting Booked",
            "converted": "Converted",
            "not_interested": "Not Interested",
        }

        for status_key, label in status_labels.items():
            count = stats.get(status_key, 0)
            style = "green" if status_key in ("converted", "meeting_booked") else ""
            table.add_row(label, f"[{style}]{count}[/]" if style else str(count))

        table.add_section()
        table.add_row("Total", str(stats.get("total", 0)), style="bold")
        console.print(table)

        # Show follow-ups due
        due = manager.get_follow_ups_due()
        if due:
            console.print(f"\n[bold yellow]Follow-ups due: {len(due)}[/]")
            for entry in due:
                console.print(f"  @{entry['username']} — {entry.get('status', '?')}")

    elif args.outreach_command == "update":
        if not args.username or not args.status:
            console.print("[red]Usage: outreach update <username> <status>[/]")
            console.print(f"  Valid statuses: {', '.join(ALL_STATUSES)}")
            return
        manager.update_status(args.username, args.status, args.notes or "")
        console.print(f"[green]Updated @{args.username} → {args.status}[/]")

        if args.followup:
            manager.set_follow_up(args.username, days_from_now=args.followup)
            console.print(f"  Follow-up set in {args.followup} days")

    elif args.outreach_command == "export":
        path = manager.export_csv()
        console.print(f"[green]Exported outreach data to {path}[/]")

    else:
        console.print("[red]Unknown outreach command. Use: generate, status, update, export[/]")


# ── Whop Sellers ─────────────────────────────────────────────────────

def cmd_whop_sellers(args):
    storage = Storage()

    if args.whop_command == "add":
        for username in args.usernames:
            username = username.strip().lstrip("@").split("?")[0].split("/")[-1]
            if storage.add_whop_seller(username):
                console.print(f"  [green]Added: @{username}[/]")
            else:
                console.print(f"  [dim]Already exists: @{username}[/]")

    elif args.whop_command == "list":
        sellers = storage.get_whop_sellers()
        if sellers:
            console.print(f"\n[bold]Known Whop sellers ({len(sellers)}):[/]")
            for s in sellers:
                console.print(f"  @{s}")
        else:
            console.print("[dim]No known Whop sellers yet.[/]")
            console.print("Add them with: python main.py whop-sellers add @username1 @username2")

    elif args.whop_command == "remove":
        for username in args.usernames:
            username = username.strip().lstrip("@").split("?")[0].split("/")[-1]
            if storage.remove_whop_seller(username):
                console.print(f"  [green]Removed: @{username}[/]")
            else:
                console.print(f"  [dim]Not found: @{username}[/]")

    else:
        console.print("[red]Usage: whop-sellers add|list|remove[/]")

    sellers = storage.get_whop_sellers()
    console.print(f"\nTotal known Whop sellers: {len(sellers)}")


# ── Whop Prospects ───────────────────────────────────────────────────

def cmd_whop_prospects(args):
    if not check_keys():
        return

    from whop_prospect_finder import find_prospects

    find_prospects(
        partners=args.partners,
        skip_new=args.skip_new,
        min_score=args.min_score,
    )


# ── Hotmart Scrape ───────────────────────────────────────────────────

def cmd_hotmart_scrape(args):
    from hotmart_scraper import discover_seeds_from_hotmart

    console.print("\n[bold cyan]Scraping Hotmart marketplace for seeds...[/]\n")
    added = discover_seeds_from_hotmart()
    if added:
        console.print(f"\n[green]Added {len(added)} new seeds from Hotmart:[/]")
        for s in added:
            console.print(f"  @{s}")
    else:
        console.print("[dim]No new seeds found from Hotmart marketplace.[/]")

    storage = Storage()
    console.print(f"\nTotal seeds: {len(storage.get_seeds())}")


# ── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Hotmart Lead Finder V2 — find referral partners on Instagram",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="verbose logging")
    subparsers = parser.add_subparsers(dest="command", help="command to run")

    # add-seed
    p_seed = subparsers.add_parser("add-seed", help="Add seed accounts")
    p_seed.add_argument("usernames", nargs="+", help="Instagram usernames or URLs")

    # discover-seeds
    subparsers.add_parser("discover-seeds", help="Auto-discover seed accounts")

    # run
    p_run = subparsers.add_parser("run", help="Run the full pipeline")
    p_run.add_argument("--legacy", action="store_true", help="Use legacy (non-graph) mode")

    # graph-stats
    subparsers.add_parser("graph-stats", help="Show network graph statistics")

    # calibrate
    p_cal = subparsers.add_parser("calibrate", help="Run calibration on a sample")
    p_cal.add_argument("--count", type=int, default=20, help="accounts to analyse (default: 20)")

    # leads
    p_leads = subparsers.add_parser("leads", help="Show top leads")
    p_leads.add_argument("--top", type=int, default=20, help="number to show (default: 20)")
    p_leads.add_argument("--tier", type=str, default="", help="filter by tier")

    # export
    subparsers.add_parser("export", help="Export leads to CSV")

    # status
    subparsers.add_parser("status", help="Show pipeline status")

    # test-api
    subparsers.add_parser("test-api", help="Test Instagram API connection")

    # monitor
    p_monitor = subparsers.add_parser("monitor", help="Run continuous monitoring daemon")
    p_monitor.add_argument(
        "--interval", type=float, default=0,
        help=f"hours between runs (default: {config.MONITOR_INTERVAL_HOURS})"
    )

    # outreach
    p_outreach = subparsers.add_parser("outreach", help="Outreach pipeline")
    outreach_sub = p_outreach.add_subparsers(dest="outreach_command")

    p_out_gen = outreach_sub.add_parser("generate", help="Generate DM drafts")
    p_out_gen.add_argument("--tier", type=str, default="", help="filter by tier")
    p_out_gen.add_argument("--count", type=int, default=0, help="max leads to generate for")

    outreach_sub.add_parser("status", help="Show outreach funnel")

    p_out_update = outreach_sub.add_parser("update", help="Update outreach status")
    p_out_update.add_argument("username", help="Instagram username")
    p_out_update.add_argument("status", help="New status")
    p_out_update.add_argument("--notes", type=str, default="", help="Notes")
    p_out_update.add_argument("--followup", type=int, default=0, help="Set follow-up in N days")

    outreach_sub.add_parser("export", help="Export outreach data")

    # whop-prospects
    p_prospects = subparsers.add_parser(
        "whop-prospects", help="Find Whop prospects from referral partner followings"
    )
    p_prospects.add_argument(
        "--skip-new", action="store_true",
        help="Only match against existing leads (no new Claude analysis)"
    )
    p_prospects.add_argument(
        "--min-score", type=float, default=0.0,
        help=f"Minimum score threshold (default: {config.LEAD_SCORE_THRESHOLD})"
    )
    p_prospects.add_argument(
        "--partners", nargs="+", default=None,
        help="Limit to specific partners (e.g. --partners fedmkt yanvispo)"
    )

    # hotmart-scrape
    subparsers.add_parser("hotmart-scrape", help="Scrape Hotmart marketplace for seeds")

    # whop-sellers
    p_whop = subparsers.add_parser("whop-sellers", help="Manage known Whop sellers")
    whop_sub = p_whop.add_subparsers(dest="whop_command")
    p_whop_add = whop_sub.add_parser("add", help="Add known Whop sellers")
    p_whop_add.add_argument("usernames", nargs="+", help="Instagram usernames")
    whop_sub.add_parser("list", help="List known Whop sellers")
    p_whop_rm = whop_sub.add_parser("remove", help="Remove a Whop seller")
    p_whop_rm.add_argument("usernames", nargs="+", help="Instagram usernames")

    args = parser.parse_args()
    setup_logging(args.verbose)

    commands = {
        "add-seed": cmd_add_seed,
        "discover-seeds": cmd_discover_seeds,
        "run": cmd_run,
        "calibrate": cmd_calibrate,
        "graph-stats": cmd_graph_stats,
        "leads": cmd_leads,
        "export": cmd_export,
        "status": cmd_status,
        "test-api": cmd_test_api,
        "monitor": cmd_monitor,
        "outreach": cmd_outreach,
        "whop-prospects": cmd_whop_prospects,
        "hotmart-scrape": cmd_hotmart_scrape,
        "whop-sellers": cmd_whop_sellers,
    }

    if args.command in commands:
        commands[args.command](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
