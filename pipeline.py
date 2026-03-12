"""
Pipeline orchestration for Hotmart Lead Finder V2.

Two modes of operation:

  1. GRAPH MODE (default): Build a follow-graph across all seeds first,
     then prioritize analysis by cross-seed appearance count.
     Only Claude-analyze accounts that appear across 2+ seeds.

  2. LEGACY MODE: Process seeds one at a time (original behavior).

Both modes use the keyword pre-filter to skip obvious non-leads
before making Claude API calls (~70% cost reduction).
"""

import logging
import time
from datetime import datetime, timezone

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.table import Table

import config
from models import InstagramProfile, Lead, LeadClassification, LeadTier
from instagram_client import InstagramClient, InstagramAPIError
from lead_analyzer import LeadAnalyzer, CreditExhaustedError
from network_graph import NetworkGraph
from prefilter import prefilter_bio
from storage import Storage

logger = logging.getLogger(__name__)
console = Console()


class Pipeline:

    def __init__(self):
        self.ig = InstagramClient()
        self.analyzer = LeadAnalyzer()
        self.storage = Storage()
        self.graph = NetworkGraph(ig_client=self.ig, storage=self.storage)
        self._prefilter_stats = {"auto_pass": 0, "pass": 0, "skip": 0}

    # ── Main entry points ────────────────────────────────────────────

    def run(self, seeds: list[str] | None = None, max_depth: int | None = None):
        """Run the full pipeline — graph mode."""
        seeds = seeds or self.storage.get_seeds()
        max_depth = max_depth if max_depth is not None else config.MAX_CRAWL_DEPTH

        if not seeds:
            console.print(
                "[bold red]No seed accounts configured.[/]\n"
                "Add seeds with: python main.py add-seed <username>"
            )
            return

        console.print(f"\n[bold green]Starting pipeline V2 with {len(seeds)} seed(s)[/]")
        console.print(f"Seeds: {', '.join('@' + s for s in seeds)}\n")

        # Phase 1: Build the network graph
        console.print("[bold cyan]Phase 1: Building network graph[/]")
        graph_stats = self.graph.build_for_all_seeds(seeds)
        console.print(
            f"  Graph built: {graph_stats['unique_accounts']} unique accounts, "
            f"{graph_stats.get('multi_seed_accounts', 0)} appear in 2+ seeds\n"
        )

        # Phase 2: Analyze accounts prioritized by appearance count
        console.print("[bold cyan]Phase 2: Analyzing accounts by priority[/]")
        self._analyze_graph_accounts(seeds, max_depth)

        # Phase 3: Depth crawl high-scoring leads
        self._depth_crawl(max_depth)

        # Export results
        csv_path = self.storage.export_csv()
        stats = self.storage.stats()

        console.print("\n[bold green]Pipeline V2 complete![/]")
        self._print_stats(stats)
        self._print_prefilter_stats()
        console.print(f"\nLeads exported to: [bold]{csv_path}[/]")

    def run_legacy(self, seeds: list[str] | None = None, max_depth: int | None = None):
        """Run the original pipeline (seed-by-seed, no graph)."""
        seeds = seeds or self.storage.get_seeds()
        max_depth = max_depth if max_depth is not None else config.MAX_CRAWL_DEPTH

        if not seeds:
            console.print("[bold red]No seed accounts configured.[/]")
            return

        console.print(f"\n[bold green]Starting legacy pipeline with {len(seeds)} seed(s)[/]")
        self._run_depth(seeds, depth=0, max_depth=max_depth)

        csv_path = self.storage.export_csv()
        stats = self.storage.stats()

        console.print("\n[bold green]Pipeline complete![/]")
        self._print_stats(stats)
        self._print_prefilter_stats()
        console.print(f"\nLeads exported to: [bold]{csv_path}[/]")

    def calibrate(self, seeds: list[str] | None = None, count: int = 20):
        """Run on a small sample for calibration."""
        seeds = seeds or self.storage.get_seeds()
        if not seeds:
            console.print("[bold red]No seed accounts. Add with: python main.py add-seed <username>[/]")
            return

        console.print(f"\n[bold cyan]Calibration mode[/] — analysing up to {count} accounts")
        console.print(f"Seed: @{seeds[0]}\n")

        seed = seeds[0]
        console.print(f"Fetching following list for @{seed} ...")
        try:
            following_raw = self.ig.get_all_following(seed, limit=count * 3)
        except Exception as e:
            console.print(f"[red]Error fetching following: {e}[/]")
            return
        console.print(f"Got {len(following_raw)} accounts\n")

        analysed = 0
        for i, user_data in enumerate(following_raw):
            if analysed >= count:
                break

            username = user_data.get("username", "")
            if not username:
                continue

            console.rule(f"[bold]Account {analysed + 1}/{count}: @{username}[/]")

            try:
                profile = self.ig.get_profile(username)
                if not profile:
                    console.print("  [dim]Could not fetch profile — skipping[/]")
                    continue

                if profile.is_private:
                    console.print("  [dim]Private account — skipping[/]")
                    continue

                # Pre-filter check
                pf_result = prefilter_bio(username, profile.bio, profile.follower_count)
                console.print(f"  [bold]Pre-filter:[/] {pf_result}")

                if pf_result == "skip":
                    console.print(f"  [dim]Skipped by pre-filter[/]")
                    console.print(f"  [bold]Bio:[/] {profile.bio}")
                    console.print()
                    analysed += 1
                    continue

                # Show profile info
                console.print(f"  [bold]Name:[/] {profile.full_name}")
                console.print(f"  [bold]Bio:[/] {profile.bio}")
                console.print(f"  [bold]Link:[/] {profile.bio_link or '(none)'}")
                console.print(
                    f"  [bold]Followers:[/] {profile.follower_count:,}  "
                    f"[bold]Following:[/] {profile.following_count:,}"
                )

                # Run full analysis
                analysis = self._analyse_account(profile, seed, depth=0)

                # Show results
                console.print(f"\n  [bold]Overall Score:[/] {analysis.overall_score:.2f}")
                console.print(f"  [bold]Classification:[/] {analysis.classification.value}")
                console.print(f"  [bold]Tier:[/] {analysis.tier.value}")
                console.print(f"  [bold]Lead Type:[/] {analysis.lead_type}")
                console.print(f"  [bold]Summary:[/] {analysis.summary}")

                if analysis.bio_result:
                    console.print(
                        f"\n  [cyan]Bio Analysis ({analysis.bio_result.score:.2f}):[/] "
                        f"{analysis.bio_result.reasoning}"
                    )
                    signals = analysis.bio_result.details.get("key_signals", [])
                    if signals:
                        console.print(f"    Signals: {', '.join(signals)}")

                if analysis.website_result:
                    console.print(
                        f"  [cyan]Website ({analysis.website_result.score:.2f}):[/] "
                        f"{analysis.website_result.reasoning}"
                    )

                if analysis.caption_result:
                    console.print(
                        f"  [cyan]Captions ({analysis.caption_result.score:.2f}):[/] "
                        f"{analysis.caption_result.reasoning}"
                    )

                if analysis.event_result:
                    console.print(
                        f"  [cyan]Events ({analysis.event_result.score:.2f}):[/] "
                        f"{analysis.event_result.reasoning}"
                    )

                console.print()
                analysed += 1

            except InstagramAPIError as e:
                console.print(f"  [red]API error: {e}[/]")
            except Exception as e:
                console.print(f"  [red]Error: {e}[/]")
                logger.exception("Calibration error")

        console.print(f"\n[bold green]Calibration complete — analysed {analysed} accounts[/]")
        self._print_prefilter_stats()
        csv_path = self.storage.export_csv()
        console.print(f"Results exported to: {csv_path}")

    # ── Graph-based analysis ─────────────────────────────────────────

    def _analyze_graph_accounts(self, seeds: list[str], max_depth: int):
        """Analyze accounts from the graph, prioritized by appearance count."""
        # Get accounts sorted by appearance count
        # First pass: analyze all accounts with 2+ appearances
        prioritized = self.graph.get_prioritized_accounts(
            min_appearances=config.GRAPH_MIN_APPEARANCES,
            exclude_processed=True,
        )

        if prioritized:
            console.print(
                f"  [bold]High-priority accounts (2+ seed appearances):[/] "
                f"{len(prioritized)}"
            )
            self._analyze_account_batch(prioritized, seeds, max_depth)

        # Second pass: analyze remaining 1-appearance accounts
        remaining = self.graph.get_prioritized_accounts(
            min_appearances=1,
            exclude_processed=True,
        )

        if remaining:
            console.print(
                f"\n  [bold]Remaining accounts (1 appearance):[/] "
                f"{len(remaining)}"
            )
            self._analyze_account_batch(remaining, seeds, max_depth)

    def _analyze_account_batch(
        self,
        accounts: list[tuple[str, int, list[str]]],
        seeds: list[str],
        max_depth: int,
    ):
        """Analyze a batch of accounts with progress tracking."""
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            console=console,
        ) as progress:
            task = progress.add_task("Analyzing", total=len(accounts))

            for username, appearance_count, followed_by in accounts:
                progress.update(task, description=f"@{username} (×{appearance_count})")

                # Skip known Whop sellers — no need to contact them
                if self.storage.is_whop_seller(username):
                    seed = followed_by[0] if followed_by else seeds[0]
                    self.storage.mark_processed(username, seed, 0)
                    progress.advance(task)
                    continue

                try:
                    profile = self.ig.get_profile(username)
                    if not profile or profile.is_private:
                        # Mark as processed with first seed
                        seed = followed_by[0] if followed_by else seeds[0]
                        self.storage.mark_processed(username, seed, 0)
                        progress.advance(task)
                        continue

                    # Pre-filter
                    pf_result = prefilter_bio(username, profile.bio, profile.follower_count)
                    self._prefilter_stats[pf_result] = self._prefilter_stats.get(pf_result, 0) + 1

                    if pf_result == "skip":
                        seed = followed_by[0] if followed_by else seeds[0]
                        self.storage.mark_processed(username, seed, 0)
                        progress.advance(task)
                        continue

                    # Full analysis
                    seed = followed_by[0] if followed_by else seeds[0]
                    analysis = self._analyse_account(
                        profile, seed, depth=0, appearance_override=appearance_count
                    )

                    # Save qualified leads
                    if analysis.classification in (
                        LeadClassification.HIGH_VALUE,
                        LeadClassification.POTENTIAL_VALUE,
                    ):
                        lead = Lead(
                            profile=profile,
                            analysis=analysis,
                            found_via_seeds=followed_by,
                            seed_depth=0,
                        )
                        self.storage.save_lead(lead)

                    # Mark processed with all seeds
                    for s in followed_by:
                        self.storage.mark_processed(username, s, analysis.overall_score)

                except CreditExhaustedError:
                    console.print(
                        "\n[bold red]Anthropic API credits depleted![/]\n"
                        "Top up at: https://console.anthropic.com/settings/billing\n"
                        "Then re-run: python main.py run  (it will resume where it left off)\n"
                    )
                    progress.stop()
                    return  # Save progress and exit batch
                except InstagramAPIError as e:
                    logger.warning(f"API error for @{username}: {e}")
                except Exception as e:
                    logger.warning(f"Error analysing @{username}: {e}")

                progress.advance(task)

    # ── Depth crawling ───────────────────────────────────────────────

    def _depth_crawl(self, max_depth: int):
        """Crawl the following lists of top-scoring leads."""
        if max_depth < 1:
            return

        leads = self.storage.get_all_leads()
        to_crawl = [
            l["username"] for l in leads
            if l.get("overall_score", 0) >= config.DEPTH_CRAWL_THRESHOLD
        ]

        if not to_crawl:
            return

        console.print(
            f"\n[bold yellow]Phase 3: Depth crawling {len(to_crawl)} "
            f"top leads[/]"
        )

        for username in to_crawl:
            # Check if we already have their following in the graph
            try:
                following = self.ig.get_all_following(username)
            except InstagramAPIError:
                continue

            console.print(f"  Crawling @{username}: {len(following)} accounts")

            for user_data in following:
                uname = user_data.get("username", "")
                if not uname:
                    continue

                if self.storage.is_processed(uname):
                    self.storage.increment_appearance(uname, username)
                    continue

                try:
                    profile = self.ig.get_profile(uname)
                    if not profile or profile.is_private:
                        self.storage.mark_processed(uname, username, 0)
                        continue

                    pf_result = prefilter_bio(uname, profile.bio, profile.follower_count)
                    self._prefilter_stats[pf_result] = self._prefilter_stats.get(pf_result, 0) + 1

                    if pf_result == "skip":
                        self.storage.mark_processed(uname, username, 0)
                        continue

                    analysis = self._analyse_account(profile, username, depth=1)

                    if analysis.classification in (
                        LeadClassification.HIGH_VALUE,
                        LeadClassification.POTENTIAL_VALUE,
                    ):
                        lead = Lead(
                            profile=profile,
                            analysis=analysis,
                            found_via_seeds=[username],
                            seed_depth=1,
                        )
                        self.storage.save_lead(lead)

                    self.storage.mark_processed(uname, username, analysis.overall_score)

                except InstagramAPIError as e:
                    logger.warning(f"API error for @{uname}: {e}")
                except Exception as e:
                    logger.warning(f"Error analysing @{uname}: {e}")

    # ── Legacy mode ──────────────────────────────────────────────────

    def _run_depth(self, seeds: list[str], depth: int, max_depth: int):
        """Process one depth level of the legacy pipeline."""
        next_depth_seeds = []

        for seed in seeds:
            console.print(f"\n[bold]Processing seed @{seed} (depth {depth})[/]")

            console.print(f"  Fetching following list ...")
            try:
                following_raw = self.ig.get_all_following(seed)
            except InstagramAPIError as e:
                console.print(f"  [red]API error fetching following: {e}[/]")
                continue

            console.print(f"  Found {len(following_raw)} accounts to analyse")

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("{task.completed}/{task.total}"),
                console=console,
            ) as progress:
                task = progress.add_task(
                    f"Analysing @{seed}'s following", total=len(following_raw)
                )

                for user_data in following_raw:
                    username = user_data.get("username", "")
                    if not username:
                        progress.advance(task)
                        continue

                    progress.update(task, description=f"@{username}")

                    if self.storage.is_processed(username):
                        self.storage.increment_appearance(username, seed)
                        progress.advance(task)
                        continue

                    try:
                        profile = self.ig.get_profile(username)
                        if not profile or profile.is_private:
                            self.storage.mark_processed(username, seed, 0)
                            progress.advance(task)
                            continue

                        # Pre-filter
                        pf_result = prefilter_bio(username, profile.bio, profile.follower_count)
                        self._prefilter_stats[pf_result] = self._prefilter_stats.get(pf_result, 0) + 1

                        if pf_result == "skip":
                            self.storage.mark_processed(username, seed, 0)
                            progress.advance(task)
                            continue

                        analysis = self._analyse_account(profile, seed, depth)

                        if analysis.classification in (
                            LeadClassification.HIGH_VALUE,
                            LeadClassification.POTENTIAL_VALUE,
                        ):
                            lead = Lead(
                                profile=profile,
                                analysis=analysis,
                                found_via_seeds=[seed],
                                seed_depth=depth,
                            )
                            self.storage.save_lead(lead)

                            if (
                                depth < max_depth
                                and analysis.overall_score >= config.DEPTH_CRAWL_THRESHOLD
                            ):
                                next_depth_seeds.append(username)

                        self.storage.mark_processed(
                            username, seed, analysis.overall_score
                        )

                    except InstagramAPIError as e:
                        logger.warning(f"API error for @{username}: {e}")
                    except Exception as e:
                        logger.warning(f"Error analysing @{username}: {e}")

                    progress.advance(task)

        if next_depth_seeds and depth < max_depth:
            console.print(
                f"\n[bold yellow]Depth crawling {len(next_depth_seeds)} "
                f"top leads (depth {depth + 1})[/]"
            )
            self._run_depth(next_depth_seeds, depth + 1, max_depth)

    # ── Account analysis ─────────────────────────────────────────────

    def _analyse_account(
        self, profile: InstagramProfile, seed: str, depth: int,
        appearance_override: int | None = None,
    ) -> "LeadAnalysis":
        """Run the full analysis pipeline on a single account."""
        # Step 1: Bio analysis
        bio_result = self.analyzer.analyze_bio(profile)

        website_result = None
        caption_result = None
        event_result = None

        # Step 2: If bio looks promising, do deep analysis
        if bio_result.score >= config.BIO_SCORE_THRESHOLD:
            if profile.bio_link:
                website_result = self.analyzer.analyze_website(profile)

            try:
                posts = self.ig.get_posts(profile.user_id)
                if posts:
                    caption_result = self.analyzer.analyze_captions(posts, profile)
                    event_result = self.analyzer.analyze_post_images(posts, profile)
            except InstagramAPIError as e:
                logger.warning(f"Could not fetch posts for @{profile.username}: {e}")

        # Step 3: Aggregate scores
        appearance_count = appearance_override or max(
            1, self.storage.get_appearance_count(profile.username)
        )

        return self.analyzer.calculate_overall_score(
            bio_result=bio_result,
            website_result=website_result,
            caption_result=caption_result,
            event_result=event_result,
            appearance_count=appearance_count,
            follower_count=profile.follower_count,
        )

    # ── Display helpers ──────────────────────────────────────────────

    def _print_stats(self, stats: dict):
        table = Table(title="Pipeline Results")
        table.add_column("Metric", style="bold")
        table.add_column("Value", justify="right")

        table.add_row("Seed accounts", str(stats["seed_accounts"]))
        table.add_row("Total processed", str(stats["total_processed"]))
        table.add_row("Total leads", str(stats["total_leads"]))
        table.add_row("High value leads", str(stats["high_value"]))
        table.add_row("Potential value leads", str(stats["potential_value"]))
        table.add_row("Boosted (multi-seed)", str(stats["boosted_leads"]))
        table.add_row("Avg lead score", f"{stats['avg_score']:.2f}")

        # Tier breakdown
        tiers = stats.get("tiers", {})
        if tiers:
            table.add_section()
            table.add_row("Tier 1 (Whales)", str(tiers.get("tier1_whale", 0)))
            table.add_row("Tier 2 (Agencies)", str(tiers.get("tier2_agency", 0)))
            table.add_row("Tier 3 (Affiliates)", str(tiers.get("tier3_affiliate", 0)))
            table.add_row("Tier 4 (Sellers)", str(tiers.get("tier4_seller", 0)))

        console.print(table)

    def _print_prefilter_stats(self):
        total = sum(self._prefilter_stats.values())
        if total == 0:
            return
        skipped = self._prefilter_stats.get("skip", 0)
        console.print(
            f"\n[dim]Pre-filter: {total} accounts checked — "
            f"{skipped} skipped ({skipped * 100 // total}% cost savings), "
            f"{self._prefilter_stats.get('auto_pass', 0)} auto-passed, "
            f"{self._prefilter_stats.get('pass', 0)} sent to Claude[/]"
        )

    def show_leads(self, top_n: int = 20):
        """Display top leads in a formatted table."""
        leads = self.storage.get_all_leads()[:top_n]
        if not leads:
            console.print("[dim]No leads found yet.[/]")
            return

        table = Table(title=f"Top {min(top_n, len(leads))} Leads")
        table.add_column("#", style="dim", width=3)
        table.add_column("Username", style="bold cyan")
        table.add_column("Score", justify="right")
        table.add_column("Tier", style="magenta")
        table.add_column("Type", style="green")
        table.add_column("Bio", max_width=35)
        table.add_column("App.", justify="right", style="yellow")
        table.add_column("Foll.", justify="right", style="dim")

        for i, lead in enumerate(leads, 1):
            score = lead["overall_score"]
            score_style = "bold green" if score >= 0.7 else "yellow" if score >= 0.55 else "dim"

            tier_display = lead.get("tier", "untiered")
            tier_short = {
                "tier1_whale": "T1 Whale",
                "tier2_agency": "T2 Agency",
                "tier3_affiliate": "T3 Affil.",
                "tier4_seller": "T4 Seller",
                "untiered": "-",
            }.get(tier_display, tier_display)

            bio = lead.get("bio", "")
            bio_display = (bio[:35] + "...") if len(bio) > 35 else bio

            followers = lead.get("follower_count", 0)
            if followers >= 1_000_000:
                foll_display = f"{followers / 1_000_000:.1f}M"
            elif followers >= 1_000:
                foll_display = f"{followers / 1_000:.1f}K"
            else:
                foll_display = str(followers)

            table.add_row(
                str(i),
                f"@{lead['username']}",
                f"[{score_style}]{score:.2f}[/]",
                tier_short,
                lead.get("lead_type", ""),
                bio_display,
                str(lead.get("appearance_count", 1)),
                foll_display,
            )

        console.print(table)
