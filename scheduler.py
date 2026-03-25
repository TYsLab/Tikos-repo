"""
Pre-Market Monitor — Scheduler
Runs the full pipeline on a schedule (default: 4:00 AM ET daily).
Also supports running the full pipeline manually right now.
"""

import sys
from pathlib import Path
from dotenv import load_dotenv
from rich.console import Console

load_dotenv(Path(__file__).parent / ".env", override=True)
console = Console()


def run_pipeline():
    """Run the full collect → analyze → report pipeline."""
    from collector import run_collector
    from analyzer import run_analyzer
    from report import run_report

    console.print("\n[bold bright_blue]━━━ Running Full Pre-Market Pipeline ━━━[/bold bright_blue]")

    total_fetched, counts, missing = run_collector()
    analyzed, ideas = run_analyzer()
    run_report()

    console.print(f"\n[bold green]Pipeline complete — {total_fetched} fetched, {analyzed} analyzed, {ideas} trade ideas[/bold green]")


def run_scheduled():
    """Run on a schedule — 4:00 AM ET daily."""
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        console.print("[red]APScheduler not installed. Run: pip install apscheduler[/red]")
        return

    scheduler = BlockingScheduler(timezone="America/New_York")

    # Full pipeline at 4:00 AM ET (pre-market starts 4 AM)
    scheduler.add_job(run_pipeline, CronTrigger(hour=4, minute=0),
                      id="premarket_pipeline", name="Pre-Market Pipeline")

    console.print("[bold cyan]Scheduler started — pipeline runs daily at 4:00 AM ET[/bold cyan]")
    console.print("Press Ctrl+C to stop\n")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        console.print("\n[yellow]Scheduler stopped.[/yellow]")
        scheduler.shutdown()


if __name__ == "__main__":
    if "--schedule" in sys.argv:
        run_scheduled()
    else:
        # Default: run immediately
        run_pipeline()
