"""
Pre-Market Monitor — Report Generator
Reads from SQLite and prints a full pre-market briefing to the terminal.
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.columns import Columns
from rich.text import Text
from rich import box

load_dotenv(Path(__file__).parent / ".env", override=True)

console = Console()
DB_PATH = Path(__file__).parent / "premarket.db"


def sentiment_bar(score, width=16):
    normalized = (score + 1) / 2
    filled = int(normalized * width)
    bar = "█" * filled + "░" * (width - filled)
    color = "green" if score > 0.2 else "red" if score < -0.2 else "yellow"
    return f"[{color}][{bar}][/{color}]"


def direction_badge(direction):
    colors = {"LONG": "green", "SHORT": "red", "WATCH": "yellow"}
    return f"[{colors.get(direction,'white')}]{direction}[/{colors.get(direction,'white')}]"


def run_report():
    if not DB_PATH.exists():
        console.print("[bold red]No database found. Run collector.py first.[/bold red]")
        return

    conn = sqlite3.connect(DB_PATH)

    # ── Header ────────────────────────────────────────────────────────────────
    now = datetime.now().strftime("%A, %B %d %Y — %I:%M %p")
    console.print()
    console.rule()
    console.print(Panel(
        f"[bold white]📈 PRE-MARKET BRIEFING[/bold white]\n[dim]{now}[/dim]",
        box=box.HEAVY, border_style="bright_blue"
    ))
    console.rule()

    # ── Database stats ────────────────────────────────────────────────────────
    total_items   = conn.execute("SELECT COUNT(*) FROM raw_items").fetchone()[0]
    analyzed      = conn.execute("SELECT COUNT(*) FROM analysis_results").fetchone()[0]
    total_ideas   = conn.execute("SELECT COUNT(*) FROM trade_ideas").fetchone()[0]

    stats = Table(box=box.SIMPLE, show_header=False)
    stats.add_column(width=20)
    stats.add_column(width=10)
    stats.add_row("[cyan]Total Items[/cyan]",    str(total_items))
    stats.add_row("[cyan]Analyzed[/cyan]",        str(analyzed))
    stats.add_row("[cyan]Trade Ideas[/cyan]",     f"[bold green]{total_ideas}[/bold green]")
    console.print(stats)

    # ── Source breakdown ──────────────────────────────────────────────────────
    console.print(Panel("[bold]Source Breakdown[/bold]", style="cyan"))
    source_rows = conn.execute("""
        SELECT source_name, COUNT(*) as cnt,
               ROUND(AVG(a.sentiment), 3) as avg_sent
        FROM raw_items r
        LEFT JOIN analysis_results a ON r.id = a.item_id
        GROUP BY source_name
        ORDER BY cnt DESC
    """).fetchall()

    src_table = Table(box=box.SIMPLE)
    src_table.add_column("Source", style="cyan", width=22)
    src_table.add_column("Items", justify="right", width=8)
    src_table.add_column("Avg Sentiment", justify="right", width=14)
    src_table.add_column("Bar", width=18)

    for name, cnt, avg_sent in source_rows:
        avg_sent = avg_sent or 0.0
        src_table.add_row(name or "Unknown", str(cnt),
                          f"{avg_sent:+.3f}", sentiment_bar(avg_sent))
    console.print(src_table)

    # ── Overall market sentiment ──────────────────────────────────────────────
    overall = conn.execute("SELECT AVG(sentiment) FROM analysis_results").fetchone()[0] or 0.0
    bull = conn.execute("SELECT COUNT(*) FROM analysis_results WHERE sentiment > 0.2").fetchone()[0]
    bear = conn.execute("SELECT COUNT(*) FROM analysis_results WHERE sentiment < -0.2").fetchone()[0]
    neut = analyzed - bull - bear

    console.print(Panel("[bold]Overall Market Sentiment[/bold]", style="magenta"))
    label = "BULLISH" if overall > 0.1 else "BEARISH" if overall < -0.1 else "NEUTRAL"
    color = "green" if overall > 0.1 else "red" if overall < -0.1 else "yellow"

    sent_text = Text()
    sent_text.append(f"{label}  ", style=f"bold {color}")
    sent_text.append(f"{overall:+.3f}  ", style=color)
    sent_text.append(sentiment_bar(overall, width=24))
    console.print(sent_text)
    console.print(f"  [green]Bullish: {bull}[/green]  [yellow]Neutral: {neut}[/yellow]  [red]Bearish: {bear}[/red]")

    # ── Top ticker mentions ───────────────────────────────────────────────────
    console.print(Panel("[bold]Most Mentioned Tickers[/bold]", style="yellow"))
    ticker_rows = conn.execute("""
        SELECT a.tickers, a.sentiment
        FROM analysis_results a
        WHERE a.tickers != '[]' AND a.tickers IS NOT NULL
    """).fetchall()

    ticker_counts = {}
    ticker_sentiment = {}
    for tickers_json, sentiment in ticker_rows:
        try:
            tickers = json.loads(tickers_json)
            for t in tickers:
                if t and len(t) <= 6:
                    ticker_counts[t] = ticker_counts.get(t, 0) + 1
                    ticker_sentiment.setdefault(t, []).append(sentiment or 0)
        except Exception:
            pass

    if ticker_counts:
        sorted_tickers = sorted(ticker_counts.items(), key=lambda x: x[1], reverse=True)[:15]
        tick_table = Table(box=box.SIMPLE)
        tick_table.add_column("Ticker", style="cyan", width=8)
        tick_table.add_column("Mentions", justify="right", width=10)
        tick_table.add_column("Avg Sentiment", justify="right", width=14)
        tick_table.add_column("Bar", width=18)

        for ticker, count in sorted_tickers:
            avg = sum(ticker_sentiment[ticker]) / len(ticker_sentiment[ticker])
            color = "green" if avg > 0.2 else "red" if avg < -0.2 else "yellow"
            tick_table.add_row(ticker, str(count), f"[{color}]{avg:+.3f}[/{color}]",
                               sentiment_bar(avg))
        console.print(tick_table)
    else:
        console.print("[yellow]  No ticker data yet — run analyzer.py first.[/yellow]")

    # ── Top stories ───────────────────────────────────────────────────────────
    console.print(Panel("[bold]Top Stories (Highest Impact)[/bold]", style="bright_blue"))
    stories = conn.execute("""
        SELECT r.title, r.source_name, r.url, a.sentiment, a.summary, a.tickers
        FROM raw_items r
        JOIN analysis_results a ON r.id = a.item_id
        ORDER BY ABS(a.sentiment) DESC
        LIMIT 10
    """).fetchall()

    for i, (title, source, url, sentiment, summary, tickers_json) in enumerate(stories, 1):
        sentiment = sentiment or 0.0
        color = "green" if sentiment > 0.2 else "red" if sentiment < -0.2 else "yellow"
        tickers = []
        try:
            tickers = json.loads(tickers_json or "[]")
        except Exception:
            pass

        ticker_str = "  ".join(f"[cyan]{t}[/cyan]" for t in tickers[:5])
        console.print(f"\n  [bold]{i}.[/bold] [{color}]{sentiment:+.2f}[/{color}]  "
                      f"[white]{(title or '')[:80]}[/white]  [dim]{source}[/dim]")
        if ticker_str:
            console.print(f"     Tickers: {ticker_str}")
        if summary:
            console.print(f"     [dim]{summary[:120]}[/dim]")

    # ── Trade ideas ───────────────────────────────────────────────────────────
    console.print()
    console.print(Panel("[bold]Trade Ideas[/bold]", style="bright_green"))
    ideas = conn.execute("""
        SELECT t.ticker, t.direction, t.confidence, t.catalyst, t.entry_note, t.risk_note,
               r.source_name, r.title
        FROM trade_ideas t
        JOIN raw_items r ON t.item_id = r.id
        ORDER BY t.confidence DESC, t.created_at DESC
    """).fetchall()

    if not ideas:
        console.print("[yellow]  No trade ideas found yet.[/yellow]")
    else:
        for ticker, direction, confidence, catalyst, entry, risk, source, title in ideas:
            dir_color = "green" if direction == "LONG" else "red" if direction == "SHORT" else "yellow"
            console.print(
                f"\n  [bold cyan]{ticker}[/bold cyan]  "
                f"[{dir_color}][bold]{direction}[/bold][/{dir_color}]  "
                f"[white]{confidence:.0f}% confidence[/white]  [dim]via {source}[/dim]"
            )
            if catalyst:
                console.print(f"  [bold]Catalyst:[/bold] {catalyst}")
            if entry:
                console.print(f"  [bold]Entry:[/bold] {entry}")
            if risk:
                console.print(f"  [bold]Risk:[/bold] [red]{risk}[/red]")

    # ── Bearish alerts ────────────────────────────────────────────────────────
    bearish = conn.execute("""
        SELECT r.title, r.source_name, a.sentiment, a.summary
        FROM raw_items r
        JOIN analysis_results a ON r.id = a.item_id
        WHERE a.sentiment < -0.5
        ORDER BY a.sentiment ASC
        LIMIT 5
    """).fetchall()

    if bearish:
        console.print()
        console.print(Panel("[bold red]⚠ High-Impact Bearish Alerts[/bold red]", style="red"))
        for title, source, sentiment, summary in bearish:
            console.print(f"  [red]{sentiment:+.2f}[/red]  [white]{(title or '')[:80]}[/white]  [dim]{source}[/dim]")
            if summary:
                console.print(f"  [dim]{summary[:120]}[/dim]")

    console.print()
    console.rule("[dim]End of Pre-Market Briefing[/dim]")
    conn.close()


if __name__ == "__main__":
    run_report()
