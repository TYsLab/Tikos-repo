"""
Pre-Market Monitor — Analyzer
Reads unanalyzed items from SQLite, sends them to Gemini in batches,
extracts sentiment scores, ticker mentions, and trade ideas.
"""

import os
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, SpinnerColumn
from rich import box
from rich.table import Table

load_dotenv(Path(__file__).parent / ".env", override=True)

console = Console()
DB_PATH = Path(__file__).parent / "premarket.db"

BATCH_SIZE = 10  # items per Gemini call to stay within token limits

ANALYSIS_PROMPT = """You are a pre-market trading analyst. Analyze the following news/social media items and return structured JSON.

For each item, provide:
1. sentiment_score: float from -1.0 (very bearish) to +1.0 (very bullish), 0.0 = neutral
2. tickers: list of stock tickers mentioned (e.g. ["AAPL", "MSFT"]). Empty list if none.
3. summary: one sentence summary of the market impact
4. is_trade_idea: true if this has actionable trading implications

If a clear trade idea exists, also include:
5. trade: {
     "ticker": "TICKER",
     "direction": "LONG" or "SHORT" or "WATCH",
     "confidence": 0-100,
     "catalyst": "what is driving this",
     "entry_note": "when/how to enter",
     "risk_note": "key risk to this trade"
   }

Items to analyze:
{items}

Return a JSON array with one object per item, in the same order. Use this exact format:
[
  {
    "id": "item_id_here",
    "sentiment_score": 0.0,
    "tickers": [],
    "summary": "...",
    "is_trade_idea": false
  },
  ...
]
"""


def get_unanalyzed(conn, limit=50):
    """Fetch items not yet analyzed."""
    rows = conn.execute("""
        SELECT r.id, r.source, r.source_name, r.title, r.content
        FROM raw_items r
        LEFT JOIN analysis_results a ON r.id = a.item_id
        WHERE a.item_id IS NULL
        ORDER BY r.fetched_at DESC
        LIMIT ?
    """, (limit,)).fetchall()
    return [{"id": r[0], "source": r[1], "source_name": r[2],
             "title": r[3], "content": (r[4] or "")[:500]} for r in rows]


def save_analysis(conn, item_id, sentiment, tickers, summary):
    conn.execute("""
        INSERT OR REPLACE INTO analysis_results (item_id, sentiment, tickers, summary, analyzed_at)
        VALUES (?, ?, ?, ?, ?)
    """, (item_id, sentiment, json.dumps(tickers), summary, datetime.utcnow().isoformat()))
    conn.execute("UPDATE raw_items SET analyzed=1 WHERE id=?", (item_id,))


def save_trade_idea(conn, item_id, trade):
    conn.execute("""
        INSERT INTO trade_ideas (item_id, ticker, direction, confidence, catalyst, entry_note, risk_note, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        item_id,
        trade.get("ticker", ""),
        trade.get("direction", "WATCH"),
        trade.get("confidence", 50),
        trade.get("catalyst", ""),
        trade.get("entry_note", ""),
        trade.get("risk_note", ""),
        datetime.utcnow().isoformat(),
    ))


def analyze_batch(client, items):
    """Send a batch of items to Gemini for analysis."""
    from google.genai import types

    items_text = "\n\n".join(
        f"ID: {item['id']}\nSource: {item['source_name']}\nTitle: {item['title']}\nContent: {item['content']}"
        for item in items
    )
    prompt = ANALYSIS_PROMPT.replace("{items}", items_text)

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
        ),
    )

    text = response.text.strip()
    # Strip markdown code fences if present
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])
    # Find outermost JSON array
    start = text.find("[")
    end = text.rfind("]") + 1
    if start >= 0 and end > start:
        return json.loads(text[start:end])
    return []


def run_analyzer():
    console.print("\n[bold cyan]Pre-Market Analyzer — Scoring items with Gemini...[/bold cyan]")

    gemini_key = os.getenv("GEMINI_API_KEY")
    if not gemini_key:
        console.print("[bold red]Error: GEMINI_API_KEY not set in .env[/bold red]")
        console.print("  Get a free key at https://aistudio.google.com/apikey")
        console.print("  Add to .env:  GEMINI_API_KEY=your_key")
        return 0, 0

    from google import genai
    client = genai.Client(api_key=gemini_key)

    conn = sqlite3.connect(DB_PATH)
    items = get_unanalyzed(conn, limit=100)

    if not items:
        console.print("[yellow]No new items to analyze.[/yellow]")
        conn.close()
        return 0, 0

    console.print(f"  Analyzing [bold]{len(items)}[/bold] items in batches of {BATCH_SIZE}...")

    analyzed = 0
    trade_ideas = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    ) as progress:
        task = progress.add_task("Analyzing...", total=len(items))

        for i in range(0, len(items), BATCH_SIZE):
            batch = items[i:i + BATCH_SIZE]
            try:
                results = analyze_batch(client, batch)

                for result in results:
                    item_id   = result.get("id", "")
                    sentiment = result.get("sentiment_score", 0.0)
                    tickers   = result.get("tickers", [])
                    summary   = result.get("summary", "")
                    is_trade  = result.get("is_trade_idea", False)
                    trade     = result.get("trade")

                    save_analysis(conn, item_id, sentiment, tickers, summary)
                    analyzed += 1

                    if is_trade and trade and trade.get("ticker"):
                        save_trade_idea(conn, item_id, trade)
                        trade_ideas += 1

                conn.commit()

            except Exception as e:
                console.print(f"[yellow]Batch error: {e}[/yellow]")

            progress.advance(task, len(batch))

    # Summary
    console.print(f"\n[green]✓ Analyzed {analyzed} items[/green]")
    console.print(f"[green]✓ Found {trade_ideas} trade ideas[/green]")

    # Show top trade ideas
    if trade_ideas > 0:
        ideas = conn.execute("""
            SELECT t.ticker, t.direction, t.confidence, t.catalyst, r.source_name
            FROM trade_ideas t
            JOIN raw_items r ON t.item_id = r.id
            ORDER BY t.confidence DESC, t.created_at DESC
            LIMIT 10
        """).fetchall()

        table = Table(title="Top Trade Ideas Found", box=box.ROUNDED)
        table.add_column("Ticker", style="cyan", width=8)
        table.add_column("Direction", width=8)
        table.add_column("Confidence", justify="right", width=10)
        table.add_column("Catalyst", width=40)
        table.add_column("Source", style="dim", width=15)

        for row in ideas:
            ticker, direction, confidence, catalyst, source = row
            dir_color = "green" if direction == "LONG" else "red" if direction == "SHORT" else "yellow"
            table.add_row(
                ticker,
                f"[{dir_color}]{direction}[/{dir_color}]",
                f"{confidence:.0f}%",
                (catalyst or "")[:40],
                source or "",
            )

        console.print(table)

    conn.close()
    return analyzed, trade_ideas


if __name__ == "__main__":
    run_analyzer()
