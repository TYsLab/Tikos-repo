"""
Pre-Market Monitor — Collector
Fetches news (RSS), Reddit, SEC filings, and optionally NewsAPI
into a local SQLite database.
"""

import os
import sqlite3
import hashlib
import requests
import feedparser
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich import box

load_dotenv(Path(__file__).parent / ".env", override=True)

console = Console()
DB_PATH = Path(__file__).parent / "premarket.db"

# ─── RSS FEEDS (no API key required) ─────────────────────────────────────────

RSS_FEEDS = {
    "Yahoo Finance":   "https://finance.yahoo.com/news/rssindex",
    "MarketWatch":     "https://feeds.marketwatch.com/marketwatch/topstories/",
    "Reuters Biz":     "https://feeds.reuters.com/reuters/businessNews",
    "CNBC Top":        "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "CNBC Earnings":   "https://www.cnbc.com/id/15839135/device/rss/rss.html",
    "Seeking Alpha":   "https://seekingalpha.com/market_currents.xml",
    "Benzinga":        "https://www.benzinga.com/feed",
    "Investopedia":    "https://www.investopedia.com/feedbuilder/feed/getfeed/?feedName=rss_headline",
}

# ─── HIGH-SIGNAL X ACCOUNTS ──────────────────────────────────────────────────
# Only pull from verified big names — no noise from random accounts

X_ACCOUNTS = [
    # Fund managers / investors
    "BillAckman", "chamath", "dan_niles", "fundstrat",
    "GundlachCapital", "elerianm", "RayDalio",
    # Wall St analysts / commentators
    "LizAnnSonders", "ReformedBroker", "ritholtz", "jimcramer",
    # Institutions / official
    "GoldmanSachs", "jpmorgan", "BlackRock", "federalreserve", "SEC_News",
    # Financial media
    "WSJ", "FT", "business", "markets",
]

# ─── GEMINI WEB SEARCH QUERIES ────────────────────────────────────────────────

GEMINI_WEB_QUERIES = [
    "top stock market moving news today earnings reports premarket movers",
    "Federal Reserve interest rate policy economic data released today",
    "S&P 500 analyst upgrades downgrades price targets today",
    "major merger acquisition IPO company announcement today",
]

# ─── DB SETUP ─────────────────────────────────────────────────────────────────

def purge_old_items(conn, days=2):
    """Delete items older than `days` days from all tables."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    conn.execute("DELETE FROM trade_ideas WHERE item_id IN (SELECT id FROM raw_items WHERE fetched_at < ?)", (cutoff,))
    conn.execute("DELETE FROM analysis_results WHERE item_id IN (SELECT id FROM raw_items WHERE fetched_at < ?)", (cutoff,))
    conn.execute("DELETE FROM raw_items WHERE fetched_at < ?", (cutoff,))
    conn.commit()
    return conn.total_changes


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS raw_items (
            id          TEXT PRIMARY KEY,
            source      TEXT NOT NULL,
            source_name TEXT,
            title       TEXT,
            content     TEXT,
            url         TEXT,
            published_at TEXT,
            fetched_at  TEXT,
            analyzed    INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS trade_ideas (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id     TEXT,
            ticker      TEXT,
            direction   TEXT,
            confidence  REAL,
            catalyst    TEXT,
            entry_note  TEXT,
            risk_note   TEXT,
            created_at  TEXT
        );
        CREATE TABLE IF NOT EXISTS analysis_results (
            item_id     TEXT PRIMARY KEY,
            sentiment   REAL,
            tickers     TEXT,
            summary     TEXT,
            analyzed_at TEXT
        );
    """)
    conn.commit()
    return conn


def insert_item(conn, item_id, source, source_name, title, content, url, published_at):
    try:
        conn.execute("""
            INSERT OR IGNORE INTO raw_items
            (id, source, source_name, title, content, url, published_at, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (item_id, source, source_name, title, content, url, published_at,
              datetime.utcnow().isoformat()))
        return conn.total_changes
    except Exception:
        return 0


def make_id(*parts):
    return hashlib.md5("|".join(str(p) for p in parts).encode()).hexdigest()

# ─── COLLECTORS ───────────────────────────────────────────────────────────────

def collect_rss(conn):
    """Fetch all RSS feeds — no API key needed."""
    counts = {}
    cutoff = datetime.utcnow() - timedelta(days=2)

    for feed_name, url in RSS_FEEDS.items():
        count = 0
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                title   = getattr(entry, "title", "")
                content = getattr(entry, "summary", "") or getattr(entry, "description", "")
                link    = getattr(entry, "link", "")
                pub     = getattr(entry, "published", datetime.utcnow().isoformat())

                # Skip articles older than 2 days
                try:
                    import email.utils
                    pub_dt = datetime(*email.utils.parsedate(str(pub))[:6]) if pub else None
                    if pub_dt and pub_dt < cutoff:
                        continue
                except Exception:
                    pass

                item_id = make_id("rss", feed_name, link or title)
                before  = conn.execute("SELECT COUNT(*) FROM raw_items").fetchone()[0]
                insert_item(conn, item_id, "news_rss", feed_name, title, content, link, str(pub))
                after   = conn.execute("SELECT COUNT(*) FROM raw_items").fetchone()[0]
                if after > before:
                    count += 1

        except Exception as e:
            console.print(f"[yellow]RSS error ({feed_name}): {e}[/yellow]")

        counts[feed_name] = count

    conn.commit()
    return counts


def collect_x(conn):
    """
    Fetch recent posts from high-signal financial X accounts via X/Twitter API v2.
    Targets specific verified big names instead of broad keyword searches.
    Requires X_BEARER_TOKEN in .env.
    """
    bearer_token = os.getenv("X_BEARER_TOKEN")
    if not bearer_token:
        return {"X/Twitter": 0}, "missing_key"

    # Build batched from: queries — X API allows up to ~512 chars per query.
    # Split the account list into groups of ~8 to stay well within limits.
    BATCH_SIZE = 8
    account_batches = [
        X_ACCOUNTS[i:i + BATCH_SIZE]
        for i in range(0, len(X_ACCOUNTS), BATCH_SIZE)
    ]

    headers = {"Authorization": f"Bearer {bearer_token}"}
    total_count = 0

    try:
        for batch in account_batches:
            from_clause = " OR ".join(f"from:{acct}" for acct in batch)
            query = f"({from_clause}) -is:retweet lang:en"

            try:
                r = requests.get(
                    "https://api.twitter.com/2/tweets/search/recent",
                    headers=headers,
                    params={
                        "query": query,
                        "max_results": 20,
                        "tweet.fields": "created_at,text,author_id",
                        "expansions": "author_id",
                        "user.fields": "username",
                    },
                    timeout=10,
                )

                if r.status_code == 429:
                    console.print("[yellow]X API rate limit hit — skipping remaining batches[/yellow]")
                    break
                if r.status_code != 200:
                    console.print(f"[yellow]X API error {r.status_code}: {r.text[:100]}[/yellow]")
                    continue

                data = r.json()
                tweets = data.get("data", [])
                users  = {u["id"]: u["username"] for u in data.get("includes", {}).get("users", [])}

                for tweet in tweets:
                    tweet_id = tweet.get("id", "")
                    text     = tweet.get("text", "")
                    created  = tweet.get("created_at", datetime.utcnow().isoformat())
                    author   = users.get(tweet.get("author_id", ""), "unknown")
                    url      = f"https://twitter.com/{author}/status/{tweet_id}"
                    item_id  = make_id("x", tweet_id)
                    title    = f"@{author}: {text[:100]}"

                    before = conn.execute("SELECT COUNT(*) FROM raw_items").fetchone()[0]
                    insert_item(conn, item_id, "x_twitter", f"@{author}", title, text, url, created)
                    after  = conn.execute("SELECT COUNT(*) FROM raw_items").fetchone()[0]
                    if after > before:
                        total_count += 1

            except Exception as e:
                console.print(f"[yellow]X batch error: {e}[/yellow]")

        conn.commit()
        return {"X/Twitter": total_count}, "ok"

    except Exception as e:
        return {"X/Twitter": 0}, str(e)


def collect_gemini_web(conn):
    """
    Use Gemini with Google Search grounding to fetch fresh market intelligence.
    Requires GEMINI_API_KEY in .env.
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return {"Gemini Web": 0}, "missing_key"

    try:
        import google.generativeai as genai
        from google.generativeai import types
    except ImportError:
        return {"Gemini Web": 0}, "missing_package: pip install google-generativeai"

    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(
            model_name="gemini-1.5-flash",
            tools=[types.Tool(google_search=types.GoogleSearch())],
        )

        count = 0
        today = datetime.utcnow().strftime("%B %d, %Y")

        for query in GEMINI_WEB_QUERIES:
            try:
                prompt = f"Today is {today}. {query}. Provide 3-5 specific news items with headline and brief detail."
                response = model.generate_content(prompt)
                text = response.text.strip()
                if not text:
                    continue

                item_id = make_id("gemini", query, today)
                before  = conn.execute("SELECT COUNT(*) FROM raw_items").fetchone()[0]
                insert_item(
                    conn, item_id, "gemini_web", "Gemini Web Search",
                    query[:120], text, "", datetime.utcnow().isoformat()
                )
                after = conn.execute("SELECT COUNT(*) FROM raw_items").fetchone()[0]
                if after > before:
                    count += 1

            except Exception as e:
                console.print(f"[yellow]Gemini query error: {e}[/yellow]")

        conn.commit()
        return {"Gemini Web": count}, "ok"

    except Exception as e:
        return {"Gemini Web": 0}, str(e)


def collect_sec(conn):
    """
    Fetch recent 8-K filings from SEC EDGAR — completely free, no API key.
    8-Ks are material event disclosures (earnings, M&A, guidance, etc.)
    """
    count = 0
    try:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

        url = (
            "https://efts.sec.gov/LATEST/search-index?q=&forms=8-K"
            f"&dateRange=custom&startdt={yesterday}&enddt={today}"
            "&hits.hits._source=period_of_report,entity_name,file_date,form_type"
            "&hits.hits.total.value=true&hits.hits.highlight=true"
        )
        headers = {"User-Agent": "PreMarketMonitor research@example.com"}
        r = requests.get(url, headers=headers, timeout=15)
        data = r.json()

        filings = data.get("hits", {}).get("hits", [])
        for filing in filings[:50]:
            src     = filing.get("_source", {})
            entity  = src.get("entity_name", "Unknown")
            date    = src.get("file_date", today)
            form    = src.get("form_type", "8-K")
            acc     = filing.get("_id", "")
            link    = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&filenum={acc}"
            title   = f"{form}: {entity}"
            content = f"SEC {form} filing by {entity} on {date}"
            item_id = make_id("sec", acc or entity, date)

            before = conn.execute("SELECT COUNT(*) FROM raw_items").fetchone()[0]
            insert_item(conn, item_id, "sec", "SEC EDGAR", title, content, link, date)
            after  = conn.execute("SELECT COUNT(*) FROM raw_items").fetchone()[0]
            if after > before:
                count += 1

        conn.commit()

    except Exception as e:
        console.print(f"[yellow]SEC EDGAR error: {e}[/yellow]")

    return {"SEC EDGAR (8-K)": count}


def collect_newsapi(conn):
    """Fetch from NewsAPI.org — requires free API key."""
    api_key = os.getenv("NEWS_API_KEY")
    if not api_key:
        return {"NewsAPI": 0}, "missing_key"

    count = 0
    try:
        yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
        url = (
            f"https://newsapi.org/v2/everything"
            f"?q=stock+market+earnings+Fed&language=en"
            f"&from={yesterday}&sortBy=publishedAt&pageSize=50"
        )
        r = requests.get(url, headers={"X-Api-Key": api_key}, timeout=10)
        articles = r.json().get("articles", [])

        for a in articles:
            title   = a.get("title", "")
            content = a.get("description", "") or a.get("content", "")
            link    = a.get("url", "")
            pub     = a.get("publishedAt", datetime.utcnow().isoformat())
            source  = a.get("source", {}).get("name", "NewsAPI")
            item_id = make_id("newsapi", link or title)

            before = conn.execute("SELECT COUNT(*) FROM raw_items").fetchone()[0]
            insert_item(conn, item_id, "newsapi", source, title, content, link, pub)
            after  = conn.execute("SELECT COUNT(*) FROM raw_items").fetchone()[0]
            if after > before:
                count += 1

        conn.commit()
        return {"NewsAPI": count}, "ok"

    except Exception as e:
        return {"NewsAPI": 0}, str(e)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def run_collector():
    console.print("\n[bold cyan]Pre-Market Collector — Fetching data...[/bold cyan]")
    conn = init_db()

    # Wipe anything older than 2 days before collecting fresh data
    console.print("  [dim]Purging items older than 2 days...[/dim]")
    purge_old_items(conn, days=2)

    all_counts = {}
    missing_keys = []

    # 1. RSS (always works — no key needed)
    console.print("  [dim]Fetching RSS feeds...[/dim]")
    rss_counts = collect_rss(conn)
    all_counts.update(rss_counts)

    # 2. X/Twitter
    console.print("  [dim]Fetching X/Twitter...[/dim]")
    x_counts, x_status = collect_x(conn)
    all_counts.update(x_counts)
    if x_status == "missing_key":
        missing_keys.append("X_BEARER_TOKEN")

    # 3. SEC EDGAR
    console.print("  [dim]Fetching SEC 8-K filings...[/dim]")
    sec_counts = collect_sec(conn)
    all_counts.update(sec_counts)

    # 4. NewsAPI (optional)
    console.print("  [dim]Fetching NewsAPI...[/dim]")
    news_counts, news_status = collect_newsapi(conn)
    all_counts.update(news_counts)
    if news_status == "missing_key":
        missing_keys.append("NEWS_API_KEY")

    # 5. Gemini Web Search (optional — high-signal grounded results)
    console.print("  [dim]Fetching Gemini web search...[/dim]")
    gemini_counts, gemini_status = collect_gemini_web(conn)
    all_counts.update(gemini_counts)
    if gemini_status == "missing_key":
        missing_keys.append("GEMINI_API_KEY")

    # ── Summary table
    table = Table(title="Items Fetched", box=box.ROUNDED)
    table.add_column("Source", style="cyan")
    table.add_column("New Items", justify="right", style="green")

    total = 0
    for source, count in all_counts.items():
        color = "green" if count > 0 else "yellow"
        table.add_row(source, f"[{color}]{count}[/{color}]")
        total += count

    table.add_row("[bold]TOTAL[/bold]", f"[bold]{total}[/bold]")
    console.print(table)

    # ── Missing key warnings
    if missing_keys:
        console.print("\n[bold yellow]⚠ Missing API Keys (sources returned 0):[/bold yellow]")
        key_info = {
            "X_BEARER_TOKEN": (
                "From developer.twitter.com → your app → Keys and Tokens → Bearer Token\n"
                "Add to .env:\n  X_BEARER_TOKEN=your_bearer_token"
            ),
            "NEWS_API_KEY": (
                "Free at https://newsapi.org/register (500 req/day free tier)\n"
                "Add to .env:\n  NEWS_API_KEY=your_key"
            ),
            "GEMINI_API_KEY": (
                "Free at aistudio.google.com → Get API Key\n"
                "Also run: pip install google-generativeai\n"
                "Add to .env:\n  GEMINI_API_KEY=your_key"
            ),
        }
        for key in missing_keys:
            console.print(f"\n  [bold red]{key}[/bold red]")
            console.print(f"  {key_info.get(key, 'See .env.example')}")

    conn.close()
    return total, all_counts, missing_keys


if __name__ == "__main__":
    run_collector()
