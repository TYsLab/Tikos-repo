#!/usr/bin/env python3
"""
Local dev server for MarketPulse Morning Briefing.

Serves index.html + exposes JSON endpoints:
  GET  /api/recap           — reads premarket.db, returns briefing JSON
  POST /api/run-pipeline    — runs collector + analyzer, returns {status}
  GET  /api/pipeline-status — poll for pipeline progress

Usage:
  python local_server.py
  Then open http://localhost:8765
"""

import json
import sqlite3
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

PORT      = 8765
DB_PATH   = Path(__file__).parent / "premarket.db"
HTML_PATH = Path(__file__).parent / "index.html"


# ── Build recap JSON from SQLite ──────────────────────────────────────────────

def build_recap():
    if not DB_PATH.exists():
        return {"error": "No database found. Run the pipeline first."}

    conn = sqlite3.connect(DB_PATH)

    total_items = conn.execute("SELECT COUNT(*) FROM raw_items").fetchone()[0]
    analyzed    = conn.execute("SELECT COUNT(*) FROM analysis_results").fetchone()[0]
    total_ideas = conn.execute("SELECT COUNT(*) FROM trade_ideas").fetchone()[0]

    overall = conn.execute("SELECT AVG(sentiment) FROM analysis_results").fetchone()[0] or 0.0
    bullish = conn.execute("SELECT COUNT(*) FROM analysis_results WHERE sentiment > 0.2").fetchone()[0]
    bearish = conn.execute("SELECT COUNT(*) FROM analysis_results WHERE sentiment < -0.2").fetchone()[0]
    neutral = analyzed - bullish - bearish

    source_rows = conn.execute("""
        SELECT source_name, COUNT(*) as cnt, ROUND(AVG(a.sentiment), 3) as avg_sent
        FROM raw_items r
        LEFT JOIN analysis_results a ON r.id = a.item_id
        GROUP BY source_name ORDER BY cnt DESC
    """).fetchall()
    sources = [{"name": r[0] or "Unknown", "count": r[1], "sentiment": r[2] or 0.0}
               for r in source_rows]

    ticker_rows = conn.execute(
        "SELECT tickers, sentiment FROM analysis_results WHERE tickers != '[]' AND tickers IS NOT NULL"
    ).fetchall()
    ticker_counts, ticker_sent = {}, {}
    for tickers_json, sent in ticker_rows:
        try:
            for t in json.loads(tickers_json):
                if t and len(t) <= 6:
                    ticker_counts[t] = ticker_counts.get(t, 0) + 1
                    ticker_sent.setdefault(t, []).append(sent or 0)
        except Exception:
            pass
    tickers = sorted(
        [{"ticker": t, "mentions": c,
          "sentiment": round(sum(ticker_sent[t]) / len(ticker_sent[t]), 3)}
         for t, c in ticker_counts.items()],
        key=lambda x: x["mentions"], reverse=True
    )[:15]

    story_rows = conn.execute("""
        SELECT r.title, r.source_name, r.url, a.sentiment, a.summary, a.tickers
        FROM raw_items r
        JOIN analysis_results a ON r.id = a.item_id
        ORDER BY ABS(a.sentiment) DESC LIMIT 10
    """).fetchall()
    stories = []
    for title, source, url, sent, summary, tickers_json in story_rows:
        t_list = []
        try: t_list = json.loads(tickers_json or "[]")
        except Exception: pass
        stories.append({
            "title":     (title or "")[:120],
            "source":    source or "",
            "url":       url or "",
            "sentiment": round(sent or 0.0, 3),
            "summary":   (summary or "")[:200],
            "tickers":   t_list[:5],
        })

    idea_rows = conn.execute("""
        SELECT t.ticker, t.direction, t.confidence, t.catalyst,
               t.entry_note, t.risk_note, r.source_name
        FROM trade_ideas t
        JOIN raw_items r ON t.item_id = r.id
        ORDER BY t.confidence DESC, t.created_at DESC
    """).fetchall()
    ideas = [{"ticker": r[0], "direction": r[1], "confidence": r[2],
              "catalyst": r[3] or "", "entry": r[4] or "", "risk": r[5] or "",
              "source": r[6] or ""} for r in idea_rows]

    alert_rows = conn.execute("""
        SELECT r.title, r.source_name, a.sentiment, a.summary
        FROM raw_items r JOIN analysis_results a ON r.id = a.item_id
        WHERE a.sentiment < -0.5 ORDER BY a.sentiment ASC LIMIT 5
    """).fetchall()
    alerts = [{"title": (r[0] or "")[:120], "source": r[1] or "",
               "sentiment": round(r[2] or 0.0, 3), "summary": (r[3] or "")[:200]}
              for r in alert_rows]

    conn.close()
    return {
        "generated_at": datetime.now().strftime("%A, %B %d %Y — %I:%M %p"),
        "stats":   {"total": total_items, "analyzed": analyzed, "ideas": total_ideas},
        "overall": {"score": round(overall, 3), "bullish": bullish, "neutral": neutral, "bearish": bearish},
        "sources": sources,
        "tickers": tickers,
        "stories": stories,
        "ideas":   ideas,
        "alerts":  alerts,
    }


# ── Pipeline runner ───────────────────────────────────────────────────────────

_status = {"running": False, "message": "idle", "done": False, "error": None}

def _run_pipeline():
    global _status
    try:
        _status = {"running": True, "message": "Collecting news...", "done": False, "error": None}
        from collector import run_collector
        run_collector()

        _status["message"] = "Analyzing with Gemini..."
        from analyzer import run_analyzer
        run_analyzer()

        _status = {"running": False, "message": "Complete", "done": True, "error": None}
    except Exception as e:
        _status = {"running": False, "message": str(e), "done": False, "error": str(e)}


# ── Request handler ───────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"  [{datetime.now().strftime('%H:%M:%S')}] {fmt % args}")

    def send_json(self, data, status=200):
        body = json.dumps(data, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        if self.path in ('/', '/index.html'):
            content = HTML_PATH.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
            return

        if self.path == '/api/recap':
            self.send_json(build_recap())
            return

        if self.path == '/api/pipeline-status':
            self.send_json(_status)
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        if self.path == '/api/run-pipeline':
            if _status["running"]:
                self.send_json({"error": "Pipeline already running"}, 409)
                return
            threading.Thread(target=_run_pipeline, daemon=True).start()
            self.send_json({"status": "started"})
            return
        self.send_response(404)
        self.end_headers()


if __name__ == "__main__":
    server = HTTPServer(("localhost", PORT), Handler)
    print(f"\n  MarketPulse local server")
    print(f"  Open → http://localhost:{PORT}")
    print(f"  Ctrl+C to stop\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Stopped.")
