import os
import pyodbc
import requests
import logging
import azure.functions as func
from datetime import datetime, timedelta

app = func.FunctionApp()

# ─── CONFIG ───────────────────────────────────────────────────────────────────

DB_USER      = os.environ.get("DB_USER")
DB_PASSWORD  = os.environ.get("DB_PASSWORD")
FRED_API_KEY = os.environ.get("FRED_API_KEY")
AV_API_KEY   = os.environ.get("ALPHA_VANTAGE_KEY")

YAHOO_TICKERS = {
    "S&P 500":           "^GSPC",
    "NASDAQ Composite":  "^IXIC",
    "FTSE 100":          "^FTSE",
    "DAX":               "^GDAXI",
    "VIX":               "^VIX",
    "Gold ($/oz)":       "GC=F",
}

FRED_SERIES = {
    "Fed Funds Rate":      "DFF",
    "2Y Treasury (%)":     "DGS2",
    "5Y Treasury (%)":     "DGS5",
    "10Y Treasury (%)":    "DGS10",
    "30Y Treasury (%)":    "DGS30",
    "2Y/10Y Spread":       "T10Y2Y",
    "Gas Regular ($/gal)": "GASREGW",  # EIA weekly retail regular unleaded pump price
    "Oil WTI ($/bbl)":     "DCOILWTICO",  # EIA WTI crude oil spot price (more accurate than futures)
}

# ─── DB ───────────────────────────────────────────────────────────────────────

def get_conn():
    return pyodbc.connect(
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=tcp:tikos-sql-server.database.windows.net,1433;"
        "Database=tikos-market-db;"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )

# ─── FETCH ────────────────────────────────────────────────────────────────────

def fetch_yahoo():
    import yfinance as yf, math
    results = {}
    end   = datetime.today()
    start = end - timedelta(days=10)  # wider window to handle futures data lag
    for name, ticker in YAHOO_TICKERS.items():
        try:
            data = yf.download(ticker, start=start.strftime("%Y-%m-%d"),
                               end=end.strftime("%Y-%m-%d"), progress=False)
            if not data.empty:
                for val in reversed(data["Close"].values.flatten()):
                    v = float(val)
                    if not math.isnan(v):
                        results[name] = round(v, 4)
                        logging.info(f"Yahoo | {name}: {results[name]}")
                        break
                else:
                    logging.warning(f"Yahoo | {name}: all nan — skipped")
        except Exception as e:
            logging.warning(f"Yahoo | {name}: ERROR — {e}")
    return results

def fetch_fred():
    results = {}
    for name, series_id in FRED_SERIES.items():
        try:
            url = (
                f"https://api.stlouisfed.org/fred/series/observations"
                f"?series_id={series_id}&api_key={FRED_API_KEY}"
                f"&sort_order=desc&limit=5&file_type=json"
            )
            r = requests.get(url, timeout=10)
            obs = r.json().get("observations", [])
            for o in obs:
                val = o.get("value", ".")
                if val != ".":
                    results[name] = round(float(val), 4)
                    logging.info(f"FRED | {name}: {results[name]}")
                    break
        except Exception as e:
            logging.warning(f"FRED | {name}: ERROR — {e}")
    return results

# ─── SAVE ─────────────────────────────────────────────────────────────────────

def save_to_db(all_data, asset_date):
    conn   = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='MarketData' AND xtype='U')
        CREATE TABLE MarketData (
            Id         INT IDENTITY(1,1) PRIMARY KEY,
            AssetDate  NVARCHAR(50),
            Category   NVARCHAR(100),
            Asset      NVARCHAR(100),
            ClosePrice FLOAT
        )
    """)

    import math
    inserted = 0
    for category, data in all_data.items():
        for asset, price in data.items():
            if price is None or (isinstance(price, float) and math.isnan(price)):
                logging.warning(f"Skipping nan value for {asset}")
                continue
            cursor.execute("""
                IF EXISTS (SELECT 1 FROM MarketData WHERE AssetDate=? AND Asset=?)
                    UPDATE MarketData SET ClosePrice=? WHERE AssetDate=? AND Asset=?
                ELSE
                    INSERT INTO MarketData (AssetDate, Category, Asset, ClosePrice)
                    VALUES (?, ?, ?, ?)
            """, asset_date, asset, price, asset_date, asset,
                 asset_date, category, asset, price)
            inserted += 1

    conn.commit()
    conn.close()
    logging.info(f"Saved {inserted} rows for {asset_date}")

# ─── MANUAL REFETCH TRIGGER ───────────────────────────────────────────────────
# POST /api/market-refetch?week=Week+10  — re-fetches live data and overwrites DB

@app.route(route="market-refetch", auth_level=func.AuthLevel.ANONYMOUS, methods=["GET", "POST"])
def market_refetch(req: func.HttpRequest) -> func.HttpResponse:
    import json
    week = req.params.get("week")
    if not week:
        base_date = datetime(2026, 1, 22)
        week_num  = ((datetime.today() - base_date).days // 7) + 1
        week = f"Week {week_num}"

    logging.info(f"Manual refetch triggered for {week}")
    try:
        yahoo = fetch_yahoo()
        fred  = fetch_fred()
        pe    = fetch_pe_ratio()

        all_data = {
            "Equities":    {k: v for k, v in yahoo.items() if k in ["S&P 500", "NASDAQ Composite"]},
            "Non-US":      {k: v for k, v in yahoo.items() if k in ["FTSE 100", "DAX"]},
            "Volatility":  {k: v for k, v in yahoo.items() if k in ["VIX"]},
            "Commodities": {**{k: v for k, v in yahoo.items() if k in ["Gold ($/oz)"]},
                            **{k: v for k, v in fred.items() if k in ["Oil WTI ($/bbl)"]}},
            "Rates":       {k: v for k, v in fred.items() if k not in ["Oil WTI ($/bbl)"]},
            "Valuation":   {"S&P 500 P/E": pe} if pe else {},
        }

        save_to_db(all_data, week)
        return func.HttpResponse(
            body=json.dumps({"status": "ok", "week": week, "data": {k: dict(v) for k, v in all_data.items()}}),
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )
    except Exception as e:
        logging.error(f"market_refetch error: {e}")
        return func.HttpResponse(json.dumps({"status": "error", "message": str(e)}), status_code=500,
                                 mimetype="application/json")

# ─── OIL PRICE BACKFILL ───────────────────────────────────────────────────────
# GET /api/market-fix-oil — corrects all weeks' WTI oil price using FRED historical data

@app.route(route="market-fix-oil", auth_level=func.AuthLevel.ANONYMOUS)
def market_fix_oil(req: func.HttpRequest) -> func.HttpResponse:
    import json
    base_date = datetime(2026, 1, 22)
    results = []

    try:
        conn   = get_conn()
        cursor = conn.cursor()

        # Get all distinct weeks in DB
        cursor.execute("SELECT DISTINCT AssetDate FROM MarketData WHERE AssetDate LIKE 'Week %'")
        weeks = [r[0] for r in cursor.fetchall()]

        for week_label in weeks:
            try:
                week_num = int(week_label.replace("Week ", "").strip())
            except ValueError:
                continue

            # Calculate the Thursday of that week (when data was collected)
            week_start = base_date + timedelta(weeks=week_num - 1)
            week_end   = week_start + timedelta(days=10)

            # Fetch WTI spot price from FRED for that week
            url = (
                f"https://api.stlouisfed.org/fred/series/observations"
                f"?series_id=DCOILWTICO&api_key={FRED_API_KEY}&file_type=json"
                f"&observation_start={week_start.strftime('%Y-%m-%d')}"
                f"&observation_end={week_end.strftime('%Y-%m-%d')}"
                f"&sort_order=desc&limit=5"
            )
            obs = requests.get(url, timeout=10).json().get("observations", [])
            price = None
            for o in obs:
                if o["value"] != ".":
                    price = round(float(o["value"]), 4)
                    break

            if price is None:
                results.append({"week": week_label, "status": "no data"})
                continue

            # Update or insert correct oil price
            cursor.execute("""
                IF EXISTS (SELECT 1 FROM MarketData WHERE AssetDate=? AND Asset=?)
                    UPDATE MarketData SET ClosePrice=? WHERE AssetDate=? AND Asset=?
                ELSE
                    INSERT INTO MarketData (AssetDate, Category, Asset, ClosePrice)
                    VALUES (?, 'Commodities', ?, ?)
            """, week_label, "Oil WTI ($/bbl)", price, week_label, "Oil WTI ($/bbl)",
                 week_label, "Oil WTI ($/bbl)", price)

            results.append({"week": week_label, "oil_price": price, "status": "updated"})
            logging.info(f"Fixed oil for {week_label}: ${price}")

        conn.commit()
        conn.close()

        return func.HttpResponse(
            body=json.dumps({"status": "ok", "fixed": results}),
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )
    except Exception as e:
        logging.error(f"market_fix_oil error: {e}")
        return func.HttpResponse(json.dumps({"status": "error", "message": str(e)}),
                                 status_code=500, mimetype="application/json")

# ─── HTTP TRIGGER ─────────────────────────────────────────────────────────────

@app.route(route="market-data", auth_level=func.AuthLevel.ANONYMOUS)
def market_api(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn   = get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT AssetDate, Category, Asset, ClosePrice FROM MarketData ORDER BY AssetDate, Category, Asset")
        rows = cursor.fetchall()
        conn.close()

        data = [
            {"AssetDate": r[0], "Category": r[1], "Asset": r[2], "ClosePrice": r[3]}
            for r in rows
        ]
        return func.HttpResponse(
            body=__import__('json').dumps(data),
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )
    except Exception as e:
        logging.error(f"market_api error: {e}")
        return func.HttpResponse(str(e), status_code=500)

# ─── PERFORMANCE ENDPOINT ────────────────────────────────────────────────────
# Returns current, 3-month-ago, and 1-year-ago prices for all assets

@app.route(route="market-performance", auth_level=func.AuthLevel.ANONYMOUS)
def market_performance(req: func.HttpRequest) -> func.HttpResponse:
    import json, time as _time

    today = datetime.today()
    d_3m  = today - timedelta(days=91)
    d_1y  = today - timedelta(days=365)

    def yahoo_chart(ticker):
        """Fetch 1y daily chart from Yahoo Finance v8 API."""
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
            f"?interval=1d&range=13mo&includePrePost=false"
        )
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=15)
        result = r.json().get("chart", {}).get("result", [])
        if not result:
            return None
        timestamps = result[0].get("timestamp", [])
        closes     = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
        return list(zip(timestamps, closes))

    def price_on(pairs, target_dt):
        """Find closing price closest to target date."""
        target_ts = _time.mktime(target_dt.timetuple())
        best = None
        best_diff = float("inf")
        for ts, price in pairs:
            if price is None: continue
            diff = abs(ts - target_ts)
            if diff < best_diff:
                best_diff = diff
                best = price
        return round(best, 4) if best is not None else None

    results = {}

    # Yahoo Finance — direct API
    for name, ticker in YAHOO_TICKERS.items():
        try:
            pairs = yahoo_chart(ticker)
            if not pairs: continue
            current = next((p for _, p in reversed(pairs) if p is not None), None)
            results[name] = {
                "current":  round(current, 4) if current else None,
                "price_3m": price_on(pairs, d_3m),
                "price_1y": price_on(pairs, d_1y),
            }
            logging.info(f"Perf | {name}: {results[name]['current']}")
        except Exception as e:
            logging.warning(f"Perf Yahoo | {name}: {e}")

    # FRED rates
    def fred_on(series_id, d):
        url = (
            f"https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={series_id}&api_key={FRED_API_KEY}&file_type=json"
            f"&observation_start={d.strftime('%Y-%m-%d')}"
            f"&observation_end={(d + timedelta(days=10)).strftime('%Y-%m-%d')}"
            f"&sort_order=asc&limit=5"
        )
        obs = requests.get(url, timeout=10).json().get("observations", [])
        for o in obs:
            if o["value"] != ".":
                return round(float(o["value"]), 4)
        return None

    for name, series_id in FRED_SERIES.items():
        try:
            cur  = fred_on(series_id, today - timedelta(days=5))
            p3m  = fred_on(series_id, d_3m)
            p1y  = fred_on(series_id, d_1y)
            results[name] = {"current": cur, "price_3m": p3m, "price_1y": p1y}
        except Exception as e:
            logging.warning(f"Perf FRED | {name}: {e}")

    return func.HttpResponse(
        body=json.dumps(results),
        mimetype="application/json",
        headers={"Access-Control-Allow-Origin": "*"}
    )

# ─── P/E RATIO ────────────────────────────────────────────────────────────────

def fetch_pe_ratio():
    """Current S&P 500 trailing P/E from SPY info."""
    try:
        import yfinance as yf
        pe = yf.Ticker("SPY").info.get("trailingPE")
        if pe:
            v = round(float(pe), 2)
            logging.info(f"PE | S&P 500 P/E: {v}")
            return v
    except Exception as e:
        logging.warning(f"PE | ERROR: {e}")
    return None

# ─── P/E ENDPOINT ─────────────────────────────────────────────────────────────
# Returns current, Jan 2026, and ~1yr-ago S&P 500 trailing P/E

@app.route(route="market-pe", auth_level=func.AuthLevel.ANONYMOUS)
def market_pe(req: func.HttpRequest) -> func.HttpResponse:
    import json, re

    result = {}

    # Current trailing P/E from yfinance
    pe = fetch_pe_ratio()
    if pe:
        result["current"] = pe

    # Historical monthly P/E from multpl.com
    try:
        r = requests.get(
            "https://www.multpl.com/s-p-500-pe-ratio/table/by-month",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=15
        )
        # Rows: <td>Mon D, YYYY</td><td>\n&#x2002;\n26.23\n</td>
        pairs = re.findall(r'<td>(\w+\s+\d+,\s*\d{4})</td>\s*<td>\s*(?:&#x2002;)?\s*([\d.]+)\s*</td>', r.text)
        date_pe = {d.strip(): float(v) for d, v in pairs}

        # Jan 2026 = beginning of year reference; fallback to Dec 2024
        for d, v in date_pe.items():
            if "Jan" in d and "2026" in d:
                result["jan_2026"] = v
                break
        if "jan_2026" not in result:
            for d, v in date_pe.items():
                if "Dec" in d and "2024" in d:
                    result["jan_2026"] = v  # closest available
                    break

        # ~1yr ago: prefer Mar 2025, fallback to Feb/Apr
        for month in ["Mar", "Feb", "Apr"]:
            for d, v in date_pe.items():
                if month in d and "2025" in d:
                    result["yr_ago"] = v
                    break
            if "yr_ago" in result:
                break

    except Exception as e:
        logging.warning(f"PE historical error: {e}")

    return func.HttpResponse(
        body=json.dumps(result),
        mimetype="application/json",
        headers={"Access-Control-Allow-Origin": "*"}
    )

# ─── TIMER TRIGGER ────────────────────────────────────────────────────────────
# Runs every Thursday at 9 PM UTC (4 PM EST / 5 PM EDT)

@app.timer_trigger(schedule="0 0 21 * * 4", arg_name="myTimer",
                   run_on_startup=False, use_monitor=False)
def fetch_weekly_market_data(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("Timer is past due — running now.")

    base_date = datetime(2026, 1, 22)
    week_num  = ((datetime.today() - base_date).days // 7) + 1
    today     = f"Week {week_num}"
    logging.info(f"Fetching market data for {today}...")

    yahoo = fetch_yahoo()
    fred  = fetch_fred()
    pe    = fetch_pe_ratio()

    all_data = {
        "Equities":    {k: v for k, v in yahoo.items() if k in ["S&P 500", "NASDAQ Composite"]},
        "Non-US":      {k: v for k, v in yahoo.items() if k in ["FTSE 100", "DAX"]},
        "Volatility":  {k: v for k, v in yahoo.items() if k in ["VIX"]},
        "Commodities": {**{k: v for k, v in yahoo.items() if k in ["Gold ($/oz)"]},
                        **{k: v for k, v in fred.items() if k in ["Oil WTI ($/bbl)"]}},
        "Rates":       {k: v for k, v in fred.items() if k not in ["Oil WTI ($/bbl)"]},
        "Valuation":   {"S&P 500 P/E": pe} if pe else {},
    }

    save_to_db(all_data, today)
    logging.info("Done.")
