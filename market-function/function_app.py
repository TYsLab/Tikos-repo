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
    "Oil WTI ($/bbl)":   "CL=F",
    "Nat Gas ($/MMBtu)": "NG=F",
}

FRED_SERIES = {
    "Fed Funds Rate":   "DFF",
    "2Y Treasury (%)":  "DGS2",
    "5Y Treasury (%)":  "DGS5",
    "10Y Treasury (%)": "DGS10",
    "30Y Treasury (%)": "DGS30",
    "2Y/10Y Spread":    "T10Y2Y",
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
    import yfinance as yf
    results = {}
    end   = datetime.today()
    start = end - timedelta(days=7)
    for name, ticker in YAHOO_TICKERS.items():
        try:
            data = yf.download(ticker, start=start.strftime("%Y-%m-%d"),
                               end=end.strftime("%Y-%m-%d"), progress=False)
            if not data.empty:
                val = data["Close"].iloc[-1]
                results[name] = round(float(val.values[0]) if hasattr(val, 'values') else float(val), 4)
                logging.info(f"Yahoo | {name}: {results[name]}")
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

    inserted = 0
    for category, data in all_data.items():
        for asset, price in data.items():
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
    import json, yfinance as yf

    today   = datetime.today()
    d_3m    = today - timedelta(days=91)
    d_1y    = today - timedelta(days=365)
    start   = d_1y - timedelta(days=10)

    def closest(df, target):
        ts = target.replace(tzinfo=None)
        idx = df.index.searchsorted(ts)
        if idx >= len(df): idx = len(df) - 1
        if idx > 0 and abs((df.index[idx-1] - ts).days) < abs((df.index[idx] - ts).days):
            idx -= 1
        return round(float(df["Close"].iloc[idx]), 4)

    results = {}

    # Yahoo Finance
    for name, ticker in YAHOO_TICKERS.items():
        try:
            t  = yf.Ticker(ticker)
            df = t.history(start=start.strftime("%Y-%m-%d"), interval="1d")
            if df.empty: continue
            df.index = df.index.tz_localize(None) if df.index.tzinfo else df.index
            results[name] = {
                "current":  round(float(df["Close"].iloc[-1]), 4),
                "price_3m": closest(df, d_3m),
                "price_1y": closest(df, d_1y),
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

    all_data = {
        "Equities":    {k: v for k, v in yahoo.items() if k in ["S&P 500", "NASDAQ Composite"]},
        "Non-US":      {k: v for k, v in yahoo.items() if k in ["FTSE 100", "DAX"]},
        "Volatility":  {k: v for k, v in yahoo.items() if k in ["VIX"]},
        "Commodities": {k: v for k, v in yahoo.items() if k in ["Gold ($/oz)", "Oil WTI ($/bbl)", "Nat Gas ($/MMBtu)"]},
        "Rates":       fred,
    }

    save_to_db(all_data, today)
    logging.info("Done.")
