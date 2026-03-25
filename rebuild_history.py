"""
Wipes MarketData and rebuilds from scratch using real Thursday closing prices.
Weeks 1-8: Jan 22 – Mar 12, 2026 (every Thursday)
"""
import os
import pyodbc
import yfinance as yf
import requests
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

load_dotenv()

DB_USER      = os.getenv("DB_USER")
DB_PASSWORD  = os.getenv("DB_PASSWORD")
FRED_API_KEY = os.getenv("FRED_API_KEY")

BASE_DATE = date(2026, 1, 22)  # Week 1

THURSDAYS = [(i + 1, BASE_DATE + timedelta(weeks=i)) for i in range(8)]

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

CATEGORIES = {
    "S&P 500":           "Equities",
    "NASDAQ Composite":  "Equities",
    "FTSE 100":          "Non-US",
    "DAX":               "Non-US",
    "VIX":               "Volatility",
    "Gold ($/oz)":       "Commodities",
    "Oil WTI ($/bbl)":   "Commodities",
    "Nat Gas ($/MMBtu)": "Commodities",
    "Fed Funds Rate":    "Rates",
    "2Y Treasury (%)":   "Rates",
    "5Y Treasury (%)":   "Rates",
    "10Y Treasury (%)":  "Rates",
    "30Y Treasury (%)":  "Rates",
    "2Y/10Y Spread":     "Rates",
}

# ─── FETCH YAHOO ─────────────────────────────────────────────────────────────

def fetch_yahoo_history():
    start = (BASE_DATE - timedelta(days=5)).strftime("%Y-%m-%d")
    end   = (BASE_DATE + timedelta(weeks=8, days=3)).strftime("%Y-%m-%d")

    results = {}
    for name, ticker in YAHOO_TICKERS.items():
        try:
            t   = yf.Ticker(ticker)
            df  = t.history(start=start, end=end, interval="1d")
            if df.empty:
                print(f"  EMPTY: {name}")
                continue

            daily = {}
            for ts, row in df.iterrows():
                d = ts.date() if hasattr(ts, 'date') else ts
                daily[d] = round(float(row["Close"]), 4)

            results[name] = daily
            print(f"  Yahoo  | {name}: {len(daily)} days fetched (first: {min(daily)}, last: {max(daily)})")
        except Exception as e:
            print(f"  Yahoo  | {name}: ERROR — {e}")
    return results

def get_on_or_before(daily, target):
    for delta in range(5):
        d = target - timedelta(days=delta)
        if d in daily:
            return daily[d]
    return None

# ─── FETCH FRED ──────────────────────────────────────────────────────────────

def fetch_fred_series(series_id, start_date, end_date):
    url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}&api_key={FRED_API_KEY}"
        f"&observation_start={start_date}&observation_end={end_date}"
        f"&file_type=json"
    )
    try:
        r = requests.get(url, timeout=10)
        obs = r.json().get("observations", [])
        result = {}
        for o in obs:
            val = o.get("value", ".")
            if val != ".":
                result[date.fromisoformat(o["date"])] = round(float(val), 4)
        return result
    except Exception as e:
        print(f"  FRED   | {series_id}: ERROR — {e}")
        return {}

# ─── MAIN ────────────────────────────────────────────────────────────────────

print("\nFetching Yahoo Finance history (daily)...")
yahoo_history = fetch_yahoo_history()

print("\nFetching FRED history...")
fred_start = (BASE_DATE - timedelta(days=7)).isoformat()
fred_end   = (BASE_DATE + timedelta(weeks=8)).isoformat()
fred_history = {}
for name, series_id in FRED_SERIES.items():
    data = fetch_fred_series(series_id, fred_start, fred_end)
    fred_history[name] = data
    print(f"  FRED   | {name}: {len(data)} observations")

# ─── BUILD ROWS ──────────────────────────────────────────────────────────────

all_rows = []
for week_num, thursday in THURSDAYS:
    week_label = f"Week {week_num}"
    print(f"\n{week_label} — {thursday}")

    for name, daily in yahoo_history.items():
        price = get_on_or_before(daily, thursday)
        if price is not None:
            all_rows.append({"AssetDate": week_label, "Category": CATEGORIES[name], "Asset": name, "ClosePrice": price})
            print(f"  {name}: {price}")
        else:
            print(f"  {name}: NO DATA")

    for name, daily in fred_history.items():
        price = get_on_or_before(daily, thursday)
        if price is not None:
            all_rows.append({"AssetDate": week_label, "Category": CATEGORIES[name], "Asset": name, "ClosePrice": price})
            print(f"  {name}: {price}")
        else:
            print(f"  {name}: NO DATA")

# ─── SAVE ────────────────────────────────────────────────────────────────────

print(f"\nConnecting to Azure SQL...")
conn = pyodbc.connect(
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=tcp:tikos-sql-server.database.windows.net,1433;"
    "Database=tikos-market-db;"
    f"UID={DB_USER};"
    f"PWD={DB_PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)
cursor = conn.cursor()

cursor.execute("DELETE FROM MarketData")
print(f"Cleared {cursor.rowcount} existing rows")

for r in all_rows:
    cursor.execute(
        "INSERT INTO MarketData (AssetDate, Category, Asset, ClosePrice) VALUES (?, ?, ?, ?)",
        r["AssetDate"], r["Category"], r["Asset"], r["ClosePrice"]
    )

conn.commit()
conn.close()
print(f"\nDone — {len(all_rows)} rows inserted for Weeks 1-8")
