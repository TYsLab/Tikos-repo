import os
import pyodbc
import yfinance as yf
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

DB_USER         = os.getenv("DB_USER")
DB_PASSWORD     = os.getenv("DB_PASSWORD")
FRED_API_KEY    = os.getenv("FRED_API_KEY")
AV_API_KEY      = os.getenv("ALPHA_VANTAGE_KEY")

# ─── DB CONNECTION ────────────────────────────────────────────────────────────

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

# ─── YAHOO FINANCE ────────────────────────────────────────────────────────────
# Free, no key needed

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

def fetch_yahoo():
    results = {}
    end   = datetime.today()
    start = end - timedelta(days=7)
    for name, ticker in YAHOO_TICKERS.items():
        try:
            data = yf.download(ticker, start=start.strftime("%Y-%m-%d"),
                               end=end.strftime("%Y-%m-%d"), progress=False)
            if not data.empty:
                results[name] = round(float(data["Close"].iloc[-1]), 4)
                print(f"  Yahoo  | {name}: {results[name]}")
        except Exception as e:
            print(f"  Yahoo  | {name}: ERROR — {e}")
    return results

# ─── FRED ─────────────────────────────────────────────────────────────────────
# Rates data

FRED_SERIES = {
    "Fed Funds Rate":   "DFF",
    "2Y Treasury (%)":  "DGS2",
    "5Y Treasury (%)":  "DGS5",
    "10Y Treasury (%)": "DGS10",
    "30Y Treasury (%)": "DGS30",
    "2Y/10Y Spread":    "T10Y2Y",
}

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
                    print(f"  FRED   | {name}: {results[name]}")
                    break
        except Exception as e:
            print(f"  FRED   | {name}: ERROR — {e}")
    return results

# ─── ALPHA VANTAGE ────────────────────────────────────────────────────────────
# Additional equity data (25 requests/day on free tier)

AV_SYMBOLS = {
    # Add any extra tickers you want here
    # "Symbol Label": "TICKER",
}

def fetch_alpha_vantage():
    results = {}
    for name, symbol in AV_SYMBOLS.items():
        try:
            url = (
                f"https://www.alphavantage.co/query"
                f"?function=GLOBAL_QUOTE&symbol={symbol}&apikey={AV_API_KEY}"
            )
            r = requests.get(url, timeout=10)
            price = r.json().get("Global Quote", {}).get("05. price")
            if price:
                results[name] = round(float(price), 4)
                print(f"  AlphaV | {name}: {results[name]}")
        except Exception as e:
            print(f"  AlphaV | {name}: ERROR — {e}")
    return results

# ─── SAVE TO AZURE SQL ────────────────────────────────────────────────────────

def save_to_db(all_data, asset_date):
    conn   = get_conn()
    cursor = conn.cursor()

    # Ensure table exists
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
            # Upsert — update if exists, insert if not
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
    print(f"\nSaved {inserted} rows for {asset_date}")

# ─── MAIN ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    base_date = datetime(2026, 1, 22)
    week_num  = ((datetime.today() - base_date).days // 7) + 1
    today     = f"Week {week_num}"
    print(f"\nFetching market data for {today}...\n")

    print("[ Yahoo Finance ]")
    yahoo = fetch_yahoo()

    print("\n[ FRED ]")
    fred = fetch_fred()

    print("\n[ Alpha Vantage ]")
    av = fetch_alpha_vantage()

    all_data = {
        "Equities":    {k: v for k, v in yahoo.items() if k in ["S&P 500", "NASDAQ Composite"]},
        "Non-US":      {k: v for k, v in yahoo.items() if k in ["FTSE 100", "DAX"]},
        "Volatility":  {k: v for k, v in yahoo.items() if k in ["VIX"]},
        "Commodities": {k: v for k, v in yahoo.items() if k in ["Gold ($/oz)", "Oil WTI ($/bbl)", "Nat Gas ($/MMBtu)"]},
        "Rates":       fred,
        "Other":       av,
    }

    save_to_db(all_data, today)
