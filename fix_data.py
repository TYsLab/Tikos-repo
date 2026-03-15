import os
import pyodbc
from dotenv import load_dotenv

load_dotenv()

DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

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

# Delete all rows for weeks 7 and 8 and the Mar 14 fetch — all bad data
cursor.execute("DELETE FROM MarketData WHERE AssetDate IN ('Mar 5', 'Mar 12', 'Mar 14, 2026')")
deleted = cursor.rowcount
conn.commit()
conn.close()

print(f"Deleted {deleted} rows (Mar 5, Mar 12, Mar 14, 2026)")
