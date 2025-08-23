"""
Stage 1.1 – Daily OHLCV Ingestion (Deduplication-Safe)
------------------------------------------------------
Upgrades ingestion pipeline to:
- Fetch only the latest daily OHLCV bars (SPX, ES, VIX, VVIX).
- Ensure Snowflake tables exist with full schema (DATE, OPEN, HIGH, LOW, CLOSE, VOLUME).
- Delete any overlapping dates before inserting new rows → prevents duplicates.
- Verify last 5 rows after insert for confidence.

Usage:
- Designed for daily scheduled runs (period="1d").
- Can safely be switched to "5d" if desired; deduplication will still hold.
"""

import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import yfinance as yf
from dotenv import load_dotenv

# -----------------------------
# 1. Load environment variables
# -----------------------------
load_dotenv()

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# -----------------------------
# 2. Helpers
# -----------------------------
def normalize_df(df):
    """Normalize Yahoo Finance DataFrame into DATE, OPEN, HIGH, LOW, CLOSE, VOLUME."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["DATE","OPEN","HIGH","LOW","CLOSE","VOLUME"])

    df = df.reset_index()
    df.columns = [c if not isinstance(c, tuple) else c[0] for c in df.columns]

    if "Date" in df.columns:
        df.rename(columns={"Date": "DATE"}, inplace=True)

    df = df[["DATE", "Open", "High", "Low", "Close", "Volume"]]
    df.rename(columns={"Open":"OPEN", "High":"HIGH", "Low":"LOW", 
                       "Close":"CLOSE", "Volume":"VOLUME"}, inplace=True)

    df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
    df = df.round({"OPEN":2, "HIGH":2, "LOW":2, "CLOSE":2})

    return df.dropna()

def get_spx():
    return normalize_df(yf.download("^GSPC", period="1d", interval="1d"))

def get_es():
    return normalize_df(yf.download("ES=F", period="1d", interval="1d"))

def get_vix():
    return normalize_df(yf.download("^VIX", period="1d", interval="1d"))

def get_vvix():
    return normalize_df(yf.download("^VVIX", period="1d", interval="1d"))

def upsert_daily(conn, df: pd.DataFrame, table_name: str):
    """Delete any rows for the date range being inserted, then reload clean data."""
    if df.empty:
        print(f"⚠️ No data to insert for {table_name}")
        return

    min_date = df["DATE"].min()
    max_date = df["DATE"].max()

    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                DATE DATE, 
                OPEN FLOAT, 
                HIGH FLOAT, 
                LOW FLOAT, 
                CLOSE FLOAT, 
                VOLUME FLOAT
            )
        """)
        cur.execute(
            f"DELETE FROM {table_name} WHERE DATE BETWEEN %s AND %s",
            (min_date, max_date),
        )
        print(f"🧹 Removed existing rows in {table_name} from {min_date} → {max_date}")

    write_pandas(conn, df.reset_index(drop=True), table_name)
    print(f"✅ Inserted {len(df)} rows into {table_name}")

def verify_table(conn, table_name: str):
    """Print last 5 rows from a table for verification"""
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {table_name} ORDER BY DATE DESC LIMIT 5")
        rows = cur.fetchall()
        print(f"\n📊 Last 5 rows in {table_name}:")
        for row in rows:
            print(row)

# -----------------------------
# 3. Fetch latest daily bars
# -----------------------------
spx_df = get_spx()
es_df = get_es()
vix_df = get_vix()
vvix_df = get_vvix()

print("✅ Data fetched:")
print("SPX:", spx_df.tail(1))
print("ES:", es_df.tail(1))
print("VIX:", vix_df.tail(1))
print("VVIX:", vvix_df.tail(1))

# -----------------------------
# 4. Connect + load into Snowflake
# -----------------------------
try:
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    print(f"✅ Connected to Snowflake account {SNOWFLAKE_ACCOUNT} as {SNOWFLAKE_USER}")

    upsert_daily(conn, spx_df, "SPX_HISTORICAL")
    upsert_daily(conn, es_df, "ES_HISTORICAL")
    upsert_daily(conn, vix_df, "VIX_HISTORICAL")
    upsert_daily(conn, vvix_df, "VVIX_HISTORICAL")

    verify_table(conn, "SPX_HISTORICAL")
    verify_table(conn, "ES_HISTORICAL")
    verify_table(conn, "VIX_HISTORICAL")
    verify_table(conn, "VVIX_HISTORICAL")

    print("🎉 Daily SPX, ES, VIX, VVIX OHLCV ingestion complete.")
    conn.close()
except Exception as e:
    print("❌ Snowflake load failed:", e)
    raise
