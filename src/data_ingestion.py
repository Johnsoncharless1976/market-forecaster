"""
Stage 1.1 + 1.2 – Daily OHLCV Ingestion with Monitoring (Polished)
------------------------------------------------------------------
- Ingest SPX, ES, VIX, VVIX OHLCV daily from Yahoo Finance.
- Deduplication-safe upsert: removes any overlap in date range before insert.
- Verify pipeline health (schema, freshness, duplicates).
- Log verification results into MONITORING_LOG table in Snowflake.
- Patched Yahoo auto_adjust warning.
- Patched datetime deprecation warning.
"""

import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import yfinance as yf
from dotenv import load_dotenv
from datetime import datetime, UTC

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
def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
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

def get_spx(): return normalize_df(yf.download("^GSPC", period="1d", interval="1d", auto_adjust=False))
def get_es():  return normalize_df(yf.download("ES=F", period="1d", interval="1d", auto_adjust=False))
def get_vix(): return normalize_df(yf.download("^VIX", period="1d", interval="1d", auto_adjust=False))
def get_vvix():return normalize_df(yf.download("^VVIX", period="1d", interval="1d", auto_adjust=False))

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
            f"DELETE FROM {table_name} WHERE DATE BETWEEN '{min_date}' AND '{max_date}'"
        )
        print(f"🧹 Removed existing rows in {table_name} from {min_date} → {max_date}")

    write_pandas(conn, df.reset_index(drop=True), table_name)
    print(f"✅ Inserted {len(df)} rows into {table_name}")

# -----------------------------
# 3. Monitoring Helpers
# -----------------------------
def log_monitoring(conn, table_name: str, check_type: str, status: str, details: str = ""):
    """Insert monitoring results into MONITORING_LOG."""
    run_ts = datetime.now(UTC)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS MONITORING_LOG (
                RUN_TS TIMESTAMP, 
                TABLE_NAME STRING, 
                CHECK_TYPE STRING, 
                STATUS STRING, 
                DETAILS STRING
            )
        """)
        cur.execute(f"""
            INSERT INTO MONITORING_LOG (RUN_TS, TABLE_NAME, CHECK_TYPE, STATUS, DETAILS)
            VALUES ('{run_ts}', '{table_name}', '{check_type}', '{status}', '{details}')
        """)

def run_monitoring(conn, table_name: str):
    """Run freshness, duplicates, schema checks for a table and log results."""
    try:
        with conn.cursor() as cur:
            # Freshness
            cur.execute(f"SELECT MAX(DATE) FROM {table_name}")
            last_date = cur.fetchone()[0]
            if last_date:
                log_monitoring(conn, table_name, "Freshness", "OK", f"Last date = {last_date}")
            else:
                log_monitoring(conn, table_name, "Freshness", "FAIL", "No rows found")

            # Duplicates
            cur.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT DATE FROM {table_name} GROUP BY DATE HAVING COUNT(*) > 1
                )
            """)
            dups = cur.fetchone()[0]
            if dups == 0:
                log_monitoring(conn, table_name, "Duplicates", "OK")
            else:
                log_monitoring(conn, table_name, "Duplicates", "FAIL", f"{dups} duplicate dates found")

            # Schema
            cur.execute(f"DESC TABLE {table_name}")
            cols = [row[0] for row in cur.fetchall()]
            expected = ["DATE","OPEN","HIGH","LOW","CLOSE","VOLUME"]
            if all(col in cols for col in expected):
                log_monitoring(conn, table_name, "Schema", "OK")
            else:
                log_monitoring(conn, table_name, "Schema", "FAIL", f"Columns present: {cols}")

    except Exception as e:
        log_monitoring(conn, table_name, "General", "FAIL", str(e))

# -----------------------------
# 4. Main Run
# -----------------------------
spx_df, es_df, vix_df, vvix_df = get_spx(), get_es(), get_vix(), get_vvix()

print("✅ Data fetched:")
print("SPX:", spx_df.tail(1))
print("ES:", es_df.tail(1))
print("VIX:", vix_df.tail(1))
print("VVIX:", vvix_df.tail(1))

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

    # Ingestion
    upsert_daily(conn, spx_df, "SPX_HISTORICAL")
    upsert_daily(conn, es_df, "ES_HISTORICAL")
    upsert_daily(conn, vix_df, "VIX_HISTORICAL")
    upsert_daily(conn, vvix_df, "VVIX_HISTORICAL")

    # Monitoring
    for tbl in ["SPX_HISTORICAL","ES_HISTORICAL","VIX_HISTORICAL","VVIX_HISTORICAL"]:
        run_monitoring(conn, tbl)

    print("🎉 Ingestion + Monitoring complete. Results logged in MONITORING_LOG.")
    conn.close()
except Exception as e:
    print("❌ Pipeline failed:", e)
    raise
