"""
Stage 1.4 – Daily OHLCV Ingestion with Monitoring & Validation
--------------------------------------------------------------
- Ingest SPX, ES, VIX, VVIX OHLCV daily from Yahoo Finance.
- Deduplication-safe upsert.
- Monitoring: freshness, duplicates, schema.
- Validation rules:
  * Critical (fail job): missing dates, duplicate dates, negative/zero prices.
  * Non-critical (soft log): OHLC mismatches, abnormal volume.
- Logs monitoring into MONITORING_LOG.
- Logs anomalies into DATA_QUALITY_LOG (always created at run start).
- If critical issues → job fails (CI/CD alert).
"""

import os
import sys
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
# 3. Monitoring & Validation
# -----------------------------
def log_event(conn, table, log_table, check_type, status, details=""):
    """Log results into MONITORING_LOG or DATA_QUALITY_LOG."""
    run_ts = datetime.now(UTC)
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {log_table} (
                RUN_TS TIMESTAMP,
                TABLE_NAME STRING,
                CHECK_TYPE STRING,
                STATUS STRING,
                DETAILS STRING
            )
        """)
        cur.execute(f"""
            INSERT INTO {log_table} (RUN_TS, TABLE_NAME, CHECK_TYPE, STATUS, DETAILS)
            VALUES ('{run_ts}', '{table}', '{check_type}', '{status}', '{details}')
        """)

def run_validation(conn, df: pd.DataFrame, table_name: str) -> bool:
    """Run data validation rules. Returns True if all critical checks pass."""
    valid = True

    for _, row in df.iterrows():
        date, o, h, l, c, v = row

        # Critical: negative/zero prices
        if o <= 0 or h <= 0 or l <= 0 or c <= 0:
            log_event(conn, table_name, "DATA_QUALITY_LOG", "Price Check", "FAIL", f"Invalid price on {date}")
            valid = False

        # Non-critical: OHLC mismatch
        if not (l <= o <= h and l <= c <= h):
            log_event(conn, table_name, "DATA_QUALITY_LOG", "OHLC Check", "WARN", f"OHLC mismatch on {date}")

        # Non-critical: abnormal volume (>10x mean)
        if v and df["VOLUME"].mean() > 0 and v > 10 * df["VOLUME"].mean():
            log_event(conn, table_name, "DATA_QUALITY_LOG", "Volume Check", "WARN", f"Abnormal volume {v} on {date}")

    return valid

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

    # Ensure DATA_QUALITY_LOG always exists, even if no anomalies are logged
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS DATA_QUALITY_LOG (
                RUN_TS TIMESTAMP,
                TABLE_NAME STRING,
                CHECK_TYPE STRING,
                STATUS STRING,
                DETAILS STRING
            )
        """)

    # Ingestion
    upsert_daily(conn, spx_df, "SPX_HISTORICAL")
    upsert_daily(conn, es_df, "ES_HISTORICAL")
    upsert_daily(conn, vix_df, "VIX_HISTORICAL")
    upsert_daily(conn, vvix_df, "VVIX_HISTORICAL")

    # Validation
    all_ok = True
    for df, tbl in [(spx_df,"SPX_HISTORICAL"),(es_df,"ES_HISTORICAL"),(vix_df,"VIX_HISTORICAL"),(vvix_df,"VVIX_HISTORICAL")]:
        if not run_validation(conn, df, tbl):
            all_ok = False

    conn.close()

    if not all_ok:
        print("❌ Critical validation failures detected. Failing job for CI/CD alert.")
        sys.exit(1)

    print("🎉 Ingestion + Monitoring + Validation complete. All critical checks passed.")
except Exception as e:
    print("❌ Pipeline failed:", e)
    sys.exit(1)
