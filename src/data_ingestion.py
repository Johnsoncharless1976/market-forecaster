"""
Stage 1.5.5 – Historical Backfill Validation (Market-Calendar + COVID Aware)
----------------------------------------------------------------------------
Purpose:
- Validate 5y backfill vs daily ingestion.
- Uses official NYSE market calendar (not just federal holidays).
- COVID-era window (2020-01-01 → 2021-06-30): gaps logged as WARN, not FAIL.
- Uses LOAD_TS to detect backfill vs daily cutoff.
- Prints summary + FAIL details before exit.

Critical FAIL:
- Missing non-holiday trading days (outside COVID window)
- Empty table
- Overlap mismatch (only when multiple LOAD_TS exist)
- OHLC mismatch > tolerance

Warnings only:
- Only one LOAD_TS group (backfill-only mode)
- COVID-era missing dates
- Empty partition at cutoff
"""

import os, sys
import pandas as pd
import snowflake.connector
from datetime import datetime, UTC
from dotenv import load_dotenv
from collections import Counter
import pandas_market_calendars as mcal

# -----------------------------
# 1. Load env vars
# -----------------------------
load_dotenv()
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# -----------------------------
# 2. Logging
# -----------------------------
results = Counter()
fail_messages = []

def log_event(conn, table, log_table, check_type, status, details=""):
    run_ts = datetime.now(UTC)
    results[status] += 1
    if status == "FAIL":
        fail_messages.append(f"{table} | {check_type} | {details}")
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

# -----------------------------
# 3. Validation
# -----------------------------
def fetch_table(conn, table_name: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(f"SELECT DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, LOAD_TS FROM {table_name} ORDER BY DATE")
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=cols)

def check_continuity(conn, df, table_name) -> bool:
    ok = True
    df["DATE"] = pd.to_datetime(df["DATE"])

    nyse = mcal.get_calendar("XNYS")
    trading_days = nyse.valid_days(start_date=df["DATE"].min(), end_date=df["DATE"].max()).date

    missing = sorted(set(trading_days) - set(df["DATE"].dt.date))

    covid_start = pd.Timestamp("2020-01-01").date()
    covid_end   = pd.Timestamp("2021-06-30").date()

    covid_missing = [d for d in missing if covid_start <= d <= covid_end]
    real_missing  = [d for d in missing if d < covid_start or d > covid_end]

    if real_missing:
        log_event(conn, table_name, "MONITORING_LOG", "Continuity Check", "FAIL",
                  f"Missing trading days: {real_missing[:5]}...")
        ok = False
    if covid_missing:
        log_event(conn, table_name, "MONITORING_LOG", "Continuity Check", "WARN",
                  f"COVID-era gaps: {covid_missing[:5]}...")
    if not missing:
        log_event(conn, table_name, "MONITORING_LOG", "Continuity Check", "OK")
    return ok

def detect_cutoff(df):
    ts_groups = df.groupby("LOAD_TS")["DATE"].max().sort_index()
    if len(ts_groups) < 2:
        return None
    return ts_groups.iloc[0]

def check_overlap_and_integrity(conn, df, table_name, tolerance=0.005) -> bool:
    ok = True
    cutoff = detect_cutoff(df)
    if cutoff is None:
        log_event(conn, table_name, "MONITORING_LOG", "Data Split", "WARN",
                  "Only one LOAD_TS group (backfill-only).")
        return ok
    backfill = df[df["DATE"] <= cutoff]
    daily = df[df["DATE"] >= cutoff]
    if backfill.empty or daily.empty:
        log_event(conn, table_name, "MONITORING_LOG", "Data Split", "WARN",
                  "Backfill or daily empty.")
        return ok
    if backfill["DATE"].max() != daily["DATE"].min():
        log_event(conn, table_name, "MONITORING_LOG", "Overlap Check", "FAIL",
                  f"Backfill last={backfill['DATE'].max()} vs Daily first={daily['DATE'].min()}")
        ok = False
    else:
        log_event(conn, table_name, "MONITORING_LOG", "Overlap Check", "OK")
    overlap = pd.merge(backfill.tail(1), daily.head(1), on="DATE", suffixes=("_HIST","_DAILY"))
    for _, row in overlap.iterrows():
        for col in ["OPEN","HIGH","LOW","CLOSE"]:
            val_hist, val_daily = row[f"{col}_HIST"], row[f"{col}_DAILY"]
            if abs(val_hist - val_daily) / max(val_hist,1e-9) > tolerance:
                log_event(conn, table_name, "DATA_QUALITY_LOG", f"{col} Integrity", "FAIL",
                          f"{col} mismatch {row['DATE']} hist={val_hist} daily={val_daily}")
                ok = False
            else:
                log_event(conn, table_name, "DATA_QUALITY_LOG", f"{col} Integrity", "OK")
    return ok

# -----------------------------
# 4. Main Run
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
    print(f"✅ Connected to Snowflake {SNOWFLAKE_ACCOUNT}")

    all_ok = True
    for symbol in ["SPX","ES","VIX","VVIX"]:
        tbl = f"{symbol}_HISTORICAL"
        df = fetch_table(conn, tbl)
        if df.empty:
            log_event(conn, symbol, "MONITORING_LOG", "Data Availability", "FAIL", "Empty table")
            all_ok = False
            continue
        if not check_continuity(conn, df, tbl): all_ok = False
        if not check_overlap_and_integrity(conn, df, tbl): all_ok = False

    conn.close()

    print(f"📊 Summary → OK={results['OK']} | WARN={results['WARN']} | FAIL={results['FAIL']}")
    if results["FAIL"] > 0:
        print("❌ FAIL Details:")
        for msg in fail_messages[-10:]:
            print("   -", msg)
        sys.exit(1)

    print("🎉 Stage 1.5.5 Historical Backfill Validation complete. All checks passed.")

except Exception as e:
    print("❌ Stage 1.5.5 job failed:", e)
    sys.exit(1)

