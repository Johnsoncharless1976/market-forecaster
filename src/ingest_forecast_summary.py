# File: src/ingest_forecast_summary.py
# Title: Stage 5 – Forecast Summary Builder (Aligned Schema)
# Commit Notes:
# - Corrected column names: SUPPORTS / RESISTANCES (not SUPPORT_LEVELS/RESISTANCE_LEVELS).
# - Inserts only fields that exist in FORECAST_SUMMARY.
# - Merges latest forecast + audit notes into a single row.

import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

REQ_VARS = [
    "SNOWFLAKE_USER","SNOWFLAKE_PASSWORD","SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_WAREHOUSE","SNOWFLAKE_DATABASE","SNOWFLAKE_SCHEMA"
]
missing = [v for v in REQ_VARS if not os.getenv(v)]
if missing:
    raise EnvironmentError(f"Missing env vars: {', '.join(missing)}")

cfg = {k: os.getenv(k) for k in REQ_VARS}

def fetch_latest_forecast(cur):
    cur.execute("""
        SELECT DATE, INDEX, FORECAST_BIAS, ATM_STRADDLE,
               SUPPORT_LEVELS, RESISTANCE_LEVELS, NOTES, FORECAST_TS
        FROM FORECAST_JOBS
        ORDER BY FORECAST_TS DESC
        LIMIT 1
    """)
    return cur.fetchone()

def fetch_latest_audit(cur):
    cur.execute("""
        SELECT NOTES, LOAD_TS
        FROM FORECAST_AUDIT_LOG
        ORDER BY LOAD_TS DESC
        LIMIT 1
    """)
    return cur.fetchone()

def insert_summary(cur, summary):
    cur.execute("""
        INSERT INTO FORECAST_SUMMARY
            (DATE, INDEX, FORECAST_BIAS, SUPPORTS,
             RESISTANCES, ATM_STRADDLE, NOTES)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        summary["DATE"], summary["INDEX"], summary["FORECAST_BIAS"],
        summary["SUPPORTS"], summary["RESISTANCES"],
        summary["ATM_STRADDLE"], summary["NOTES"]
    ))

def main():
    conn = snowflake.connector.connect(
        user=cfg["SNOWFLAKE_USER"], password=cfg["SNOWFLAKE_PASSWORD"],
        account=cfg["SNOWFLAKE_ACCOUNT"], warehouse=cfg["SNOWFLAKE_WAREHOUSE"],
        database=cfg["SNOWFLAKE_DATABASE"], schema=cfg["SNOWFLAKE_SCHEMA"]
    )
    cur = conn.cursor()

    forecast = fetch_latest_forecast(cur)
    audit = fetch_latest_audit(cur)

    if not forecast:
        print("❌ No forecast available"); return

    (f_date, idx, bias, straddle, support, resistance, notes, f_ts) = forecast
    audit_notes, load_ts = audit if audit else (None, None)

    summary = {
        "DATE": f_date,
        "INDEX": idx,
        "FORECAST_BIAS": bias,
        "SUPPORTS": support,
        "RESISTANCES": resistance,
        "ATM_STRADDLE": straddle,
        "NOTES": notes or audit_notes,
    }

    insert_summary(cur, summary)
    conn.commit()
    print(f"✅ Forecast Summary inserted for {idx} on {f_date}")

    cur.close(); conn.close()

if __name__ == "__main__":
    main()
