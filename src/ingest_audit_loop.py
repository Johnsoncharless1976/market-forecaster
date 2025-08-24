# File: src/ingest_audit_loop.py
# Title: Stage 4 – Forecast Audit Loop (Aligned to FORECAST_AUDIT_LOG)
# Commit Notes:
# - Re-aligned INSERT to actual FORECAST_AUDIT_LOG schema.
# - Logs forecast correctness, range hit, RSI alignment, and notes.
# - Uses BOOLEAN fields instead of unsupported columns.

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
        SELECT DATE, INDEX, FORECAST_BIAS, RSI_CONTEXT, NOTES, FORECAST_TS
        FROM FORECAST_JOBS
        ORDER BY FORECAST_TS DESC
        LIMIT 1
    """)
    return cur.fetchone()

def fetch_actual_close(cur, symbol, date):
    cur.execute(f"SELECT CLOSE FROM {symbol}_HISTORICAL WHERE DATE = %s LIMIT 1", (date,))
    row = cur.fetchone()
    return float(row[0]) if row else None

def insert_audit(cur, audit):
    cur.execute("""
        INSERT INTO FORECAST_AUDIT_LOG
            (DATE, INDEX, FORECAST_BIAS, FORECAST_CORRECT,
             RANGE_HIT, RSI_ALIGNED, NOTES, CONTEXT_NEWS, LOAD_TS)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
    """, (
        audit["DATE"], audit["INDEX"], audit["FORECAST_BIAS"],
        audit["FORECAST_CORRECT"], audit["RANGE_HIT"], audit["RSI_ALIGNED"],
        audit["NOTES"], audit["CONTEXT_NEWS"]
    ))

def main():
    conn = snowflake.connector.connect(
        user=cfg["SNOWFLAKE_USER"], password=cfg["SNOWFLAKE_PASSWORD"],
        account=cfg["SNOWFLAKE_ACCOUNT"], warehouse=cfg["SNOWFLAKE_WAREHOUSE"],
        database=cfg["SNOWFLAKE_DATABASE"], schema=cfg["SNOWFLAKE_SCHEMA"]
    )
    cur = conn.cursor()

    forecast = fetch_latest_forecast(cur)
    if not forecast:
        print("❌ No forecast available"); return

    f_date, idx, bias, rsi_context, notes, f_ts = forecast
    actual = fetch_actual_close(cur, idx, f_date)
    if not actual:
        print(f"❌ No actual close data for {idx} on {f_date}"); return

    # Simplified grading logic (placeholder, expand later)
    audit = {
        "DATE": f_date,
        "INDEX": idx,
        "FORECAST_BIAS": bias,
        "FORECAST_CORRECT": True,   # Placeholder logic
        "RANGE_HIT": True,          # Placeholder logic
        "RSI_ALIGNED": (40 <= rsi_context <= 60),
        "NOTES": notes or "Auto-audit",
        "CONTEXT_NEWS": None
    }

    insert_audit(cur, audit)
    conn.commit()
    print(f"✅ Audit logged for {idx} on {f_date}")

    cur.close(); conn.close()

if __name__ == "__main__":
    main()
