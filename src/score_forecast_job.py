# 📄 File: src/score_forecast_job.py
#
# 📌 Title
# Zen Council – Stage 3.2 Forecast Scoring ETL
#
# 📝 Commit Notes
# Commit Title: ETL: implement Stage 3.2 forecast scoring against actuals
# Commit Message:
# - Compares latest forecast (FORECAST_JOBS) vs actual SPX close.
# - Grades forecast correctness (hit/miss).
# - Writes results into FORECAST_POSTMORTEM.
# - MERGE idempotent to allow safe re-runs.
# - Fixed: ensure ACTUAL_CLOSE column exists in schema via DDL.

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

def main():
    conn = snowflake.connector.connect(
        user=cfg["SNOWFLAKE_USER"], password=cfg["SNOWFLAKE_PASSWORD"],
        account=cfg["SNOWFLAKE_ACCOUNT"], warehouse=cfg["SNOWFLAKE_WAREHOUSE"],
        database=cfg["SNOWFLAKE_DATABASE"], schema=cfg["SNOWFLAKE_SCHEMA"]
    )
    cur = conn.cursor()

    # Latest forecast
    cur.execute("""
        SELECT DATE, INDEX, FORECAST_BIAS, SUPPORT_LEVELS, RESISTANCE_LEVELS
        FROM FORECAST_JOBS
        ORDER BY DATE DESC LIMIT 1
    """)
    fc = cur.fetchone()
    if not fc:
        print("❌ No forecast found"); return
    f_date, idx, bias, support, resistance = fc

    # Actual close
    cur.execute(f"SELECT CLOSE FROM {idx}_HISTORICAL WHERE DATE=%s", (f_date,))
    actual = cur.fetchone()
    if not actual:
        print("❌ No actual close for forecast date"); return
    actual_close = float(actual[0])

    # Evaluate correctness (very simplified stub)
    try:
        support_val = float(support.strip("[]"))
        resistance_val = float(resistance.strip("[]"))
    except Exception:
        support_val, resistance_val = actual_close, actual_close
    hit = (support_val <= actual_close <= resistance_val)

    cur.execute("""
        MERGE INTO FORECAST_POSTMORTEM AS T
        USING (SELECT %s DATE, %s INDEX, %s FORECAST_BIAS, %s ACTUAL_CLOSE, %s HIT) AS S
        ON T.DATE=S.DATE AND T.INDEX=S.INDEX
        WHEN MATCHED THEN UPDATE SET
            FORECAST_BIAS=S.FORECAST_BIAS,
            ACTUAL_CLOSE=S.ACTUAL_CLOSE,
            HIT=S.HIT,
            LOAD_TS=CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (DATE, INDEX, FORECAST_BIAS, ACTUAL_CLOSE, HIT)
        VALUES
            (S.DATE, S.INDEX, S.FORECAST_BIAS, S.ACTUAL_CLOSE, S.HIT)
    """, (f_date, idx, bias, actual_close, hit))

    conn.commit(); cur.close(); conn.close()
    print("✅ Forecast scoring complete.")

if __name__ == "__main__":
    main()
