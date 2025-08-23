# 📄 File: src/ingest_audit_loop.py
#
# 📌 Title
# Zen Council – Stage 4 Audit Loop ETL
#
# 📝 Commit Notes
# Commit Title: ETL: implement Stage 4 audit loop with type-safe inserts (MERGE)
# Commit Message:
# - Reads forecasts from FORECAST_JOBS and outcomes from SPX/ES historicals.
# - Grades forecasts with placeholder logic (Correct?, Range Hit?, RSI Aligned?).
# - Casts all values into native Python types to prevent Snowflake binding errors.
# - MERGEs results into FORECAST_AUDIT_LOG safely and idempotently.
# - Provides the audit discipline loop to track forecast accuracy daily.

import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

REQUIRED_VARS = [
    "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA",
]
missing = [v for v in REQUIRED_VARS if not os.getenv(v)]
if missing:
    raise EnvironmentError(f"Missing env vars: {', '.join(missing)}")

cfg = {k: os.getenv(k) for k in REQUIRED_VARS}

def fetch_forecasts(cur) -> pd.DataFrame:
    cur.execute("SELECT DATE, INDEX, FORECAST_BIAS FROM FORECAST_JOBS ORDER BY DATE")
    df = pd.DataFrame(cur.fetchall(), columns=["DATE", "INDEX", "FORECAST_BIAS"])
    df["DATE"] = pd.to_datetime(df["DATE"])
    return df

def fetch_outcomes(cur) -> pd.DataFrame:
    cur.execute("""
        SELECT DATE, 'SPX' AS INDEX, CLOSE FROM SPX_HISTORICAL
        UNION ALL
        SELECT DATE, 'ES' AS INDEX, CLOSE FROM ES_HISTORICAL
    """)
    df = pd.DataFrame(cur.fetchall(), columns=["DATE", "INDEX", "CLOSE"])
    df["DATE"] = pd.to_datetime(df["DATE"])
    return df

def grade_forecasts(forecasts: pd.DataFrame, outcomes: pd.DataFrame) -> pd.DataFrame:
    df = forecasts.merge(outcomes, on=["DATE", "INDEX"], how="inner")
    df["FORECAST_CORRECT"] = True
    df["RANGE_HIT"] = True
    df["RSI_ALIGNED"] = True
    df["NOTES"] = "Auto-graded placeholder"
    return df

def merge_audit(df: pd.DataFrame, cur):
    cur.execute("""
        CREATE OR REPLACE TEMP TABLE STG_FORECAST_AUDIT_LOG (
            DATE DATE,
            INDEX STRING,
            FORECAST_BIAS STRING,
            FORECAST_CORRECT BOOLEAN,
            RANGE_HIT BOOLEAN,
            RSI_ALIGNED BOOLEAN,
            NOTES STRING
        )
    """)

    rows = [
        (
            pd.to_datetime(r.DATE).date(),
            str(r.INDEX),
            str(r.FORECAST_BIAS) if r.FORECAST_BIAS is not None else None,
            bool(r.FORECAST_CORRECT) if r.FORECAST_CORRECT is not None else None,
            bool(r.RANGE_HIT) if r.RANGE_HIT is not None else None,
            bool(r.RSI_ALIGNED) if r.RSI_ALIGNED is not None else None,
            str(r.NOTES) if r.NOTES is not None else None,
        )
        for r in df.itertuples(index=False)
    ]

    cur.executemany("""
        INSERT INTO STG_FORECAST_AUDIT_LOG
        (DATE, INDEX, FORECAST_BIAS, FORECAST_CORRECT, RANGE_HIT, RSI_ALIGNED, NOTES)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, rows)

    cur.execute("""
        MERGE INTO FORECAST_AUDIT_LOG T
        USING STG_FORECAST_AUDIT_LOG S
        ON T.DATE = S.DATE AND T.INDEX = S.INDEX
        WHEN MATCHED THEN UPDATE SET
            T.FORECAST_BIAS = S.FORECAST_BIAS,
            T.FORECAST_CORRECT = S.FORECAST_CORRECT,
            T.RANGE_HIT = S.RANGE_HIT,
            T.RSI_ALIGNED = S.RSI_ALIGNED,
            T.NOTES = S.NOTES,
            T.LOAD_TS = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (DATE, INDEX, FORECAST_BIAS, FORECAST_CORRECT, RANGE_HIT, RSI_ALIGNED, NOTES)
        VALUES
            (S.DATE, S.INDEX, S.FORECAST_BIAS, S.FORECAST_CORRECT, S.RANGE_HIT, S.RSI_ALIGNED, S.NOTES)
    """)

def main():
    conn = snowflake.connector.connect(
        user=cfg["SNOWFLAKE_USER"],
        password=cfg["SNOWFLAKE_PASSWORD"],
        account=cfg["SNOWFLAKE_ACCOUNT"],
        warehouse=cfg["SNOWFLAKE_WAREHOUSE"],
        database=cfg["SNOWFLAKE_DATABASE"],
        schema=cfg["SNOWFLAKE_SCHEMA"],
    )
    cur = conn.cursor()

    forecasts = fetch_forecasts(cur)
    outcomes = fetch_outcomes(cur)
    graded = grade_forecasts(forecasts, outcomes)
    merge_audit(graded, cur)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Stage 4 Audit Loop ingestion complete (MERGE).")

if __name__ == "__main__":
    main()
