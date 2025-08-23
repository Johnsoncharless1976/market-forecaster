# 📄 File: src/ingest_vix_term_structure.py
#
# 📌 Title
# Zen Council – Stage 2.3 VIX Term Structure Ingestion (Idempotent MERGE)
#
# 📝 Commit Notes
# Commit Title: ETL: add Stage 2.3 VIX term structure ingestion (CSV-from-stage, MERGE-idempotent)
# Commit Message:
# - Loads VIX futures curve from a Snowflake external stage CSV into VIX_TERM_STRUCTURE.
# - Expected staged file path: @FORECASTING_STAGE/vix_term_structure.csv.gz (or .csv)
# - CSV columns (header required): DATE, CONTRACT_MNEMONIC, EXPIRY, SETTLE, SOURCE
# - Computes DAYS_TO_EXPIRY server-side, MERGE on (DATE, CONTRACT_MNEMONIC) → idempotent re-runs.
# - No external HTTP calls; fully CI-friendly. If no file present, exits gracefully with a clear message.
# - Next: add CI job after Stage 2.2 correlations; optional validator to assert monotone curve by tenor.

import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

REQ_VARS = [
    "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA",
]
missing = [v for v in REQ_VARS if not os.getenv(v)]
if missing:
    raise EnvironmentError(f"Missing required env vars: {', '.join(missing)}")

cfg = {k: os.getenv(k) for k in REQ_VARS}

# Stage location and filename are configurable via env, default to FORECASTING_STAGE
STAGE_NAME = os.getenv("VIX_TS_STAGE", "FORECASTING_STAGE")
STAGE_FILE = os.getenv("VIX_TS_FILE", "vix_term_structure.csv.gz")  # allow .csv as well

DDL_STAGING = f"""
CREATE OR REPLACE TEMP TABLE STG_VIX_TERM_STRUCTURE (
    DATE STRING,
    CONTRACT_MNEMONIC STRING,
    EXPIRY STRING,
    SETTLE FLOAT,
    SOURCE STRING
)
"""

COPY_INTO = f"""
COPY INTO STG_VIX_TERM_STRUCTURE
FROM @{STAGE_NAME}/{STAGE_FILE}
FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"')
ON_ERROR='ABORT_STATEMENT'
"""

MERGE_SQL = """
MERGE INTO VIX_TERM_STRUCTURE AS T
USING (
    SELECT
        TO_DATE(DATE) AS DATE,
        CONTRACT_MNEMONIC,
        TRY_TO_DATE(EXPIRY) AS EXPIRY,
        DATEDIFF('day', TO_DATE(DATE), TRY_TO_DATE(EXPIRY)) AS DAYS_TO_EXPIRY,
        SETTLE,
        SOURCE
    FROM STG_VIX_TERM_STRUCTURE
) AS S
ON T.DATE = S.DATE AND T.CONTRACT_MNEMONIC = S.CONTRACT_MNEMONIC
WHEN MATCHED THEN UPDATE SET
    T.EXPIRY         = S.EXPIRY,
    T.DAYS_TO_EXPIRY = S.DAYS_TO_EXPIRY,
    T.SETTLE         = S.SETTLE,
    T.SOURCE         = S.SOURCE,
    T.LOAD_TS        = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
    (DATE, CONTRACT_MNEMONIC, EXPIRY, DAYS_TO_EXPIRY, SETTLE, SOURCE)
VALUES
    (S.DATE, S.CONTRACT_MNEMONIC, S.EXPIRY, S.DAYS_TO_EXPIRY, S.SETTLE, S.SOURCE)
"""

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

    # 1) Ensure target table exists (no-op if already created)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS VIX_TERM_STRUCTURE LIKE VIX_TERM_STRUCTURE
    """)  # safe: LIKE ensures no change if exists

    # 2) Create temp staging
    cur.execute(DDL_STAGING)

    # 3) Attempt COPY from stage; if missing file, exit with a clear message
    try:
        cur.execute(COPY_INTO)
        # Verify we actually loaded rows
        cur.execute("SELECT COUNT(*) FROM STG_VIX_TERM_STRUCTURE")
        cnt = cur.fetchone()[0]
        if cnt == 0:
            print(f"⚠ No rows loaded from @{STAGE_NAME}/{STAGE_FILE}. Nothing to MERGE.")
            cur.close(); conn.close()
            return
    except snowflake.connector.errors.ProgrammingError as e:
        msg = str(e).lower()
        if "file does not exist" in msg or "not found" in msg:
            print(f"⚠ Staged file not found: @{STAGE_NAME}/{STAGE_FILE}. Skipping load.")
            cur.close(); conn.close()
            return
        raise

    # 4) MERGE → idempotent
    cur.execute(MERGE_SQL)
    conn.commit()
    cur.close(); conn.close()
    print("✅ VIX term structure ingestion complete (MERGE).")

if __name__ == "__main__":
    main()
