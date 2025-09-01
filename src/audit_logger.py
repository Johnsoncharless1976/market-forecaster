# File: src/audit_logger.py
# Title: Stage 1.0 – Implement Audit Logger for Snowflake (AUDIT_LOG)
# Commit Notes:
# - Added Python-based audit logger to create immutable records in Snowflake.
# - Logs stage, UTC timestamp, input data, output data, and pipeline job ID.
# - Computes SHA256 hash for tamper detection of inputs/outputs.
# - Uses existing snowflake-connector-python inside .venv.
# - Example provided for Stage 4 Forecast logging.

import os
import json
import datetime
import hashlib
import snowflake.connector

def audit_log(stage, input_data, output_data, pipeline_job_id="manual-run"):
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    cur = conn.cursor()

    # Create tamper-proof hash
    combined = json.dumps(input_data) + "|" + json.dumps(output_data)
    sha256_hash = hashlib.sha256(combined.encode()).hexdigest()

    # Insert into AUDIT_LOG
    cur.execute("""
        INSERT INTO AUDIT_LOG(stage, run_timestamp, input_data, output_data, pipeline_job_id, sha256_hash)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        stage,
        datetime.datetime.utcnow(),
        json.dumps(input_data),
        json.dumps(output_data),
        pipeline_job_id,
        sha256_hash
    ))

    cur.close()
    conn.close()
    print(f"✅ Audit log inserted for {stage}")

if __name__ == "__main__":
    # Example usage
    audit_log(
        stage="Stage4_Forecast",
        input_data={"SPX":6411.37,"VIX":15.78,"ATM_Straddle":25.5},
        output_data={"Bias":"Neutral","Forecast":"SPX between 6400–6450"},
        pipeline_job_id="manual-run"
    )
