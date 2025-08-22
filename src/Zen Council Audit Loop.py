"""
Stage 5 – Zen Council Audit Loop (v1.1)
---------------------------------------
Automates classification of forecast outcomes from FORECAST_POSTMORTEM.
Misses and partial hits are tagged with reason categories and stored in ZEN_AUDIT_LIBRARY.

Changes in v1.1:
- Always creates ZEN_AUDIT_LIBRARY table upfront (even if no misses exist).
- Ensures schema visibility in Snowflake for inspection.
"""

import os
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv

# -----------------------------
# 1. Load Environment Variables
# -----------------------------
load_dotenv()

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# -----------------------------
# 2. Connect to Snowflake
# -----------------------------
def get_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )

# -----------------------------
# 3. Ensure Audit Table Exists
# -----------------------------
def ensure_audit_table(conn):
    create_stmt = """
    CREATE TABLE IF NOT EXISTS ZEN_AUDIT_LIBRARY (
        DATE DATE,
        INDEX STRING,
        FORECAST_BIAS STRING,
        RESULT STRING,
        REASON_TAGS STRING,
        NOTES STRING
    )
    """
    with conn.cursor() as cur:
        cur.execute(create_stmt)

# -----------------------------
# 4. Fetch Post-Mortem Results
# -----------------------------
def fetch_postmortem(conn, limit=5):
    query = f"""
    SELECT *
    FROM FORECAST_POSTMORTEM
    ORDER BY DATE DESC
    LIMIT {limit}
    """
    return pd.read_sql(query, conn)

# -----------------------------
# 5. Basic Audit Classification
# -----------------------------
def classify_audit(row):
    """
    Placeholder logic:
    - Misses get tagged with "OTHER" for now.
    - Future: analyze volatility/news/macro for real tags.
    """
    if row["FORECAST_CORRECT"] == "❌":
        return {
            "DATE": row["DATE"],
            "INDEX": row["INDEX"],
            "FORECAST_BIAS": row["FORECAST_BIAS"],
            "RESULT": row["FORECAST_CORRECT"],
            "REASON_TAGS": "OTHER",
            "NOTES": f"Miss on {row['DATE']} - placeholder classification"
        }
    else:
        return None

# -----------------------------
# 6. Write Audit to Snowflake
# -----------------------------
def write_audit(conn, audit: dict):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ZEN_AUDIT_LIBRARY
            (DATE, INDEX, FORECAST_BIAS, RESULT, REASON_TAGS, NOTES)
            VALUES (%(DATE)s, %(INDEX)s, %(FORECAST_BIAS)s, %(RESULT)s,
                    %(REASON_TAGS)s, %(NOTES)s)
        """, audit)

# -----------------------------
# 7. Main Entrypoint
# -----------------------------
if __name__ == "__main__":
    conn = get_connection()

    # Always ensure table exists
    ensure_audit_table(conn)

    postmortems = fetch_postmortem(conn)
    print(f"🔍 Found {len(postmortems)} recent Post-Mortem records")

    for _, row in postmortems.iterrows():
        audit = classify_audit(row)
        if audit:
            print("📝 Audit record to insert:", audit)
            write_audit(conn, audit)
            print("✅ Audit written:", audit)

    conn.close()
