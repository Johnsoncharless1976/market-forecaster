# src/send_email.py

from datetime import datetime
import pytz
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# Snowflake connection
# -----------------------------
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

def fetch_latest(conn, table):
    with conn.cursor() as cur:
        cur.execute(f"SELECT DATE, CLOSE FROM {table} ORDER BY DATE DESC LIMIT 1")
        row = cur.fetchone()
        if not row or row[1] is None:
            return "--"
        return round(float(row[1]), 2)

spx_val = fetch_latest(conn, "SPX_HISTORICAL")
es_val = fetch_latest(conn, "ES_HISTORICAL")
vix_val = fetch_latest(conn, "VIX_HISTORICAL")
vvix_val = fetch_latest(conn, "VVIX_HISTORICAL")
conn.close()

# -----------------------------
# Timestamp (Eastern)
# -----------------------------
eastern = pytz.timezone("US/Eastern")
now_et = datetime.now(eastern)
formatted_time = now_et.strftime("%b %d, %Y (%I:%M %p ET)")

# -----------------------------
# Email body (same format you had)
# -----------------------------
email_body = f"""
📈 ZeroDay Zen Forecast – {formatted_time}

SPX Spot: {spx_val}
/ES: {es_val}
VIX: {vix_val}
VVIX: {vvix_val}
"""

print(email_body)
