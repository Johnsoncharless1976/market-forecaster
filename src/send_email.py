# src/send_email.py

from datetime import datetime
import pytz
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
# Email body
# -----------------------------
email_body = f"""
📌 ZeroDay Zen Forecast – {formatted_time}

SPX: {spx_val}
/ES: {es_val}
VIX: {vix_val}
VVIX: {vvix_val}

🎯 Bias
Neutral

🗝️ Key Levels
Resistance: 6423.25
Support: 6390.25

📉 Probable Path
Base Case: SPX → 6420–6425 zone
Bear Case: < 6390 rejection
Bull Case: if > 6425, can extend 20 pts

💡 Trade Implications
Neutral bias → consider Iron Condor around straddle range.

📰 Context / News Check
Markets steady ahead of Powell speech
VIX / VVIX confirm calm conditions

📊 Summary
Bias: Neutral. Watch 6420–6425 zone and volatility cues.
"""

print(email_body)
