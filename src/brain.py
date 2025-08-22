# brain.py
# 🧠 ZeroDay Zen Council – Forecast Brain
# Purpose: Pulls raw market data from Snowflake, applies interpretation logic,
# and outputs a formatted forecast body for send_email.py

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
            return None
        return float(row[1])

spx = fetch_latest(conn, "SPX_HISTORICAL")
es = fetch_latest(conn, "ES_HISTORICAL")
vix = fetch_latest(conn, "VIX_HISTORICAL")
vvix = fetch_latest(conn, "VVIX_HISTORICAL")
conn.close()

# -----------------------------
# Example interpretation logic
# -----------------------------
bias = "Neutral"
notes = []

# Simple example: RSI placeholder logic (replace later with actual RSI fetch)
rsi = 55  # placeholder
if rsi > 70:
    bias = "Bearish Reversal Watch"
    notes.append("RSI > 70 → Overbought risk.")
elif rsi < 30:
    bias = "Bullish Reversal Watch"
    notes.append("RSI < 30 → Oversold risk.")
else:
    notes.append("RSI mid-range → No extreme pressure.")

# Volatility interpretation
if vix and vix > 20:
    notes.append("VIX > 20 → elevated volatility.")
if vvix and vvix > 100:
    notes.append("VVIX > 100 → hedging demand.")

# -----------------------------
# Build forecast body
# -----------------------------
eastern = pytz.timezone("US/Eastern")
now_et = datetime.now(eastern).strftime("%b %d, %Y (%I:%M %p ET)")

forecast_body = f"""
📈 ZeroDay Zen Forecast – {now_et}

🧠 Bias: {bias}

SPX: {round(spx, 2) if spx else '--'}
/ES: {round(es, 2) if es else '--'}
VIX: {round(vix, 2) if vix else '--'}
VVIX: {round(vvix, 2) if vvix else '--'}

🔍 Interpretation Notes:
- {' '.join(notes)}
"""

# This will be imported by send_email.py
if __name__ == "__main__":
    print(forecast_body)
