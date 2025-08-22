
"""
Stage 2 – Forecast Engine
-------------------------
Generates a daily SPX forecast using Zen Rules v1.0.
- Pulls SPX, ES, VIX, VVIX data from Snowflake (ingested by Stage 1).
- Applies Zen Rules (bias, support/resistance, volatility context).
- Inserts a single forecast row into FORECAST_SUMMARY table in Snowflake.
- Designed for daily run (e.g. 8:30 AM ET) via CI/CD or cron.

Future expansions:
- Replace RSI placeholder with live RSI (2-min from ThinkOrSwim/Polygon).
- Replace macro placeholder with real calendar feed (CPI, FOMC, Jobs).
- Replace static ATM straddle (±25) with live options chain.
"""

import os
import snowflake.connector
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# -----------------------------
# 1. Load Environment Variables
# -----------------------------
# Pull Snowflake credentials from .env so nothing sensitive is hardcoded.
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
    """
    Open Snowflake connection using environment variables.
    Returns a live Snowflake connection object.
    """
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


# -----------------------------
# 3. Fetch Recent Market Data
# -----------------------------
def fetch_recent_data(conn, lookback_days=5):
    """
    Pull last N days of SPX, ES, VIX, VVIX closes from Snowflake.
    This lets us compare today vs yesterday for forecast logic.
    """
    query = f"""
    WITH latest_date AS (SELECT MAX(DATE) as DATE FROM SPX_HISTORICAL)
    SELECT s.DATE,
           s.CLOSE AS SPX,
           e.CLOSE AS ES,
           v.CLOSE AS VIX,
           vv.CLOSE AS VVIX
    FROM SPX_HISTORICAL s
    JOIN ES_HISTORICAL e ON s.DATE = e.DATE
    JOIN VIX_HISTORICAL v ON s.DATE = v.DATE
    JOIN VVIX_HISTORICAL vv ON s.DATE = vv.DATE
    WHERE s.DATE >= (SELECT DATEADD(day, -{lookback_days}, MAX(DATE)) FROM SPX_HISTORICAL)
    ORDER BY s.DATE DESC
    """
    return pd.read_sql(query, conn)


# -----------------------------
# 4. Placeholder RSI + Macro Data
# -----------------------------
def get_rsi_placeholder():
    """
    Temporary stand-in for real RSI feed.
    Returns 50 (neutral) until we hook in ThinkOrSwim or Polygon RSI.
    """
    return 50.0

def get_macro_flag():
    """
    Temporary stand-in for real economic calendar integration.
    Returns True if a major macro event is 'pending' today.
    """
    return False


# -----------------------------
# 5. Apply Zen Rules Logic
# -----------------------------
def apply_zen_rules(df: pd.DataFrame):
    """
    Apply Zen Rules v1.0 to the most recent data row.
    Decides bias, support/resistance, and volatility context.
    """
    today_row = df.iloc[0]
    yesterday_row = df.iloc[1]

    # Pull placeholder RSI and macro flags
    rsi = get_rsi_placeholder()
    macro_event_pending = get_macro_flag()

    # ---- Forecast Bias ----
    if macro_event_pending:
        bias = "Hold/Skip"
    elif 40 <= rsi <= 60 and abs(today_row["SPX"] - yesterday_row["SPX"]) < 10:
        bias = "Neutral"
    elif rsi > 60 and today_row["SPX"] >= yesterday_row["SPX"]:
        bias = "Bullish"
    elif rsi < 40 and today_row["SPX"] <= yesterday_row["SPX"]:
        bias = "Bearish"
    else:
        bias = "Neutral"

    # ---- Support / Resistance ----
    support_levels = [
        round(yesterday_row["SPX"] - 25, 2),  # Yesterday's close minus straddle width
        round(min(yesterday_row["SPX"], today_row["ES"]) - 10, 2)  # ES overnight low-ish
    ]
    resistance_levels = [
        round(yesterday_row["SPX"] + 25, 2),  # Yesterday's close plus straddle width
        round(max(yesterday_row["SPX"], today_row["ES"]) + 10, 2)  # ES overnight high-ish
    ]

    # ---- Volatility Context ----
    notes = []
    if today_row["VIX"] > yesterday_row["VIX"] * 1.05:
        notes.append("Volatility expansion")
    if today_row["VVIX"] > 100:
        notes.append("VVIX elevated → hedging demand")
    if today_row["VIX"] > yesterday_row["VIX"] and today_row["VVIX"] > yesterday_row["VVIX"]:
        notes.append("Volatility shock")
    if not notes:
        notes.append("Stable vol structure")

    # ---- Final Forecast ----
    forecast = {
        "DATE": today_row["DATE"],
        "INDEX": "SPX",
        "FORECAST_BIAS": bias,
        "SUPPORTS": ", ".join(map(str, support_levels)),
        "RESISTANCES": ", ".join(map(str, resistance_levels)),
        "ATM_STRADDLE": "±25",  # static placeholder until live options feed
        "NOTES": "; ".join(notes),
    }
    return forecast


# -----------------------------
# 6. Write Forecast to Snowflake
# -----------------------------
def write_forecast(conn, forecast: dict):
    """
    Creates FORECAST_SUMMARY if missing,
    deletes any existing row for the date,
    then inserts the new forecast.
    """
    create_stmt = """
    CREATE TABLE IF NOT EXISTS FORECAST_SUMMARY (
        DATE DATE PRIMARY KEY,
        INDEX STRING,
        FORECAST_BIAS STRING,
        SUPPORTS STRING,
        RESISTANCES STRING,
        ATM_STRADDLE STRING,
        NOTES STRING
    )
    """
    with conn.cursor() as cur:
        cur.execute(create_stmt)
        cur.execute("DELETE FROM FORECAST_SUMMARY WHERE DATE = %s", (forecast["DATE"],))
        cur.execute("""
            INSERT INTO FORECAST_SUMMARY 
            (DATE, INDEX, FORECAST_BIAS, SUPPORTS, RESISTANCES, ATM_STRADDLE, NOTES)
            VALUES (%(DATE)s, %(INDEX)s, %(FORECAST_BIAS)s, %(SUPPORTS)s, 
                    %(RESISTANCES)s, %(ATM_STRADDLE)s, %(NOTES)s)
        """, forecast)


# -----------------------------
# 7. Main Entrypoint
# -----------------------------
if __name__ == "__main__":
    conn = get_connection()
    df = fetch_recent_data(conn)
    forecast = apply_zen_rules(df)
    write_forecast(conn, forecast)
    print("✅ Forecast written:", forecast)
    conn.close()
