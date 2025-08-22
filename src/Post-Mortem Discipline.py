"""
Stage 3 – Post-Mortem Discipline
--------------------------------
Grades daily forecasts against actual SPX/MES outcomes.

⚠️ Current limitation: Stage 1 only ingests DATE + CLOSE, so grading is based
on closes (vs yesterday and vs forecast levels). Full OHLCV support will be
added in Stage 1.1, at which point this script can expand to richer grading.

- Pulls forecast from FORECAST_SUMMARY (Stage 2).
- Pulls actual SPX/ES closes from Snowflake (Stage 1).
- Applies grading logic (bias correctness, range hit check, RSI alignment placeholder).
- Inserts results into FORECAST_POSTMORTEM for Zen Council learning.
"""

import os
import snowflake.connector
import pandas as pd
from datetime import date, timedelta
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
# 3. Fetch Forecast + Market Data
# -----------------------------
def fetch_forecast(conn, audit_date):
    query = f"""
    SELECT *
    FROM FORECAST_SUMMARY
    WHERE DATE = '{audit_date}'
    """
    return pd.read_sql(query, conn)


def fetch_actuals(conn, audit_date):
    query = f"""
    SELECT s.DATE,
           s.CLOSE AS SPX_CLOSE,
           e.CLOSE AS ES_CLOSE
    FROM SPX_HISTORICAL s
    JOIN ES_HISTORICAL e ON s.DATE = e.DATE
    WHERE s.DATE = '{audit_date}'
    """
    return pd.read_sql(query, conn)


# -----------------------------
# 4. Post-Mortem Grading Logic
# -----------------------------
def grade_forecast(forecast_row, actual_row, yesterday_close):
    bias = forecast_row["FORECAST_BIAS"]
    support_levels = [float(x) for x in forecast_row["SUPPORTS"].split(", ")]
    resistance_levels = [float(x) for x in forecast_row["RESISTANCES"].split(", ")]

    spx_close = actual_row["SPX_CLOSE"]

    # --- Forecast Correctness ---
    correct = False
    price_action_result = ""

    if bias == "Neutral":
        if min(support_levels) <= spx_close <= max(resistance_levels):
            correct = True
            price_action_result = "Close inside straddle"
        else:
            price_action_result = "Close broke range"

    elif bias == "Bullish":
        if spx_close > yesterday_close:
            correct = True
            price_action_result = "Closed Higher"
        else:
            price_action_result = "Closed Lower"

    elif bias == "Bearish":
        if spx_close < yesterday_close:
            correct = True
            price_action_result = "Closed Lower"
        else:
            price_action_result = "Closed Higher"

    elif bias == "Hold/Skip":
        correct = True
        price_action_result = "Skipped (Macro Event)"

    # --- Range Hit? ---
    range_hit = (spx_close <= max(support_levels)) or (spx_close >= min(resistance_levels))

    # --- RSI Alignment (placeholder) ---
    rsi_aligned = "Pending"

    # --- Net Move & % Change ---
    net_move = round(spx_close - yesterday_close, 2)
    pct_change = round((net_move / yesterday_close) * 100, 2)

    return {
        "DATE": actual_row["DATE"],
        "INDEX": forecast_row["INDEX"],
        "FORECAST_BIAS": bias,
        "PRICE_ACTION_RESULT": price_action_result,
        "FORECAST_CORRECT": "✅" if correct else "❌",
        "RANGE_HIT": "Yes" if range_hit else "No",
        "RSI_ALIGNED": rsi_aligned,
        "NET_MOVE": net_move,
        "PCT_CHANGE": pct_change,
        "NOTES": "Auto-graded (close-only) by Zen Council v1.0"
    }


# -----------------------------
# 5. Write Post-Mortem to Snowflake
# -----------------------------
def write_postmortem(conn, postmortem: dict):
    create_stmt = """
    CREATE TABLE IF NOT EXISTS FORECAST_POSTMORTEM (
        DATE DATE PRIMARY KEY,
        INDEX STRING,
        FORECAST_BIAS STRING,
        PRICE_ACTION_RESULT STRING,
        FORECAST_CORRECT STRING,
        RANGE_HIT STRING,
        RSI_ALIGNED STRING,
        NET_MOVE FLOAT,
        PCT_CHANGE FLOAT,
        NOTES STRING
    )
    """
    with conn.cursor() as cur:
        cur.execute(create_stmt)
        cur.execute("DELETE FROM FORECAST_POSTMORTEM WHERE DATE = %s", (postmortem["DATE"],))
        cur.execute("""
            INSERT INTO FORECAST_POSTMORTEM
            (DATE, INDEX, FORECAST_BIAS, PRICE_ACTION_RESULT, FORECAST_CORRECT,
             RANGE_HIT, RSI_ALIGNED, NET_MOVE, PCT_CHANGE, NOTES)
            VALUES (%(DATE)s, %(INDEX)s, %(FORECAST_BIAS)s, %(PRICE_ACTION_RESULT)s,
                    %(FORECAST_CORRECT)s, %(RANGE_HIT)s, %(RSI_ALIGNED)s,
                    %(NET_MOVE)s, %(PCT_CHANGE)s, %(NOTES)s)
        """, postmortem)


# -----------------------------
# 6. Main Entrypoint (with debug logs)
# -----------------------------
if __name__ == "__main__":
    conn = get_connection()

    # Find most recent forecast date
    latest_forecast_query = "SELECT MAX(DATE) AS LAST_DATE FROM FORECAST_SUMMARY"
    latest_date = pd.read_sql(latest_forecast_query, conn).iloc[0, 0]

    if latest_date is None:
        print("⚠️ No forecasts available in FORECAST_SUMMARY.")
        conn.close()
        exit()

    audit_date = str(latest_date)
    print(f"🔍 Auditing forecast for {audit_date}")

    forecast_df = fetch_forecast(conn, audit_date)
    actual_df = fetch_actuals(conn, audit_date)

    print(f"📊 Forecast rows fetched: {len(forecast_df)}")
    print(f"📊 Actual rows fetched: {len(actual_df)}")

    if forecast_df.empty or actual_df.empty:
        print(f"⚠️ Missing forecast or actuals for {audit_date}")
    else:
        forecast_row = forecast_df.iloc[0]
        actual_row = actual_df.iloc[0]

        # Fetch yesterday's SPX close
        yesterday_query = f"""
        SELECT CLOSE FROM SPX_HISTORICAL
        WHERE DATE = DATEADD(day, -1, '{audit_date}')
        """
        yesterday_result = pd.read_sql(yesterday_query, conn)
        print(f"📊 Yesterday result rows: {len(yesterday_result)}")

        if yesterday_result.empty:
            print(f"⚠️ No SPX close found for day before {audit_date}")
        else:
            yesterday_close = yesterday_result.iloc[0, 0]
            print(f"📊 Yesterday close = {yesterday_close}")

            postmortem = grade_forecast(forecast_row, actual_row, yesterday_close)
            print("📝 Post-Mortem record to insert:", postmortem)

            write_postmortem(conn, postmortem)
            print("✅ Post-Mortem written:", postmortem)

    conn.close()

