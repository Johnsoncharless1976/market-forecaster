# File: src/ingest_forecast_job.py
# Title: Stage 3 – Forecast ETL with Timestamp
# Commit Notes: Added FORECAST_TS = CURRENT_TIMESTAMP to MERGE/INSERT so 
# each forecast entry carries a timestamp for later stages.

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

def fetch_latest(symbol, cur):
    cur.execute(f"SELECT DATE, CLOSE FROM {symbol}_HISTORICAL ORDER BY DATE DESC LIMIT 1")
    row = cur.fetchone()
    return {"DATE": row[0], "CLOSE": row[1]} if row else None

def fetch_atr(symbol, cur):
    cur.execute(f"SELECT ATR_14D FROM FORECAST_DERIVED_METRICS WHERE SYMBOL='{symbol}' ORDER BY DATE DESC LIMIT 1")
    row = cur.fetchone()
    return float(row[0]) if row else 20

def fetch_rsi(symbol, cur):
    cur.execute(f"SELECT DAILY_RETURN FROM FORECAST_DERIVED_METRICS WHERE SYMBOL='{symbol}' ORDER BY DATE DESC LIMIT 1")
    row = cur.fetchone()
    return float(row[0]) if row else 50

def insert_forecast(forecast, cur):
    cur.execute("""
        MERGE INTO FORECAST_JOBS AS T
        USING (SELECT %s DATE, %s INDEX, %s FORECAST_BIAS, %s ATM_STRADDLE,
                      %s SUPPORT_LEVELS, %s RESISTANCE_LEVELS, %s RSI_CONTEXT, %s NOTES,
                      CURRENT_TIMESTAMP FORECAST_TS) AS S
        ON T.DATE = S.DATE AND T.INDEX = S.INDEX
        WHEN MATCHED THEN UPDATE SET
            FORECAST_BIAS=S.FORECAST_BIAS,
            ATM_STRADDLE=S.ATM_STRADDLE,
            SUPPORT_LEVELS=S.SUPPORT_LEVELS,
            RESISTANCE_LEVELS=S.RESISTANCE_LEVELS,
            RSI_CONTEXT=S.RSI_CONTEXT,
            NOTES=S.NOTES,
            LOAD_TS=CURRENT_TIMESTAMP,
            FORECAST_TS=S.FORECAST_TS
        WHEN NOT MATCHED THEN INSERT
            (DATE,INDEX,FORECAST_BIAS,ATM_STRADDLE,SUPPORT_LEVELS,
             RESISTANCE_LEVELS,RSI_CONTEXT,NOTES,FORECAST_TS)
        VALUES
            (S.DATE,S.INDEX,S.FORECAST_BIAS,S.ATM_STRADDLE,S.SUPPORT_LEVELS,
             S.RESISTANCE_LEVELS,S.RSI_CONTEXT,S.NOTES,S.FORECAST_TS)
    """, (
        forecast["DATE"], forecast["INDEX"], forecast["FORECAST_BIAS"],
        forecast["ATM_STRADDLE"], forecast["SUPPORT_LEVELS"],
        forecast["RESISTANCE_LEVELS"], forecast["RSI_CONTEXT"], forecast["NOTES"]
    ))

def main():
    conn = snowflake.connector.connect(
        user=cfg["SNOWFLAKE_USER"], password=cfg["SNOWFLAKE_PASSWORD"],
        account=cfg["SNOWFLAKE_ACCOUNT"], warehouse=cfg["SNOWFLAKE_WAREHOUSE"],
        database=cfg["SNOWFLAKE_DATABASE"], schema=cfg["SNOWFLAKE_SCHEMA"]
    )
    cur = conn.cursor()
    latest = fetch_latest("SPX", cur)
    if not latest:
        print("❌ No latest SPX data found"); return

    atr, rsi = fetch_atr("SPX", cur), fetch_rsi("SPX", cur)

    forecast = {
        "DATE": latest["DATE"], "INDEX": "SPX", "FORECAST_BIAS": "Neutral",
        "ATM_STRADDLE": latest["CLOSE"]*0.01,
        "SUPPORT_LEVELS": str([latest["CLOSE"] - atr]),
        "RESISTANCE_LEVELS": str([latest["CLOSE"] + atr]),
        "RSI_CONTEXT": rsi, "NOTES": "Auto-generated baseline forecast."
    }

    insert_forecast(forecast, cur)
    conn.commit(); cur.close(); conn.close()
    print("✅ Forecast entry inserted into FORECAST_JOBS with timestamp.")

if __name__ == "__main__":
    main()
