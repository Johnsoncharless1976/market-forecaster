# src/data_ingestion.py
import requests
import snowflake.connector
import pandas as pd
import yfinance as yf
from datetime import date

API_KEY = "jYeR6QVhnmhFe7V0aQm1_ZuGM6QawAEO"

# --- Fetch SPY (Polygon) ---
def fetch_polygon_close(ticker):
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev?apiKey={API_KEY}"
    resp = requests.get(url).json()
    if "results" in resp and resp["results"]:
        d = date.fromtimestamp(resp["results"][0]["t"] / 1000)
        close = resp["results"][0]["c"]
        return d, close
    return None, None

spy_date, spy_close = fetch_polygon_close("SPY")

# --- Fetch ES (Yahoo Finance continuous futures) ---
try:
    es_data = yf.download("ES=F", period="1d", interval="1d")
    if not es_data.empty:
        es_date = es_data.index[-1].date()
        es_close = float(es_data["Close"].iloc[-1])
    else:
        es_date, es_close = spy_date, None
except Exception as e:
    print(f"Error fetching ES: {e}")
    es_date, es_close = spy_date, None

# --- Fetch VIX (CBOE CSV) ---
try:
    vix_df = pd.read_csv("https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv")
    vix_df["DATE"] = pd.to_datetime(vix_df["DATE"])
    vix_df = vix_df.sort_values("DATE")
    vix_date = vix_df["DATE"].iloc[-1].date()
    vix_close = float(vix_df["CLOSE"].iloc[-1])
except Exception as e:
    print(f"Error fetching VIX: {e}")
    vix_date, vix_close = spy_date, None

# --- Fetch VVIX (CBOE CSV) ---
try:
    vvix_df = pd.read_csv("https://cdn.cboe.com/api/global/us_indices/daily_prices/VVIX_History.csv")
    vvix_df["DATE"] = pd.to_datetime(vvix_df["DATE"])
    vvix_df = vvix_df.sort_values("DATE")
    vvix_date = vvix_df["DATE"].iloc[-1].date()
    vvix_close = float(vvix_df["VVIX"].iloc[-1])
except Exception as e:
    print(f"Error fetching VVIX: {e}")
    vvix_date, vvix_close = spy_date, None

# --- Use SPY’s date as primary trade date ---
trade_date = spy_date or vix_date or vvix_date or es_date

# --- Connect to Snowflake ---
conn = snowflake.connector.connect(
    user="JOHNSONCHARLESS",
    password="s7AfXRG7krgnh3H",
    account="GFXGPHR-HXC94041",
    warehouse="COMPUTE_WH",
    database="ZEN_MARKET",
    schema="FORECASTING"
)
cur = conn.cursor()
cur.execute("USE WAREHOUSE COMPUTE_WH;")

# --- Create unified table if not exists ---
cur.execute("""
    CREATE TABLE IF NOT EXISTS DAILY_MARKET_DATA (
        DATE DATE PRIMARY KEY,
        SPY_CLOSE FLOAT,
        ES_CLOSE FLOAT,
        VIX_CLOSE FLOAT,
        VVIX_CLOSE FLOAT
    )
""")

# --- Upsert daily row ---
cur.execute("""
    MERGE INTO DAILY_MARKET_DATA t
    USING (SELECT %s::DATE AS DATE, %s::FLOAT AS SPY_CLOSE,
                  %s::FLOAT AS ES_CLOSE, %s::FLOAT AS VIX_CLOSE,
                  %s::FLOAT AS VVIX_CLOSE) s
    ON t.DATE = s.DATE
    WHEN MATCHED THEN UPDATE SET
        SPY_CLOSE = s.SPY_CLOSE,
        ES_CLOSE = s.ES_CLOSE,
        VIX_CLOSE = s.VIX_CLOSE,
        VVIX_CLOSE = s.VVIX_CLOSE
    WHEN NOT MATCHED THEN
        INSERT (DATE, SPY_CLOSE, ES_CLOSE, VIX_CLOSE, VVIX_CLOSE)
        VALUES (s.DATE, s.SPY_CLOSE, s.ES_CLOSE, s.VIX_CLOSE, s.VVIX_CLOSE)
""", (trade_date, spy_close, es_close, vix_close, vvix_close))

conn.commit()
cur.close()
conn.close()

print(f"✅ Updated {trade_date}: SPY={spy_close}, ES={es_close}, VIX={vix_close}, VVIX={vvix_close}")

