# src/data_ingestion.py
import os
import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import yfinance as yf

# -----------------------------
# 1. Load secrets from GitLab CI/CD variables
# -----------------------------
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

# -----------------------------
# 2. Data Fetch Functions
# -----------------------------

# SPY (Polygon fallback → Yahoo)
def get_spy():
    if POLYGON_API_KEY:
        url = f"https://api.polygon.io/v2/aggs/ticker/SPY/prev?apiKey={POLYGON_API_KEY}"
        resp = requests.get(url).json()
        if "results" in resp:
            return pd.DataFrame([{
                "DATE": pd.to_datetime(resp["results"][0]["t"], unit="ms").date(),
                "CLOSE": resp["results"][0]["c"]
            }])
        else:
            print(f"⚠️ Polygon failed: {resp} — falling back to Yahoo")
    # Yahoo fallback
    df = yf.download("SPY", period="5d", interval="1d")
    df = df.reset_index()[["Date", "Close"]]
    df.rename(columns={"Date": "DATE", "Close": "CLOSE"}, inplace=True)
    df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
    return df.tail(1)  # just latest row

# ES Futures (Yahoo)
def get_es():
    df = yf.download("ES=F", period="5d", interval="1d")
    df = df.reset_index()[["Date", "Close"]]
    df.rename(columns={"Date": "DATE", "Close": "CLOSE"}, inplace=True)
    df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
    return df.tail(1)

# VIX (Yahoo)
def get_vix():
    df = yf.download("^VIX", period="5d", interval="1d")
    df = df.reset_index()[["Date", "Close"]]
    df.rename(columns={"Date": "DATE", "Close": "CLOSE"}, inplace=True)
    df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
    return df.tail(1)

# VVIX (Yahoo)
def get_vvix():
    df = yf.download("^VVIX", period="5d", interval="1d")
    df = df.reset_index()[["Date", "Close"]]
    df.rename(columns={"Date": "DATE", "Close": "CLOSE"}, inplace=True)
    df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
    return df.tail(1)

# -----------------------------
# 3. Fetch Data
# -----------------------------
spy_df = get_spy()
es_df = get_es()
vix_df = get_vix()
vvix_df = get_vvix()

# -----------------------------
# 4. Connect to Snowflake
# -----------------------------
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse="COMPUTE_WH",
    database="ZEN_MARKET",
    schema="FORECASTING"
)

# -----------------------------
# 5. Insert into Snowflake
# -----------------------------
write_pandas(conn, spy_df, "SPY_HISTORICAL")
write_pandas(conn, es_df, "ES_HISTORICAL")
write_pandas(conn, vix_df, "VIX_HISTORICAL")
write_pandas(conn, vvix_df, "VVIX_HISTORICAL")

print("✅ SPY, ES, VIX, VVIX loaded into Snowflake successfully.")
