# src/data_ingestion.py
import os
import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import yfinance as yf
from dotenv import load_dotenv

# -----------------------------
# 1. Load environment variables
# -----------------------------
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),  # no https://, no .snowflakecomputing.com
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

# -----------------------------
# 2. Helpers
# -----------------------------
def normalize_df(df):
    """
    Normalize any DataFrame from yfinance or polygon
    into 2 columns: DATE, CLOSE
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["DATE", "CLOSE"])

    # Flatten multi-index columns (if yfinance returns them)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join([str(c) for c in col if c]) for col in df.columns]

    df = df.reset_index()

    # Rename common yfinance column names
    rename_map = {}
    if "Date" in df.columns:
        rename_map["Date"] = "DATE"
    if "Close" in df.columns:
        rename_map["Close"] = "CLOSE"
    if "Close_" in "".join(df.columns):
        # Handle multi-index column like ('Close','SPY')
        for c in df.columns:
            if "Close" in c:
                rename_map[c] = "CLOSE"

    df.rename(columns=rename_map, inplace=True)

    # Keep only DATE and CLOSE if they exist
    keep_cols = [c for c in ["DATE", "CLOSE"] if c in df.columns]
    df = df[keep_cols]

    # Standardize types
    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
    if "CLOSE" in df.columns:
        df["CLOSE"] = pd.to_numeric(df["CLOSE"], errors="coerce")

    df = df.dropna()
    return df.tail(1)


# -----------------------------
# 3. Data fetch functions
# -----------------------------

# SPY (Polygon, with fallback to Yahoo)
def get_spy():
    if POLYGON_API_KEY:
        url = f"https://api.polygon.io/v2/aggs/ticker/SPY/prev?apiKey={POLYGON_API_KEY}"
        resp = requests.get(url).json()
        if "results" in resp:
            df = pd.DataFrame([{
                "DATE": pd.to_datetime(resp["results"][0]["t"], unit="ms").date(),
                "CLOSE": resp["results"][0]["c"]
            }])
            return normalize_df(df)
        else:
            print(f"⚠️ Polygon SPY fetch failed: {resp} — using Yahoo fallback")
    df = yf.download("SPY", period="5d", interval="1d")
    return normalize_df(df)

# ES Futures (Yahoo)
def get_es():
    df = yf.download("ES=F", period="5d", interval="1d")
    return normalize_df(df)

# VIX (Yahoo)
def get_vix():
    df = yf.download("^VIX", period="5d", interval="1d")
    return normalize_df(df)

# VVIX (Yahoo)
def get_vvix():
    df = yf.download("^VVIX", period="5d", interval="1d")
    return normalize_df(df)

# -----------------------------
# 4. Fetch data
# -----------------------------
spy_df = get_spy()
es_df = get_es()
vix_df = get_vix()
vvix_df = get_vvix()

print("✅ Data fetched:")
print("SPY:", spy_df)
print("ES:", es_df)
print("VIX:", vix_df)
print("VVIX:", vvix_df)

# -----------------------------
# 5. Connect to Snowflake
# -----------------------------
if not SNOWFLAKE_ACCOUNT or not SNOWFLAKE_USER:
    raise ValueError("❌ Missing Snowflake env vars. Check your .env or GitLab CI/CD settings.")

print(f"🔎 Connecting to Snowflake account: {SNOWFLAKE_ACCOUNT}, user: {SNOWFLAKE_USER}")

conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse="COMPUTE_WH",
    database="ZEN_MARKET",
    schema="FORECASTING"
)

# -----------------------------
# 6. Insert into Snowflake
# -----------------------------
write_pandas(conn, spy_df, "SPY_HISTORICAL")
write_pandas(conn, es_df, "ES_HISTORICAL")
write_pandas(conn, vix_df, "VIX_HISTORICAL")
write_pandas(conn, vvix_df, "VVIX_HISTORICAL")

print("🎉 SPY, ES, VIX, VVIX loaded into Snowflake successfully.")
