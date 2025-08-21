# src/data_ingestion.py
import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# -----------------------------
# 1. API Keys / Config
# -----------------------------
POLYGON_API_KEY = "<YOUR_POLYGON_KEY>"  # already on file with me
SNOWFLAKE_USER = "<YOUR_SNOWFLAKE_USER>"
SNOWFLAKE_PASSWORD = "<YOUR_SNOWFLAKE_PASSWORD>"
SNOWFLAKE_ACCOUNT = "<YOUR_SNOWFLAKE_ACCOUNT>.snowflakecomputing.com"

# -----------------------------
# 2. Data Fetch Functions
# -----------------------------

# SPY (Polygon)
def get_spy():
    url = f"https://api.polygon.io/v2/aggs/ticker/SPY/prev?apiKey={POLYGON_API_KEY}"
    resp = requests.get(url).json()
    
    # Debug check
    if "results" not in resp:
        raise ValueError(f"Polygon API error: {resp}")
    
    data = [{
        "DATE": pd.to_datetime(resp["results"][0]["t"], unit="ms").date(),
        "CLOSE": resp["results"][0]["c"]
    }]
    return pd.DataFrame(data)


# Yahoo Finance (ES, VIX, VVIX)
def get_yahoo(symbol, lookback="5d"):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range={lookback}"
    resp = requests.get(url).json()["chart"]["result"][0]
    timestamps = resp["timestamp"]
    closes = resp["indicators"]["quote"][0]["close"]
    df = pd.DataFrame({
        "DATE": pd.to_datetime(timestamps, unit="s").date,
        "CLOSE": closes
    })
    return df.dropna()

# -----------------------------
# 3. Fetch Data
# -----------------------------
spy_df = get_spy()
es_df = get_yahoo("ES=F")      # S&P 500 E-mini futures
vix_df = get_yahoo("^VIX")
vvix_df = get_yahoo("^VVIX")

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
