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
load_dotenv()  # local use; in GitLab CI/CD vars are injected automatically

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# Verify required values exist
for var, value in {
    "SNOWFLAKE_USER": SNOWFLAKE_USER,
    "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
    "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
    "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
    "SNOWFLAKE_DATABASE": SNOWFLAKE_DATABASE,
    "SNOWFLAKE_SCHEMA": SNOWFLAKE_SCHEMA,
}.items():
    if not value:
        raise EnvironmentError(f"❌ Missing environment variable: {var}")

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

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join([str(c) for c in col if c]) for col in df.columns]

    df = df.reset_index()

    rename_map = {}
    if "Date" in df.columns:
        rename_map["Date"] = "DATE"
    if "Close" in df.columns:
        rename_map["Close"] = "CLOSE"
    if "Close_" in "".join(df.columns):
        for c in df.columns:
            if "Close" in c:
                rename_map[c] = "CLOSE"

    df.rename(columns=rename_map, inplace=True)

    keep_cols = [c for c in ["DATE", "CLOSE"] if c in df.columns]
    df = df[keep_cols]

    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
    if "CLOSE" in df.columns:
        df["CLOSE"] = pd.to_numeric(df["CLOSE"], errors="coerce")

    df = df.dropna()
    return df.tail(1)

# -----------------------------
# 3. Data fetch functions
# -----------------------------
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

def get_es():
    df = yf.download("ES=F", period="5d", interval="1d")
    return normalize_df(df)

def get_vix():
    df = yf.download("^VIX", period="5d", interval="1d")
    return normalize_df(df)

def get_vvix():
    df = yf.download("^VVIX", period="5d", interval="1d")
    return normalize_df(df)

# -----------------------------
# 4. Deduplication helper
# -----------------------------
def upsert_daily(conn, df: pd.DataFrame, table_name: str):
    """Delete existing row for today's DATE, then insert fresh row"""
    if df.empty:
        print(f"⚠️ No data to insert for {table_name}")
        return

    today = df["DATE"].iloc[-1]

    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {table_name} WHERE DATE = %s", (today,))
        print(f"🧹 Removed existing rows for {today} in {table_name}")

    write_pandas(conn, df.reset_index(drop=True), table_name)
    print(f"✅ Inserted new row for {today} into {table_name}")

def verify_table(conn, table_name: str):
    """Print last 5 rows from a table for verification"""
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {table_name} ORDER BY DATE DESC LIMIT 5")
        rows = cur.fetchall()
        print(f"\n📊 Last 5 rows in {table_name}:")
        for row in rows:
            print(row)

# -----------------------------
# 5. Fetch data
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
# 6. Connect + load into Snowflake
# -----------------------------
try:
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    print(f"✅ Connected to Snowflake account {SNOWFLAKE_ACCOUNT} as {SNOWFLAKE_USER}")

    upsert_daily(conn, spy_df, "SPY_HISTORICAL")
    upsert_daily(conn, es_df, "ES_HISTORICAL")
    upsert_daily(conn, vix_df, "VIX_HISTORICAL")
    upsert_daily(conn, vvix_df, "VVIX_HISTORICAL")

    verify_table(conn, "SPY_HISTORICAL")
    verify_table(conn, "ES_HISTORICAL")
    verify_table(conn, "VIX_HISTORICAL")
    verify_table(conn, "VVIX_HISTORICAL")

    print("🎉 SPY, ES, VIX, VVIX upserted into Snowflake successfully.")
    conn.close()
except Exception as e:
    print("❌ Snowflake load failed:", e)
    raise

