# src/data_ingestion.py
# src/data_ingestion.py
import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import yfinance as yf
from dotenv import load_dotenv

# -----------------------------
# 1. Load environment variables
# -----------------------------
load_dotenv()

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
def clean_columns(df):
    """Flatten any multi-index columns so Snowflake sees clean names."""
    df.columns = [
        str(c[0]) if isinstance(c, tuple) else str(c)
        for c in df.columns
    ]
    return df

def normalize_df(df):
    """Normalize Yahoo Finance DataFrame into DATE, CLOSE with 2-decimal rounding"""
    if df is None or df.empty:
        return pd.DataFrame(columns=["DATE", "CLOSE"])

    df = df.reset_index()

    # Flatten column names if multi-index
    df.columns = [c if not isinstance(c, tuple) else c[0] for c in df.columns]

    # Rename properly
    if "Date" in df.columns:
        df.rename(columns={"Date": "DATE"}, inplace=True)
    if "Close" in df.columns:
        df.rename(columns={"Close": "CLOSE"}, inplace=True)

    df = df[["DATE", "CLOSE"]]
    df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
    df["CLOSE"] = pd.to_numeric(df["CLOSE"], errors="coerce").round(2)  # 👈 round here

    return df.dropna().tail(1)


def get_spx():
    return normalize_df(yf.download("^GSPC", period="5d", interval="1d"))

def get_es():
    return normalize_df(yf.download("ES=F", period="5d", interval="1d"))

def get_vix():
    return normalize_df(yf.download("^VIX", period="5d", interval="1d"))

def get_vvix():
    return normalize_df(yf.download("^VVIX", period="5d", interval="1d"))

def upsert_daily(conn, df: pd.DataFrame, table_name: str):
    """Delete existing row for today's DATE, then insert fresh row"""
    if df.empty:
        print(f"⚠️ No data to insert for {table_name}")
        return

    df = clean_columns(df)  # ensure clean column names
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
# 3. Fetch data
# -----------------------------
spx_df = get_spx()
es_df = get_es()
vix_df = get_vix()
vvix_df = get_vvix()

print("✅ Data fetched:")
print("SPX:", spx_df)
print("ES:", es_df)
print("VIX:", vix_df)
print("VVIX:", vvix_df)

# -----------------------------
# 4. Connect + load into Snowflake
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

    upsert_daily(conn, spx_df, "SPX_HISTORICAL")
    upsert_daily(conn, es_df, "ES_HISTORICAL")
    upsert_daily(conn, vix_df, "VIX_HISTORICAL")
    upsert_daily(conn, vvix_df, "VVIX_HISTORICAL")

    verify_table(conn, "SPX_HISTORICAL")
    verify_table(conn, "ES_HISTORICAL")
    verify_table(conn, "VIX_HISTORICAL")
    verify_table(conn, "VVIX_HISTORICAL")

    print("🎉 SPX, ES, VIX, VVIX upserted into Snowflake successfully.")
    conn.close()
except Exception as e:
    print("❌ Snowflake load failed:", e)
    raise
