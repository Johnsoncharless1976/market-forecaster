# stage3_snapshot.py

import snowflake.connector
import pandas as pd

# --- Snowflake connection ---
conn = snowflake.connector.connect(
    user="JOHNSONCHARLESS",
    password="s7AfXRG7krgnh3H",
    account="GFXGPHR-HXC94041",
    warehouse="COMPUTE_WH",
    database="ZEN_MARKET",
    schema="FORECASTING"
)

# --- Pull aligned data from MARKET_MASTER ---
query = "SELECT date, spy_close, es_close, vix_close, vvix_close FROM MARKET_MASTER ORDER BY date;"
df = pd.read_sql(query, conn)
conn.close()

# --- Normalize column names ---
df.columns = df.columns.str.lower()

row_count = len(df)
print(f"\n📊 Pulled {row_count} rows from MARKET_MASTER")

if row_count < 30:
    print("⚠️ Not enough rows to calculate 30-day rolling correlations. Need at least 30.")
    exit(0)

df["date"] = pd.to_datetime(df["date"])
df.set_index("date", inplace=True)

# --- Daily % changes ---
changes = pd.DataFrame({
    "spy_pct": df["spy_close"].pct_change() * 100,
    "es_pct": df["es_close"].pct_change() * 100,
    "vix_pct": df["vix_close"].pct_change() * 100,
    "vvix_pct": df["vvix_close"].pct_change() * 100
})

latest_date = changes.dropna().index[-1]
latest_changes = changes.dropna().iloc[-1]

print("\n=== Daily % Change Snapshot ===")
print(f"Date: {latest_date.date()}")
print(f"SPY   : {latest_changes.spy_pct:.2f}%")
print(f"ES    : {latest_changes.es_pct:.2f}%")
print(f"VIX   : {latest_changes.vix_pct:.2f}%")
print(f"VVIX  : {latest_changes.vvix_pct:.2f}%")

# --- Rolling 30-day correlations ---
corr_df = pd.DataFrame({
    "spy_es_corr": df["spy_close"].rolling(30).corr(df["es_close"]),
    "spy_vix_corr": df["spy_close"].rolling(30).corr(df["vix_close"]),
    "vix_vvix_corr": df["vix_close"].rolling(30).corr(df["vvix_close"])
})

latest_corr_date = corr_df.dropna().index[-1]
latest_corr = corr_df.dropna().iloc[-1]

print("\n=== Rolling 30-Day Correlation Snapshot ===")
print(f"Date: {latest_corr_date.date()}")
print(f"SPY–ES   : {latest_corr.spy_es_corr:.3f}")
print(f"SPY–VIX  : {latest_corr.spy_vix_corr:.3f}")
print(f"VIX–VVIX : {latest_corr.vix_vvix_corr:.3f}")
