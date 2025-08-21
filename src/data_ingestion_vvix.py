# src/data_ingestion_vvix.py
import snowflake.connector
import pandas as pd

# --- Fetch VVIX from CBOE ---
url = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VVIX_History.csv"
df = pd.read_csv(url)
df["DATE"] = pd.to_datetime(df["DATE"])
df = df.sort_values("DATE")

vvix_date = df["DATE"].iloc[-1].date()
vvix_close = float(df["VVIX"].iloc[-1])

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

# --- Create table if not exists ---
cur.execute("""
    CREATE TABLE IF NOT EXISTS VVIX_HISTORICAL (
        DATE DATE PRIMARY KEY,
        CLOSE FLOAT
    )
""")

# --- Upsert daily close ---
cur.execute("""
    MERGE INTO VVIX_HISTORICAL t
    USING (SELECT %s::DATE AS DATE, %s::FLOAT AS CLOSE) s
    ON t.DATE = s.DATE
    WHEN MATCHED THEN UPDATE SET CLOSE = s.CLOSE
    WHEN NOT MATCHED THEN INSERT (DATE, CLOSE) VALUES (s.DATE, s.CLOSE)
""", (vvix_date, vvix_close))

conn.commit()
cur.close()
conn.close()

print(f"✅ Inserted/updated VVIX close for {vvix_date}: {vvix_close}")
