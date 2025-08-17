import requests
import snowflake.connector
import csv
from io import StringIO
from datetime import datetime

VIX_URL = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
VVIX_URL = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VVIX_History.csv"

def load_index_to_snowflake(name, url, table):
    # Download CSV
    resp = requests.get(url)
    resp.raise_for_status()

    # Parse CSV
    f = StringIO(resp.text)
    reader = list(csv.DictReader(f))

    if not reader:
        print(f"No data found in {name} CSV.")
        return

    print(f"{name} headers: {list(reader[0].keys())}")
    latest = reader[-1]

    # Handle VVIX (special case)
    if name == "VVIX":
        close_key = "VVIX"
    else:
        # General case: find close column
        close_key = None
        for key in latest.keys():
            if "close" in key.lower() or "px_last" in key.lower():
                close_key = key
                break
        if not close_key:
            raise KeyError(f"No CLOSE column found in {name} CSV. Headers: {reader[0].keys()}")

    idx_date = datetime.strptime(latest["DATE"], "%m/%d/%Y").date()
    idx_close = float(latest[close_key])

    # Connect to Snowflake
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

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            DATE DATE PRIMARY KEY,
            CLOSE FLOAT
        )
    """)

    cur.execute(f"""
        MERGE INTO {table} t
        USING (SELECT %s::DATE AS DATE, %s::FLOAT AS CLOSE) s
        ON t.DATE = s.DATE
        WHEN MATCHED THEN UPDATE SET CLOSE = s.CLOSE
        WHEN NOT MATCHED THEN INSERT (DATE, CLOSE) VALUES (s.DATE, s.CLOSE)
    """, (idx_date, idx_close))

    conn.commit()
    cur.close()
    conn.close()

    print(f"Inserted/updated {name} close for {idx_date}: {idx_close}")


if __name__ == "__main__":
    load_index_to_snowflake("VIX", VIX_URL, "VIX_HISTORICAL")
    load_index_to_snowflake("VVIX", VVIX_URL, "VVIX_HISTORICAL")

