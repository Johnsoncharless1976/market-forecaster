import requests
import snowflake.connector
from datetime import date

API_KEY = "jYeR6QVhnmhFe7V0aQm1_ZuGM6QawAEO"

# Polygon previous close endpoint
url = f"https://api.polygon.io/v2/aggs/ticker/SPY/prev?apiKey={API_KEY}"
resp = requests.get(url).json()

if "results" in resp and resp["results"]:
    spy_date = date.fromtimestamp(resp["results"][0]["t"] / 1000)
    spy_close = resp["results"][0]["c"]

    conn = snowflake.connector.connect(
        user="JOHNSONCHARLESS",
        password="s7AfXRG7krgnh3H",
        account="GFXGPHR-HXC94041",
        warehouse="COMPUTE_WH",   # update if your active warehouse has a different name
        database="ZEN_MARKET",
        schema="FORECASTING"
    )
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE COMPUTE_WH;")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS SPY_HISTORICAL (
            DATE DATE PRIMARY KEY,
            CLOSE FLOAT
        )
    """)

    cur.execute("""
        MERGE INTO SPY_HISTORICAL t
        USING (SELECT %s::DATE AS DATE, %s::FLOAT AS CLOSE) s
        ON t.DATE = s.DATE
        WHEN MATCHED THEN UPDATE SET CLOSE = s.CLOSE
        WHEN NOT MATCHED THEN INSERT (DATE, CLOSE) VALUES (s.DATE, s.CLOSE)
    """, (spy_date, spy_close))

    conn.commit()
    cur.close()
    conn.close()

    print(f"Inserted/updated SPY close for {spy_date}: {spy_close}")
else:
    print("No SPY data available from Polygon /prev endpoint.")
