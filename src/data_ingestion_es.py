import yfinance as yf
import snowflake.connector
from datetime import date, timedelta

# Fetch yesterday’s ES=F data
symbol = "ES=F"
ticker = yf.Ticker(symbol)
hist = ticker.history(period="5d", interval="1d")  # last 5 days daily bars

if not hist.empty:
    last_row = hist.tail(1)
    es_date = last_row.index[-1].date()
    es_close = float(last_row["Close"].iloc[0])

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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ES_HISTORICAL (
            DATE DATE PRIMARY KEY,
            CLOSE FLOAT
        )
    """)

    cur.execute("""
        MERGE INTO ES_HISTORICAL t
        USING (SELECT %s::DATE AS DATE, %s::FLOAT AS CLOSE) s
        ON t.DATE = s.DATE
        WHEN MATCHED THEN UPDATE SET CLOSE = s.CLOSE
        WHEN NOT MATCHED THEN INSERT (DATE, CLOSE) VALUES (s.DATE, s.CLOSE)
    """, (es_date, es_close))

    conn.commit()
    cur.close()
    conn.close()

    print(f"Inserted/updated ES close for {es_date}: {es_close}")
else:
    print("No ES data available from Yahoo Finance.")
