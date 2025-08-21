# src/data_ingestion.py
# src/data_ingestion.py

import datetime
import yfinance as yf
import requests
import os
import sys
from SnowflakeConfig import get_snowflake_connection  # reuse your config

# =============== HELPERS ==================

def insert_to_snowflake(table, date_val, close_val):
    """Insert a single row into Snowflake"""
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        query = f"INSERT INTO {table} (DATE, CLOSE) VALUES (%s, %s)"
        cur.execute(query, (date_val, close_val))
        rowcount = cur.rowcount
        conn.commit()
        if rowcount == 1:
            print(f"[OK] {table}: {date_val} = {close_val}")
        else:
            print(f"[WARN] {table}: No row inserted (maybe duplicate?)")
    except Exception as e:
        print(f"[ERR] Failed inserting into {table}: {e}")
    finally:
        cur.close()
        conn.close()



# =============== SPY via Polygon ==================

def fetch_spy():
    api_key = os.getenv("POLYGON_API_KEY")
    url = f"https://api.polygon.io/v2/aggs/ticker/SPY/prev?adjusted=true&apiKey={api_key}"
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    if "results" not in data:
        raise ValueError(f"Polygon returned no SPY data: {data}")
    result = data["results"][0]
    date_val = datetime.date.fromtimestamp(result["t"] / 1000)
    close_val = result["c"]
    insert_to_snowflake("SPY_HISTORICAL", date_val, close_val)


# =============== ES via Yahoo ==================

def fetch_es():
    ticker = yf.Ticker("ES=F")
    hist = ticker.history(period="1d")
    if hist.empty:
        raise ValueError("No ES data from Yahoo")
    date_val = hist.index[-1].date()
    close_val = hist["Close"].iloc[-1]
    insert_to_snowflake("ES_HISTORICAL", date_val, float(close_val))


# =============== VIX via Polygon ==================

def fetch_vix():
    api_key = os.getenv("POLYGON_API_KEY")
    url = f"https://api.polygon.io/v2/aggs/ticker/VIX/prev?adjusted=true&apiKey={api_key}"
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    if "results" not in data:
        raise ValueError(f"Polygon returned no VIX data: {data}")
    result = data["results"][0]
    date_val = datetime.date.fromtimestamp(result["t"] / 1000)
    close_val = result["c"]
    insert_to_snowflake("VIX_HISTORICAL", date_val, close_val)


# =============== VVIX via Yahoo ==================

def fetch_vvix():
    ticker = yf.Ticker("^VVIX")
    hist = ticker.history(period="1d")
    if hist.empty:
        raise ValueError("No VVIX data from Yahoo")
    date_val = hist.index[-1].date()
    close_val = hist["Close"].iloc[-1]
    insert_to_snowflake("VVIX_HISTORICAL", date_val, float(close_val))


# =============== MAIN ==================

def main():
    print("=== Starting Daily Ingestion ===")
    try:
        fetch_spy()
    except Exception as e:
        print(f"[WARN] SPY ingestion failed: {e}")

    try:
        fetch_es()
    except Exception as e:
        print(f"[WARN] ES ingestion failed: {e}")

    try:
        fetch_vix()
    except Exception as e:
        print(f"[WARN] VIX ingestion failed: {e}")

    try:
        fetch_vvix()
    except Exception as e:
        print(f"[WARN] VVIX ingestion failed: {e}")

    print("=== Ingestion Complete ===")


if __name__ == "__main__":
    main()
