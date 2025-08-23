"""
Zen Council – Stage 2.1 Derived Metrics Ingestion Job (Idempotent)
------------------------------------------------------------------
Computes Daily Return, 10D Volatility, ATR 14D from *_HISTORICAL tables and
MERGEs results into FORECAST_DERIVED_METRICS without duplicates.
"""

import os
from typing import List, Tuple
import pandas as pd
import numpy as np
import snowflake.connector
from dotenv import load_dotenv

# Load env
load_dotenv()
REQUIRED_VARS = [
    "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA",
]
missing = [v for v in REQUIRED_VARS if not os.getenv(v)]
if missing:
    raise EnvironmentError(f"Missing required env vars: {', '.join(missing)}")

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

def calculate_daily_return(df: pd.DataFrame) -> pd.Series:
    return df["CLOSE"].pct_change()

def calculate_volatility(df: pd.DataFrame, window: int = 10) -> pd.Series:
    return df["DAILY_RETURN"].rolling(window=window).std()

def calculate_atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    high_low = df["HIGH"] - df["LOW"]
    high_close = (df["HIGH"] - df["CLOSE"].shift()).abs()
    low_close = (df["LOW"] - df["CLOSE"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(window=window).mean()

def fetch_historical(symbol: str, cur) -> pd.DataFrame:
    cur.execute(f"""
        SELECT DATE, OPEN, HIGH, LOW, CLOSE
        FROM {symbol}_HISTORICAL
        ORDER BY DATE
    """)
    df = pd.DataFrame(cur.fetchall(), columns=["DATE", "OPEN", "HIGH", "LOW", "CLOSE"])
    df["DATE"] = pd.to_datetime(df["DATE"])
    return df

def build_rows(df: pd.DataFrame, symbol: str) -> List[Tuple]:
    data = df[["DATE", "DAILY_RETURN", "VOLATILITY_10D", "ATR_14D"]].dropna()
    return [(r.DATE.date(), symbol, float(r.DAILY_RETURN), float(r.VOLATILITY_10D), float(r.ATR_14D))
            for r in data.itertuples()]

def merge_metrics(rows: List[Tuple], cur):
    # Create a temporary staging table
    cur.execute("""
        CREATE OR REPLACE TEMP TABLE STG_FORECAST_DERIVED_METRICS (
            DATE DATE,
            SYMBOL STRING,
            DAILY_RETURN FLOAT,
            VOLATILITY_10D FLOAT,
            ATR_14D FLOAT
        )
    """)
    # Bulk load into staging table
    cur.executemany("""
        INSERT INTO STG_FORECAST_DERIVED_METRICS
        (DATE, SYMBOL, DAILY_RETURN, VOLATILITY_10D, ATR_14D)
        VALUES (%s, %s, %s, %s, %s)
    """, rows)

    # Idempotent upsert into target
    cur.execute("""
        MERGE INTO FORECAST_DERIVED_METRICS AS T
        USING STG_FORECAST_DERIVED_METRICS AS S
        ON T.DATE = S.DATE AND T.SYMBOL = S.SYMBOL
        WHEN MATCHED THEN UPDATE SET
            T.DAILY_RETURN   = S.DAILY_RETURN,
            T.VOLATILITY_10D = S.VOLATILITY_10D,
            T.ATR_14D        = S.ATR_14D,
            T.LOAD_TS        = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (DATE, SYMBOL, DAILY_RETURN, VOLATILITY_10D, ATR_14D)
        VALUES
            (S.DATE, S.SYMBOL, S.DAILY_RETURN, S.VOLATILITY_10D, S.ATR_14D)
    """)

def main():
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()

    for symbol in ["SPX", "ES", "VIX", "VVIX"]:
        df = fetch_historical(symbol, cur)
        df["DAILY_RETURN"] = calculate_daily_return(df)
        df["VOLATILITY_10D"] = calculate_volatility(df)
        df["ATR_14D"] = calculate_atr(df)
        rows = build_rows(df, symbol)
        if rows:
            merge_metrics(rows, cur)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Derived metrics ingestion (MERGE) complete.")

if __name__ == "__main__":
    main()
