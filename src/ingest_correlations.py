# 📄 File: src/ingest_correlations.py
#
# 📌 Title
# Zen Council – Stage 2.2 Rolling Correlations Ingestion (Idempotent MERGE)
#
# 📝 Commit Notes
# Commit Title: ETL: add Stage 2.2 rolling correlations (SPX/ES/VIX/VVIX) with MERGE + env hardening
# Commit Message:
# - Computes 10D/20D/60D rolling correlations of daily returns for all unique symbol pairs among SPX, ES, VIX, VVIX.
# - Sources: *_HISTORICAL (DATE, OPEN, HIGH, LOW, CLOSE) -> computes pct_change on CLOSE.
# - Loads results into a TEMP staging table, then MERGE into FORECAST_CORRELATIONS keyed by (DATE, SYMBOL_X, SYMBOL_Y).
# - Adds .env loading + required environment validation for Snowflake connection.
# - Idempotent and safe to re-run; designed to follow Stage 2.1 in CI.
#
# Next Step:
# - Add CI job (metrics stage) to run this after Stage 2.1, maintaining ingest -> derived -> correlations order.

import os
from typing import List, Tuple
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from itertools import combinations

# -----------------------------
# Environment & Snowflake Setup
# -----------------------------
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

SYMBOLS = ["SPX", "ES", "VIX", "VVIX"]
WINDOWS = [10, 20, 60]  # rolling windows (days)

# -----------------------------
# Data Access Helpers
# -----------------------------
def fetch_close_series(symbol: str, cur) -> pd.DataFrame:
    cur.execute(f"""
        SELECT DATE, CLOSE
        FROM {symbol}_HISTORICAL
        ORDER BY DATE
    """)
    df = pd.DataFrame(cur.fetchall(), columns=["DATE", f"{symbol}_CLOSE"])
    df["DATE"] = pd.to_datetime(df["DATE"])
    return df

def build_returns_frame(cur) -> pd.DataFrame:
    # Join close series on DATE and compute returns per symbol.
    frames = [fetch_close_series(sym, cur) for sym in SYMBOLS]
    base = frames[0]
    for extra in frames[1:]:
        base = base.merge(extra, on="DATE", how="inner")
    # compute daily returns
    for sym in SYMBOLS:
        base[f"{sym}_RET"] = base[f"{sym}_CLOSE"].pct_change()
    return base

# -----------------------------
# Correlation Computation
# -----------------------------
def calc_pair_rolling_corr(df: pd.DataFrame, sym_x: str, sym_y: str) -> pd.DataFrame:
    out = df[["DATE", f"{sym_x}_RET", f"{sym_y}_RET"]].copy()
    for w in WINDOWS:
        out[f"CORR_{w}D"] = (
            out[f"{sym_x}_RET"]
            .rolling(window=w)
            .corr(out[f"{sym_y}_RET"])
        )
    out["SYMBOL_X"] = sym_x
    out["SYMBOL_Y"] = sym_y
    return out

def assemble_all_pairs(df: pd.DataFrame) -> pd.DataFrame:
    pair_frames = []
    for sym_x, sym_y in combinations(SYMBOLS, 2):
        pair_frames.append(calc_pair_rolling_corr(df, sym_x, sym_y))
    # stack all pairs
    res = pd.concat(pair_frames, ignore_index=True)
    # keep only rows where at least the largest window is non-null
    res = res.dropna(subset=[f"CORR_{WINDOWS[-1]}D"])
    return res

# -----------------------------
# Snowflake Load (Idempotent)
# -----------------------------
def merge_correlations(rows: List[Tuple], cur):
    cur.execute("""
        CREATE OR REPLACE TEMP TABLE STG_FORECAST_CORRELATIONS (
            DATE DATE,
            SYMBOL_X STRING,
            SYMBOL_Y STRING,
            CORR_10D FLOAT,
            CORR_20D FLOAT,
            CORR_60D FLOAT
        )
    """)
    cur.executemany("""
        INSERT INTO STG_FORECAST_CORRELATIONS
        (DATE, SYMBOL_X, SYMBOL_Y, CORR_10D, CORR_20D, CORR_60D)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, rows)

    cur.execute("""
        MERGE INTO FORECAST_CORRELATIONS AS T
        USING STG_FORECAST_CORRELATIONS AS S
        ON T.DATE = S.DATE AND T.SYMBOL_X = S.SYMBOL_X AND T.SYMBOL_Y = S.SYMBOL_Y
        WHEN MATCHED THEN UPDATE SET
            T.CORR_10D = S.CORR_10D,
            T.CORR_20D = S.CORR_20D,
            T.CORR_60D = S.CORR_60D,
            T.LOAD_TS  = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (DATE, SYMBOL_X, SYMBOL_Y, CORR_10D, CORR_20D, CORR_60D)
        VALUES
            (S.DATE, S.SYMBOL_X, S.SYMBOL_Y, S.CORR_10D, S.CORR_20D, S.CORR_60D)
    """)

def build_rows(df: pd.DataFrame) -> List[Tuple]:
    payload = []
    for r in df.itertuples(index=False):
        payload.append((
            pd.to_datetime(r.DATE).date(),
            r.SYMBOL_X, r.SYMBOL_Y,
            float(r.CORR_10D), float(r.CORR_20D), float(r.CORR_60D)
        ))
    return payload

# -----------------------------
# Main
# -----------------------------
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

    # Build daily returns for all symbols (aligned on DATE)
    df = build_returns_frame(cur)

    # Compute rolling correlations for every symbol pair
    corr_df = assemble_all_pairs(df)

    # Load to Snowflake idempotently
    rows = build_rows(corr_df)
    if rows:
        merge_correlations(rows, cur)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Rolling correlations (10D/20D/60D) ingestion complete (MERGE).")

if __name__ == "__main__":
    main()
