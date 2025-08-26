# File: vscode_snowflake_starter/src/build_features.py
# Title: Stage 2 – Build FEATURES_DAILY with Wilder’s RSI/ATR
# Commit Notes:
# - Adds rma(), rsi_wilder(), atr_wilder() (EMA with alpha=1/14 per Wilder).
# - Reads MARKET_OHLCV, computes RETURN_1D, RSI_14, ATR_14.
# - Stages rows into TEMP_FEATURES_STAGE and MERGEs into FEATURES_DAILY.
# - UTC load_ts; robust date filtering (days window with warmup).
# - Optional AUDIT_LOG insert with job context.

import argparse
import json
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import snowflake.connector  # required for cursor type hints / safety

from snowflake_conn import get_conn

TGT_TABLE = "FEATURES_DAILY"
SRC_TABLE = "MARKET_OHLCV"
TEMP_TABLE = "TEMP_FEATURES_STAGE"

ORDERED = [
    "symbol",
    "trade_date",
    "close",
    "adj_close",
    "return_1d",
    "rsi_14",
    "atr_14",
    "source",
    "load_ts",
]

def rma(x: pd.Series, n: int = 14) -> pd.Series:
    """Wilder’s RMA via EMA(alpha=1/n)."""
    return x.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()

def rsi_wilder(close: pd.Series, n: int = 14) -> pd.Series:
    """Wilder’s RSI using RMA of up/down moves."""
    delta = close.diff()
    up = delta.clip(lower=0.0)
    down = (-delta).clip(lower=0.0)
    rs = rma(up, n) / rma(down, n)
    return 100 - (100 / (1 + rs))

def atr_wilder(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
    """Wilder’s ATR using RMA of True Range."""
    tr = pd.concat(
        [
            (high - low),
            (high - close.shift()).abs(),
            (low - close.shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return rma(tr, n)

def metrics(cur: "snowflake.connector.cursor.SnowflakeCursor", sym: str):
    cur.execute(
        f"SELECT COUNT(*), TO_CHAR(MAX(TRADE_DATE),'YYYY-MM-DD') FROM {TGT_TABLE} WHERE SYMBOL=%s",
        (sym,),
    )
    cnt, max_dt = cur.fetchone()
    return (int(cnt or 0), max_dt)

def fetch_src(
    cur: "snowflake.connector.cursor.SnowflakeCursor",
    sym: str,
    start_dt: date,
) -> pd.DataFrame:
    cur.execute(
        f"""
        SELECT
          TO_CHAR(TRADE_DATE,'YYYY-MM-DD') AS trade_date,
          OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, SOURCE
        FROM {SRC_TABLE}
        WHERE SYMBOL=%s AND TRADE_DATE >= %s
        ORDER BY TRADE_DATE
        """,
        (sym, start_dt),
    )
    rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(
            columns=["trade_date", "open", "high", "low", "close", "adj_close", "source"]
        )
    df = pd.DataFrame(
        rows,
        columns=["trade_date", "open", "high", "low", "close", "adj_close", "source"],
    )
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date  # pure DATE
    return df

def compute_features(sym: str, df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["return_1d"] = df["close"].pct_change()
    df["rsi_14"] = rsi_wilder(df["close"], 14)
    df["atr_14"] = atr_wilder(df["high"], df["low"], df["close"], 14)

    feats = pd.DataFrame(
        {
            "symbol": sym,
            "trade_date": df["trade_date"],
            "close": df["close"],
            "adj_close": df["adj_close"],
            "return_1d": df["return_1d"],
            "rsi_14": df["rsi_14"],
            "atr_14": df["atr_14"],
            "source": df["source"].fillna("derived"),
        }
    )
    feats["load_ts"] = pd.Timestamp.now(tz=timezone.utc)
    return feats

def stage_and_merge(
    conn: "snowflake.connector.connection.SnowflakeConnection",
    cur: "snowflake.connector.cursor.SnowflakeCursor",
    feats: pd.DataFrame,
):
    if feats.empty:
        return 0

    # Temp table shape == target
    cur.execute(f"CREATE TEMPORARY TABLE {TEMP_TABLE} LIKE {TGT_TABLE}")

    cols_sql = ",".join([c.upper() for c in ORDERED])
    placeholders = ",".join(["%s"] * len(ORDERED))

    # Convert to tuples for executemany; ensure Python date and datetime types are present
    rows = []
    for _, r in feats.iterrows():
        rows.append(
            (
                r["symbol"],
                r["trade_date"],  # datetime.date
                float(r["close"]) if pd.notna(r["close"]) else None,
                float(r["adj_close"]) if pd.notna(r["adj_close"]) else None,
                float(r["return_1d"]) if pd.notna(r["return_1d"]) else None,
                float(r["rsi_14"]) if pd.notna(r["rsi_14"]) else None,
                float(r["atr_14"]) if pd.notna(r["atr_14"]) else None,
                r["source"] if pd.notna(r["source"]) else None,
                r["load_ts"].to_pydatetime(),  # tz-aware -> python datetime
            )
        )

    cur.executemany(
        f"INSERT INTO {TEMP_TABLE} ({cols_sql}) VALUES ({placeholders})",
        rows,
    )

    # Upsert into target
    cur.execute(
        f"""
        MERGE INTO {TGT_TABLE} t
        USING {TEMP_TABLE} s
          ON t.SYMBOL = s.SYMBOL AND t.TRADE_DATE = s.TRADE_DATE
        WHEN MATCHED THEN UPDATE SET
          CLOSE = s.CLOSE,
          ADJ_CLOSE = s.ADJ_CLOSE,
          RETURN_1D = s.RETURN_1D,
          RSI_14 = s.RSI_14,
          ATR_14 = s.ATR_14,
          SOURCE = s.SOURCE,
          LOAD_TS = s.LOAD_TS
        WHEN NOT MATCHED THEN INSERT ({cols_sql})
          VALUES ({cols_sql.replace(",", ", ")})
        """
    )
    # Count staged rows (for log)
    return len(rows)

def log_audit(cur, job: str, sym: str, pre, post, rows_staged: int):
    """Insert a simple audit record into AUDIT_LOG (if table exists)."""
    try:
        stage = "Stage 2: Build Features (Wilder)"
        inp = {"symbol": sym, "pre_count": pre[0], "pre_max": pre[1]}
        out = {"post_count": post[0], "post_max": post[1], "rows_staged": rows_staged}
        cur.execute(
            """
            INSERT INTO AUDIT_LOG (stage, run_timestamp, input_data, output_data, pipeline_job_id)
            SELECT %s, CURRENT_TIMESTAMP, PARSE_JSON(%s), PARSE_JSON(%s), %s
            """,
            (stage, json.dumps(inp), json.dumps(out), job),
        )
    except Exception:
        # Non-fatal if AUDIT_LOG is missing; we still print to stdout
        pass

def main():
    parser = argparse.ArgumentParser(description="Build FEATURES_DAILY with Wilder’s RSI/ATR.")
    parser.add_argument("--symbols", required=True, help="Comma-separated list, e.g. \"^VIX,^VVIX,^GSPC,ES=F\"")
    parser.add_argument("--days", type=int, default=180, help="Window to persist (warmup added internally).")
    parser.add_argument("--job", default="stage2_features", help="Audit job identifier.")
    args = parser.parse_args()

    syms = [s.strip() for s in args.symbols.split(",") if s.strip()]
    # Warmup buffer so RSI/ATR have enough lookback; 60 is conservative
    start_dt = (date.today() - timedelta(days=args.days + 60))
    cutoff = (date.today() - timedelta(days=args.days))

    with get_conn() as conn:
        cur = conn.cursor()

        for sym in syms:
            pre = metrics(cur, sym)

            df = fetch_src(cur, sym, start_dt)
            feats = compute_features(sym, df)
            if not feats.empty:
                # Keep only requested window (by DATE)
                feats = feats[feats["trade_date"] >= cutoff]

            if feats.empty:
                print({"event": "features_noop", "symbol": sym, "reason": "no rows in window"})
                continue

            rows_staged = stage_and_merge(conn, cur, feats)
            post = metrics(cur, sym)
            conn.commit()

            # JSON line for your terminal + optional audit table
            print(
                {
                    "event": "features_ingested",
                    "symbol": sym,
                    "rows_staged": rows_staged,
                    "pre_count": pre[0],
                    "pre_max": pre[1],
                    "post_count": post[0],
                    "post_max": post[1],
                }
            )
            log_audit(cur, args.job, sym, pre, post, rows_staged)
            conn.commit()

if __name__ == "__main__":
    main()
