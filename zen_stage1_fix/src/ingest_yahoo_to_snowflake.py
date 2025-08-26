"""
# ZEN COMMIT HEADER
# Title: Stage 1 Fix — Yahoo → Snowflake Ingestion with Fail-Fast & Diffs
# Owner: ZenMarket AI (Zen Council)
# Date: 2025-08-25
# Purpose: Pull OHLCV from Yahoo (VIX, VVIX, SPX, ES), upsert into Snowflake, with fail-fast safeguards and pre/post diffs.
# Status: Production-intent; idempotent; logs JSON; optional Slack alert.
# Notes: SPY retired. Source of truth = Yahoo Finance. Keys: (symbol, trade_date).
"""

import os
import sys
import uuid
import json
import argparse
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import pandas as pd
import yfinance as yf
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ============== Config / Env ==================

DEFAULT_SYMBOLS = ["^VIX", "^VVIX", "^GSPC", "ES=F"]  # VIX, VVIX, SPX, ES
TARGET_TABLE = os.getenv("TARGET_TABLE", "MARKET_OHLCV")
STRICT_ZERO_WRITE = os.getenv("STRICT_ZERO_WRITE", "true").lower() in ["1", "true", "yes", "y"]
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

# Snowflake env
SF_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SF_USER      = os.getenv("SNOWFLAKE_USER")
SF_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SF_ROLE      = os.getenv("SNOWFLAKE_ROLE")
SF_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SF_DATABASE  = os.getenv("SNOWFLAKE_DATABASE")
SF_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA")

REQUIRED_VARS = ["SNOWFLAKE_ACCOUNT","SNOWFLAKE_USER","SNOWFLAKE_PASSWORD","SNOWFLAKE_ROLE","SNOWFLAKE_WAREHOUSE","SNOWFLAKE_DATABASE","SNOWFLAKE_SCHEMA"]

# ============== Utility: Logging ==================

def jlog(event: str, **kwargs):
    payload = {"event": event, "ts": datetime.now(timezone.utc).isoformat(), **kwargs}
    print(json.dumps(payload, default=str, ensure_ascii=False), flush=True)

def post_slack(text: str):
    if not SLACK_WEBHOOK_URL:
        return
    try:
        import requests
        requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)
    except Exception as e:
        jlog("slack_error", error=str(e))

# ============== Snowflake Helpers ==================

def get_conn():
    missing = [v for v in REQUIRED_VARS if not os.getenv(v)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {missing}")
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        role=SF_ROLE,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        client_session_keep_alive=True,
    )

def exec_sql(cur, sql_text: str, params: Dict[str, Any] | None = None):
    cur.execute(sql_text, params or {})
    try:
        return cur.fetchall()
    except Exception:
        return None

def ensure_table(cur, table: str):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        SYMBOL      STRING NOT NULL,
        TRADE_DATE  DATE   NOT NULL,
        OPEN        FLOAT,
        HIGH        FLOAT,
        LOW         FLOAT,
        CLOSE       FLOAT,
        ADJ_CLOSE   FLOAT,
        VOLUME      NUMBER,
        SOURCE      STRING,
        LOAD_TS     TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
    );
    """
    cur.execute(ddl)
    try:
        cur.execute(f"CREATE OR REPLACE UNIQUE INDEX IF NOT EXISTS IDX_{table}_PK ON {table} (SYMBOL, TRADE_DATE);")
    except Exception:
        pass

def table_metrics(cur, table: str, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    in_list = ",".join([f"'{s}'" for s in symbols])
    q = f"""
      SELECT SYMBOL, COUNT(*) AS ROWS_CNT, TO_CHAR(MAX(TRADE_DATE),'YYYY-MM-DD') AS MAX_DATE
      FROM {table}
      WHERE SYMBOL IN ({in_list})
      GROUP BY SYMBOL
    """
    exec_sql(cur, "ALTER SESSION SET TIMEZONE = 'UTC'")
    rows = exec_sql(cur, q) or []
    out = {s: {"rows": 0, "max_date": None} for s in symbols}
    for sym, cnt, maxd in rows:
        out[sym] = {"rows": int(cnt or 0), "max_date": maxd}
    return out

# ============== Yahoo Fetch ==================

def fetch_yahoo(symbol: str, start: datetime | None = None, end: datetime | None = None) -> pd.DataFrame:
    kw = {}
    if start: kw["start"] = start
    if end: kw["end"] = end
    data = yf.download(symbol, interval="1d", progress=False, **kw)
    if data is None or data.empty:
        return pd.DataFrame()
    df = data.reset_index().rename(columns={
        "Date":"trade_date",
        "Open":"open","High":"high","Low":"low","Close":"close",
        "Adj Close":"adj_close","Volume":"volume"
    })
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.tz_localize(None).dt.date
    df["symbol"] = symbol
    df["source"] = "yahoo"
    df["load_ts"] = datetime.now(timezone.utc)
    cols = ["symbol","trade_date","open","high","low","close","adj_close","volume","source","load_ts"]
    return df[cols].sort_values(["trade_date"]).reset_index(drop=True)

def fetch_incremental(cur, table: str, symbol: str, lookback_days: int = 7) -> pd.DataFrame:
    q = f"SELECT MAX(TRADE_DATE) FROM {table} WHERE SYMBOL = %s"
    cur.execute(q, (symbol,))
    row = cur.fetchone()
    last_date = row[0] if row and row[0] else None
    start = None
    if last_date:
        start = datetime.combine(last_date, datetime.min.time()).replace(tzinfo=timezone.utc) - timedelta(days=lookback_days)
    return fetch_yahoo(symbol, start=start)

# ============== Upsert via Temp Table + MERGE ==================

def merge_upsert(cur, table: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    temp = f"TEMP_OHLCV_{uuid.uuid4().hex[:8]}"
    cur.execute(f"CREATE TEMPORARY TABLE {temp} LIKE {table}")
    success, nchunks, nrows, _ = write_pandas(cur.connection, df, temp)
    if not success:
        raise RuntimeError("write_pandas returned success=False")
    merge_sql = f'''
        MERGE INTO {table} t
        USING {temp} s
          ON t.SYMBOL = s.SYMBOL AND t.TRADE_DATE = s.TRADE_DATE
        WHEN MATCHED THEN UPDATE SET
          OPEN = s.OPEN, HIGH = s.HIGH, LOW = s.LOW, CLOSE = s.CLOSE,
          ADJ_CLOSE = s.ADJ_CLOSE, VOLUME = s.VOLUME, SOURCE = s.SOURCE, LOAD_TS = s.LOAD_TS
        WHEN NOT MATCHED THEN INSERT (SYMBOL, TRADE_DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME, SOURCE, LOAD_TS)
          VALUES (s.SYMBOL, s.TRADE_DATE, s.OPEN, s.HIGH, s.LOW, s.CLOSE, s.ADJ_CLOSE, s.VOLUME, s.SOURCE, s.LOAD_TS);
    '''
    cur.execute(merge_sql)
    cur.execute(f"DROP TABLE IF EXISTS {temp}")
    return int(nrows)

# ============== Main ==================

def main():
    parser = argparse.ArgumentParser(description="Yahoo → Snowflake Stage 1 Ingestion with fail-fast and diffs")
    parser.add_argument("--symbols", type=str, default=",".join(DEFAULT_SYMBOLS),
                        help="Comma-separated list of Yahoo symbols (default: ^VIX,^VVIX,^GSPC,ES=F)")
    parser.add_argument("--full-refresh", action="store_true", help="Fetch from 2000-01-01 instead of incremental")
    parser.add_argument("--start", type=str, default=None, help="Override start date YYYY-MM-DD")
    parser.add_argument("--strict-zero-write", action="store_true", help="Force failure if zero net inserts/updates detected")
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    strict = STRICT_ZERO_WRITE or args.strict_zero_write

    with get_conn() as conn:
        conn.cursor().execute("ALTER SESSION SET TIMEZONE = 'UTC'")
        with conn.cursor() as cur:
            ensure_table(cur, TARGET_TABLE)

            pre = table_metrics(cur, TARGET_TABLE, symbols)
            jlog("pre_metrics", table=TARGET_TABLE, metrics=pre)

            all_dfs = []
            for s in symbols:
                if args.full_refresh:
                    start = datetime(2000,1,1, tzinfo=timezone.utc)
                    if args.start:
                        start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
                    df = fetch_yahoo(s, start=start)
                else:
                    df = fetch_incremental(cur, TARGET_TABLE, s, lookback_days=7)
                jlog("fetched", symbol=s, rows=0 if df is None else int(len(df)))
                if df is not None and not df.empty:
                    all_dfs.append(df)

            if not all_dfs:
                msg = "No data fetched for any symbol."
                jlog("no_data_fetched", message=msg)
                if strict:
                    post_slack(f":warning: Zen Stage1 zero-fetch. {msg}")
                    raise SystemExit(1)
                else:
                    raise SystemExit(0)

            df_all = pd.concat(all_dfs, ignore_index=True)

            rows_staged = merge_upsert(cur, TARGET_TABLE, df_all)
            conn.commit()

            post = table_metrics(cur, TARGET_TABLE, symbols)
            jlog("post_metrics", table=TARGET_TABLE, metrics=post)

            total_increase = 0
            increased_symbols = []
            for s in symbols:
                pre_cnt = pre.get(s,{}).get("rows",0)
                post_cnt = post.get(s,{}).get("rows",0)
                if post_cnt > pre_cnt:
                    increased_symbols.append(s)
                total_increase += max(0, post_cnt - pre_cnt)

            max_date_progress = any(
                (post.get(s,{}).get("max_date") != pre.get(s,{}).get("max_date"))
                for s in symbols
            )

            jlog("delta", rows_staged=int(rows_staged), total_increase=int(total_increase),
                 increased_symbols=increased_symbols, max_date_progress=max_date_progress)

            if strict and total_increase == 0 and not max_date_progress:
                post_slack(":warning: Zen Stage1 fail-fast: zero increase and no max_date progress.")
                jlog("fail_fast_zero_delta", strict=strict)
                raise SystemExit(1)

            jlog("success", message="Ingestion completed with upsert and diffs.")

if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code if isinstance(e.code, int) else 1)
    except Exception as e:
        jlog("fatal_error", error=str(e))
        post_slack(f":x: Zen Stage1 fatal error: {e}")
        sys.exit(1)