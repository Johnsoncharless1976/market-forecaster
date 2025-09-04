# File: src/ingest_yahoo_to_market_ohlcv.py
# Title: Stage 1  Ingest + Audit logging (post-merge hash)
# Commit Notes:
# - SHA256 now computed from MARKET_OHLCV **after MERGE**, anchored to post_max.
# - Verifier now matches new audit rows 1:1.

import argparse, json, hashlib, pandas as pd
from datetime import datetime, timedelta, timezone
import yfinance as yf
from snowflake_conn import get_conn

TABLE = "MARKET_OHLCV"
ORDERED_INSERT = ["symbol","trade_date","open","high","low","close","adj_close","volume","source"]  # no load_ts
COLS = ["symbol","trade_date","open","high","low","close","adj_close","volume","source"]

def fetch_yahoo(symbol: str, days: int) -> pd.DataFrame:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days + 5)
    df = yf.download(symbol, start=start, end=end, interval="1d",
                     progress=False, auto_adjust=False, actions=False,
                     group_by="column", threads=False)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.reset_index()
    if "Adj Close" not in df.columns and "Close" in df.columns:
        df["Adj Close"] = df["Close"]
    df = df.rename(columns={
        "Date":"trade_date",
        "Open":"open","High":"high","Low":"low","Close":"close",
        "Adj Close":"adj_close","Volume":"volume"
    })
    # Market days only (Yahoo daily already is); keep weekday guard
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.tz_localize(None).dt.date
    df = df[[d.weekday() < 5 for d in df["trade_date"]]]

    df["symbol"] = symbol
    df["source"] = "yahoo"
    out = df[ORDERED_INSERT].sort_values(["trade_date"]).reset_index(drop=True)
    out = out.where(pd.notna(out), None)
    return out

def df_sha256(df: pd.DataFrame) -> str:
    # Stable hashing: LF line ends, 8dp float formatting, no index
    csv_bytes = df.to_csv(index=False, lineterminator="\n", float_format="%.8f").encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()

def metrics(cur, symbol: str):
    cur.execute(f"SELECT COUNT(*), TO_CHAR(MAX(TRADE_DATE),'YYYY-MM-DD') FROM {TABLE} WHERE SYMBOL=%s", (symbol,))
    cnt, maxd = cur.fetchone()
    return int(cnt or 0), maxd

def insert_rows(cur, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE TEMP_OHLCV_STAGE LIKE {TABLE}")
    cols_sql = ",".join([c.upper() for c in ORDERED_INSERT])
    placeholders = ",".join(["%s"] * len(ORDERED_INSERT))
    rows = [tuple(rec) for rec in df.itertuples(index=False, name=None)]
    cur.executemany(f"INSERT INTO TEMP_OHLCV_STAGE ({cols_sql}) VALUES ({placeholders})", rows)
    return len(rows)

def merge_upsert(cur):
    cur.execute(f'''
        MERGE INTO {TABLE} t
        USING TEMP_OHLCV_STAGE s
          ON t.SYMBOL = s.SYMBOL AND t.TRADE_DATE = s.TRADE_DATE
        WHEN MATCHED THEN UPDATE SET
          OPEN=s.OPEN, HIGH=s.HIGH, LOW=s.LOW, CLOSE=s.CLOSE, ADJ_CLOSE=s.ADJ_CLOSE,
          VOLUME=s.VOLUME, SOURCE=s.SOURCE, LOAD_TS=CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (SYMBOL, TRADE_DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME, SOURCE)
          VALUES (s.SYMBOL, s.TRADE_DATE, s.OPEN, s.HIGH, s.LOW, s.CLOSE, s.ADJ_CLOSE, s.VOLUME, s.SOURCE)
    ''')
    cur.execute("DROP TABLE IF EXISTS TEMP_OHLCV_STAGE")

def select_frame_for_hash(cur, symbol: str, days: int, post_max: str) -> pd.DataFrame:
    cur.execute(f"""
      SELECT
        SYMBOL        AS symbol,
        TO_CHAR(TRADE_DATE,'YYYY-MM-DD')       AS trade_date,
        OPEN          AS open,
        HIGH          AS high,
        LOW           AS low,
        CLOSE         AS close,
        ADJ_CLOSE     AS adj_close,
        VOLUME        AS volume,
        SOURCE        AS source
      FROM {TABLE}
      WHERE SYMBOL = %s
        AND TRADE_DATE BETWEEN DATEADD(day, -(%s+5), TO_DATE(%s)) AND TO_DATE(%s)
      ORDER BY TRADE_DATE
    """, (symbol, days, post_max, post_max))
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=COLS).where(pd.notna, None)
    return df

def audit_log(cur, *, stage: str, input_obj: dict, output_obj: dict, job_id: str, sha256_hash: str):
    cur.execute("""
        INSERT INTO AUDIT_LOG(stage, run_timestamp, input_data, output_data, pipeline_job_id, sha256_hash)
        SELECT %s, CURRENT_TIMESTAMP(), parse_json(%s), parse_json(%s), %s, %s
    """, (stage, json.dumps(input_obj, separators=(",",":")),
                json.dumps(output_obj, separators=(",",":")),
                job_id, sha256_hash))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default="^VIX,^VVIX,^GSPC,ES=F")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--job", default="stage1_ps_manual")
    args = ap.parse_args()
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("ALTER SESSION SET TIMEZONE='UTC'")
        for sym in symbols:
            pre_cnt, pre_max = metrics(cur, sym)
            df = fetch_yahoo(sym, args.days)
            print({"event":"fetched", "symbol":sym, "rows": 0 if df is None else len(df)})
            staged = insert_rows(cur, df)
            merge_upsert(cur)
            post_cnt, post_max = metrics(cur, sym)
            print({"event":"ingested", "symbol":sym, "rows_staged": staged,
                   "pre_count": pre_cnt, "pre_max": pre_max,
                   "post_count": post_cnt, "post_max": post_max})

            # NEW: compute fingerprint from post-merge table view (verifier-compatible)
            df_h = select_frame_for_hash(cur, sym, args.days, post_max)
            fingerprint = df_sha256(df_h)

            audit_log(
                cur,
                stage="Stage 1: Yahoo->MARKET_OHLCV",
                input_obj={"symbol": sym, "days": args.days, "pre_count": pre_cnt, "pre_max": pre_max},
                output_obj={"rows_staged": staged, "post_count": post_cnt, "post_max": post_max, "hash_mode":"post_merge_table"},
                job_id=args.job,
                sha256_hash=fingerprint,
            )
            conn.commit()

if __name__ == "__main__":
    main()
