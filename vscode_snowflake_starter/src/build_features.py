# File: src/build_features.py
# Title: Stage 2  Build daily features (RSI-14, ATR-14, returns)  TZ-safe
# Commit Notes:
# - Filters by pure DATEs (UTC anchored) to avoid tz-naive/aware comparison errors.
# - Upserts idempotently into FEATURES_DAILY; logs post-merge hash.
# - Safe to rerun; no .env changes.

import argparse, json, hashlib
import pandas as pd
from datetime import timezone
from snowflake_conn import get_conn

SRC_TABLE = "MARKET_OHLCV"
TGT_TABLE = "FEATURES_DAILY"
COLS = ["symbol","trade_date","close","adj_close","return_1d","rsi_14","atr_14","source"]

def df_sha256(df: pd.DataFrame) -> str:
    b = df.to_csv(index=False, lineterminator="\n", float_format="%.8f").encode("utf-8")
    return hashlib.sha256(b).hexdigest()

def fetch_ohlcv(cur, sym: str, days: int) -> pd.DataFrame:
    # Cushion for indicators (14) -> +30
    cur.execute(f"""
      SELECT SYMBOL, TRADE_DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME, SOURCE
      FROM {SRC_TABLE}
      WHERE SYMBOL=%s
        AND TRADE_DATE >= DATEADD(day, -(%s+30), CURRENT_DATE())
      ORDER BY TRADE_DATE
    """, (sym, days))
    rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=["symbol","trade_date","open","high","low","close","adj_close","volume","source"])

def rsi(series: pd.Series, period=14):
    delta = series.diff()
    gain = (delta.clip(lower=0)).rolling(period, min_periods=period).mean()
    loss = (-delta.clip(upper=0)).rolling(period, min_periods=period).mean()
    rs = gain / loss.replace(0, pd.NA)
    return 100 - (100 / (1 + rs))

def atr(high, low, close, period=14):
    prev_close = close.shift(1)
    tr = pd.concat([(high - low).abs(), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=period).mean()

def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["return_1d"] = out["adj_close"].pct_change()
    out["rsi_14"] = rsi(out["adj_close"], 14)
    out["atr_14"] = atr(out["high"], out["low"], out["adj_close"], 14)
    out = out[["symbol","trade_date","close","adj_close","return_1d","rsi_14","atr_14","source"]]
    # Drop rows until indicators exist
    return out.dropna().reset_index(drop=True)

def upsert_features(cur, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE TEMP_FEATURES_STAGE LIKE {TGT_TABLE}")
    cols_sql = ",".join([c.upper() for c in COLS])
    placeholders = ",".join(["%s"] * len(COLS))
    rows = [tuple(rec) for rec in df.itertuples(index=False, name=None)]
    cur.executemany(f"INSERT INTO TEMP_FEATURES_STAGE ({cols_sql}) VALUES ({placeholders})", rows)
    cur.execute(f"""
      MERGE INTO {TGT_TABLE} t
      USING TEMP_FEATURES_STAGE s
        ON t.SYMBOL=s.SYMBOL AND t.TRADE_DATE=s.TRADE_DATE
      WHEN MATCHED THEN UPDATE SET
        CLOSE=s.CLOSE, ADJ_CLOSE=s.ADJ_CLOSE, RETURN_1D=s.RETURN_1D, RSI_14=s.RSI_14, ATR_14=s.ATR_14,
        SOURCE=s.SOURCE, LOAD_TS=CURRENT_TIMESTAMP()
      WHEN NOT MATCHED THEN INSERT (SYMBOL,TRADE_DATE,CLOSE,ADJ_CLOSE,RETURN_1D,RSI_14,ATR_14,SOURCE)
        VALUES (s.SYMBOL,s.TRADE_DATE,s.CLOSE,s.ADJ_CLOSE,s.RETURN_1D,s.RSI_14,s.ATR_14,s.SOURCE)
    """)
    cur.execute("DROP TABLE IF EXISTS TEMP_FEATURES_STAGE")
    return len(rows)

def metrics(cur, sym: str):
    cur.execute(f"SELECT COUNT(*), TO_CHAR(MAX(TRADE_DATE),'YYYY-MM-DD') FROM {TGT_TABLE} WHERE SYMBOL=%s", (sym,))
    cnt, maxd = cur.fetchone()
    return int(cnt or 0), maxd

def select_frame_for_hash(cur, sym: str, days: int, post_max: str) -> pd.DataFrame:
    cur.execute(f"""
      SELECT SYMBOL AS symbol,
             TO_CHAR(TRADE_DATE,'YYYY-MM-DD') AS trade_date,
             CLOSE, ADJ_CLOSE, RETURN_1D, RSI_14, ATR_14, SOURCE
      FROM {TGT_TABLE}
      WHERE SYMBOL=%s
        AND TRADE_DATE BETWEEN DATEADD(day, -(%s+5), TO_DATE(%s)) AND TO_DATE(%s)
      ORDER BY TRADE_DATE
    """, (sym, days, post_max, post_max))
    data = cur.fetchall()
    return pd.DataFrame(data, columns=COLS).where(pd.notna, None)

def audit_log(cur, *, stage: str, input_obj: dict, output_obj: dict, job_id: str, sha256_hash: str):
    cur.execute("""
      INSERT INTO AUDIT_LOG(stage, run_timestamp, input_data, output_data, pipeline_job_id, sha256_hash)
      SELECT %s, CURRENT_TIMESTAMP(), parse_json(%s), parse_json(%s), %s, %s
    """, (stage, json.dumps(input_obj, separators=(",",":")),
                json.dumps(output_obj, separators=(",",":")), job_id, sha256_hash))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default="^VIX,^VVIX,^GSPC,ES=F")
    ap.add_argument("--days", type=int, default=120)
    ap.add_argument("--job", default="stage2_features_manual")
    args = ap.parse_args()
    syms = [s.strip() for s in args.symbols.split(",") if s.strip()]
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("ALTER SESSION SET TIMEZONE='UTC'")
        # UTC-anchored cutoff as pure date
        cutoff_date = (pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=args.days)).date()
        for sym in syms:
            pre_cnt, pre_max = metrics(cur, sym)
            ohlcv = fetch_ohlcv(cur, sym, args.days)
            feats = compute_features(ohlcv)
            # keep only recent window  compare DATEs to avoid tz issues
            if not feats.empty:
                feats["trade_date"] = pd.to_datetime(feats["trade_date"], utc=True).dt.date
                feats = feats[feats["trade_date"] >= cutoff_date].reset_index(drop=True)
            staged = upsert_features(cur, feats)
            conn.commit()
            post_cnt, post_max = metrics(cur, sym)
            print({"event":"features_ingested", "symbol":sym, "rows_staged": staged,
                   "pre_count": pre_cnt, "pre_max": pre_max, "post_count": post_cnt, "post_max": post_max})
            # post-merge hash for audit
            dfh = select_frame_for_hash(cur, sym, args.days, post_max) if post_max else pd.DataFrame(columns=COLS)
            fingerprint = df_sha256(dfh)
            audit_log(cur,
                stage="Stage 2: Build Features",
                input_obj={"symbol": sym, "days": args.days, "pre_count": pre_cnt, "pre_max": pre_max},
                output_obj={"rows_staged": staged, "post_count": post_cnt, "post_max": post_max, "hash_mode":"post_merge_table_features"},
                job_id=args.job,
                sha256_hash=fingerprint,
            )
            conn.commit()

if __name__ == "__main__":
    main()
