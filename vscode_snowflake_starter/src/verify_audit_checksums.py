# File: src/verify_audit_checksums.py
# Title: Audit Verifier  Recompute & compare checksums (run-anchored, dtype-stable)
# Commit Notes:
# - Anchors to output.post_max per run.
# - Matches ingest CSV formatting: lineterminator="\n", float_format="%.8f".
# - Ensures volume is int (when present) and trade_date is a date object.

import argparse, pandas as pd, hashlib, datetime
from decimal import Decimal
from snowflake_conn import get_conn

STAGE_DEFAULT = "Stage 1: Yahoo->MARKET_OHLCV"
COLS = ["symbol","trade_date","open","high","low","close","adj_close","volume","source"]

def to_float(x):
    return float(x) if x is not None else None

def to_int_or_none(x):
    if x is None:
        return None
    # Decimal -> int; floats/int pass through
    if isinstance(x, Decimal):
        return int(x)
    return int(x)

def to_date(x):
    # Snowflake returns date as datetime.date already; if string, parse
    if isinstance(x, datetime.date):
        return x
    return datetime.date.fromisoformat(str(x))

def df_sha256(df: pd.DataFrame) -> str:
    csv_bytes = df.to_csv(index=False, lineterminator="\n", float_format="%.8f").encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=5)
    ap.add_argument("--stage", default=STAGE_DEFAULT)
    args = ap.parse_args()

    where = "WHERE 1=1"
    params = []
    if args.stage:
        where += " AND stage = %s"
        params.append(args.stage)
    where += " ORDER BY created_at DESC LIMIT %s"
    params.append(args.limit)

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
          SELECT
            log_id,
            input_data:"symbol"::string       AS sym,
            input_data:"days"::int            AS days,
            output_data:"post_max"::string    AS post_max,
            sha256_hash                       AS sha
          FROM AUDIT_LOG
          {where}
        """, params)
        audits = cur.fetchall()

        for log_id, sym, days, post_max, saved_sha in audits:
            cur.execute(f"""
              SELECT
                SYMBOL,
                TRADE_DATE,
                OPEN,
                HIGH,
                LOW,
                CLOSE,
                ADJ_CLOSE,
                VOLUME,
                SOURCE
              FROM MARKET_OHLCV
              WHERE SYMBOL = %s
                AND TRADE_DATE BETWEEN DATEADD(day, -(%s+5), TO_DATE(%s)) AND TO_DATE(%s)
              ORDER BY TRADE_DATE
            """, (sym, days, post_max, post_max))
            rows = cur.fetchall()

            # Rebuild with ingest dtypes/ordering
            recs = []
            for (symbol, trade_date, open_, high, low, close, adj_close, volume, source) in rows:
                recs.append({
                    "symbol": symbol,
                    "trade_date": to_date(trade_date),
                    "open": to_float(open_),
                    "high": to_float(high),
                    "low": to_float(low),
                    "close": to_float(close),
                    "adj_close": to_float(adj_close),
                    "volume": None if volume is None else to_int_or_none(volume),
                    "source": source
                })
            df = pd.DataFrame(recs, columns=COLS).where(pd.notna, None)
            recomputed = df_sha256(df)

            print({"log_id": log_id, "symbol": sym, "days": int(days) if days is not None else None,
                   "post_max": post_max, "match": (recomputed == saved_sha)})

if __name__ == "__main__":
    main()
