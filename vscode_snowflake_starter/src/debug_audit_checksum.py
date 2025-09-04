# File: src/debug_audit_checksum.py
# Title: Audit Debug  Show recomputed hashes & sample CSV
# Commit Notes:
# - Anchors window to output.post_max and input.days (like ingest intent).
# - Shows two hashes: (A) float_format="%.8f", (B) default formatting.
# - Prints first 5 CSV lines being hashed to spot formatting/dtype differences.

import argparse, pandas as pd, hashlib, datetime
from decimal import Decimal
from snowflake_conn import get_conn

COLS = ["symbol","trade_date","open","high","low","close","adj_close","volume","source"]

def to_float(x): return float(x) if x is not None else None
def to_int_or_none(x):
    if x is None: return None
    if isinstance(x, Decimal): return int(x)
    try: return int(x)
    except Exception: return None
def to_date(x):
    if isinstance(x, datetime.date): return x
    return datetime.date.fromisoformat(str(x))

def make_df(cur, sym, days, post_max):
    cur.execute("""
      SELECT
        SYMBOL,
        TRADE_DATE,
        OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME, SOURCE
      FROM MARKET_OHLCV
      WHERE SYMBOL = %s
        AND TRADE_DATE BETWEEN DATEADD(day, -(%s+5), TO_DATE(%s)) AND TO_DATE(%s)
      ORDER BY TRADE_DATE
    """, (sym, days, post_max, post_max))
    rows = cur.fetchall()
    recs = []
    for (symbol, trade_date, o,h,l,c,adj,vol,src) in rows:
        recs.append({
            "symbol": symbol,
            "trade_date": to_date(trade_date),
            "open": to_float(o), "high": to_float(h), "low": to_float(l),
            "close": to_float(c), "adj_close": to_float(adj),
            "volume": None if vol is None else to_int_or_none(vol),
            "source": src
        })
    return pd.DataFrame(recs, columns=COLS).where(pd.notna, None)

def sha_of_csv(df: pd.DataFrame, float_fmt=None):
    kw = {"index": False, "lineterminator": "\n"}
    if float_fmt: kw["float_format"] = float_fmt
    csv_bytes = df.to_csv(**kw).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest(), csv_bytes

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log-id", required=True)
    args = ap.parse_args()

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
          SELECT
            input_data:"symbol"::string,
            input_data:"days"::int,
            output_data:"post_max"::string,
            sha256_hash
          FROM AUDIT_LOG
          WHERE log_id = %s
        """, (args.log_id,))
        row = cur.fetchone()
        if not row:
            print({"error":"log_id not found"})
            return
        sym, days, post_max, saved_sha = row

        df = make_df(cur, sym, days, post_max)
        sha_a, bytes_a = sha_of_csv(df, float_fmt="%.8f")  # our intended ingest format
        sha_b, bytes_b = sha_of_csv(df, float_fmt=None)     # pandas default

        # Print summary + first 5 CSV lines for both modes
        def first_lines(b): return b.decode("utf-8").splitlines()[:5]
        print({"symbol": sym, "days": int(days), "post_max": post_max, "rows": len(df)})
        print({"saved_sha": saved_sha})
        print({"sha_a_fmt_8dp": sha_a, "matches_saved": sha_a == saved_sha})
        print({"sha_b_default": sha_b, "matches_saved": sha_b == saved_sha})
        print({"sample_a": first_lines(bytes_a)})
        print({"sample_b": first_lines(bytes_b)})

if __name__ == "__main__":
    main()
