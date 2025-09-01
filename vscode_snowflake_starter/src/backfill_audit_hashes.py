# File: src/backfill_audit_hashes.py
# Title: Audit Backfill  Normalize old rows to post-merge hashes
# Commit Notes:
# - Finds recent Stage 1 entries missing output_data.hash_mode='post_merge_table'.
# - Recomputes SHA-256 from MARKET_OHLCV (run-anchored) and updates the row.

import argparse, pandas as pd, hashlib
from snowflake_conn import get_conn

STAGE = "Stage 1: Yahoo->MARKET_OHLCV"
COLS  = ["symbol","trade_date","open","high","low","close","adj_close","volume","source"]

def df_sha256(df: pd.DataFrame) -> str:
    b = df.to_csv(index=False, lineterminator="\n", float_format="%.8f").encode("utf-8")
    return hashlib.sha256(b).hexdigest()

def fetch_frame(cur, sym, days, post_max):
    cur.execute(f"""
      SELECT
        SYMBOL        AS symbol,
        TO_CHAR(TRADE_DATE,'YYYY-MM-DD') AS trade_date,
        OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME, SOURCE
      FROM MARKET_OHLCV
      WHERE SYMBOL = %s
        AND TRADE_DATE BETWEEN DATEADD(day, -(%s+5), TO_DATE(%s)) AND TO_DATE(%s)
      ORDER BY TRADE_DATE
    """, (sym, days, post_max, post_max))
    data = cur.fetchall()
    return pd.DataFrame(data, columns=COLS).where(pd.notna, None)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=50, help="Max rows to backfill")
    args = ap.parse_args()

    with get_conn() as conn:
        cur = conn.cursor()
        # find candidate rows (no/old hash_mode)
        cur.execute(f"""
          SELECT log_id,
                 input_data:"symbol"::string  AS sym,
                 input_data:"days"::int      AS days,
                 output_data:"post_max"::string AS post_max
          FROM AUDIT_LOG
          WHERE stage = %s
            AND (output_data:"hash_mode" IS NULL OR output_data:"hash_mode"::string <> 'post_merge_table')
          ORDER BY created_at DESC
          LIMIT %s
        """, (STAGE, args.limit))
        rows = cur.fetchall()

        updated = 0
        for log_id, sym, days, post_max in rows:
            df = fetch_frame(cur, sym, days, post_max)
            new_sha = df_sha256(df)
            cur.execute("""
              UPDATE AUDIT_LOG
              SET sha256_hash = %s,
                  output_data = OBJECT_INSERT(output_data, 'hash_mode', 'post_merge_table', TRUE)
              WHERE log_id = %s
            """, (new_sha, log_id))
            updated += 1
        conn.commit()
        print({"stage": STAGE, "updated": updated})

if __name__ == "__main__":
    main()
