# File: vscode_snowflake_starter/src/exec_features_summary.py
# Title: Stage 2  FEATURES_DAILY Executive Report (Wilder RSI/ATR)
# Commit Notes:
# - Counts duplicates, weekend rows, nulls, and out-of-bounds RSI/ATR.
# - Confirms alignment with MARKET_OHLCV (missing feature rows).
# - Emits Markdown + CSV into audit_exports/stage2_exec_YYYYMMDD_HHMMSS.

from datetime import datetime, timezone
import os, json
import snowflake.connector
from snowflake_conn import get_conn

def q(cur, sql, params=None):
    cur.execute(sql, params or {})
    return cur.fetchall()

def main():
    stamp = datetime.now(timezone.utc).astimezone().strftime("%Y%m%d_%H%M%S")
    base = os.path.join(os.getcwd(), "audit_exports", f"stage2_exec_{stamp}")
    os.makedirs(base, exist_ok=True)

    with get_conn() as conn, conn.cursor(snowflake.connector.DictCursor) as cur:
        # 1) Duplicates (symbol, trade_date)
        dups = q(cur, """
          SELECT 1 FROM FEATURES_DAILY
          QUALIFY ROW_NUMBER() OVER (PARTITION BY SYMBOL, TRADE_DATE ORDER BY TRADE_DATE)=2
        """)
        n_dup = len(dups)

        # 2) Weekend rows (should be zero)
        wknd = q(cur, """
          SELECT 1 FROM FEATURES_DAILY
          WHERE DAYOFWEEKISO(TRADE_DATE) IN (6,7)
        """)
        n_wknd = len(wknd)

        # 3) Nulls in critical fields
        nulls = q(cur, """
          SELECT 1 FROM FEATURES_DAILY
          WHERE CLOSE IS NULL OR ADJ_CLOSE IS NULL OR RSI_14 IS NULL OR ATR_14 IS NULL
        """)
        n_null = len(nulls)

        # 4) Out-of-bounds RSI/ATR
        oob = q(cur, """
          SELECT 1 FROM FEATURES_DAILY
          WHERE RSI_14 < 0 OR RSI_14 > 100 OR ATR_14 < 0
        """)
        n_oob = len(oob)

        # 5) Missing features for market trading days (market has a day we don't)
        miss = q(cur, """
          SELECT 1
          FROM MARKET_OHLCV m
          LEFT JOIN FEATURES_DAILY f
            ON f.SYMBOL=m.SYMBOL AND f.TRADE_DATE=m.TRADE_DATE
          WHERE f.TRADE_DATE IS NULL
        """)
        n_miss = len(miss)

    # Build PASS/FAIL quickly
    pf = lambda n: "PASS" if n==0 else "FAIL"

    md_path  = os.path.join(base, "EXEC_REPORT.md")
    csv_path = os.path.join(base, "EXEC_summary.csv")

    md = f"""# Stage 2 Audit  Executive Report ({stamp})

| Check                                | Violations | Status |
|--------------------------------------|-----------:|:------:|
| Duplicates (SYMBOL, TRADE_DATE)      | {n_dup}    | {pf(n_dup)} |
| Weekend rows                         | {n_wknd}   | {pf(n_wknd)} |
| Nulls in CLOSE/ADJ/RSI/ATR           | {n_null}   | {pf(n_null)} |
| Out-of-bounds RSI/ATR                | {n_oob}    | {pf(n_oob)} |
| Missing features vs MARKET_OHLCV     | {n_miss}   | {pf(n_miss)} |

_Scope_: Stage 2 (Feature integrity, Wilder RSI/ATR).  _Notes_: RSI in [0,100], ATR  0.
"""
    csv = "check,violations,status\n" + "\n".join([
        f"duplicates,{n_dup},{pf(n_dup)}",
        f"weekend_rows,{n_wknd},{pf(n_wknd)}",
        f"nulls_critical,{n_null},{pf(n_null)}",
        f"oob_rsi_atr,{n_oob},{pf(n_oob)}",
        f"missing_vs_market,{n_miss},{pf(n_miss)}",
    ]) + "\n"

    with open(md_path, "w", encoding="utf-8") as f: f.write(md)
    with open(csv_path, "w", encoding="utf-8") as f: f.write(csv)

    print(json.dumps({"exec_md": md_path, "exec_csv": csv_path}, indent=2))
if __name__ == "__main__":
    main()
