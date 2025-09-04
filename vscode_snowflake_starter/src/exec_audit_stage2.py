# File: vscode_snowflake_starter/src/exec_audit_stage2.py
# Title: Stage 2  Executive Audit Report (FEATURES_DAILY, Wilder RSI/ATR)
# Commit Notes:
# - Counts duplicates, weekend rows, nulls, out-of-bounds RSI/ATR
# - Counts missing features after 14-day warmup vs MARKET_OHLCV
# - Writes Markdown + CSV to audit_exports/stage2_exec_<timestamp>

from pathlib import Path
import csv, json, datetime as dt
from snowflake_conn import get_conn

def fetchval(cur, sql):
    cur.execute(sql)
    return cur.fetchone()[0]

def main():
    base = Path.cwd() / "audit_exports"
    base.mkdir(exist_ok=True)
    stamp = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    outdir = base / f"stage2_exec_{stamp}"
    outdir.mkdir(exist_ok=True)

    with get_conn() as conn, conn.cursor() as cur:
        dup = fetchval(cur, """
            SELECT COUNT(*) FROM (
              SELECT SYMBOL, TRADE_DATE, COUNT(*) c
              FROM FEATURES_DAILY
              GROUP BY 1,2 HAVING c>1
            )
        """)
        wknd = fetchval(cur, """
            SELECT COUNT(*) FROM FEATURES_DAILY
            WHERE DAYOFWEEKISO(TRADE_DATE) IN (6,7)
        """)
        nulls = fetchval(cur, """
            SELECT COUNT(*) FROM FEATURES_DAILY
            WHERE CLOSE IS NULL OR ADJ_CLOSE IS NULL OR RSI_14 IS NULL OR ATR_14 IS NULL
        """)
        oob = fetchval(cur, """
            SELECT COUNT(*) FROM FEATURES_DAILY
            WHERE (RSI_14 < 0 OR RSI_14 > 100 OR ATR_14 < 0)
        """)
        missing = fetchval(cur, """
            WITH m AS (SELECT SYMBOL, TRADE_DATE FROM MARKET_OHLCV),
                 f AS (SELECT SYMBOL, TRADE_DATE FROM FEATURES_DAILY),
                 b AS (
                   SELECT SYMBOL, DATEADD(day, 13, MIN(TRADE_DATE)) AS CUTOFF
                   FROM FEATURES_DAILY
                   GROUP BY SYMBOL
                 ),
                 missing AS (
                   SELECT m.SYMBOL, m.TRADE_DATE
                   FROM m JOIN b USING (SYMBOL)
                   LEFT JOIN f ON f.SYMBOL=m.SYMBOL AND f.TRADE_DATE=m.TRADE_DATE
                   WHERE m.TRADE_DATE > b.CUTOFF AND f.TRADE_DATE IS NULL
                 )
            SELECT COUNT(*) FROM missing
        """)

    rows = [
        ("Duplicates (SYMBOL, TRADE_DATE)", dup),
        ("Weekend rows", wknd),
        ("Nulls in CLOSE/ADJ/RSI/ATR", nulls),
        ("Out-of-bounds RSI/ATR", oob),
        ("Missing features vs MARKET_OHLCV (post-warmup)", missing),
    ]
    def status(n): return "PASS" if n==0 else "FAIL"

    # CSV
    csv_path = outdir / "summary.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["check","violations","status"])
        for name, n in rows:
            w.writerow([name, n, status(n)])

    # Markdown
    md_path = outdir / "REPORT_EXEC.md"
    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# Stage 2 Audit  Executive Report ({ts})\n\n")
        f.write("| Check | Violations | Status |\n|---|---:|:---:|\n")
        for name, n in rows:
            f.write(f"| {name} | {n} | {status(n)} |\n")
        f.write("\n_Scope_: Stage 2 (Feature integrity, Wilder RSI/ATR).  _Notes_: RSI in [0,100], ATR  0.\n")

    print(json.dumps({"saved": True, "dir": str(outdir), "md": str(md_path), "csv": str(csv_path)}))
if __name__ == "__main__":
    main()