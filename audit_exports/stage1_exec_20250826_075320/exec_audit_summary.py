# File: vscode_snowflake_starter/src/exec_audit_summary.py
# Title: Stage 1  Executive Audit Summary (PASS/FAIL table)
# Commit Notes:
# - Queries Snowflake directly (no parsing of prior .out files).
# - Prints PASS/FAIL table and writes REPORT_EXEC.md + summary.csv
# - If Snowflake is unreachable, exits cleanly with a one-line error.

import os, sys, json
from datetime import datetime

# ensure we can import our Snowflake connector helper
here = os.path.dirname(__file__)
sys.path.insert(0, here)
from snowflake_conn import get_conn

SUMMARY_SQL = """
WITH
dups AS (
  SELECT COUNT(*) AS violations
  FROM (
    SELECT SYMBOL, TRADE_DATE, COUNT(*) c
    FROM MARKET_OHLCV
    GROUP BY 1,2
    HAVING COUNT(*) > 1
  )
),
wknd AS (
  SELECT COUNT(*) AS violations
  FROM MARKET_OHLCV
  WHERE DAYOFWEEKISO(TRADE_DATE) IN (6,7)
),
nulls AS (
  SELECT COUNT(*) AS violations
  FROM MARKET_OHLCV
  WHERE OPEN IS NULL OR HIGH IS NULL OR LOW IS NULL OR CLOSE IS NULL OR ADJ_CLOSE IS NULL
),
ohlc AS (
  SELECT COUNT(*) AS violations
  FROM MARKET_OHLCV
  WHERE NOT (
    LOW  <= LEAST(OPEN, CLOSE, ADJ_CLOSE)
    AND HIGH >= GREATEST(OPEN, CLOSE, ADJ_CLOSE)
    AND HIGH >= LOW
  )
),
rng AS (
  SELECT SYMBOL, MIN(TRADE_DATE) AS MIN_DATE, MAX(TRADE_DATE) AS MAX_DATE
  FROM MARKET_OHLCV GROUP BY SYMBOL
),
cal AS (
  SELECT r.SYMBOL, DATEADD(day, seq4(), r.MIN_DATE) AS D
  FROM rng r, TABLE(GENERATOR(ROWCOUNT => 20000))
  WHERE DATEADD(day, seq4(), r.MIN_DATE) <= r.MAX_DATE
    AND DAYOFWEEKISO(DATEADD(day, seq4(), r.MIN_DATE)) BETWEEN 1 AND 5
),
missing AS (
  SELECT c.SYMBOL, c.D
  FROM cal c
  LEFT JOIN MARKET_OHLCV m
    ON m.SYMBOL = c.SYMBOL AND m.TRADE_DATE = c.D
  WHERE m.TRADE_DATE IS NULL
),
gaps AS ( SELECT COUNT(*) AS violations FROM missing )
SELECT TO_JSON(OBJECT_CONSTRUCT('check','duplicates','violations',(SELECT violations FROM dups)))          AS JSON_ROW
UNION ALL
SELECT TO_JSON(OBJECT_CONSTRUCT('check','weekend_rows','violations',(SELECT violations FROM wknd)))        AS JSON_ROW
UNION ALL
SELECT TO_JSON(OBJECT_CONSTRUCT('check','null_prices','violations',(SELECT violations FROM nulls)))        AS JSON_ROW
UNION ALL
SELECT TO_JSON(OBJECT_CONSTRUCT('check','ohlc_sanity','violations',(SELECT violations FROM ohlc)))         AS JSON_ROW
UNION ALL
SELECT TO_JSON(OBJECT_CONSTRUCT('check','weekday_gaps','violations',(SELECT violations FROM gaps)))        AS JSON_ROW
;
"""

def main():
    # Pick/create an output folder
    root = os.path.abspath(os.path.join(os.path.dirname(here), "audit_exports"))
    os.makedirs(root, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = os.path.join(root, f"stage1_exec_{stamp}")
    os.makedirs(outdir, exist_ok=True)

    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(SUMMARY_SQL)
            rows = cur.fetchall()
    except Exception as e:
        print("Snowflake unreachable  cannot produce counts. " + str(e))
        return

    # Parse JSON rows
    parsed = []
    for (json_row,) in rows:
        obj = json.loads(json_row) if isinstance(json_row, str) else json_row
        check = obj.get("check")
        viol  = int(obj.get("violations", 0))
        status = "PASS" if viol == 0 else "FAIL"
        parsed.append((check, viol, status))

    # Print executive table
    print("\n# Stage 1 Audit  Executive Report ({})".format(stamp))
    print("\n| Check | Violations | Status |")
    print("|---|---:|:---:|")
    for check, viol, status in sorted(parsed, key=lambda t: t[0]):
        print(f"| {check} | {viol} | {status} |")

    # Write CSV + Markdown
    csv_path = os.path.join(outdir, "summary.csv")
    md_path  = os.path.join(outdir, "REPORT_EXEC.md")
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        f.write("check,violations,status\n")
        for check, viol, status in sorted(parsed):
            f.write(f"{check},{viol},{status}\n")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("# Stage 1 Audit  Executive Report ({})\n\n".format(stamp))
        f.write("| Check | Violations | Status |\n|---|---:|:---:|\n")
        for check, viol, status in sorted(parsed):
            f.write(f"| {check} | {viol} | {status} |\n")
        f.write("\n_Scope_: Stage 1 (Ingestion Integrity & Controls).  ")
        f.write("_Note_: `weekday_gaps` may include official market holidays; these can be suppressed with a holiday calendar.\n")

    print("\nSaved:")
    print(" -", md_path)
    print(" -", csv_path)

if __name__ == "__main__":
    main()
