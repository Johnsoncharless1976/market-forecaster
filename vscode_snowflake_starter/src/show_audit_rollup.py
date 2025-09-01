# File: src/show_audit_rollup.py
# Title: Audit Rollup  Per-day summary + latest run details
# Commit Notes:
# - No DB changes; queries AUDIT_LOG directly.
# - Filters by lookback window and (optionally) stage label.

import argparse
from snowflake_conn import get_conn

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7, help="Look back N days")
    ap.add_argument("--stage", default="Stage 1: Yahoo->MARKET_OHLCV", help="Stage label to filter (empty = all)")
    args = ap.parse_args()

    where = "WHERE run_timestamp >= DATEADD(day, -%s, CURRENT_TIMESTAMP())"
    params = [args.days]
    if args.stage:
        where += " AND stage = %s"
        params.append(args.stage)

    sql = f"""
      WITH base AS (
        SELECT
          TO_DATE(run_timestamp)                AS run_date,
          input_data:"symbol"::string           AS symbol,
          run_timestamp                         AS run_ts,
          pipeline_job_id                       AS job,
          sha256_hash                           AS sha,
          output_data:"post_count"::number      AS post_count,
          output_data:"post_max"::string        AS post_max
        FROM AUDIT_LOG
        {where}
      ),
      agg AS (
        SELECT run_date, symbol, COUNT(*) AS run_count
        FROM base
        GROUP BY 1,2
      ),
      latest AS (
        SELECT run_date, symbol, run_ts, job, sha, post_count, post_max,
               ROW_NUMBER() OVER (PARTITION BY run_date, symbol ORDER BY run_ts DESC) rn
        FROM base
      )
      SELECT a.run_date, a.symbol, a.run_count,
             l.run_ts AS last_run_ts, l.job AS last_job,
             l.post_count AS last_post_count, l.post_max AS last_post_max, l.sha AS last_sha
      FROM agg a
      JOIN latest l
        ON a.run_date = l.run_date AND a.symbol = l.symbol AND l.rn = 1
      ORDER BY a.run_date DESC, a.symbol;
    """

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        for (run_date, symbol, run_count, last_run_ts, last_job, last_post_count, last_post_max, last_sha) in cur.fetchall():
            print({
                "date": str(run_date),
                "symbol": symbol,
                "run_count": int(run_count),
                "last_run_ts": str(last_run_ts),
                "last_job": last_job,
                "last_post_count": int(last_post_count) if last_post_count is not None else None,
                "last_post_max": last_post_max,
                "last_sha": last_sha
            })

if __name__ == "__main__":
    main()
