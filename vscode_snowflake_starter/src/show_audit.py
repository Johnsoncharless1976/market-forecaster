# File: src/show_audit.py
# Title: Audit Viewer  Recent AUDIT_LOG entries
# Commit Notes:
# - Prints last N entries with input/output JSON and job id.
# - Supports optional --stage and --job filters.

import argparse, json
from snowflake_conn import get_conn

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=12)
    ap.add_argument("--stage", default=None)
    ap.add_argument("--job", default=None)
    args = ap.parse_args()

    where = []
    params = []
    if args.stage:
        where.append("stage = %s")
        params.append(args.stage)
    if args.job:
        where.append("pipeline_job_id = %s")
        params.append(args.job)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
      SELECT
        log_id,
        stage,
        TO_CHAR(run_timestamp,'YYYY-MM-DD HH24:MI:SS TZH:TZM') AS run_ts,
        pipeline_job_id,
        sha256_hash,
        TO_JSON(input_data)  AS input_json,
        TO_JSON(output_data) AS output_json
      FROM AUDIT_LOG
      {where_sql}
      ORDER BY created_at DESC
      LIMIT %s
    """
    params.append(args.limit)

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        for row in cur.fetchall():
            print({
                "log_id": row[0],
                "stage": row[1],
                "run_ts": row[2],
                "job": row[3],
                "sha256": row[4],
                "input": json.loads(row[5]) if row[5] else None,
                "output": json.loads(row[6]) if row[6] else None
            })

if __name__ == "__main__":
    main()
