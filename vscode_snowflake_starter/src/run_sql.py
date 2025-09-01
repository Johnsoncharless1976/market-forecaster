# File: src/run_sql.py
# Title: One-shot SQL Runner (env-driven)
# Commit Notes:
# - Executes a single .sql file against current DB/SCHEMA from .env.
# - Prints success or the exact error for quick fixes.
import os, sys, argparse
sys.path.append(os.path.dirname(__file__))
from snowflake_conn import get_conn

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--file", required=True)
    args = p.parse_args()
    sql_path = args.file
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("select current_database(), current_schema()")
        db, schema = cur.fetchone()
        print({"event":"context","database":db,"schema":schema})
        cur.execute(sql)
        print({"event":"applied","file":sql_path})
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print({"error": str(e)})
        sys.exit(1)
