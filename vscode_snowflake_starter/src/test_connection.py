# File: src/test_connection.py
# Title: Snowflake Connection Sanity Check
# Commit Notes:
# - Loads .env and prints version/role/warehouse/database/schema.
# - Validates that VS Code + env wiring is correct.
import sys
from snowflake_conn import get_conn

def main():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("select current_version(), current_role(), current_warehouse(), current_database(), current_schema()")
        version, role, wh, db, schema = cur.fetchone()
        print({"snowflake_version": version, "role": role, "warehouse": wh, "database": db, "schema": schema})

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print({"error": str(e)})
        sys.exit(1)
