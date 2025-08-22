# Base_Snowflake_Connection.py
import os
from dotenv import load_dotenv
import snowflake.connector

# Load environment variables
load_dotenv()

user = os.getenv("SNOWFLAKE_USER")
password = os.getenv("SNOWFLAKE_PASSWORD")
account = os.getenv("SNOWFLAKE_ACCOUNT")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
database = os.getenv("SNOWFLAKE_DATABASE")
schema = os.getenv("SNOWFLAKE_SCHEMA")

print("Loaded values:")
print("User:", user)
print("Account:", account)
print("Warehouse:", warehouse)
print("Database:", database)
print("Schema:", schema)
print("Password set:", bool(password))

try:
    # Open connection
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    print("✅ Connected to Snowflake!")

    # Use cursor in a "with" block so it's safe
    with conn.cursor() as cur:
        cur.execute("SELECT CURRENT_VERSION()")
        version = cur.fetchone()[0]
        print("Snowflake version:", version)

        cur.execute("SELECT CURRENT_DATE()")
        today = cur.fetchone()[0]
        print("Current date in Snowflake:", today)

    # Close connection after queries
    conn.close()

except Exception as e:
    print("❌ Connection failed:", e)
