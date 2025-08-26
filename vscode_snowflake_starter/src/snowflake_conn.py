# File: src/snowflake_conn.py
# Title: Snowflake Connector (env-driven)
# Commit Notes:
# - Loads .env; supports password and externalbrowser auth.
# - Returns live connection with keep-alive.
import os
from dotenv import load_dotenv, find_dotenv
import snowflake.connector

load_dotenv(find_dotenv())

REQUIRED = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
]

def get_conn():
    missing = [k for k in REQUIRED if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {missing}")

    auth = os.getenv("SNOWFLAKE_AUTH","password").lower()
    kwargs = dict(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        client_session_keep_alive=True,
    )
    if auth == "externalbrowser":
        kwargs["authenticator"] = "externalbrowser"
    else:
        pwd = os.getenv("SNOWFLAKE_PASSWORD")
        if not pwd:
            raise RuntimeError("SNOWFLAKE_PASSWORD is required when SNOWFLAKE_AUTH=password")
        kwargs["password"] = pwd
    return snowflake.connector.connect(**kwargs)
