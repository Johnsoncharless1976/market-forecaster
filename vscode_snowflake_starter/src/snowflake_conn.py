import os
import snowflake.connector
from contextlib import contextmanager

def _env(name, required=True, default=None):
    v = os.getenv(name, default)
    if required and (v is None or v == ""):
        raise RuntimeError(f"Missing required env var: {name}")
    return v

def connect_kwargs():
    kw = dict(
        account      = _env("SNOWFLAKE_ACCOUNT"),
        user         = _env("SNOWFLAKE_USER"),
        password     = _env("SNOWFLAKE_PASSWORD"),
        role         = _env("SNOWFLAKE_ROLE"),
        warehouse    = _env("SNOWFLAKE_WAREHOUSE"),
        database     = _env("SNOWFLAKE_DATABASE"),
        schema       = _env("SNOWFLAKE_SCHEMA"),
        application  = "ZenMarketStage1",
        session_parameters = {
            "TIMEZONE": "UTC",
            "STATEMENT_TIMEOUT_IN_SECONDS": _env("SNOWFLAKE_STMT_TIMEOUT", required=False, default="90"),
            "QUERY_TAG": _env("SNOWFLAKE_QUERY_TAG", required=False, default=_env("JOB", required=False, default="stage1_run")),
        },
    )
    return kw

@contextmanager
def get_conn():
    conn = snowflake.connector.connect(**connect_kwargs())
    try:
        yield conn
    finally:
        try:
            conn.close()
        except:
            pass