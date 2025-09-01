# File: claude_audit_writer.py
# Purpose: Snowflake-only audit logger for Claude Runner (no Notion)
# Deps:  pip install snowflake-connector-python python-dotenv (dotenv optional)

import os
import hashlib
from datetime import datetime
from typing import List, Union, Optional

try:
    from dotenv import load_dotenv  # optional
    load_dotenv()
except Exception:
    pass

import snowflake.connector


# -------------------------
# Env config (required)
# -------------------------
SF_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SF_USER      = os.getenv("SNOWFLAKE_USER")
SF_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SF_ROLE      = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
SF_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SF_DATABASE  = os.getenv("SNOWFLAKE_DATABASE")
SF_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA")
SF_TABLE     = os.getenv("SNOWFLAKE_TABLE", "CLAUDE_AUDIT_LOG")  # default


def _require_env():
    missing = [
        k for k, v in dict(
            SNOWFLAKE_ACCOUNT=SF_ACCOUNT,
            SNOWFLAKE_USER=SF_USER,
            SNOWFLAKE_PASSWORD=SF_PASSWORD,
            SNOWFLAKE_WAREHOUSE=SF_WAREHOUSE,
            SNOWFLAKE_DATABASE=SF_DATABASE,
            SNOWFLAKE_SCHEMA=SF_SCHEMA,
        ).items() if not v
    ]
    if missing:
        raise RuntimeError(f"Missing required Snowflake env vars: {', '.join(missing)}")


def snowflake_connect():
    """Create a Snowflake connection with database, schema, role, and warehouse set."""
    _require_env()
    ctx = snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        role=SF_ROLE,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        client_session_keep_alive=True,
    )
    return ctx


def fqtn(table: Optional[str] = None) -> str:
    """Fully-qualified table name."""
    t = table or SF_TABLE
    return f'{SF_DATABASE}.{SF_SCHEMA}.{t}'


def ensure_table(conn, table: Optional[str] = None):
    """Create audit table if it does not exist."""
    name = fqtn(table)
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {name} (
        audit_timestamp TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
        title          STRING,
        tags           STRING,
        prompt         STRING,
        response       STRING,
        prompt_hash    STRING,
        source         STRING,
        created_at     TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)


def compute_hash(text: str) -> str:
    """Stable SHA256 for deduplication."""
    if text is None:
        text = ""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _normalize_tags(tags: Union[str, List[str], None]) -> str:
    if tags is None:
        return ""
    if isinstance(tags, str):
        return ",".join([t.strip() for t in tags.split(",") if t.strip()])
    return ",".join([str(t).strip() for t in tags if str(t).strip()])


def insert_prompt_session(
    title: str,
    tags: Union[str, List[str], None],
    prompt: str,
    response: str,
    source: str = "claude_runner",
    table: Optional[str] = None,
    dedupe_on_hash: bool = True,
) -> dict:
    """
    Insert a single prompt/response row into Snowflake.
    - Creates table if needed
    - Dedupe (default) via MERGE on prompt_hash to avoid duplicates
    """
    _require_env()
    phash = compute_hash(prompt or "")

    conn = snowflake_connect()
    try:
        ensure_table(conn, table=table)
        name = fqtn(table)
        tags_str = _normalize_tags(tags)

        if dedupe_on_hash:
            # Use MERGE to insert only if this prompt_hash is new
            sql = f"""
            MERGE INTO {name} t
            USING (
                SELECT
                    %s::STRING  AS prompt_hash,
                    %s::STRING  AS title,
                    %s::STRING  AS tags,
                    %s::STRING  AS prompt,
                    %s::STRING  AS response,
                    %s::STRING  AS source
            ) s
            ON t.prompt_hash = s.prompt_hash
            WHEN NOT MATCHED THEN INSERT (
                audit_timestamp, title, tags, prompt, response, prompt_hash, source, created_at
            ) VALUES (
                CURRENT_TIMESTAMP(), s.title, s.tags, s.prompt, s.response, s.prompt_hash, s.source, CURRENT_TIMESTAMP()
            )
            """
            params = (phash, title or "", tags_str, prompt or "", response or "", source or "claude_runner")
        else:
            sql = f"""
            INSERT INTO {name} (
                audit_timestamp, title, tags, prompt, response, prompt_hash, source, created_at
            ) VALUES (CURRENT_TIMESTAMP(), %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
            """
            params = (title or "", tags_str, prompt or "", response or "", phash, source or "claude_runner")

        with conn.cursor() as cur:
            cur.execute(sql, params)

        return {"ok": True, "table": name, "prompt_hash": phash}
    finally:
        conn.close()


# -------------------------
# Optional: simple self-test (won't run unless called directly)
# -------------------------
if __name__ == "__main__":
    print("[Audit] Writing test row (Snowflake-only)…")
    res = insert_prompt_session(
        title="Test Audit Row",
        tags=["smoke", "audit"],
        prompt="What is the capital of France?",
        response="Paris.",
        source="manual_test",
    )
    print("[Audit] Done:", res)
