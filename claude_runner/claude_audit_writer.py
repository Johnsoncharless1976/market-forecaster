# File: claude_runner/claude_audit_writer.py
# Title: Claude Prompt Snowflake Logger
# Commit Notes: Logs Claude prompt + response sessions to Snowflake audit table

import os
import hashlib
from datetime import datetime
import snowflake.connector


def get_env(var, default=None):
    value = os.getenv(var)
    if not value and default is None:
        raise EnvironmentError(f"Missing required env var: {var}")
    return value or default


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=get_env("SNOWFLAKE_ACCOUNT"),
        user=get_env("SNOWFLAKE_USER"),
        password=get_env("SNOWFLAKE_PASSWORD"),
        role=get_env("SNOWFLAKE_ROLE"),
        warehouse=get_env("SNOWFLAKE_WAREHOUSE"),
        database=get_env("SNOWFLAKE_DATABASE"),
        schema=get_env("SNOWFLAKE_SCHEMA")
    )


def hash_prompt(prompt):
    return hashlib.sha256(prompt.encode("utf-8")).hexdigest()


def create_claude_audit_table():
    """Create the Claude audit table if it doesn't exist"""
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        table = get_env("SNOWFLAKE_TABLE", "CLAUDE_AUDIT_LOG")
        query = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                audit_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                title VARCHAR(500),
                tags VARCHAR(1000),
                prompt TEXT,
                response TEXT,
                prompt_hash VARCHAR(64),
                source VARCHAR(50) DEFAULT 'claude_runner',
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """
        cursor.execute(query)
        conn.commit()
        print(f"SUCCESS: Table {table} created/verified")
    except Exception as e:
        print(f"ERROR: Creating table: {e}")
    finally:
        cursor.close()
        conn.close()


def insert_prompt_session(title, tags, prompt, response):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prompt_hash = hash_prompt(prompt)
    tag_string = ", ".join(tags) if isinstance(tags, list) else str(tags) if tags else ""
    source = "claude_runner"

    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        table = get_env("SNOWFLAKE_TABLE", "CLAUDE_AUDIT_LOG")
        query = f"""
            INSERT INTO {table} (
                audit_timestamp, title, tags, prompt, response, prompt_hash, source
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (
            timestamp, title or "Untitled", tag_string, prompt, response, prompt_hash, source
        ))
        conn.commit()
        print(f"SUCCESS: Logged prompt session to {table}")
        return True
    except Exception as e:
        print(f"ERROR: Failed to log to Snowflake: {e}")
        return False
    finally:
        cursor.close()
        conn.close()


def test_connection():
    """Test Snowflake connection"""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_DATABASE()")
        result = cursor.fetchone()
        print(f"SUCCESS: Connected to Snowflake")
        print(f"Version: {result[0]}, User: {result[1]}, Database: {result[2]}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"ERROR: Connection failed: {e}")
        return False


if __name__ == "__main__":
    print("Claude Audit Writer - Testing Snowflake Connection")
    
    if test_connection():
        print("\nCreating/verifying audit table...")
        create_claude_audit_table()
        
        print("\nTesting prompt logging...")
        success = insert_prompt_session(
            title="Test Prompt",
            tags=["test", "debug"],
            prompt="What is the capital of France?",
            response="The capital of France is Paris."
        )
        
        if success:
            print("SUCCESS: Claude Audit Writer test completed successfully!")
        else:
            print("ERROR: Test failed - check error messages above")
    else:
        print("ERROR: Connection test failed - check your .env configuration")