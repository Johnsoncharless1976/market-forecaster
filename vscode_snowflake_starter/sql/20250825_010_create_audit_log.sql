-- File: vscode_snowflake_starter/sql/20250825_010_create_audit_log.sql
-- Title: Stage X  Create AUDIT_LOG (run-level audit envelope)
-- Commit Notes:
-- - Stores per-run inputs/outputs as JSON (VARIANT) for external audit.
-- - No secrets; .env remains untouched.
-- - log_id is external (UUID); created_at auto-populated.
CREATE TABLE IF NOT EXISTS AUDIT_LOG (
    log_id STRING DEFAULT UUID_STRING(),
    stage STRING,
    run_timestamp TIMESTAMP,
    input_data VARIANT,
    output_data VARIANT,
    pipeline_job_id STRING,
    sha256_hash STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);