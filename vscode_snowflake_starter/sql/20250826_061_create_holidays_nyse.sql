-- File: vscode_snowflake_starter/sql/20250826_061_create_holidays_nyse.sql
-- Title: Stage 1  Create HOLIDAYS_NYSE
-- Commit Notes:
-- - Stores NYSE full-day market closures as DATE + NOTE; PK on HOLIDAY.
CREATE TABLE IF NOT EXISTS HOLIDAYS_NYSE (
  HOLIDAY DATE PRIMARY KEY,
  NOTE    STRING
);