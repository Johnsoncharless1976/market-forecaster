-- File: vscode_snowflake_starter/sql/20250825_001_create_market_ohlcv.sql
-- Title: Create table MARKET_OHLCV (Yahoo OHLCV)
-- Commit Notes:
-- - Keys: (SYMBOL, TRADE_DATE).
-- - SPY retired; Yahoo-only (^VIX, ^VVIX, ^GSPC, ES=F).
CREATE TABLE IF NOT EXISTS MARKET_OHLCV (
    SYMBOL      STRING NOT NULL,
    TRADE_DATE  DATE   NOT NULL,
    OPEN        FLOAT,
    HIGH        FLOAT,
    LOW         FLOAT,
    CLOSE       FLOAT,
    ADJ_CLOSE   FLOAT,
    VOLUME      NUMBER,
    SOURCE      STRING,
    LOAD_TS     TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);