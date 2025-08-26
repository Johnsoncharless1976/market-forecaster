-- ZEN COMMIT HEADER
-- Title: MARKET_OHLCV table (Yahoo OHLCV for VIX, VVIX, SPX, ES)
-- Purpose: Target table for Stage 1 ingestion with idempotent keys.
-- Notes: Keys = (SYMBOL, TRADE_DATE)

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