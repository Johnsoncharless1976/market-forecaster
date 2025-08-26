# ZenMarket – Session Summary (2025-08-25)

**Data:** Yahoo Finance (^VIX, ^VVIX, ^GSPC, ES=F), weekday-only.  
**Snowflake:** ZEN_MARKET.FORECASTING (ROLE ACCOUNTADMIN).

**Stage 1  Ingest**
- ingest_yahoo_to_market_ohlcv.py (temp stage  MERGE, symbol quoting fixed)
- Post-merge SHA256 audit; verifier true on new rows
- stage1_weekday.ps1 logs OK/FAIL; audit CSV export working

**Stage 2  Features**
- FEATURES_DAILY created
- build_features.py (tz-safe) computes RSI-14, ATR-14, return_1d; upsert + audit
- Populated for ^VIX, ^VVIX, ^GSPC, ES=F

**Stage 3  Forecasts (next)**
- FORECAST_DAILY table + show_forecast.py stub

**Conventions/Guardrails**
- Headers: File / Title / Commit Notes
- SQL: UTF-8 no BOM, one statement per file
- Audits: post-merge table snapshot hashing
- Job tags: stage1_weekday_auto_YYYYMMDD
- Color code: Green=done/tested; Yellow=partial; Red=not started