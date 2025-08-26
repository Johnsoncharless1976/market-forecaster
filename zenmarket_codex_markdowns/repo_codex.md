# ZenMarket AI – Repo Codex

Last updated: 2025-08-24

## Stage 0 – Guardrails
- No consumer SMTP in CI/CD (SendGrid only).
- All scripts require `# Title:` and `# Commit Notes:` headers.
- Schemas must be documented in `schema_codex.md`.

## Stage 1 – Ingestion
- (planned) `data_ingestion.py` → Raw market data into SPX, ES, VIX, VVIX.

## Stage 2 – Derived Metrics
- (planned) `ingest_derived_metrics.py`.

## Stage 3 – Forecast Insert
- `ingest_forecast_job.py` → Inserts forecasts into `FORECAST_JOBS`.

## Stage 4 – Audit Loop
- `ingest_audit_loop.py` → Compares forecasts vs actuals, inserts into `FORECAST_AUDIT_LOG`.

## Stage 5 – Forecast Summary
- `ingest_forecast_summary.py` → Aggregates forecast + audit into `FORECAST_SUMMARY`.

## Stage 6 – Email Delivery
- `format_and_send_forecast.py` → Generates and sends daily forecast email (SendGrid).

## Stage 7+ – Planned
- External data (options, Greeks, macro news).
- End-to-end automation.
- Beta testing.
- Zen Council Casebook (Stage 10).
