# ZenMarket AI – Script Documentation Standards

Last updated: 2025-08-24

## Required Header Format

```python
# File: ingest_forecast_job.py
# Title: Stage 3 – Forecast Insert
# Commit Notes:
# - Inserts forecasts into FORECAST_JOBS.
# - Fields: DATE, INDEX, FORECAST_BIAS, ATM_STRADDLE, SUPPORT_LEVELS, RESISTANCE_LEVELS, RSI_CONTEXT, NOTES.
# - Adds timestamp FORECAST_TS for latest snapshot.
```

## Required Sections
1. **Imports** – group stdlib, third-party, project imports.
2. **Environment Handling** – load vars via dotenv, fail if missing.
3. **Functions** – each function must have a docstring.
4. **Main Entry Point** – must have `if __name__ == "__main__": main()`.
5. **Error Handling** – catch predictable errors, log ❌/✅ clearly.

## Commit Note Standards
- Short summary, bullet points.
- Explain what changed and why.

## ✅ Checklist Before Commit
- [ ] Header present
- [ ] Functions documented
- [ ] Env vars checked
- [ ] Logs print ❌/✅ outcomes
- [ ] Full script (not a snippet)
- [ ] Commit notes updated
