# ZenMarket AI – Operational Playbook

Last updated: 2025-08-24

## 🚦 Daily Operations
1. **Pipeline Flow**
   - Stage 3 → `ingest_forecast_job.py`
   - Stage 4 → `ingest_audit_loop.py`
   - Stage 5 → `ingest_forecast_summary.py`
   - Stage 6 → `format_and_send_forecast.py` (SendGrid)

2. **Artifacts to Check**
   - `forecast_log.txt`
   - `audit_log.txt`
   - `summary_log.txt`

3. **Email Delivery**
   - Emails go out via SendGrid API.
   - Confirm domain authentication (CNAME + DMARC live).
   - If SendGrid fails → pipeline still runs, logs saved.

## 🛠 Troubleshooting
- **SQL Errors (`invalid identifier`)** → Run `DESC TABLE`.
- **Pipeline Failures** → Check artifacts before debugging code.
- **DNS / Email Issues** → Run `nslookup` and verify propagation.
- **Schema Drift** → Update `schema_codex.md` immediately.

## ⚓ Guardrail Reminders
- **No Gmail SMTP** in CI/CD.
- **Artifacts mandatory** for forecast, audit, summary.
- **Full scripts only** — no snippets.
- **Codex sync required**.

## 📅 Weekly Maintenance
- Review `audit_log.txt` for accuracy of forecasts.
- Update `lessons.md` with new mistakes/regrets.
- Sync Codex if schema/stages change.
- Check SendGrid dashboard for bounce/spam.

## 🕴 Handoff Checklist
- Walk through `repo_codex.md`.
- Review `schema_codex.md`.
- Reiterate guardrails in `guardrails.md`.
- Share lessons in `lessons.md`.
- Verify pipeline runs clean.

## ✅ Field Order
- Data always comes first.
- Forecasts must be logged before emails.
- No stage skipped without Council approval.
