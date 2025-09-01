# ZenMarket AI – Guardrails (Zen Council Orders)

Last updated: 2025-08-24

## Data & Schema
- Always document schemas in `schema_codex.md` before writing queries.
- Run `DESC TABLE` after new columns are added.
- No assumptions about columns — verify in schema codex.

## Scripts & Code
- Every script must have headers:
  - `# File:`
  - `# Title:`
  - `# Commit Notes:`
- All scripts must be **complete** (no snippets).
- Commit notes must explain exactly what changed.

## CI/CD (GitLab)
- Stages must follow: Forecast → Audit → Summary → Notify.
- Each stage must save artifacts (`forecast_log.txt`, `audit_log.txt`, `summary_log.txt`).
- Keep pipeline lean: remove unused legacy jobs after stabilization.
- No consumer-grade SMTP (Gmail) in CI/CD — **SendGrid API only**.

## Knowledge & Memory
- Codex is your memory. Consult it before you speak.
- If schema, stage definitions, or guardrails change, ensure Codex is updated.
- Do not repeat mistakes logged in `lessons.md`.

## Zen Council Principles
- **Audit Discipline**: every forecast must be checked against outcomes.
- **Incremental Evolution (1% Rule)**: small, testable improvements.
- **Reality Bias**: decisions must be grounded in data.
- **Council Consensus**: no wild ideas without balance between news, quant, macro, and behavioral logic.
