# ZenMarket AI – Lessons Learned & Regrets

Last updated: 2025-08-24

## Major Lessons
- **SMTP via Gmail in CI/CD** → wasted hours with errors. Should have gone to SendGrid from the beginning.
- **Schema drift** → caused `invalid identifier` errors. Solution: always update schema_codex.md after changes.
- **Re-upload fatigue** → too much time wasted feeding the assistant CSVs and SQL. Codex now replaces this.
- **Fragmented scripts** → over-explaining or partial scripts slowed progress. Solution: only full, documented scripts.

## Regrets
- Not standing up Codex earlier — would have saved hours of schema mismatches and repeated uploads.
- Allowing duplicated email scripts instead of consolidating early.
- Spending effort stabilizing Gmail SMTP instead of moving straight to SendGrid.

## Positive Corrections
- Artifacts added in CI/CD for audit, forecast, and summary.
- Codex structure established for schema, repo, guardrails, and lessons.
- Mirror GitLab → GitHub to keep Codex synced automatically.
