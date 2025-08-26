# Stage 1 Audit  Executive Report (20250826_075320)

| Check | Violations | Status |
|---|---:|:---:|
| duplicates | 0 | PASS |
| null_prices | 0 | PASS |
| ohlc_sanity | 0 | PASS |
| weekday_gaps | 22 | FAIL |
| weekend_rows | 0 | PASS |

_Scope_: Stage 1 (Ingestion Integrity & Controls).  _Note_: `weekday_gaps` may include official market holidays; these can be suppressed with a holiday calendar.
