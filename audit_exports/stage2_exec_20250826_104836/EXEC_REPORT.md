# Stage 2 Audit  Executive Report (20250826_104836)

| Check                                | Violations | Status |
|--------------------------------------|-----------:|:------:|
| Duplicates (SYMBOL, TRADE_DATE)      | 0    | PASS |
| Weekend rows                         | 0   | PASS |
| Nulls in CLOSE/ADJ/RSI/ATR           | 0   | PASS |
| Out-of-bounds RSI/ATR                | 0    | PASS |
| Missing features vs MARKET_OHLCV     | 56   | FAIL |

_Scope_: Stage 2 (Feature integrity, Wilder RSI/ATR).  _Notes_: RSI in [0,100], ATR  0.
