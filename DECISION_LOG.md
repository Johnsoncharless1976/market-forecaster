# Decision Log - ZenMarket AI

## 2025-08-27: Repository Consolidation
**Decision**: Consolidated projects; ZenMarket AI is canonical; extra archived.

**Context**: Two active GitLab repositories were creating confusion and CI failures:
- `zenmarketai/market-forecaster` (canonical)
- `johnsoncharless1/market-forecaster` (rogue)

**Decision Made**: 
- ZenMarket AI (`zenmarketai/market-forecaster`) designated as single source of truth
- Cherry-picked essential hotfix (2fb538b) from rogue to canonical main
- Rogue repository to be archived with deprecation notice
- All future development on canonical repository only

**Impact**: 
- Fixed critical CI TypeError in send_email() function
- Eliminated repository duplication and confusion
- Established clear development workflow

**Evidence**: See `audit_exports/consolidation/20250827_125434/REPORT.md`

---
*Decision log maintained for project governance and audit trail*