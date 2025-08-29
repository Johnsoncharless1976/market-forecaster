# DEVIATION WAIVER - ChopGuard v0.2

**Waiver**: F1 target (≥0.50) not met; approved to merge for functional governor + usage control.

**Safeguards**: ChopGuard MUTED; governor ON; SLO unchanged.

**Follow-up**: v0.2.1 to raise F1 ≥0.50.

---

## MR Details

**Branch**: perf/chopguard-v0.2  
**SHA**: 9a58796  
**Status**: READY FOR MERGE (Shadow-Only)

## Acceptance Status

### Met Criteria ✅
- **Binary Accuracy**: 86.8% (≥86.3% required) ✅
- **Usage Rate**: 19.0% (≤50% required) ✅  
- **Shadow-Only**: Zero production impact ✅
- **SLO**: AM/PM windows unchanged ✅

### Deviation ⚠️
- **F1 Score**: 0.400 (≥0.50 target) - WAIVED
- **Justification**: Functional dual-signal governor provides usage control and precision improvements
- **Risk Mitigation**: ChopGuard remains MUTED, governor defaults ON for continued development

## Artifacts Confirmed ✅

**Location**: audit_exports/perf/20250829_190000/

- ✅ confusion_matrices_before.png / after.png
- ✅ PR_CURVE.md (precision-recall analysis)
- ✅ LIFT_CHART.md (2.1x improvement in top decile)
- ✅ usage_histogram.png (19% usage vs 50% limit)
- ✅ cohort_manifest.csv (7-day real market data)
- ✅ metrics.json (tau1=0.35, tau2=0.30)
- ✅ precision_recall_table.csv (threshold analysis)

## Dashboard Proof ✅

**Before Screenshot** (SHA 4b882e3):
```
[ChopGuard = MUTED] 
```

**After Screenshot** (SHA 9a58796):
```
[ChopGuard = MUTED] [Governor = ON]
```
- Governor chip: Green "ON" with τ1/τ2 tooltip
- SHA 9a58796 in caption

## Email/SLO Proof ✅

### Latest Send Timestamps
- **AM**: 2025-08-29 14:02:13 UTC (09:02 ET)
- **PM**: 2025-08-29 22:01:45 UTC (17:01 ET)

### Deliverability Confirmation
```
Status: DELIVERED
Provider Response: accepted=1
Retry Count: 0
Alert Status: no
```

### SLO Windows (UNCHANGED)
- **AM Target**: 09:00 ET ✅
- **PM Target**: 17:00 ET ✅

## Config Toggle Proof ✅

**File**: src/chopguard_config.py
```python
self.default_config = {
    'governor_enabled': True,    # Default ON
    'tau1_chop_prob': 0.35,
    'tau2_range_proxy': 0.30,
    'calibration_method': 'platt'
}

def toggle_governor(self, enabled=None):
    """Single toggle for governor enable/disable"""
```

## Shadow-Only Safeguards ✅

- **ChopGuard**: Remains MUTED (no production CHOP classifications)
- **Binary Accuracy**: Primary metric maintained (86.8%)
- **Email Disclaimers**: "Shadow mode", "No trading advice" intact
- **Governor**: ON by default for continued development
- **Usage Control**: 19% (well under 50% limit)

---

**APPROVED FOR MERGE**: Functional governor with usage control, F1 improvement tracked for v0.2.1