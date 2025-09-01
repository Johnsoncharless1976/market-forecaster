# Issue: perf/chopguard-v0.2.1 — Raise F1 to ≥0.50 (7-day real cohort)

**Epic**: ChopGuard Performance Improvements  
**Milestone**: Q4 2025  
**Priority**: Medium  
**Labels**: performance, chopguard, shadow-mode

## Context

ChopGuard v0.2 merged with interim F1=0.40 (2/3 acceptance criteria met). Need to achieve F1≥0.50 target while maintaining:
- Binary accuracy ≥86.3% ✅
- Usage rate ≤50% ✅ 
- Shadow-only operation ✅

## Tasks

### 1. τ-sweep Grid Search
- **Scope**: Grid search τ1∈[0.30..0.55], τ2∈[0.25..0.50] (step 0.05)
- **Objective**: Find Pareto optimal point: max F1 with usage ≤50% and ΔAcc tracked
- **Deliverable**: Parameter optimization report with F1/usage trade-off curve

### 2. Recalibrate Probabilities  
- **Method**: Refit Platt/Isotonic on latest cohort data
- **Validation**: k-fold by day to reduce temporal leakage
- **Enhancement**: Compare Platt vs Isotonic calibration performance
- **Deliverable**: Improved calibrated probability estimates

### 3. Range Proxy Hygiene
- **Smoothing**: Apply EMA-3 to ATR/straddle ratio for noise reduction
- **Thresholding**: Optimize overnight gap threshold (current >0.4%)  
- **Constraint**: No new features beyond v0.2 scope (normalized TR, gap flag, day-of-week)
- **Deliverable**: Cleaned range proxy signals

### 4. Daily Shadow Scorecard (7 days)
- **Output**: audit_exports/perf/YYYYMMDD/metrics.json each trading day
- **Tracking**: F1, binary accuracy, usage rate, τ1/τ2 performance
- **Mini PR**: Daily commit with scorecard updates
- **Duration**: 7 consecutive trading days validation

## Acceptance Criteria

### Primary Target
- **F1 Score**: ≥0.50 (up from 0.40)
- **Binary Accuracy**: ≥86.3% (maintained)  
- **Usage Rate**: ≤50% (maintained)
- **Delta Accuracy**: Track progress toward ≥+2pp future unmute

### Secondary Objectives  
- **Precision**: Improve from current level
- **Recall**: Balance with precision for optimal F1
- **Governor Stability**: Consistent performance across 7-day validation
- **Shadow Compliance**: Zero production impact maintained

## Deliverables

### Code
- Updated τ1/τ2 thresholds in chopguard_config.py
- Recalibrated probability models  
- Enhanced range proxy calculations
- Daily scorecard automation

### Artifacts
- Grid search results (tau_sweep_results.csv)
- Calibration comparison report
- 7-day validation scorecards (metrics.json × 7)
- Final performance report with F1≥0.50 proof

### Dashboard
- Updated governor chip with optimized τ1/τ2 values
- Performance trend visualization
- Daily scorecard integration

## Definition of Done

- [ ] F1 Score ≥0.50 achieved on fresh 7-day cohort
- [ ] Binary accuracy ≥86.3% maintained
- [ ] Usage rate ≤50% maintained  
- [ ] 7-day daily scorecard validation complete
- [ ] All artifacts generated and linked
- [ ] Dashboard updated with new parameters
- [ ] Shadow-only compliance verified

## Timeline

**Target**: 2 weeks from v0.2 merge  
**Milestones**:
- Week 1: τ-sweep + recalibration
- Week 2: 7-day validation + final tuning

## Risk Assessment

**Low Risk**: Parameter tuning within established framework  
**Medium Risk**: F1 target may require additional feature engineering  
**Mitigation**: Fallback to partial improvement (F1≥0.45) with detailed analysis

---

**Next Steps**: Assign to performance team, schedule kick-off after v0.2 merge