# Issue: perf/chopguard-v0.2.2 — Unmute Criteria: F1≥0.65 + ΔAcc≥+2pp

**Epic**: ChopGuard Unmute Preparation  
**Priority**: High  
**Milestone**: Q4 2025  
**Labels**: performance, chopguard, shadow-mode, unmute-criteria

## Context

ChopGuard v0.2.1 achieved interim F1=0.562 target but unmute requires:
- **F1 Score**: ≥0.65 (current: 0.562, gap: +0.088)
- **Delta Accuracy**: ≥+2pp (current: -0.319, gap: +2.32pp)
- **Usage Rate**: ≤50% (current: 31.0% ✅)
- **Binary Accuracy**: ≥86.3% (current: 87.1% ✅)

## Scope Constraints

**τ refinement + calibration only; no feature sprawl**
- No new features beyond existing: normalized_tr, overnight_gap_flag, day_of_week, range_proxy
- Focus on parameter tuning and probability calibration improvements
- Maintain shadow-only operation and SLO compliance

## Tasks

### 1. Advanced τ Refinement
- **Fine-grained sweep**: τ1∈[0.40..0.65] step 0.01, τ2∈[0.15..0.40] step 0.01
- **Multi-objective optimization**: Balance F1 vs ΔAcc improvement
- **Constraint handling**: Maintain usage ≤50%, binary accuracy ≥86.3%

### 2. Enhanced Calibration
- **Cross-validation**: 5-fold temporal CV to reduce overfitting
- **Calibration comparison**: Platt vs Isotonic vs Temperature scaling
- **Ensemble methods**: Multiple calibrated models with weighted averaging
- **Probability post-processing**: Monotonic transformations to improve reliability

### 3. Range Proxy Refinement
- **EMA parameter tuning**: Test α∈[0.1, 0.2, 0.33, 0.5, 0.7] for optimal smoothing
- **Gap threshold optimization**: Sweep ε∈[0.001, 0.002, 0.003, 0.005, 0.01]
- **Symbol-specific calibration**: Account for instrument volatility differences
- **Temporal stability**: Ensure proxy performance across different market regimes

### 4. Delta Accuracy Focus
- **CHOP vs Binary comparison**: Direct head-to-head validation methodology
- **Performance attribution**: Isolate sources of ΔAcc improvement
- **Confidence intervals**: Statistical significance testing for ΔAcc gains
- **Baseline refresh**: Update binary accuracy benchmark with latest data

### 5. Extended Validation
- **14-day cohort**: Double validation period for robustness
- **Cross-market validation**: Test across different volatility regimes
- **Temporal stability**: Rolling 7-day windows for consistency check
- **Monte Carlo simulation**: 100 bootstrap samples for confidence intervals

## Acceptance Criteria

### Primary Unmute Requirements
- **F1 Score**: ≥0.65 (up from 0.562)
- **Delta Accuracy**: ≥+2pp (up from -0.319)
- **Usage Rate**: ≤50% (maintain 31.0% level)
- **Binary Accuracy**: ≥86.3% (maintain 87.1% level)

### Validation Requirements
- **14-day fresh cohort**: No data overlap with v0.2.1 validation
- **Statistical significance**: 95% confidence intervals for key metrics
- **Temporal stability**: Performance consistent across validation periods
- **Cross-market robustness**: Performance stable across volatility regimes

### Technical Requirements
- **No feature sprawl**: Use only existing 4 features
- **Shadow-only**: Zero production impact
- **SLO compliance**: No changes to AM/PM delivery windows
- **Calibration quality**: Improved reliability metrics vs v0.2.1

## Deliverables

### Code
- Optimized τ1/τ2 parameters with fine-grained search
- Enhanced calibration pipeline (best method selected)
- Refined range proxy calculations
- Extended validation framework (14-day cohort)

### Artifacts  
- metrics.json with F1≥0.65, ΔAcc≥+2pp proof
- extended_cohort_manifest.csv (14-day validation)
- calibration_comparison.csv (Platt vs Isotonic vs Temperature)
- statistical_significance.md (confidence intervals, p-values)
- temporal_stability.csv (rolling window analysis)

### Documentation
- CHOP_GUARD.md updated with unmute-ready status
- ZEN_COUNCIL_EXPLAIN.md with final parameter explanations
- UNMUTE_CRITERIA_MET.md (comprehensive validation report)

## Timeline

**Target**: 3 weeks from v0.2.1 merge
**Phases**:
- Week 1: Fine-grained τ sweep + calibration comparison
- Week 2: Extended 14-day validation + statistical testing
- Week 3: Final optimization + unmute readiness validation

## Risk Assessment

**Medium Risk**: ΔAcc gap of +2.32pp is substantial
**Mitigation Strategies**:
- Focus on calibration quality improvements
- Statistical validation of ΔAcc measurements
- Fallback to partial improvement documentation

**High Risk**: F1 gap of +0.088 requires precision improvement
**Mitigation Strategies**:
- Fine-grained parameter search
- Ensemble calibration methods
- Extended validation for robustness

## Definition of Done

- [ ] F1 Score ≥0.65 achieved and validated
- [ ] Delta Accuracy ≥+2pp achieved and validated  
- [ ] Usage rate ≤50% maintained
- [ ] Binary accuracy ≥86.3% maintained
- [ ] 14-day extended cohort validation complete
- [ ] Statistical significance demonstrated (95% CI)
- [ ] All unmute criteria artifacts generated
- [ ] ChopGuard ready for production unmute consideration

## Success Metrics

**Primary**: All 4 unmute criteria met with statistical confidence
**Secondary**: Temporal stability across extended validation period
**Tertiary**: Improved calibration quality metrics vs baseline

---

**Next Steps**: Assign to performance team, begin immediately after v0.2.1 merge approval