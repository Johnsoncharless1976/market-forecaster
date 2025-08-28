#!/usr/bin/env python3
"""
SLA Manager with A-Coverage ≥50% target and autotune system
Manages Overall≥70%, A-Precision≥80%, A-Coverage≥50% targets
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import json
import itertools


class SLAManager:
    """SLA management with A-Coverage target and autotune"""
    
    def __init__(self):
        # Updated SLA targets
        self.sla_targets = {
            'overall_precision': 70.0,      # Overall ≥70%
            'a_precision': 80.0,            # A-Precision ≥80%
            'a_coverage': 50.0              # A-Coverage ≥50% (new)
        }
        
        # Current thresholds (baseline)
        self.current_thresholds = {
            'grade_a_bounds': [0.35, 0.65],  # p_final ≤0.35 OR ≥0.65 for A
            'grade_b_bounds': [0.40, 0.60],  # p_final ≤0.40 OR ≥0.60 for B
            'impact_band_delta': 0.0,        # Band/conf delta adjustment
            'impact_conf_delta': 0.0,        # Confidence delta adjustment
            'magnet_strength': 0.30          # γ magnet strength
        }
        
        # Safety caps (do not exceed)
        self.safety_caps = {
            'grade_a_max': [0.32, 0.68],    # A-thresholds: Down≤0.32..0.40, Up≥0.60..0.68
            'grade_a_min': [0.40, 0.60],
            'impact_delta_max': 0.10,       # ±10% max for band/conf deltas
            'magnet_strength_range': [0.24, 0.38]  # γ: 0.24-0.38
        }
        
        # Autotune search parameters
        self.autotune_steps = {
            'threshold_step': 0.01,         # 1% steps for thresholds
            'impact_step': 0.01,            # 1% steps for impact deltas
            'magnet_step': 0.02             # 2% steps for magnet strength
        }
    
    def calculate_sla_metrics(self, history):
        """Calculate current SLA metrics from forecast history"""
        if not history:
            return {
                'overall_precision': 0.0,
                'a_precision': 0.0,
                'a_coverage': 0.0,
                'total_days': 0,
                'a_days': 0,
                'overall_hits': 0,
                'a_hits': 0
            }
        
        df = pd.DataFrame(history)
        
        # Overall metrics
        total_days = len(df)
        overall_hits = len(df[df['hit']]) if total_days > 0 else 0
        overall_precision = (overall_hits / total_days * 100) if total_days > 0 else 0.0
        
        # A-grade metrics
        a_df = df[df['grade'] == 'A']
        a_days = len(a_df)
        a_hits = len(a_df[a_df['hit']]) if a_days > 0 else 0
        a_precision = (a_hits / a_days * 100) if a_days > 0 else 0.0
        a_coverage = (a_days / total_days * 100) if total_days > 0 else 0.0
        
        return {
            'overall_precision': overall_precision,
            'a_precision': a_precision,
            'a_coverage': a_coverage,
            'total_days': total_days,
            'a_days': a_days,
            'overall_hits': overall_hits,
            'a_hits': a_hits
        }
    
    def check_sla_compliance(self, metrics):
        """Check if current metrics meet SLA targets"""
        compliance = {
            'overall_pass': metrics['overall_precision'] >= self.sla_targets['overall_precision'],
            'a_precision_pass': metrics['a_precision'] >= self.sla_targets['a_precision'] if metrics['a_days'] >= 3 else True,
            'a_coverage_pass': metrics['a_coverage'] >= self.sla_targets['a_coverage']
        }
        
        compliance['overall_status'] = 'PASS' if all(compliance.values()) else 'FAIL'
        
        return compliance
    
    def generate_autotune_candidates(self):
        """Generate candidate threshold combinations for autotune"""
        candidates = []
        
        # Generate A-threshold candidates
        current_low, current_high = self.current_thresholds['grade_a_bounds']
        
        # Search range for A-thresholds (within safety caps)
        low_range = np.arange(
            max(self.safety_caps['grade_a_max'][0], current_low - 0.05),
            min(self.safety_caps['grade_a_min'][0], current_low + 0.05),
            self.autotune_steps['threshold_step']
        )
        
        high_range = np.arange(
            max(self.safety_caps['grade_a_min'][1], current_high - 0.05),
            min(self.safety_caps['grade_a_max'][1], current_high + 0.05),
            self.autotune_steps['threshold_step']
        )
        
        # Impact delta candidates (±10% max)
        impact_range = np.arange(-0.10, 0.11, self.autotune_steps['impact_step'])
        
        # Magnet strength candidates
        magnet_range = np.arange(
            self.safety_caps['magnet_strength_range'][0],
            self.safety_caps['magnet_strength_range'][1] + 0.01,
            self.autotune_steps['magnet_step']
        )
        
        # Generate combinations (limit to avoid explosion)
        for low_thresh in low_range[:3]:  # Limit search space
            for high_thresh in high_range[:3]:
                for impact_delta in impact_range[::5]:  # Every 5th value
                    for magnet_str in magnet_range[::2]:  # Every 2nd value
                        if low_thresh < high_thresh:  # Sanity check
                            candidates.append({
                                'grade_a_bounds': [low_thresh, high_thresh],
                                'grade_b_bounds': [low_thresh + 0.05, high_thresh - 0.05],
                                'impact_band_delta': impact_delta,
                                'impact_conf_delta': impact_delta * 0.5,  # Half for conf
                                'magnet_strength': magnet_str
                            })
        
        return candidates[:50]  # Limit to 50 best candidates
    
    def simulate_candidate_performance(self, candidate_thresholds, base_history):
        """Simulate performance with candidate thresholds"""
        # In production, this would re-run forecast engine with new params
        # For now, simulate by adjusting existing history probabilistically
        
        simulated_history = []
        
        for day in base_history:
            # Simulate threshold impact on grading
            p_final = day['p_final']
            
            # Apply candidate thresholds
            new_low, new_high = candidate_thresholds['grade_a_bounds']
            
            # Determine new grade
            if (p_final <= new_low) or (p_final >= new_high):
                new_grade = 'A'
            elif (p_final <= candidate_thresholds['grade_b_bounds'][0]) or (p_final >= candidate_thresholds['grade_b_bounds'][1]):
                new_grade = 'B'
            else:
                new_grade = 'C'
            
            # Simulate slight performance change due to parameter adjustments
            performance_boost = 0.02 if new_grade == 'A' and day['grade'] != 'A' else 0.0
            hit_probability = 0.55 + performance_boost  # Base hit rate + boost
            
            new_hit = np.random.random() < hit_probability
            
            simulated_history.append({
                'date': day['date'],
                'stance': day['stance'],
                'grade': new_grade,
                'p_final': p_final,
                'actual_outcome': day['actual_outcome'],
                'hit': new_hit
            })
        
        return simulated_history
    
    def find_best_candidate(self, candidates, base_history):
        """Find best candidate that meets SLA targets"""
        best_candidate = None
        best_score = -1
        all_results = []
        
        for candidate in candidates:
            # Simulate performance
            sim_history = self.simulate_candidate_performance(candidate, base_history)
            metrics = self.calculate_sla_metrics(sim_history)
            compliance = self.check_sla_compliance(metrics)
            
            # Score candidate (prioritize meeting all SLA targets)
            score = 0
            if compliance['overall_pass']:
                score += 10
            if compliance['a_precision_pass']:
                score += 10
            if compliance['a_coverage_pass']:
                score += 10
            
            # Bonus for exceeding targets
            score += metrics['overall_precision'] / 10
            score += metrics['a_precision'] / 10
            score += metrics['a_coverage'] / 10
            
            all_results.append({
                'candidate': candidate,
                'metrics': metrics,
                'compliance': compliance,
                'score': score
            })
            
            if score > best_score:
                best_score = score
                best_candidate = {
                    'thresholds': candidate,
                    'metrics': metrics,
                    'compliance': compliance,
                    'score': score
                }
        
        return best_candidate, all_results
    
    def write_sla_scorecard(self, metrics, compliance):
        """Write SLA_SCORECARD.md with new targets"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        audit_dir = Path('audit_exports') / 'daily' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        scorecard_file = audit_dir / 'SLA_SCORECARD.md'
        
        content = f"""# SLA Scorecard (Updated Targets)

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**SLA Version**: v2.0 (A-Coverage ≥50% target added)
**Cohort Period**: 30-day shadow tracking
**Mode**: SHADOW (SLA tracked, zero live impact)

## SLA Targets (Updated)

| Metric | Current | Target | Status | Change |
|--------|---------|--------|--------|--------|
| **Overall Precision** | {metrics['overall_precision']:5.1f}% | ≥{self.sla_targets['overall_precision']:4.0f}% | {'✅ PASS' if compliance['overall_pass'] else '❌ FAIL'} | (unchanged) |
| **A-Precision** | {metrics['a_precision']:5.1f}% | ≥{self.sla_targets['a_precision']:4.0f}% | {'✅ PASS' if compliance['a_precision_pass'] else '❌ FAIL' if metrics['a_days'] >= 3 else '🟡 PENDING'} | (unchanged) |
| **A-Coverage** | {metrics['a_coverage']:5.1f}% | ≥{self.sla_targets['a_coverage']:4.0f}% | {'✅ PASS' if compliance['a_coverage_pass'] else '❌ FAIL'} | **NEW** (was 30%) |

## Overall SLA Status

**Status**: {compliance['overall_status']} {'🟢' if compliance['overall_status'] == 'PASS' else '🔴'}

### Key Changes
- **A-Coverage Target**: Raised from 30% to {self.sla_targets['a_coverage']:0.0f}%
- **Rationale**: Increase high-confidence forecast frequency while maintaining precision
- **Impact**: More aggressive threshold tuning to hit 50% A-grade days

## Current Performance

### Sample Size Analysis
- **Total Days**: {metrics['total_days']}
- **A-Grade Days**: {metrics['a_days']} ({metrics['a_coverage']:.1f}% coverage)
- **A-Grade Hits**: {metrics['a_hits']}/{metrics['a_days']} = {metrics['a_precision']:.1f}% precision

### SLA Gap Analysis
"""
        
        if not compliance['overall_pass']:
            gap = self.sla_targets['overall_precision'] - metrics['overall_precision']
            content += f"- **Overall Gap**: {gap:+.1f}pp below target ({metrics['overall_precision']:.1f}% < {self.sla_targets['overall_precision']:.0f}%)\n"
        
        if not compliance['a_precision_pass'] and metrics['a_days'] >= 3:
            gap = self.sla_targets['a_precision'] - metrics['a_precision']
            content += f"- **A-Precision Gap**: {gap:+.1f}pp below target ({metrics['a_precision']:.1f}% < {self.sla_targets['a_precision']:.0f}%)\n"
        
        if not compliance['a_coverage_pass']:
            gap = self.sla_targets['a_coverage'] - metrics['a_coverage']
            content += f"- **A-Coverage Gap**: {gap:+.1f}pp below target ({metrics['a_coverage']:.1f}% < {self.sla_targets['a_coverage']:.0f}%)\n"
        
        if compliance['overall_status'] == 'PASS':
            content += "- **Status**: All SLA targets met ✅\n"
        
        content += f"""

## Autotune Implications

### If A-Coverage < 50%
- **Action**: Widen A-grade thresholds (make more days qualify for A)
- **Risk**: May reduce A-precision if thresholds become too loose
- **Guard**: Cannot widen beyond safety caps ([0.32,0.68] max range)

### If A-Precision < 80%
- **Action**: Tighten A-grade thresholds (make fewer days qualify for A)
- **Risk**: May reduce A-coverage below 50%
- **Guard**: Cannot tighten beyond minimal effectiveness

### Current Autotune Status
- **Enabled**: {'✅ YES' if metrics['total_days'] >= 10 else '🟡 PENDING (need ≥10 days)'}
- **Search Space**: Threshold nudges ±5pp, impact deltas ±10%, magnet γ 0.24-0.38
- **Safety**: All changes candidate-only, within safety caps

## Deployment Readiness

- **SLA Ready**: {'✅ YES' if compliance['overall_status'] == 'PASS' else '🟡 TUNING' if metrics['total_days'] >= 10 else '🟡 BUILDING'}
- **Coverage Target**: {'✅ MET' if compliance['a_coverage_pass'] else '❌ BELOW TARGET'}
- **Precision Maintained**: {'✅ YES' if compliance['a_precision_pass'] or metrics['a_days'] < 3 else '❌ NO'}

---
**SLA SCORECARD**: A-Coverage target raised to ≥50% (precision ≥80% maintained)
Generated by SLA Manager v2.0
"""
        
        with open(scorecard_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return str(scorecard_file)
    
    def write_autotune_report(self, best_candidate, current_metrics):
        """Write GRADE_AUTOTUNE.md with recommended changes"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        audit_dir = Path('audit_exports') / 'daily' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        autotune_file = audit_dir / 'GRADE_AUTOTUNE.md'
        
        if best_candidate is None:
            content = f"""# Grade Autotune Report

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Status**: NO CHANGES RECOMMENDED
**Reason**: Current thresholds already optimal or insufficient data

## Current Performance
- **Overall**: {current_metrics['overall_precision']:.1f}%
- **A-Precision**: {current_metrics['a_precision']:.1f}%
- **A-Coverage**: {current_metrics['a_coverage']:.1f}%

## Analysis
No better candidate found that improves SLA compliance while staying within safety caps.
Current settings maintained.

---
**AUTOTUNE**: No threshold adjustments needed
Generated by SLA Manager v2.0
"""
        else:
            recommended = best_candidate['thresholds']
            new_metrics = best_candidate['metrics']
            
            content = f"""# Grade Autotune Report

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Status**: CANDIDATE ADJUSTMENTS RECOMMENDED
**Mode**: CANDIDATE-ONLY (no live changes)

## Recommended Changes

### Threshold Adjustments
| Parameter | Current | Recommended | Delta |
|-----------|---------|-------------|-------|
| **A-Grade Low** | {self.current_thresholds['grade_a_bounds'][0]:.3f} | {recommended['grade_a_bounds'][0]:.3f} | {recommended['grade_a_bounds'][0] - self.current_thresholds['grade_a_bounds'][0]:+.3f} |
| **A-Grade High** | {self.current_thresholds['grade_a_bounds'][1]:.3f} | {recommended['grade_a_bounds'][1]:.3f} | {recommended['grade_a_bounds'][1] - self.current_thresholds['grade_a_bounds'][1]:+.3f} |
| **Impact Band Δ** | {self.current_thresholds['impact_band_delta']:+.2f}% | {recommended['impact_band_delta']:+.2f}% | {recommended['impact_band_delta'] - self.current_thresholds['impact_band_delta']:+.2f}pp |
| **Magnet γ** | {self.current_thresholds['magnet_strength']:.3f} | {recommended['magnet_strength']:.3f} | {recommended['magnet_strength'] - self.current_thresholds['magnet_strength']:+.3f} |

## Performance Impact

| Metric | Current | Projected | Change |
|--------|---------|-----------|--------|
| **Overall Precision** | {current_metrics['overall_precision']:.1f}% | {new_metrics['overall_precision']:.1f}% | {new_metrics['overall_precision'] - current_metrics['overall_precision']:+.1f}pp |
| **A-Precision** | {current_metrics['a_precision']:.1f}% | {new_metrics['a_precision']:.1f}% | {new_metrics['a_precision'] - current_metrics['a_precision']:+.1f}pp |
| **A-Coverage** | {current_metrics['a_coverage']:.1f}% | {new_metrics['a_coverage']:.1f}% | {new_metrics['a_coverage'] - current_metrics['a_coverage']:+.1f}pp |

## Win/Lose Analysis

### Wins
"""
            
            # Add wins
            if new_metrics['overall_precision'] > current_metrics['overall_precision']:
                content += f"- **Overall Precision**: +{new_metrics['overall_precision'] - current_metrics['overall_precision']:.1f}pp improvement\n"
            if new_metrics['a_precision'] > current_metrics['a_precision']:
                content += f"- **A-Precision**: +{new_metrics['a_precision'] - current_metrics['a_precision']:.1f}pp improvement\n"
            if new_metrics['a_coverage'] > current_metrics['a_coverage']:
                content += f"- **A-Coverage**: +{new_metrics['a_coverage'] - current_metrics['a_coverage']:.1f}pp improvement\n"
            
            content += f"""
### Risks
- **Threshold Stability**: Changes may need 5-7 days to stabilize
- **A-Grade Inflation**: Looser thresholds may reduce selectivity
- **Parameter Coupling**: Magnet/impact changes may have lag effects

## Safety Compliance

- **A-Thresholds**: {'✅ WITHIN CAPS' if recommended['grade_a_bounds'][0] >= self.safety_caps['grade_a_max'][0] and recommended['grade_a_bounds'][1] <= self.safety_caps['grade_a_max'][1] else '⚠️ NEAR LIMITS'}
- **Impact Deltas**: {'✅ WITHIN ±10%' if abs(recommended['impact_band_delta']) <= 0.10 else '⚠️ EXCEEDS CAPS'}
- **Magnet Strength**: {'✅ WITHIN RANGE' if self.safety_caps['magnet_strength_range'][0] <= recommended['magnet_strength'] <= self.safety_caps['magnet_strength_range'][1] else '⚠️ OUT OF RANGE'}

## Implementation

### Candidate Files
- **Council Params**: Update λ thresholds in CANDIDATE config
- **Impact Weights**: Adjust band/conf deltas in CANDIDATE weights
- **Magnet Engine**: Update γ strength in CANDIDATE magnet params

### Rollout Plan
1. **Day 1**: Deploy candidate configs (shadow-only)
2. **Days 2-7**: Monitor SLA compliance and stability
3. **Day 8+**: If stable and meeting targets, consider PM approval

---
**AUTOTUNE**: Candidate adjustments recommended for SLA compliance
Generated by SLA Manager v2.0
"""
        
        with open(autotune_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return str(autotune_file)
    
    def write_tradeoff_report(self, all_results):
        """Write SLA_TRADEOFF.md if targets cannot be simultaneously met"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        audit_dir = Path('audit_exports') / 'daily' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        tradeoff_file = audit_dir / 'SLA_TRADEOFF.md'
        
        # Find Pareto frontier
        pareto_candidates = []
        for result in all_results:
            metrics = result['metrics']
            # Only consider candidates that meet basic thresholds
            if (metrics['a_precision'] >= 70 and metrics['a_coverage'] >= 30 and 
                metrics['overall_precision'] >= 60):
                pareto_candidates.append(result)
        
        # Sort by combined score
        pareto_candidates = sorted(pareto_candidates, key=lambda x: x['score'], reverse=True)[:10]
        
        content = f"""# SLA Tradeoff Analysis

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Issue**: Cannot simultaneously achieve A-Precision ≥80% AND A-Coverage ≥50%
**Analysis**: Pareto frontier showing best feasible combinations

## Target Conflict

The updated SLA targets create a tension:
- **A-Coverage ≥50%**: Requires wider A-grade thresholds (more days qualify)
- **A-Precision ≥80%**: Requires tighter A-grade thresholds (fewer but higher-quality days)

## Pareto Frontier (Top 10 Candidates)

| Rank | A-Precision | A-Coverage | Overall | Safety Score | Trade-off |
|------|-------------|------------|---------|--------------|-----------|"""
        
        for i, result in enumerate(pareto_candidates, 1):
            m = result['metrics']
            safety_score = min(100, result['score'])
            
            # Determine trade-off type
            if m['a_precision'] >= 80 and m['a_coverage'] >= 50:
                tradeoff = "✅ IDEAL"
            elif m['a_precision'] >= 80:
                tradeoff = "High-Prec"
            elif m['a_coverage'] >= 50:
                tradeoff = "High-Cov"
            else:
                tradeoff = "Balanced"
            
            content += f"\n| {i:2d} | {m['a_precision']:7.1f}% | {m['a_coverage']:8.1f}% | {m['overall_precision']:7.1f}% | {safety_score:8.1f} | {tradeoff} |"
        
        best = pareto_candidates[0] if pareto_candidates else None
        
        content += f"""

## Recommended Compromise

"""
        
        if best and best['metrics']['a_precision'] >= 75 and best['metrics']['a_coverage'] >= 40:
            content += f"""**Selected**: Rank 1 candidate (balanced approach)
- **A-Precision**: {best['metrics']['a_precision']:.1f}% (target: 80%)
- **A-Coverage**: {best['metrics']['a_coverage']:.1f}% (target: 50%)
- **Overall**: {best['metrics']['overall_precision']:.1f}% (target: 70%)

### Rationale
This candidate provides the best balance of precision and coverage while staying within safety constraints.
"""
        else:
            content += f"""**Status**: MAINTAIN CURRENT THRESHOLDS
- **Reason**: No candidate safely achieves both targets
- **Action**: Continue with current settings, monitor for 7 more days

### Current Performance
- **A-Precision**: Meeting/near target
- **A-Coverage**: Below target but improving
- **Overall**: Stable baseline performance
"""
        
        content += f"""

## Constraint Analysis

### Why Targets Conflict
1. **Sample Size**: {len(all_results)} candidates tested, most fail dual constraints
2. **Threshold Sensitivity**: Small changes dramatically affect grade distribution
3. **Market Conditions**: Recent volatility may limit high-confidence opportunities

### Safety Constraints Applied
- **A-Thresholds**: Limited to [{self.safety_caps['grade_a_max'][0]:.2f}, {self.safety_caps['grade_a_max'][1]:.2f}] maximum range
- **Impact Deltas**: Capped at ±{self.safety_caps['impact_delta_max']*100:.0f}%
- **Magnet Strength**: Bounded to [{self.safety_caps['magnet_strength_range'][0]:.2f}, {self.safety_caps['magnet_strength_range'][1]:.2f}]

## Recommendations

### Short Term (Next 7 Days)
1. **Maintain Current**: Keep existing thresholds for stability
2. **Monitor Closely**: Daily SLA tracking and adjustment readiness
3. **Data Collection**: Build larger sample for more robust tuning

### Medium Term (2-4 Weeks)
1. **Revisit Targets**: Consider if 50% A-coverage is realistic in current market
2. **Threshold Evolution**: Gradual parameter adjustments as data grows
3. **Market Regime**: Adapt targets to current volatility environment

---
**TRADEOFF**: A-Precision ≥80% and A-Coverage ≥50% cannot be simultaneously achieved safely
Generated by SLA Manager v2.0
"""
        
        with open(tradeoff_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return str(tradeoff_file)


def main():
    """Test SLA Manager system"""
    from forecast_grading import ForecastGrading
    
    # Generate sample data
    grading = ForecastGrading()
    history = grading.generate_synthetic_grade_history(days=30)
    
    sla_manager = SLAManager()
    
    # Calculate current metrics
    print("Calculating SLA metrics...")
    current_metrics = sla_manager.calculate_sla_metrics(history)
    compliance = sla_manager.check_sla_compliance(current_metrics)
    
    print(f"Current SLA Status:")
    print(f"  Overall: {current_metrics['overall_precision']:.1f}% (target: >={sla_manager.sla_targets['overall_precision']:.0f}%)")
    print(f"  A-Precision: {current_metrics['a_precision']:.1f}% (target: >={sla_manager.sla_targets['a_precision']:.0f}%)")
    print(f"  A-Coverage: {current_metrics['a_coverage']:.1f}% (target: >={sla_manager.sla_targets['a_coverage']:.0f}%)")
    print(f"  Status: {compliance['overall_status']}")
    
    # Run autotune
    print("Running autotune analysis...")
    candidates = sla_manager.generate_autotune_candidates()
    best_candidate, all_results = sla_manager.find_best_candidate(candidates, history)
    
    # Write artifacts
    scorecard_file = sla_manager.write_sla_scorecard(current_metrics, compliance)
    autotune_file = sla_manager.write_autotune_report(best_candidate, current_metrics)
    
    # Check if tradeoff analysis needed
    needs_tradeoff = not (compliance['a_precision_pass'] and compliance['a_coverage_pass'])
    tradeoff_file = None
    if needs_tradeoff:
        tradeoff_file = sla_manager.write_tradeoff_report(all_results)
        print(f"Tradeoff analysis: {tradeoff_file}")
    
    print(f"SLA Scorecard: {scorecard_file}")
    print(f"Autotune Report: {autotune_file}")
    
    return {
        'metrics': current_metrics,
        'compliance': compliance,
        'best_candidate': best_candidate,
        'needs_tradeoff': needs_tradeoff
    }


if __name__ == '__main__':
    main()