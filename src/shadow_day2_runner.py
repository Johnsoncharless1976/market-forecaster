#!/usr/bin/env python3
"""
Shadow Day 2 Runner: Compare live vs candidate parameters
Run with current live settings and log what candidates would have done
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys
import yaml

# Add src to path
sys.path.append(str(Path(__file__).parent))
from zen_council import ZenCouncil
from council_tuning import CouncilTuningGrid
from impact_shadow_mode import ImpactShadowMode
from impact_tuning import ImpactTuningGrid


class ShadowDay2Runner:
    """Shadow Day 2 with live vs candidate parameter comparisons"""
    
    def __init__(self):
        self.council = ZenCouncil()
        self.impact_shadow = ImpactShadowMode()
        
        # Load candidate parameters
        self.council_candidates = self._load_candidate_council_params()
        self.impact_candidates = self._load_candidate_impact_params()
        
    def _load_candidate_council_params(self):
        """Load Council candidate parameters"""
        try:
            with open('COUNCIL_PARAMS_CANDIDATE.yaml', 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                params = config['council_parameters']
                
                return {
                    'lambda': params['calibration']['blend_lambda'],
                    'alpha_0': params['calibration']['beta_binomial_alpha_0'],
                    'beta_0': params['calibration']['beta_binomial_beta_0'],
                    'miss_window': params['miss_tag_adjustment']['window_days'],
                    'miss_penalty': params['miss_tag_adjustment']['penalty_pct'] / 100.0,
                    'vol_widen': params['volatility_guard']['band_widen_pct'] / 100.0
                }
        except Exception as e:
            print(f"Could not load Council candidates: {e}")
            return None
    
    def _load_candidate_impact_params(self):
        """Load Impact candidate parameters"""
        try:
            with open('NEWS_WEIGHTS_CANDIDATE.yaml', 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                
                limits = config.get('impact_limits', {})
                risk_off = limits.get('risk_off', {})
                risk_on = limits.get('risk_on', {})
                
                return {
                    'news_threshold': abs(risk_off.get('news_threshold', -0.3)),
                    'macro_threshold': risk_off.get('macro_threshold', 1.0),
                    'band_adjustment': abs(risk_on.get('band_adjustment_pct', -5)) / 100.0,
                    'confidence_adjustment': abs(risk_on.get('confidence_adjustment_pct', 5)) / 100.0
                }
        except Exception as e:
            print(f"Could not load Impact candidates: {e}")
            return None
    
    def create_candidate_council(self, candidate_params):
        """Create Council instance with candidate parameters"""
        council = ZenCouncil()
        
        if candidate_params:
            council.blend_lambda = candidate_params['lambda']
            council.beta_binomial_alpha_0 = candidate_params['alpha_0']
            council.beta_binomial_beta_0 = candidate_params['beta_0']
            council.miss_tag_window_days = candidate_params['miss_window']
            council.miss_tag_penalty_pct = candidate_params['miss_penalty'] * 100
            council.vol_guard_widen_pct = candidate_params['vol_widen'] * 100
        
        return council
    
    def run_shadow_day_2(self, target_date=None):
        """Run Shadow Day 2 with live vs candidate comparisons"""
        if target_date is None:
            target_date = datetime.now().date()
        
        print(f"Running Shadow Day 2 for {target_date} (live vs candidate comparisons)...")
        
        # Step 1: Run live shadow mode (current parameters)
        print("Step 1: Running live shadow mode...")
        live_forecast, live_daily_report, live_decision_log = self.impact_shadow.run_shadow_day(target_date, with_pm_scoring=True)
        
        # Step 2: Simulate candidate Council parameters
        candidate_council_result = None
        if self.council_candidates:
            print("Step 2: Simulating candidate Council parameters...")
            candidate_council = self.create_candidate_council(self.council_candidates)
            
            try:
                candidate_council_result = candidate_council.adjust_forecast(live_forecast['p_baseline'])
                print(f"Candidate Council: {candidate_council_result['p_final']:.3f} vs Live Council: {live_forecast['p_baseline']:.3f}")
            except Exception as e:
                print(f"Error with candidate Council: {e}")
        else:
            print("No Council candidates available")
        
        # Step 3: Simulate candidate Impact parameters  
        candidate_impact_result = None
        if self.impact_candidates:
            print("Step 3: Simulating candidate Impact parameters...")
            
            # Use live impact data but apply candidate thresholds
            news_score = live_forecast['news_score']
            macro_z = live_forecast['macro_z_score']
            
            # Apply candidate thresholds
            cand_thresh = self.impact_candidates
            band_adj = 0.0
            conf_adj = 0.0
            triggers = []
            
            # Risk-off conditions with candidate thresholds
            if news_score <= -cand_thresh['news_threshold'] or (abs(macro_z) >= cand_thresh['macro_threshold'] and macro_z < 0):
                band_adj = cand_thresh['band_adjustment'] * 100  # Convert to percentage
                conf_adj = -cand_thresh['confidence_adjustment'] * 100
                
                if news_score <= -cand_thresh['news_threshold']:
                    triggers.append(f"candidate_news_risk_off (s={news_score:.3f})")
                if abs(macro_z) >= cand_thresh['macro_threshold'] and macro_z < 0:
                    triggers.append(f"candidate_macro_negative (z={macro_z:.2f})")
            
            # Risk-on conditions
            elif news_score >= cand_thresh['news_threshold'] or (abs(macro_z) >= cand_thresh['macro_threshold'] and macro_z > 0):
                band_adj = -cand_thresh['band_adjustment'] * 100
                conf_adj = cand_thresh['confidence_adjustment'] * 100
                
                if news_score >= cand_thresh['news_threshold']:
                    triggers.append(f"candidate_news_risk_on (s={news_score:.3f})")
                if abs(macro_z) >= cand_thresh['macro_threshold'] and macro_z > 0:
                    triggers.append(f"candidate_macro_positive (z={macro_z:.2f})")
            
            candidate_impact_result = {
                'band_adjustment_pct': band_adj,
                'confidence_adjustment_pct': conf_adj,
                'triggers': triggers,
                'news_threshold': cand_thresh['news_threshold'],
                'macro_threshold': cand_thresh['macro_threshold']
            }
            
            print(f"Candidate Impact: Band {band_adj:+.1f}%, Conf {conf_adj:+.1f}% vs Live: Band {live_forecast['band_adjustment_pct']:+.1f}%, Conf {live_forecast['confidence_adjustment_pct']:+.1f}%")
        else:
            print("No Impact candidates available")
        
        # Step 4: Compile comparison results
        comparison_result = {
            'date': target_date,
            'live_forecast': live_forecast,
            'candidate_council': candidate_council_result,
            'candidate_impact': candidate_impact_result,
            'council_candidates_available': self.council_candidates is not None,
            'impact_candidates_available': self.impact_candidates is not None
        }
        
        return comparison_result, live_daily_report, live_decision_log
    
    def write_shadow_day_2_reports(self, comparison_result, output_dir='audit_exports'):
        """Write Shadow Day 2 comparison reports"""
        target_date = comparison_result['date']
        timestamp = target_date.strftime('%Y%m%d')
        
        audit_dir = Path(output_dir) / 'daily' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        artifacts = {}
        
        # Write Council shadow with candidate comparison
        council_shadow_file = self._write_council_shadow_day2(comparison_result, audit_dir)
        artifacts['council_shadow'] = council_shadow_file
        
        # Write Impact shadow with candidate comparison (already exists from live run)
        # Add candidate section to existing impact shadow
        impact_shadow_file = self._append_impact_candidate_section(comparison_result, audit_dir)
        artifacts['impact_shadow'] = impact_shadow_file
        
        return artifacts
    
    def _write_council_shadow_day2(self, comparison_result, audit_dir):
        """Write COUNCIL_SHADOW_DAILY.md with candidate comparison"""
        report_file = audit_dir / 'COUNCIL_SHADOW_DAILY.md'
        
        live = comparison_result['live_forecast']
        candidate = comparison_result['candidate_council']
        
        content = f"""# Council Shadow Daily Report (Day 2)

**Date**: {comparison_result['date']}
**Mode**: SHADOW (Council suggestions logged, Baseline remains live)
**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

## Morning Forecast (AM)

### Probability Comparison
- **p0 (Baseline)**: {live['p_baseline']:.3f} ← Live decision
- **p_final (Live Council)**: {live.get('p_zen_adjusted', live['p_baseline']):.3f} ← Current shadow suggestion
"""
        
        if candidate:
            content += f"- **p_final (Candidate Council)**: {candidate['p_final']:.3f} ← Tuned shadow suggestion\n"
            content += f"- **Live vs Candidate Delta**: {candidate['p_final'] - live.get('p_zen_adjusted', live['p_baseline']):+.3f}\n"
        else:
            content += "- **p_final (Candidate Council)**: N/A (no candidate parameters)\n"
        
        content += f"""
### Council Adjustments Applied (Live)
"""
        
        # Note: live_forecast might not have Council adjustments if coming from impact shadow
        # This is a simplified version for Day 2 demo
        content += "- Standard Council rules applied (see previous reports for details)\n"
        
        if candidate and comparison_result['council_candidates_available']:
            cand_params = self.council_candidates
            content += f"""
### Candidate Council Parameters (Tuned)
- **Lambda (Blend)**: {cand_params['lambda']:.1f} vs Live: 0.7
- **Alpha0, Beta0**: {cand_params['alpha_0']}, {cand_params['beta_0']} vs Live: 2, 2
- **Miss Window**: {cand_params['miss_window']}d vs Live: 7d
- **Miss Penalty**: {cand_params['miss_penalty']*100:.0f}% vs Live: 10%
- **Vol Guard**: {cand_params['vol_widen']*100:.0f}% vs Live: 15%

### Hypothetical Candidate Impact
- **Probability Delta**: {candidate['p_final'] - live['p_baseline']:+.3f}
- **Tuning Effect**: {'More conservative' if abs(candidate['p_final'] - 0.5) < abs(live.get('p_zen_adjusted', live['p_baseline']) - 0.5) else 'More aggressive'} than live Council
"""
        
        if 'actual_outcome' in live:
            outcome_text = "UP" if live['actual_outcome'] == 1 else "DOWN"
            content += f"""
## Evening Results (PM)

### Actual Outcome: {outcome_text}

### Performance Comparison
- **Baseline Brier**: {live['baseline_brier']:.4f}
- **Live Council Brier**: {live.get('baseline_brier', 0):.4f} (same - shadow mode)
"""
            
            if candidate:
                # Calculate hypothetical candidate Brier
                candidate_brier = (candidate['p_final'] - live['actual_outcome']) ** 2
                content += f"- **Candidate Council Brier**: {candidate_brier:.4f}\n"
                content += f"- **Candidate vs Live**: {candidate_brier - live['baseline_brier']:+.4f} ({'Candidate better' if candidate_brier < live['baseline_brier'] else 'Live better'})\n"
        
        content += f"""
## Shadow Mode Status (Day 2)
- **Live Decision**: Baseline (p={live['p_baseline']:.3f}) - No change to production
- **Live Council**: {'Available' if 'p_zen_adjusted' in live else 'N/A'} shadow logging
- **Candidate Council**: {'Available' if candidate else 'No parameters'} - tuned parameters ready for testing
- **Tuning Status**: {'Parameters optimized, ready for validation' if candidate else 'Awaiting parameter tuning'}

---
Generated by Shadow Day 2 Runner v0.1.1
"""
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return str(report_file)
    
    def _append_impact_candidate_section(self, comparison_result, audit_dir):
        """Append candidate section to existing IMPACT_SHADOW_DAILY.md"""
        impact_file = audit_dir / 'IMPACT_SHADOW_DAILY.md'
        
        if not impact_file.exists():
            print("No existing impact shadow file found")
            return None
        
        live = comparison_result['live_forecast']
        candidate = comparison_result['candidate_impact']
        
        candidate_section = f"""
## Candidate Impact Parameters (Tuned - Day 2)

### Hypothetical Candidate Adjustments
"""
        
        if candidate and comparison_result['impact_candidates_available']:
            content = f"""- **News Threshold |s|**: {candidate['news_threshold']:.2f} vs Live: 0.3
- **Macro Threshold |z|**: {candidate['macro_threshold']:.1f} vs Live: 1.0
- **Candidate Band Adj**: {candidate['band_adjustment_pct']:+.1f}% vs Live: {live['band_adjustment_pct']:+.1f}%
- **Candidate Conf Adj**: {candidate['confidence_adjustment_pct']:+.1f}% vs Live: {live['confidence_adjustment_pct']:+.1f}%

### Candidate Triggers Comparison
- **Live Triggers**: {len(live['impact_triggers'])} ({', '.join(live['impact_triggers']) if live['impact_triggers'] else 'None'})
- **Candidate Triggers**: {len(candidate['triggers'])} ({', '.join(candidate['triggers']) if candidate['triggers'] else 'None'})
- **Sensitivity Difference**: {'More sensitive' if len(candidate['triggers']) > len(live['impact_triggers']) else 'Less sensitive' if len(candidate['triggers']) < len(live['impact_triggers']) else 'Same sensitivity'}

### Tuning Analysis
- **Threshold Optimization**: News {candidate['news_threshold']:.2f} vs 0.3, Macro {candidate['macro_threshold']:.1f} vs 1.0
- **Adjustment Tuning**: {'More conservative' if abs(candidate['band_adjustment_pct']) < abs(live['band_adjustment_pct']) else 'More aggressive'} band adjustments
- **Expected Impact**: {'Higher frequency triggers' if candidate['news_threshold'] < 0.3 else 'Lower frequency triggers'} with tuned thresholds
"""
        else:
            content = "- **Status**: No candidate parameters available\n- **Next Step**: Run impact tuning to generate candidates\n"
        
        candidate_section += content
        
        candidate_section += """
---
**CANDIDATE STATUS**: Parameters optimized offline, ready for shadow validation
**NO LIVE CHANGES**: All candidate adjustments are hypothetical comparisons only
"""
        
        # Append to existing file
        with open(impact_file, 'a', encoding='utf-8') as f:
            f.write(candidate_section)
        
        return str(impact_file)


def main():
    """Test Shadow Day 2 runner"""
    runner = ShadowDay2Runner()
    
    # Run Shadow Day 2
    comparison_result, live_daily_report, live_decision_log = runner.run_shadow_day_2()
    
    # Write comparison reports
    artifacts = runner.write_shadow_day_2_reports(comparison_result)
    
    print(f"\nShadow Day 2 complete!")
    print(f"Live report: {live_daily_report}")
    print(f"Council shadow: {artifacts.get('council_shadow', 'N/A')}")
    print(f"Impact shadow: {artifacts.get('impact_shadow', 'N/A')}")
    print(f"Decision log: {live_decision_log}")
    
    # Summary
    live = comparison_result['live_forecast']
    candidate_council = comparison_result['candidate_council']
    candidate_impact = comparison_result['candidate_impact']
    
    print(f"\nComparison Summary:")
    print(f"Baseline: {live['p_baseline']:.3f}")
    print(f"Live Council: {'N/A' if not candidate_council else live.get('p_zen_adjusted', 'N/A')}")
    candidate_council_str = f"{candidate_council['p_final']:.3f}" if candidate_council else 'N/A'
    print(f"Candidate Council: {candidate_council_str}")
    print(f"Live Impact: Band {live['band_adjustment_pct']:+.1f}%, Conf {live['confidence_adjustment_pct']:+.1f}%")
    candidate_impact_str = f"Band {candidate_impact['band_adjustment_pct']:+.1f}%, Conf {candidate_impact['confidence_adjustment_pct']:+.1f}%" if candidate_impact else "N/A"
    print(f"Candidate Impact: {candidate_impact_str}")
    
    return comparison_result, artifacts


if __name__ == '__main__':
    main()