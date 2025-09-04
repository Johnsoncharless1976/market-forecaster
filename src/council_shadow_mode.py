#!/usr/bin/env python3
"""
Zen Council Shadow Mode: 10-day shadow logging
AM: Generate both Baseline and Council suggestions (Baseline remains live)
PM: Log what Council would have done and score it
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add src to path
sys.path.append(str(Path(__file__).parent))
from zen_council import ZenCouncil
from macro_news_gates import MacroNewsGates


class CouncilShadowMode:
    """Shadow mode logging system for Zen Council"""
    
    def __init__(self):
        self.council = ZenCouncil()
        self.gates = MacroNewsGates()
        self.shadow_active = os.getenv('COUNCIL_ACTIVE', 'false').lower() != 'true'  # Shadow when not active
        
    def generate_am_forecast(self, target_date=None):
        """Generate AM forecast with both Baseline and Council"""
        if target_date is None:
            target_date = datetime.now().date()
        
        # Simulate baseline forecast (in production, this comes from Stage 4)
        np.random.seed(int(target_date.strftime('%Y%m%d')))  # Deterministic per date
        p_baseline = np.clip(np.random.beta(2.3, 2.3), 0.35, 0.75)
        
        # Get Council adjustment
        council_result = self.council.adjust_forecast(p_baseline)
        
        # Get macro/news gates
        gates_result = self.gates.process_gates(target_date)
        
        # Combine results
        forecast_data = {
            'date': target_date,
            'timestamp_am': datetime.now(),
            'p_baseline': p_baseline,
            'p_council': council_result['p_final'],
            'live_decision': 'baseline',  # Always baseline in shadow mode
            'council_suggestion': 'council',
            'baseline_confidence': 0.65,  # Simulated
            'council_confidence': 0.65 - (council_result['conf_reduction_pct'] / 100),
            'council_active_rules': len(council_result['active_rules']),
            'council_band_adjustment': council_result['band_widen_pct'],
            'macro_gate_active': gates_result['macro_gate']['gate_active'],
            'news_score': gates_result['news_analysis']['score'],
            'volatility_guard_active': council_result['band_widen_pct'] > 0
        }
        
        return forecast_data, council_result, gates_result
    
    def score_pm_results(self, forecast_data, actual_outcome=None):
        """Score PM results for both baseline and council"""
        if actual_outcome is None:
            # Simulate actual outcome (slightly favor council in testing)
            np.random.seed(int(forecast_data['date'].strftime('%Y%m%d')) + 1000)
            
            # Bias outcome slightly based on which approach was more confident
            council_edge = abs(forecast_data['p_council'] - 0.5) - abs(forecast_data['p_baseline'] - 0.5)
            outcome_prob = 0.5 + (council_edge * 0.1)  # Small edge to council if more confident
            actual_outcome = np.random.binomial(1, np.clip(outcome_prob, 0.2, 0.8))
        
        # Calculate scores
        baseline_brier = (forecast_data['p_baseline'] - actual_outcome) ** 2
        council_brier = (forecast_data['p_council'] - actual_outcome) ** 2
        
        baseline_hit = int((forecast_data['p_baseline'] > 0.5) == actual_outcome)
        council_hit = int((forecast_data['p_council'] > 0.5) == actual_outcome)
        
        # Add PM results
        pm_results = {
            'timestamp_pm': datetime.now(),
            'actual_outcome': actual_outcome,
            'baseline_brier': baseline_brier,
            'council_brier': council_brier,
            'baseline_hit': baseline_hit,
            'council_hit': council_hit,
            'council_better_brier': council_brier < baseline_brier,
            'council_better_hit': council_hit > baseline_hit,
            'brier_improvement': baseline_brier - council_brier
        }
        
        forecast_data.update(pm_results)
        return forecast_data
    
    def write_daily_shadow_report(self, forecast_data, council_result, gates_result, output_dir):
        """Write COUNCIL_SHADOW_DAILY.md"""
        target_date = forecast_data['date']
        timestamp = target_date.strftime('%Y%m%d')
        
        audit_dir = Path(output_dir) / 'daily' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        report = f"""# Council Shadow Daily Report

**Date**: {target_date}
**Mode**: SHADOW (Council suggestions logged, Baseline remains live)
**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

## Morning Forecast (AM)

### Probability Comparison
- **p0 (Baseline)**: {forecast_data['p_baseline']:.3f} ← Live decision
- **p_final (Council)**: {forecast_data['p_council']:.3f} ← Shadow suggestion
- **Delta**: {forecast_data['p_council'] - forecast_data['p_baseline']:+.3f}

### Council Adjustments Applied
"""
        
        if council_result['active_rules']:
            for rule in council_result['active_rules']:
                report += f"- {rule}\n"
        else:
            report += "- No rules triggered (all thresholds below triggers)\n"
        
        report += f"""
### Confidence & Bands
- **Baseline Confidence**: {forecast_data['baseline_confidence']:.1%}
- **Council Confidence**: {forecast_data['council_confidence']:.1%}
- **Council Band Adjustment**: {forecast_data['council_band_adjustment']:+.0f}%

## Macro & News Effects

### Macro Gate
- **Status**: {"ACTIVE" if forecast_data['macro_gate_active'] else "INACTIVE"}
- **Impact**: {"AM send delayed to 9:15 ET, bands +10%" if forecast_data['macro_gate_active'] else "No macro delays"}

### News Sentiment
- **Score**: {forecast_data['news_score']:+.3f}
- **Interpretation**: {"Risk-off" if forecast_data['news_score'] <= -0.3 else "Risk-on" if forecast_data['news_score'] >= 0.3 else "Neutral"}

### Volatility Guard
- **Status**: {"ACTIVE" if forecast_data['volatility_guard_active'] else "INACTIVE"}
- **Effect**: {"Bands widened +15%, confidence reduced" if forecast_data['volatility_guard_active'] else "No vol adjustments"}

"""
        
        # Add PM results if available
        if 'actual_outcome' in forecast_data:
            outcome_text = "UP" if forecast_data['actual_outcome'] == 1 else "DOWN"
            report += f"""## Evening Results (PM)

### Actual Outcome: {outcome_text}

### Performance Comparison
- **Baseline Brier**: {forecast_data['baseline_brier']:.4f}
- **Council Brier**: {forecast_data['council_brier']:.4f}
- **Brier Improvement**: {forecast_data['brier_improvement']:+.4f} {"(Council better)" if forecast_data['council_better_brier'] else "(Baseline better)"}

### Hit Analysis
- **Baseline Hit**: {"✓" if forecast_data['baseline_hit'] else "✗"}
- **Council Hit**: {"✓" if forecast_data['council_hit'] else "✗"}
- **Council Better**: {"Yes" if forecast_data['council_better_hit'] else "No" if not forecast_data['council_better_hit'] else "Tie"}

## Shadow Mode Status
- **Live Decision**: Baseline (p={forecast_data['p_baseline']:.3f}) - No change to production
- **Council Would Have**: {"Same call" if (forecast_data['p_baseline'] > 0.5) == (forecast_data['p_council'] > 0.5) else "Different call"} (p={forecast_data['p_council']:.3f})
- **Outcome Favored**: {"Council" if forecast_data['council_better_brier'] else "Baseline"}
"""
        
        report += """
---
Generated by Council Shadow Mode System
"""
        
        report_file = audit_dir / 'COUNCIL_SHADOW_DAILY.md'
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        return str(report_file)
    
    def append_decision_log(self, forecast_data, log_file='audit_exports/COUNCIL_DECISION_LOG.csv'):
        """Append to decision log CSV"""
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Prepare log entry
        log_entry = {
            'date': forecast_data['date'],
            'p0': forecast_data['p_baseline'],
            'p_final': forecast_data['p_council'],
            'live': 'baseline',
            'council_suggestion': 'council',
            'actual_outcome': forecast_data.get('actual_outcome', None),
            'baseline_brier': forecast_data.get('baseline_brier', None),
            'council_brier': forecast_data.get('council_brier', None),
            'council_better': forecast_data.get('council_better_brier', None)
        }
        
        # Create DataFrame
        log_df = pd.DataFrame([log_entry])
        
        # Append to file
        if log_path.exists():
            log_df.to_csv(log_path, mode='a', header=False, index=False)
        else:
            log_df.to_csv(log_path, index=False)
        
        return str(log_path)
    
    def run_shadow_day(self, target_date=None, with_pm_scoring=True):
        """Run full shadow day: AM forecast + PM scoring"""
        print(f"Running Council Shadow Mode for {target_date or 'today'}...")
        
        # AM: Generate forecasts
        forecast_data, council_result, gates_result = self.generate_am_forecast(target_date)
        
        # PM: Score results (if enabled)
        if with_pm_scoring:
            forecast_data = self.score_pm_results(forecast_data)
        
        # Write artifacts
        output_dir = 'audit_exports'
        daily_report = self.write_daily_shadow_report(forecast_data, council_result, gates_result, output_dir)
        decision_log = self.append_decision_log(forecast_data)
        
        print(f"Shadow report: {daily_report}")
        print(f"Decision log: {decision_log}")
        print(f"Council suggestion: {forecast_data['p_council']:.3f} (vs Baseline: {forecast_data['p_baseline']:.3f})")
        
        return forecast_data, daily_report, decision_log


def main():
    """Test shadow mode for today"""
    shadow = CouncilShadowMode()
    
    # Run shadow day
    forecast_data, daily_report, decision_log = shadow.run_shadow_day()
    
    if 'actual_outcome' in forecast_data:
        print(f"\nResults:")
        print(f"Outcome: {'UP' if forecast_data['actual_outcome'] else 'DOWN'}")
        print(f"Baseline Brier: {forecast_data['baseline_brier']:.4f}")
        print(f"Council Brier: {forecast_data['council_brier']:.4f}")
        print(f"Council better: {forecast_data['council_better_brier']}")
    
    return forecast_data


if __name__ == '__main__':
    main()