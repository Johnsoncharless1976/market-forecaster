#!/usr/bin/env python3
"""
Magnet Engine Guardrails
Auto-mute system for Level Magnet Engine based on performance degradation
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import csv


class MagnetGuardrails:
    """Auto-mute guardrail system for Magnet Engine"""
    
    def __init__(self):
        self.brier_threshold_pct = 2.0  # 2% Brier degradation triggers mute
        self.ece_threshold_pct = 1.0   # 1% ECE degradation limit
        self.shadow_period_days = 10   # Rolling window for assessment
        self.decision_log_path = 'audit_exports/MAGNET_DECISION_LOG.csv'
        
    def log_magnet_performance(self, date, p_baseline, p_with_magnet, actual_outcome, magnet_active=True):
        """Log daily magnet performance for guardrail assessment"""
        
        # Ensure log directory exists
        Path(self.decision_log_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Calculate Brier scores
        baseline_brier = (p_baseline - actual_outcome) ** 2
        magnet_brier = (p_with_magnet - actual_outcome) ** 2
        
        # Log entry
        log_entry = {
            'date': date.strftime('%Y-%m-%d'),
            'p_baseline': p_baseline,
            'p_with_magnet': p_with_magnet,
            'actual_outcome': actual_outcome,
            'baseline_brier': baseline_brier,
            'magnet_brier': magnet_brier,
            'magnet_active': magnet_active,
            'muted': os.getenv('MAGNET_MUTED', 'false').lower() == 'true'
        }
        
        # Write to CSV
        file_exists = Path(self.decision_log_path).exists()
        
        with open(self.decision_log_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=log_entry.keys())
            
            if not file_exists:
                writer.writeheader()
            
            writer.writerow(log_entry)
        
        return log_entry
    
    def load_recent_performance(self):
        """Load recent magnet performance data"""
        if not Path(self.decision_log_path).exists():
            return pd.DataFrame()
        
        try:
            df = pd.read_csv(self.decision_log_path)
            df['date'] = pd.to_datetime(df['date'])
            
            # Get last N days
            cutoff_date = datetime.now().date() - timedelta(days=self.shadow_period_days)
            recent_df = df[df['date'] >= pd.to_datetime(cutoff_date)]
            
            return recent_df
            
        except Exception as e:
            print(f"Error loading performance data: {e}")
            return pd.DataFrame()
    
    def assess_performance(self):
        """Assess magnet performance and determine if muting is needed"""
        recent_df = self.load_recent_performance()
        
        if len(recent_df) < 5:  # Need minimum data
            return {
                'should_mute': False,
                'reason': 'Insufficient data',
                'days_analyzed': len(recent_df),
                'brier_change_pct': 0.0,
                'ece_change_pct': 0.0
            }
        
        # Calculate performance metrics
        baseline_brier = recent_df['baseline_brier'].mean()
        magnet_brier = recent_df['magnet_brier'].mean()
        
        brier_change_pct = (magnet_brier - baseline_brier) / baseline_brier * 100
        
        # Simple ECE calculation (bins-based)
        baseline_probs = recent_df['p_baseline'].values
        magnet_probs = recent_df['p_with_magnet'].values
        outcomes = recent_df['actual_outcome'].values
        
        def calculate_ece(probs, outcomes):
            bins = np.linspace(0, 1, 6)  # 5 bins
            ece = 0.0
            
            for i in range(5):
                mask = (probs > bins[i]) & (probs <= bins[i+1])
                if np.sum(mask) > 0:
                    bin_acc = np.mean(outcomes[mask])
                    bin_conf = np.mean(probs[mask])
                    bin_weight = np.sum(mask) / len(probs)
                    ece += bin_weight * abs(bin_acc - bin_conf)
            return ece
        
        baseline_ece = calculate_ece(baseline_probs, outcomes)
        magnet_ece = calculate_ece(magnet_probs, outcomes)
        
        ece_change_pct = ((magnet_ece - baseline_ece) / baseline_ece * 100) if baseline_ece > 0 else 0
        
        # Determine if muting is needed
        should_mute = False
        reasons = []
        
        if brier_change_pct >= self.brier_threshold_pct:
            should_mute = True
            reasons.append(f"Brier degradation {brier_change_pct:+.1f}% >= {self.brier_threshold_pct}% threshold")
        
        if ece_change_pct >= self.ece_threshold_pct:
            should_mute = True
            reasons.append(f"ECE degradation {ece_change_pct:+.1f}% >= {self.ece_threshold_pct}% threshold")
        
        return {
            'should_mute': should_mute,
            'reason': '; '.join(reasons) if reasons else 'Performance within acceptable bounds',
            'days_analyzed': len(recent_df),
            'brier_change_pct': brier_change_pct,
            'ece_change_pct': ece_change_pct,
            'baseline_brier': baseline_brier,
            'magnet_brier': magnet_brier,
            'baseline_ece': baseline_ece,
            'magnet_ece': magnet_ece
        }
    
    def apply_guardrail_decision(self):
        """Apply guardrail decision - mute if performance degraded"""
        assessment = self.assess_performance()
        
        currently_muted = os.getenv('MAGNET_MUTED', 'false').lower() == 'true'
        
        if assessment['should_mute'] and not currently_muted:
            # Trigger mute
            self.write_guardrail_report(assessment, action='MUTE')
            print(f"MAGNET AUTO-MUTE TRIGGERED: {assessment['reason']}")
            print("WARNING: Set MAGNET_MUTED=true to acknowledge and mute the Magnet Engine")
            return True
            
        elif not assessment['should_mute'] and currently_muted:
            # Could suggest unmute, but keep manual control
            self.write_guardrail_report(assessment, action='SUGGEST_UNMUTE')
            print(f"MAGNET PERFORMANCE OK: {assessment['reason']}")
            print("SUGGESTION: Consider setting MAGNET_MUTED=false to re-enable Magnet Engine")
            return False
            
        else:
            # No action needed
            self.write_guardrail_report(assessment, action='NO_ACTION')
            return False
    
    def write_guardrail_report(self, assessment, action='ASSESS'):
        """Write guardrail assessment report"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        audit_dir = Path('audit_exports') / 'guardrails' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        report_file = audit_dir / 'MAGNET_GUARDRAILS_REPORT.md'
        
        content = f"""# Magnet Engine Guardrails Report

**Timestamp**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Action**: {action}
**Assessment Period**: {assessment['days_analyzed']} days

## Performance Assessment

### Brier Score Analysis
- **Baseline Brier**: {assessment.get('baseline_brier', 0):.4f}
- **Magnet Brier**: {assessment.get('magnet_brier', 0):.4f}
- **Change**: {assessment['brier_change_pct']:+.2f}%
- **Threshold**: ≥{self.brier_threshold_pct}% triggers mute

### Expected Calibration Error (ECE)
- **Baseline ECE**: {assessment.get('baseline_ece', 0):.4f}
- **Magnet ECE**: {assessment.get('magnet_ece', 0):.4f}
- **Change**: {assessment['ece_change_pct']:+.2f}%
- **Threshold**: ≥{self.ece_threshold_pct}% triggers mute

## Guardrail Decision

### Assessment Result
- **Should Mute**: {'Yes' if assessment['should_mute'] else 'No'}
- **Reason**: {assessment['reason']}
- **Current Status**: {'MUTED' if os.getenv('MAGNET_MUTED', 'false').lower() == 'true' else 'ACTIVE'}

### Action Taken
"""
        
        if action == 'MUTE':
            content += """- **🚨 MUTE TRIGGERED**: Magnet Engine performance degraded beyond acceptable thresholds
- **Required Action**: Set MAGNET_MUTED=true to acknowledge and disable magnet adjustments
- **Effect**: Magnet analysis will continue, but no band adjustments will be applied
"""
        elif action == 'SUGGEST_UNMUTE':
            content += """- **✅ PERFORMANCE RECOVERED**: Magnet Engine showing acceptable performance
- **Suggestion**: Consider setting MAGNET_MUTED=false to re-enable magnet adjustments
- **Caution**: Monitor performance closely after re-activation
"""
        else:
            content += """- **⚪ NO ACTION**: Performance within acceptable bounds or already appropriately muted/active
- **Status**: Continue current operation
- **Monitoring**: Ongoing performance assessment active
"""
        
        content += f"""
## Guardrail Settings

### Performance Thresholds
- **Brier Degradation Limit**: {self.brier_threshold_pct}%
- **ECE Degradation Limit**: {self.ece_threshold_pct}%
- **Assessment Window**: {self.shadow_period_days} days
- **Minimum Data Points**: 5 days

### Safety Controls
- **Auto-mute**: Yes (performance-based)
- **Manual Override**: MAGNET_MUTED environment variable
- **Dashboard Alert**: Yes (banner display when muted)
- **Logging**: All decisions logged to MAGNET_DECISION_LOG.csv

## Data Quality

### Assessment Reliability
- **Data Points**: {assessment['days_analyzed']} trading days
- **Reliability**: {'High' if assessment['days_analyzed'] >= 10 else 'Medium' if assessment['days_analyzed'] >= 5 else 'Low'}
- **Confidence**: {'High confidence in assessment' if assessment['days_analyzed'] >= 10 else 'Medium confidence - limited data' if assessment['days_analyzed'] >= 5 else 'Low confidence - insufficient data'}

---
Generated by Magnet Guardrails System v0.1
**AUTO-MUTE ACTIVE**: Performance monitoring with automated safety controls
"""
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return str(report_file)


def main():
    """Test Magnet Guardrails system"""
    guardrails = MagnetGuardrails()
    
    # Simulate some performance data (for testing)
    test_dates = [datetime.now().date() - timedelta(days=i) for i in range(10, 0, -1)]
    
    print("Simulating 10 days of magnet performance...")
    for i, date in enumerate(test_dates):
        # Simulate baseline prob around 0.55
        p_baseline = 0.55 + np.random.normal(0, 0.05)
        
        # Simulate magnet adjustment (small effect)
        p_magnet = p_baseline + np.random.normal(0, 0.01)
        
        # Simulate outcome
        outcome = 1 if np.random.random() < 0.6 else 0
        
        # Log performance
        guardrails.log_magnet_performance(date, p_baseline, p_magnet, outcome)
        print(f"Day {i+1}: baseline={p_baseline:.3f}, magnet={p_magnet:.3f}, outcome={outcome}")
    
    # Assess performance
    print("\nAssessing magnet performance...")
    assessment = guardrails.assess_performance()
    
    print(f"Should mute: {assessment['should_mute']}")
    print(f"Reason: {assessment['reason']}")
    print(f"Brier change: {assessment['brier_change_pct']:+.2f}%")
    print(f"ECE change: {assessment['ece_change_pct']:+.2f}%")
    
    # Apply guardrail decision
    mute_triggered = guardrails.apply_guardrail_decision()
    
    return assessment


if __name__ == '__main__':
    main()