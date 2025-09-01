#!/usr/bin/env python3
"""
Impact Guardrails: Auto-mute system for Event-Impact Engine
Monitors performance and triggers NEWS_IMPACT_MUTED when degradation detected
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add src to path
sys.path.append(str(Path(__file__).parent))


class ImpactGuardrails:
    """Auto-mute guardrails for Impact Engine performance protection"""
    
    def __init__(self):
        self.brier_degradation_threshold = 2.0  # Auto-mute if Brier worsens by >=2%
        self.ece_degradation_threshold = 2.0    # Auto-mute if ECE worsens by >=2%
        self.min_days_for_evaluation = 5       # Minimum days before triggering guardrails
        self.evaluation_window = 10            # Look back window for performance evaluation
        
        # Current mute status
        self.is_muted = os.getenv('NEWS_IMPACT_MUTED', 'false').lower() == 'true'
        
    def load_recent_performance(self, log_file='audit_exports/IMPACT_DECISION_LOG.csv', days=10):
        """Load recent impact performance data"""
        log_path = Path(log_file)
        
        if not log_path.exists():
            return pd.DataFrame()
        
        df = pd.read_csv(log_path)
        df['date'] = pd.to_datetime(df['date'])
        
        # Get last N days
        recent_df = df.tail(days)
        
        return recent_df
    
    def evaluate_brier_performance(self, performance_df):
        """Evaluate Brier score performance degradation"""
        if len(performance_df) < self.min_days_for_evaluation:
            return False, None, {}
        
        # Filter out rows with missing Brier scores
        valid_df = performance_df.dropna(subset=['baseline_brier', 'impact_brier'])
        
        if len(valid_df) < self.min_days_for_evaluation:
            return False, None, {}
        
        # Calculate average performance
        avg_baseline_brier = valid_df['baseline_brier'].mean()
        avg_impact_brier = valid_df['impact_brier'].mean()
        
        # Calculate degradation percentage
        brier_degradation_pct = (avg_impact_brier - avg_baseline_brier) / avg_baseline_brier * 100
        
        metrics = {
            'avg_baseline_brier': avg_baseline_brier,
            'avg_impact_brier': avg_impact_brier,
            'brier_degradation_pct': brier_degradation_pct,
            'evaluation_days': len(valid_df),
            'threshold': self.brier_degradation_threshold
        }
        
        # Check threshold
        should_mute = brier_degradation_pct >= self.brier_degradation_threshold
        reason = f"Brier degradation {brier_degradation_pct:.1f}% >= {self.brier_degradation_threshold}% over {len(valid_df)} days"
        
        return should_mute, reason if should_mute else None, metrics
    
    def evaluate_calibration_performance(self, performance_df):
        """Evaluate calibration performance (simplified ECE proxy)"""
        if len(performance_df) < self.min_days_for_evaluation:
            return False, None, {}
        
        # For simplified calibration, use the difference between prediction confidence and hit rate
        # This is a proxy for ECE - in practice would be more sophisticated
        
        valid_df = performance_df.dropna(subset=['baseline_brier', 'impact_brier'])
        
        if len(valid_df) < self.min_days_for_evaluation:
            return False, None, {}
        
        # Simplified calibration error proxy
        baseline_cal_error = valid_df['baseline_brier'].std()  # Brier variance as calibration proxy
        impact_cal_error = valid_df['impact_brier'].std()
        
        cal_degradation_pct = (impact_cal_error - baseline_cal_error) / baseline_cal_error * 100
        
        metrics = {
            'baseline_cal_error': baseline_cal_error,
            'impact_cal_error': impact_cal_error,
            'cal_degradation_pct': cal_degradation_pct,
            'evaluation_days': len(valid_df),
            'threshold': self.ece_degradation_threshold
        }
        
        # Check threshold
        should_mute = cal_degradation_pct >= self.ece_degradation_threshold
        reason = f"Calibration degradation {cal_degradation_pct:.1f}% >= {self.ece_degradation_threshold}% over {len(valid_df)} days"
        
        return should_mute, reason if should_mute else None, metrics
    
    def evaluate_edge_performance(self, performance_df):
        """Evaluate edge hit performance"""
        if len(performance_df) < self.min_days_for_evaluation:
            return False, None, {}
        
        valid_df = performance_df.dropna(subset=['edge_hit'])
        
        if len(valid_df) < self.min_days_for_evaluation:
            return False, None, {}
        
        edge_hit_rate = valid_df['edge_hit'].mean()
        expected_edge_rate = 0.1  # Expect ~10% edge hits under normal conditions
        
        # If edge hits are significantly below expected, might indicate poor band adjustments
        edge_underperformance = expected_edge_rate - edge_hit_rate
        
        metrics = {
            'edge_hit_rate': edge_hit_rate,
            'expected_edge_rate': expected_edge_rate,
            'edge_underperformance': edge_underperformance,
            'evaluation_days': len(valid_df)
        }
        
        # Conservative threshold - only mute if edge hits are extremely poor
        should_mute = edge_hit_rate < 0.02 and len(valid_df) >= 10  # Less than 2% over 10+ days
        reason = f"Edge hit rate {edge_hit_rate:.1%} < 2% over {len(valid_df)} days" if should_mute else None
        
        return should_mute, reason, metrics
    
    def run_guardrail_evaluation(self):
        """Run complete guardrail evaluation"""
        print("Running Impact Guardrails evaluation...")
        
        if self.is_muted:
            print("Impact engine already muted (NEWS_IMPACT_MUTED=true)")
            return {
                'already_muted': True,
                'should_mute': True,
                'mute_reason': 'Already muted by environment variable',
                'evaluation_performed': False
            }
        
        # Load recent performance data
        performance_df = self.load_recent_performance(days=self.evaluation_window)
        
        if len(performance_df) == 0:
            print("No performance data available for evaluation")
            return {
                'should_mute': False,
                'mute_reason': None,
                'evaluation_performed': False,
                'data_available': False
            }
        
        print(f"Evaluating performance over {len(performance_df)} days...")
        
        # Run evaluations
        brier_mute, brier_reason, brier_metrics = self.evaluate_brier_performance(performance_df)
        cal_mute, cal_reason, cal_metrics = self.evaluate_calibration_performance(performance_df)
        edge_mute, edge_reason, edge_metrics = self.evaluate_edge_performance(performance_df)
        
        # Determine overall mute decision
        should_mute = brier_mute or cal_mute or edge_mute
        
        mute_reasons = []
        if brier_mute and brier_reason:
            mute_reasons.append(brier_reason)
        if cal_mute and cal_reason:
            mute_reasons.append(cal_reason)  
        if edge_mute and edge_reason:
            mute_reasons.append(edge_reason)
        
        mute_reason = '; '.join(mute_reasons) if mute_reasons else None
        
        result = {
            'should_mute': should_mute,
            'mute_reason': mute_reason,
            'evaluation_performed': True,
            'data_available': True,
            'performance_metrics': {
                'brier': brier_metrics,
                'calibration': cal_metrics,
                'edge': edge_metrics
            },
            'evaluation_summary': {
                'days_evaluated': len(performance_df),
                'brier_triggered': brier_mute,
                'calibration_triggered': cal_mute,
                'edge_triggered': edge_mute
            }
        }
        
        if should_mute:
            print(f"GUARDRAIL TRIGGERED: {mute_reason}")
            print("Recommendation: Set NEWS_IMPACT_MUTED=true")
        else:
            print("All guardrails PASS - Impact engine performance within acceptable bounds")
        
        return result
    
    def write_guardrail_report(self, evaluation_result, output_dir='audit_exports'):
        """Write guardrail evaluation report"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        audit_dir = Path(output_dir) / 'guardrails' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        report_file = audit_dir / 'IMPACT_GUARDRAILS_REPORT.md'
        
        content = f"""# Impact Engine Guardrails Report

**Timestamp**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Status**: {'TRIGGERED' if evaluation_result['should_mute'] else 'PASS'}

## Evaluation Summary

### Decision
- **Should Mute**: {evaluation_result['should_mute']}
- **Current Status**: {'Already muted' if evaluation_result.get('already_muted', False) else 'Active'}
- **Evaluation Performed**: {evaluation_result['evaluation_performed']}

"""
        
        if evaluation_result['should_mute'] and evaluation_result.get('mute_reason'):
            content += f"""### Mute Reason
{evaluation_result['mute_reason']}

"""
        
        if evaluation_result.get('performance_metrics') and evaluation_result.get('evaluation_summary'):
            metrics = evaluation_result['performance_metrics']
            summary = evaluation_result['evaluation_summary']
            
            content += f"""## Performance Analysis

### Data Summary
- **Days Evaluated**: {summary['days_evaluated']}
- **Evaluation Window**: {self.evaluation_window} days
- **Minimum Days Required**: {self.min_days_for_evaluation}

"""
            
            # Only add sections if data is available
            if metrics.get('brier') and 'avg_baseline_brier' in metrics['brier']:
                content += f"""### Brier Score Analysis
- **Triggered**: {'YES' if summary['brier_triggered'] else 'NO'}
- **Baseline Average**: {metrics['brier']['avg_baseline_brier']:.4f}
- **Impact Average**: {metrics['brier']['avg_impact_brier']:.4f}
- **Degradation**: {metrics['brier']['brier_degradation_pct']:+.1f}%
- **Threshold**: {metrics['brier']['threshold']:.1f}%

"""
            
            if metrics.get('calibration') and 'baseline_cal_error' in metrics['calibration']:
                content += f"""### Calibration Analysis  
- **Triggered**: {'YES' if summary['calibration_triggered'] else 'NO'}
- **Baseline Cal Error**: {metrics['calibration']['baseline_cal_error']:.4f}
- **Impact Cal Error**: {metrics['calibration']['impact_cal_error']:.4f}
- **Degradation**: {metrics['calibration']['cal_degradation_pct']:+.1f}%
- **Threshold**: {metrics['calibration']['threshold']:.1f}%

"""
            
            if metrics.get('edge') and 'edge_hit_rate' in metrics['edge']:
                content += f"""### Edge Hit Analysis
- **Triggered**: {'YES' if summary['edge_triggered'] else 'NO'}
- **Edge Hit Rate**: {metrics['edge']['edge_hit_rate']:.1%}
- **Expected Rate**: {metrics['edge']['expected_edge_rate']:.1%}
- **Underperformance**: {metrics['edge']['edge_underperformance']:+.1%}

"""
        else:
            content += """## Performance Analysis

**Insufficient Data**: Not enough performance data available for detailed analysis.
Requires at least 5 days of impact shadow data for evaluation.

"""
        
        content += f"""## Guardrail Logic

### Thresholds
- **Brier Degradation**: ≥{self.brier_degradation_threshold}% triggers auto-mute
- **Calibration Degradation**: ≥{self.ece_degradation_threshold}% triggers auto-mute  
- **Edge Hit Rate**: <2% over 10+ days triggers auto-mute

### Evaluation Requirements
- Minimum {self.min_days_for_evaluation} days of data required
- Rolling {self.evaluation_window}-day evaluation window
- Protects against systematic performance degradation

## Recommendations

"""
        
        if evaluation_result['should_mute']:
            content += """### ACTION REQUIRED
1. **Set NEWS_IMPACT_MUTED=true** to disable impact adjustments
2. **Review impact engine logic** for systematic issues  
3. **Investigate performance degradation** causes
4. **Re-evaluate after fixes** by setting NEWS_IMPACT_MUTED=false

### Impact of Muting
- News/macro ingestion continues normally
- Impact adjustments (bands/confidence) disabled
- Baseline probabilities remain unchanged
- Shadow logging continues for monitoring
"""
        else:
            content += """### NO ACTION REQUIRED
- Impact engine performance within acceptable bounds
- Continue normal shadow mode operation
- Monitor daily performance via IMPACT_SHADOW_DAILY.md
- Regular guardrail evaluation recommended
"""
        
        content += f"""
## Next Steps
- **Daily Monitoring**: Review IMPACT_SHADOW_DAILY.md reports
- **Weekly Evaluation**: Run guardrail evaluation weekly
- **Performance Tracking**: Monitor IMPACT_DECISION_LOG.csv trends
- **Threshold Tuning**: Adjust thresholds based on production data

---
Generated by Impact Guardrails System
"""
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"Guardrails report: {report_file}")
        
        return str(report_file)
    
    def trigger_auto_mute(self, reason):
        """Trigger auto-mute and log decision"""
        print(f"AUTO-MUTE TRIGGERED: {reason}")
        
        # Write mute decision to NEWS_SCORE.md
        news_score_file = Path('audit_exports/daily') / datetime.now().strftime('%Y%m%d') / 'NEWS_SCORE.md'
        
        if news_score_file.exists():
            with open(news_score_file, 'a', encoding='utf-8') as f:
                f.write(f"\n\n## AUTO-MUTE TRIGGERED\n\n")
                f.write(f"**Timestamp**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n")
                f.write(f"**Reason**: {reason}\n\n")
                f.write("**Action**: NEWS_IMPACT_MUTED should be set to true\n\n")
                f.write("**Effect**: Impact adjustments disabled, news ingestion continues\n\n")
        
        # In a production environment, this would actually set the environment variable
        # For this simulation, we just log the decision
        print("In production: Set NEWS_IMPACT_MUTED=true environment variable")
        
        return True


def main():
    """Test impact guardrails"""
    guardrails = ImpactGuardrails()
    
    # Run evaluation
    result = guardrails.run_guardrail_evaluation()
    
    # Write report
    report_path = guardrails.write_guardrail_report(result)
    
    print(f"\nGuardrails evaluation complete!")
    print(f"Should mute: {result['should_mute']}")
    if result.get('mute_reason'):
        print(f"Reason: {result['mute_reason']}")
    print(f"Report: {report_path}")
    
    return result


if __name__ == '__main__':
    main()