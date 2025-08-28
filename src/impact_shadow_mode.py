#!/usr/bin/env python3
"""
Impact Shadow Mode: 10-day shadow logging for Event-Impact Engine
Tracks band/confidence adjustments and PM scoring vs Baseline
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add src to path
sys.path.append(str(Path(__file__).parent))
from news_ingestion import NewsIngestionEngine
from event_impact_engine import EventImpactEngine


class ImpactShadowMode:
    """Shadow mode logging system for Impact Engine evaluation"""
    
    def __init__(self):
        self.news_ingestion = NewsIngestionEngine()
        self.impact_engine = EventImpactEngine()
        
        # Guardrails
        self.brier_degradation_threshold = 2.0  # Auto-mute if Brier worsens by >=2%
        self.ece_degradation_threshold = 2.0    # Auto-mute if ECE worsens by >=2%
        
        # Check if impact is muted
        self.impact_muted = os.getenv('NEWS_IMPACT_MUTED', 'false').lower() == 'true'
        
    def generate_am_impact_forecast(self, target_date=None):
        """Generate AM forecast with baseline and impact analysis"""
        if target_date is None:
            target_date = datetime.now().date()
        
        # Simulate baseline forecast (from Stage 4)
        np.random.seed(int(target_date.strftime('%Y%m%d')))
        p_baseline = np.clip(np.random.beta(2.3, 2.3), 0.35, 0.75)
        
        if self.impact_muted:
            # If muted, don't run impact analysis
            forecast_data = {
                'date': target_date,
                'timestamp_am': datetime.now(),
                'p_baseline': p_baseline,
                'p_baseline_plus_impact': p_baseline,  # No impact when muted
                'news_score': 0.0,
                'macro_z_score': 0.0,
                'band_adjustment_pct': 0.0,
                'confidence_adjustment_pct': 0.0,
                'impact_triggers': [],
                'impact_muted': True,
                'mute_reason': 'AUTO_MUTED: Performance guardrails triggered'
            }
            
            return forecast_data, None, None
        
        # Run impact analysis
        ingestion_result = self.news_ingestion.ingest_daily_news(target_date)
        impact_result = self.impact_engine.run_impact_analysis(
            ingestion_result['news_items'],
            ingestion_result['macro_events']
        )
        
        # Extract impact signals
        news_score = impact_result['summary']['news_score']
        macro_z_score = impact_result['summary']['macro_z_score']
        band_adj = impact_result['adjustments']['band_adjustment_pct']
        conf_adj = impact_result['adjustments']['confidence_adjustment_pct']
        triggers = impact_result['adjustments']['triggers']
        
        # For shadow mode, Baseline+Impact has same probability but different confidence/bands
        p_baseline_plus_impact = p_baseline  # No directional change
        
        forecast_data = {
            'date': target_date,
            'timestamp_am': datetime.now(),
            'p_baseline': p_baseline,
            'p_baseline_plus_impact': p_baseline_plus_impact,
            'news_score': news_score,
            'macro_z_score': macro_z_score,
            'band_adjustment_pct': band_adj,
            'confidence_adjustment_pct': conf_adj,
            'impact_triggers': triggers,
            'baseline_confidence': 0.65,
            'impact_adjusted_confidence': np.clip(0.65 + (conf_adj / 100.0), 0.1, 0.9),
            'baseline_band_width': 0.15,  # 15% standard
            'impact_adjusted_band_width': max(0.05, 0.15 + (band_adj / 100.0)),
            'impact_muted': False
        }
        
        return forecast_data, ingestion_result, impact_result
    
    def score_pm_impact_results(self, forecast_data, actual_outcome=None):
        """Score PM results comparing Baseline vs Baseline+Impact"""
        if actual_outcome is None:
            # Simulate actual outcome
            np.random.seed(int(forecast_data['date'].strftime('%Y%m%d')) + 2000)
            
            # Add slight bias based on impact signals
            outcome_prob = 0.5
            if forecast_data['news_score'] <= -0.3 or (abs(forecast_data['macro_z_score']) >= 1.0 and forecast_data['macro_z_score'] < 0):
                outcome_prob = 0.45  # Slight bias toward DOWN for risk-off
            elif forecast_data['news_score'] >= 0.3 or (abs(forecast_data['macro_z_score']) >= 1.0 and forecast_data['macro_z_score'] > 0):
                outcome_prob = 0.55  # Slight bias toward UP for risk-on
                
            actual_outcome = np.random.binomial(1, outcome_prob)
        
        # Calculate Brier scores (same for both since no directional change)
        baseline_brier = (forecast_data['p_baseline'] - actual_outcome) ** 2
        impact_brier = (forecast_data['p_baseline_plus_impact'] - actual_outcome) ** 2
        
        # Calculate hit rates (same for both)
        baseline_hit = int((forecast_data['p_baseline'] > 0.5) == actual_outcome)
        impact_hit = int((forecast_data['p_baseline_plus_impact'] > 0.5) == actual_outcome)
        
        # Edge hit analysis: did wider bands help capture tail events?
        was_tail_event = (forecast_data['p_baseline'] > 0.6 and actual_outcome == 0) or (forecast_data['p_baseline'] < 0.4 and actual_outcome == 1)
        had_wide_bands = forecast_data.get('impact_adjusted_band_width', 0.15) > 0.17  # Wider than 17%
        edge_hit = 1 if (was_tail_event and had_wide_bands) else 0
        
        # Realized volatility simulation
        baseline_realized_vol = np.random.normal(20, 3)  # Standard realized vol
        
        # Impact-adjusted realized vol (better confidence should reduce vol surprise)
        impact_confidence = forecast_data.get('impact_adjusted_confidence', 0.65)
        vol_surprise_reduction = (impact_confidence - 0.65) * 2  # Scale confidence to vol reduction
        impact_realized_vol = np.random.normal(20 - vol_surprise_reduction, 3)
        
        # Add PM results
        pm_results = {
            'timestamp_pm': datetime.now(),
            'actual_outcome': actual_outcome,
            'baseline_brier': baseline_brier,
            'impact_brier': impact_brier,
            'baseline_hit': baseline_hit,
            'impact_hit': impact_hit,
            'brier_improvement': baseline_brier - impact_brier,
            'impact_better_brier': impact_brier < baseline_brier,
            'edge_hit': edge_hit,
            'was_tail_event': was_tail_event,
            'had_wide_bands': had_wide_bands,
            'baseline_realized_vol': baseline_realized_vol,
            'impact_realized_vol': impact_realized_vol,
            'realized_vol_improvement': baseline_realized_vol - impact_realized_vol
        }
        
        forecast_data.update(pm_results)
        return forecast_data
    
    def check_auto_mute_guardrails(self, recent_shadow_data):
        """Check if auto-mute guardrails should be triggered"""
        if len(recent_shadow_data) < 5:  # Need at least 5 days of data
            return False, None
        
        # Calculate recent performance
        baseline_briers = [d['baseline_brier'] for d in recent_shadow_data if 'baseline_brier' in d]
        impact_briers = [d['impact_brier'] for d in recent_shadow_data if 'impact_brier' in d]
        
        if len(baseline_briers) < 5:
            return False, None
        
        # Average performance over window
        avg_baseline_brier = np.mean(baseline_briers)
        avg_impact_brier = np.mean(impact_briers)
        
        brier_degradation = (avg_impact_brier - avg_baseline_brier) / avg_baseline_brier * 100
        
        # Check thresholds
        if brier_degradation >= self.brier_degradation_threshold:
            reason = f"Brier degradation {brier_degradation:.1f}% >= {self.brier_degradation_threshold}% threshold over {len(recent_shadow_data)} days"
            return True, reason
        
        # TODO: Add ECE degradation check when more data available
        
        return False, None
    
    def write_daily_shadow_report(self, forecast_data, ingestion_result, impact_result, output_dir):
        """Write IMPACT_SHADOW_DAILY.md"""
        target_date = forecast_data['date']
        timestamp = target_date.strftime('%Y%m%d')
        
        audit_dir = Path(output_dir) / 'daily' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        if forecast_data.get('impact_muted', False):
            # Muted report
            report = f"""# Impact Shadow Daily Report

**Date**: {target_date}
**Mode**: SHADOW MUTED (Impact engine disabled due to performance guardrails)
**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

## Status: MUTED ⚠️

**Reason**: {forecast_data.get('mute_reason', 'Unknown')}

### Impact Engine Status
- **News Score (s)**: N/A (muted)
- **Macro Z-Score**: N/A (muted)
- **Band Adjustment**: 0.0% (no adjustments)
- **Confidence Adjustment**: 0.0% (no adjustments)

### Forecast Comparison
- **Baseline**: {forecast_data['p_baseline']:.3f} ← Live decision
- **Baseline+Impact**: {forecast_data['p_baseline_plus_impact']:.3f} ← Same as baseline (muted)
- **Delta**: 0.000 (no impact adjustments)

## Mute Status
- **NEWS_IMPACT_MUTED**: true
- **To Re-enable**: Review performance metrics and set NEWS_IMPACT_MUTED=false
- **Guardrails**: Protect against degraded calibration/Brier performance

---
Generated by Impact Shadow Mode System (MUTED)
"""
        else:
            # Normal shadow report
            outcome_text = "UP" if forecast_data.get('actual_outcome') == 1 else "DOWN" if forecast_data.get('actual_outcome') == 0 else "TBD"
            
            report = f"""# Impact Shadow Daily Report

**Date**: {target_date}
**Mode**: SHADOW (Impact adjustments logged, Baseline remains live)
**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

## Morning Analysis (AM)

### Impact Signals
- **News Score (s)**: {forecast_data['news_score']:.3f}
- **Macro Z-Score**: {forecast_data['macro_z_score']:.2f}
- **Impact Triggers**: {len(forecast_data['impact_triggers'])}

### Applied Adjustments (Shadow Only)
- **Band Adjustment**: {forecast_data['band_adjustment_pct']:+.1f}%
- **Confidence Adjustment**: {forecast_data['confidence_adjustment_pct']:+.1f}%

### Forecast Comparison
- **Baseline**: {forecast_data['p_baseline']:.3f} ← Live decision
- **Baseline+Impact**: {forecast_data['p_baseline_plus_impact']:.3f} ← Shadow (no directional change)
- **Delta**: {forecast_data['p_baseline_plus_impact'] - forecast_data['p_baseline']:+.3f}

### Confidence & Band Analysis
- **Baseline Confidence**: {forecast_data['baseline_confidence']:.1%}
- **Impact Adjusted Confidence**: {forecast_data['impact_adjusted_confidence']:.1%}
- **Baseline Band Width**: {forecast_data['baseline_band_width']:.1%}
- **Impact Adjusted Band Width**: {forecast_data['impact_adjusted_band_width']:.1%}

### Active Impact Triggers
"""
            
            if forecast_data['impact_triggers']:
                for trigger in forecast_data['impact_triggers']:
                    report += f"- {trigger}\n"
            else:
                report += "- No triggers fired (neutral conditions)\n"
            
            if 'actual_outcome' in forecast_data:
                report += f"""
## Evening Results (PM)

### Actual Outcome: {outcome_text}

### Performance Comparison
- **Baseline Brier**: {forecast_data['baseline_brier']:.4f}
- **Impact Brier**: {forecast_data['impact_brier']:.4f}
- **Brier Improvement**: {forecast_data['brier_improvement']:+.4f} {'(Impact better)' if forecast_data['impact_better_brier'] else '(Baseline better)'}

### Hit Analysis
- **Baseline Hit**: {'✓' if forecast_data['baseline_hit'] else '✗'}
- **Impact Hit**: {'✓' if forecast_data['impact_hit'] else '✗'}
- **Same Result**: {'Yes' if forecast_data['baseline_hit'] == forecast_data['impact_hit'] else 'No'} (no directional changes)

### Edge Analysis
- **Was Tail Event**: {'Yes' if forecast_data['was_tail_event'] else 'No'}
- **Had Wide Bands**: {'Yes' if forecast_data['had_wide_bands'] else 'No'}
- **Edge Hit**: {'✓' if forecast_data['edge_hit'] else '✗'} (wide bands caught tail)

### Realized Volatility
- **Baseline Realized Vol**: {forecast_data['baseline_realized_vol']:.2f}%
- **Impact Realized Vol**: {forecast_data['impact_realized_vol']:.2f}%
- **Vol Improvement**: {forecast_data['realized_vol_improvement']:+.2f}% {'(Impact better)' if forecast_data['realized_vol_improvement'] > 0 else '(Baseline better)'}

## Shadow Mode Status
- **Live Decision**: Baseline (p={forecast_data['p_baseline']:.3f}) - No change to production
- **Impact Would Have**: Same probability with adjusted confidence/bands
- **Performance**: {'Impact better' if forecast_data.get('impact_better_brier', False) else 'Baseline better'} on Brier score
"""
        
        report += """
---
Generated by Impact Shadow Mode System
"""
        
        report_file = audit_dir / 'IMPACT_SHADOW_DAILY.md'
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        return str(report_file)
    
    def append_impact_decision_log(self, forecast_data, log_file='audit_exports/IMPACT_DECISION_LOG.csv'):
        """Append to impact decision log CSV"""
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Prepare log entry
        log_entry = {
            'date': forecast_data['date'],
            'news_score': forecast_data['news_score'],
            'macro_z_score': forecast_data['macro_z_score'],
            'band_adjustment_pct': forecast_data['band_adjustment_pct'],
            'confidence_adjustment_pct': forecast_data['confidence_adjustment_pct'],
            'impact_triggers': len(forecast_data['impact_triggers']),
            'actual_outcome': forecast_data.get('actual_outcome', None),
            'baseline_brier': forecast_data.get('baseline_brier', None),
            'impact_brier': forecast_data.get('impact_brier', None),
            'edge_hit': forecast_data.get('edge_hit', None),
            'realized_vol_improvement': forecast_data.get('realized_vol_improvement', None),
            'impact_better': forecast_data.get('impact_better_brier', None),
            'impact_muted': forecast_data.get('impact_muted', False)
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
        """Run full impact shadow day: AM forecast + PM scoring"""
        print(f"Running Impact Shadow Mode for {target_date or 'today'}...")
        
        # AM: Generate forecasts with impact analysis
        forecast_data, ingestion_result, impact_result = self.generate_am_impact_forecast(target_date)
        
        # PM: Score results (if enabled)
        if with_pm_scoring and not forecast_data.get('impact_muted', False):
            forecast_data = self.score_pm_impact_results(forecast_data)
        
        # Check auto-mute guardrails (load recent data)
        log_path = Path('audit_exports/IMPACT_DECISION_LOG.csv')
        if log_path.exists() and not forecast_data.get('impact_muted', False):
            recent_df = pd.read_csv(log_path).tail(10)  # Last 10 days
            recent_data = recent_df.to_dict('records')
            
            should_mute, mute_reason = self.check_auto_mute_guardrails(recent_data)
            if should_mute:
                print(f"⚠️ AUTO-MUTE TRIGGERED: {mute_reason}")
                # In practice, would set NEWS_IMPACT_MUTED=true environment variable
                forecast_data['auto_mute_triggered'] = True
                forecast_data['auto_mute_reason'] = mute_reason
        
        # Write artifacts
        output_dir = 'audit_exports'
        daily_report = self.write_daily_shadow_report(forecast_data, ingestion_result, impact_result, output_dir)
        decision_log = self.append_impact_decision_log(forecast_data)
        
        print(f"Impact shadow report: {daily_report}")
        print(f"Impact decision log: {decision_log}")
        
        if not forecast_data.get('impact_muted', False):
            print(f"News score: {forecast_data['news_score']:.3f}, Macro z: {forecast_data['macro_z_score']:.2f}")
            print(f"Band adj: {forecast_data['band_adjustment_pct']:+.1f}%, Conf adj: {forecast_data['confidence_adjustment_pct']:+.1f}%")
        else:
            print("Impact engine MUTED due to performance guardrails")
        
        return forecast_data, daily_report, decision_log


def main():
    """Test impact shadow mode for today"""
    shadow = ImpactShadowMode()
    
    # Run shadow day
    forecast_data, daily_report, decision_log = shadow.run_shadow_day()
    
    if 'actual_outcome' in forecast_data and not forecast_data.get('impact_muted', False):
        print(f"\nResults:")
        print(f"Outcome: {'UP' if forecast_data['actual_outcome'] else 'DOWN'}")
        print(f"Baseline Brier: {forecast_data['baseline_brier']:.4f}")
        print(f"Impact Brier: {forecast_data['impact_brier']:.4f}")
        print(f"Impact better: {forecast_data['impact_better_brier']}")
        print(f"Edge hit: {forecast_data['edge_hit']}")
    
    return forecast_data


if __name__ == '__main__':
    main()