#!/usr/bin/env python3
"""
Zen Council v0.1: Feedback loop to adjust baseline forecasts
Applies calibration + miss-tag context + volatility guards
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path


class ZenCouncil:
    """Zen Council feedback system for forecast adjustment"""
    
    def __init__(self):
        self.alpha_prior = 2.0  # Beta-binomial prior
        self.beta_prior = 2.0
        self.blend_lambda = 0.7  # Blend weight for baseline
        
    def get_calibration_data(self, lookback_days=20):
        """Get hits/misses from last N days"""
        # In production, this would read from forecast results table
        # For now, simulate realistic hit/miss data
        np.random.seed(42)  # Deterministic for testing
        
        # Simulate 20 days of results with ~65% hit rate
        results = np.random.binomial(1, 0.65, lookback_days)
        hits = int(results.sum())
        misses = lookback_days - hits
        
        return hits, misses, lookback_days
    
    def compute_calibration_prob(self, hits, misses):
        """Beta-binomial calibration: p_cal = (H + α₀) / (H + M + α₀ + β₀)"""
        numerator = hits + self.alpha_prior
        denominator = hits + misses + self.alpha_prior + self.beta_prior
        p_cal = numerator / denominator
        return p_cal
    
    def get_miss_tag_rates(self, lookback_days=7):
        """Get miss tag rates for context adjustment"""
        # In production, would read from miss_tags table
        # Simulate tag counts for last 7 days
        np.random.seed(123)
        
        tag_counts = {
            'NEWS_EVENT': np.random.poisson(0.5, lookback_days).sum(),
            'VOL_SHIFT': np.random.poisson(0.8, lookback_days).sum(), 
            'TECH_BREAK': np.random.poisson(0.3, lookback_days).sum(),
            'DRIFT_DAY': np.random.poisson(1.2, lookback_days).sum()
        }
        
        return tag_counts
    
    def compute_miss_tag_adjustment(self, tag_counts, hot_threshold=2, lookback_days=5):
        """Compute adjustment based on miss tag patterns"""
        # Check which tags are "hot" (≥2 occurrences in last 5 sessions)
        vol_shift_hot = tag_counts['VOL_SHIFT'] >= hot_threshold
        news_hot = tag_counts['NEWS_EVENT'] >= hot_threshold  
        drift_hot = tag_counts['DRIFT_DAY'] >= hot_threshold
        
        # Apply adjustments: A = 1 - 0.1*vol_hot - 0.1*news_hot + 0.05*drift_hot
        adjustment = 1.0
        adjustment -= 0.1 if vol_shift_hot else 0.0
        adjustment -= 0.1 if news_hot else 0.0
        adjustment += 0.05 if drift_hot else 0.0
        
        active_rules = []
        if vol_shift_hot:
            active_rules.append("VOL_SHIFT hot → confidence -10%")
        if news_hot:
            active_rules.append("NEWS_EVENT hot → confidence -10%")  
        if drift_hot:
            active_rules.append("DRIFT_DAY hot → confidence +5%")
            
        return adjustment, active_rules
    
    def get_volatility_metrics(self):
        """Get VIX/VVIX deltas for volatility guard"""
        # In production, would read from market data
        # Simulate realistic VIX movement
        np.random.seed(456)
        delta_vix_pre = np.random.normal(0, 1.2)  # Typical VIX daily change
        vvix_increase = max(0, np.random.normal(2, 3))  # VVIX increase
        
        return delta_vix_pre, vvix_increase
    
    def apply_volatility_guard(self, delta_vix, vvix_increase):
        """Apply volatility-based adjustments"""
        vol_guard_active = abs(delta_vix) >= 1.5 or vvix_increase >= 5.0
        
        band_widen_pct = 15.0 if vol_guard_active else 0.0
        conf_reduction_pct = 10.0 if vol_guard_active else 0.0
        
        rules = []
        if vol_guard_active:
            if abs(delta_vix) >= 1.5:
                rules.append(f"VIX Δ{delta_vix:.1f} ≥1.5 → bands +15%, confidence -10%")
            if vvix_increase >= 5.0:
                rules.append(f"VVIX ↑{vvix_increase:.1f} ≥5 → bands +15%, confidence -10%")
        
        return band_widen_pct, conf_reduction_pct, rules
    
    def adjust_forecast(self, p_baseline, symbol="^GSPC"):
        """Main Zen Council adjustment pipeline"""
        # Step 1: Calibration
        hits, misses, total_days = self.get_calibration_data()
        p_cal = self.compute_calibration_prob(hits, misses)
        
        # Step 2: Blend with baseline
        p_1 = self.blend_lambda * p_baseline + (1 - self.blend_lambda) * p_cal
        
        # Step 3: Miss tag adjustment
        tag_counts = self.get_miss_tag_rates()
        miss_tag_adj, miss_tag_rules = self.compute_miss_tag_adjustment(tag_counts)
        
        # Step 4: Volatility guard
        delta_vix, vvix_increase = self.get_volatility_metrics()
        band_widen_pct, conf_reduction_pct, vol_rules = self.apply_volatility_guard(delta_vix, vvix_increase)
        
        # Step 5: Final probability with clipping
        p_final = np.clip(miss_tag_adj * p_1, 0.05, 0.95)
        
        # Compile results
        result = {
            'symbol': symbol,
            'p_baseline': p_baseline,
            'p_calibrated': p_cal,
            'p_blended': p_1, 
            'p_final': p_final,
            'calibration_data': {'hits': hits, 'misses': misses, 'total_days': total_days},
            'miss_tag_counts': tag_counts,
            'miss_tag_adjustment': miss_tag_adj,
            'volatility_metrics': {'delta_vix': delta_vix, 'vvix_increase': vvix_increase},
            'band_widen_pct': band_widen_pct,
            'conf_reduction_pct': conf_reduction_pct,
            'active_rules': miss_tag_rules + vol_rules,
            'drivers': ['calibration', 'miss_tags', 'vol_guard']
        }
        
        return result
    
    def write_explanation(self, result, output_dir):
        """Write ZEN_COUNCIL_EXPLAIN.md artifact"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        audit_dir = Path(output_dir) / 'daily' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        explanation = f"""# Zen Council v0.1 Explanation

**Timestamp**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Symbol**: {result['symbol']}

## Probability Flow
- **p0 (baseline)**: {result['p_baseline']:.3f}
- **p_cal (calibrated)**: {result['p_calibrated']:.3f}  
- **p1 (blended)**: {result['p_blended']:.3f}
- **p_final (adjusted)**: {result['p_final']:.3f}

## Calibration Analysis
**Beta-Binomial Prior**: alpha0=2, beta0=2
**Recent Performance** (last {result['calibration_data']['total_days']} days):
- Hits (H): {result['calibration_data']['hits']}
- Misses (M): {result['calibration_data']['misses']}
- Hit Rate: {result['calibration_data']['hits']/result['calibration_data']['total_days']:.1%}

**Formula**: p_cal = (H + alpha0) / (H + M + alpha0 + beta0) = {result['p_calibrated']:.3f}

## Blend Calculation  
**lambda = 0.7** (baseline weight)
p1 = lambda*p0 + (1-lambda)*p_cal = 0.7x{result['p_baseline']:.3f} + 0.3x{result['p_calibrated']:.3f} = {result['p_blended']:.3f}

## Miss Tag Context (7-day analysis)
"""
        
        for tag, count in result['miss_tag_counts'].items():
            explanation += f"- **{tag}**: {count} occurrences\n"
        
        explanation += f"\n**Miss Tag Adjustment**: {result['miss_tag_adjustment']:.3f}\n\n"
        
        if result['active_rules']:
            explanation += "## Active Rules\n"
            for rule in result['active_rules']:
                explanation += f"- {rule}\n"
        else:
            explanation += "## Active Rules\n- None (all thresholds below triggers)\n"
        
        explanation += f"""
## Volatility Guard
- **VIX Pre-market Delta**: {result['volatility_metrics']['delta_vix']:.1f}
- **VVIX Increase**: {result['volatility_metrics']['vvix_increase']:.1f}
- **Band Widening**: +{result['band_widen_pct']:.0f}%
- **Confidence Reduction**: -{result['conf_reduction_pct']:.0f}%

## Final Calculation
p_final = clip(A*p1, 0.05, 0.95) = clip({result['miss_tag_adjustment']:.3f}x{result['p_blended']:.3f}, 0.05, 0.95) = {result['p_final']:.3f}

## Drivers Applied
{', '.join(result['drivers'])}

---
Generated by Zen Council v0.1
"""
        
        explain_file = audit_dir / 'ZEN_COUNCIL_EXPLAIN.md'
        with open(explain_file, 'w', encoding='utf-8') as f:
            f.write(explanation)
        
        print(f"Zen Council explanation: {explain_file}")
        return str(explain_file)


def main():
    """Test run of Zen Council"""
    council = ZenCouncil()
    
    # Test with sample baseline probability
    p_baseline = 0.58  # Sample S&P 500 up probability
    
    result = council.adjust_forecast(p_baseline)
    
    # Write explanation
    output_dir = 'audit_exports'
    explain_path = council.write_explanation(result, output_dir)
    
    print(f"Baseline: {p_baseline:.3f} -> Final: {result['p_final']:.3f}")
    print(f"Active rules: {len(result['active_rules'])}")
    
    return result


if __name__ == '__main__':
    main()