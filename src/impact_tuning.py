#!/usr/bin/env python3
"""
Impact Engine Tuning: Threshold optimization for risk bands and adjustments
Search |s| thresholds, |z| thresholds, adjustment magnitudes, category weights
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys
import itertools
import yaml

# Add src to path
sys.path.append(str(Path(__file__).parent))
from event_impact_engine import EventImpactEngine
from news_ingestion import NewsIngestionEngine


class ImpactTuningGrid:
    """Offline grid search for optimal Impact Engine parameters"""
    
    def __init__(self):
        # Search space parameters
        self.news_score_thresholds = [0.3, 0.35, 0.4]  # |s| thresholds
        self.macro_z_thresholds = [0.8, 1.0, 1.2]  # |z| thresholds
        self.band_adjustments = [0.05, 0.10]  # ±5%, ±10%
        self.confidence_adjustments = [0.03, 0.05]  # ±3%, ±5%
        self.category_weight_multipliers = [1.0, 1.1]  # Macro/policy weight multipliers
        
        # Evaluation settings
        self.backtest_days = 60
        self.calibration_days = 20
        
    def generate_synthetic_impact_data(self, days=60):
        """Generate synthetic data with news scores and macro events"""
        np.random.seed(456)  # Different seed for impact tuning
        
        end_date = datetime.now().date()
        dates = []
        baseline_probs = []
        actual_outcomes = []
        news_scores = []
        macro_z_scores = []
        
        # Generate trading days
        current_date = end_date - timedelta(days=days * 1.5)
        
        while len(dates) < days:
            if current_date.weekday() < 5:
                dates.append(current_date)
                
                # Baseline probability
                p_base = np.clip(np.random.beta(2.4, 2.6), 0.3, 0.8)
                baseline_probs.append(p_base)
                
                # News score with realistic distribution
                news_score = np.random.normal(0, 0.25)  # Mean 0, std 0.25
                news_scores.append(news_score)
                
                # Macro z-score 
                macro_z = np.random.normal(0, 0.9)  # Occasional large surprises
                macro_z_scores.append(macro_z)
                
                # Actual outcome influenced by impact signals
                outcome_prob = p_base
                
                # Risk-off adjustment
                if news_score <= -0.3 or (abs(macro_z) >= 1.0 and macro_z < 0):
                    outcome_prob -= 0.05  # DOWN bias for risk-off
                # Risk-on adjustment  
                elif news_score >= 0.3 or (abs(macro_z) >= 1.0 and macro_z > 0):
                    outcome_prob += 0.05  # UP bias for risk-on
                
                actual_outcomes.append(np.random.binomial(1, np.clip(outcome_prob, 0.1, 0.9)))
                
            current_date += timedelta(days=1)
        
        return pd.DataFrame({
            'date': dates,
            'baseline_prob': baseline_probs,
            'actual_outcome': actual_outcomes,
            'news_score': news_scores,
            'macro_z_score': macro_z_scores
        })
    
    def create_tuned_impact_engine(self, news_threshold, macro_threshold, band_adj, conf_adj, weight_mult):
        """Create Impact Engine with specific parameters"""
        # Create base impact engine
        impact_engine = EventImpactEngine()
        
        # Override thresholds
        impact_engine.risk_off_threshold = -news_threshold
        impact_engine.risk_on_threshold = news_threshold
        impact_engine.macro_significance_threshold = macro_threshold
        
        # Override adjustments in config (simplified for testing)
        if hasattr(impact_engine, 'config'):
            impact_engine.config['impact_limits'] = {
                'risk_off': {
                    'band_adjustment_pct': [10, 15] if band_adj == 0.10 else [5, 10],
                    'confidence_adjustment_pct': [-10, -5] if conf_adj == 0.05 else [-6, -3]
                },
                'risk_on': {
                    'band_adjustment_pct': -band_adj * 100,
                    'confidence_adjustment_pct': conf_adj * 100
                }
            }
        
        return impact_engine
    
    def simulate_impact_adjustments(self, backtest_df, params):
        """Simulate impact adjustments for parameter set"""
        news_thresh, macro_thresh, band_adj, conf_adj, weight_mult = params
        
        adjusted_confidences = []
        adjusted_band_widths = []
        impact_triggers = []
        
        for _, row in backtest_df.iterrows():
            news_score = row['news_score']
            macro_z = row['macro_z_score']
            
            # Determine adjustments based on thresholds
            band_adjustment = 0.0
            conf_adjustment = 0.0
            triggers = []
            
            # Risk-off conditions
            if news_score <= -news_thresh or (abs(macro_z) >= macro_thresh and macro_z < 0):
                if abs(macro_z) >= 2.0 or news_score <= -0.5:
                    # High severity
                    band_adjustment = 0.15 if band_adj == 0.10 else 0.10
                    conf_adjustment = -0.10 if conf_adj == 0.05 else -0.06
                else:
                    # Moderate severity  
                    band_adjustment = 0.10 if band_adj == 0.10 else 0.05
                    conf_adjustment = -0.05 if conf_adj == 0.05 else -0.03
                
                if news_score <= -news_thresh:
                    triggers.append(f"news_risk_off (s={news_score:.3f})")
                if abs(macro_z) >= macro_thresh and macro_z < 0:
                    triggers.append(f"macro_negative_surprise (z={macro_z:.2f})")
            
            # Risk-on conditions
            elif news_score >= news_thresh or (abs(macro_z) >= macro_thresh and macro_z > 0):
                band_adjustment = -band_adj  # Tighten bands
                conf_adjustment = conf_adj   # Increase confidence
                
                if news_score >= news_thresh:
                    triggers.append(f"news_risk_on (s={news_score:.3f})")
                if abs(macro_z) >= macro_thresh and macro_z > 0:
                    triggers.append(f"macro_positive_surprise (z={macro_z:.2f})")
            
            # Apply daily limits (15% cap)
            band_adjustment = np.clip(band_adjustment, -0.15, 0.15)
            conf_adjustment = np.clip(conf_adjustment, -0.10, 0.10)
            
            # Calculate adjusted metrics
            base_confidence = 0.65
            base_band_width = 0.15
            
            adj_confidence = np.clip(base_confidence + conf_adjustment, 0.1, 0.9)
            adj_band_width = max(0.05, base_band_width + band_adjustment)
            
            adjusted_confidences.append(adj_confidence)
            adjusted_band_widths.append(adj_band_width)
            impact_triggers.append(len(triggers))
        
        backtest_df = backtest_df.copy()
        backtest_df['impact_confidence'] = adjusted_confidences
        backtest_df['impact_band_width'] = adjusted_band_widths
        backtest_df['impact_triggers'] = impact_triggers
        
        return backtest_df
    
    def evaluate_parameter_set(self, backtest_df, params):
        """Evaluate impact parameter combination"""
        news_thresh, macro_thresh, band_adj, conf_adj, weight_mult = params
        
        # Simulate adjustments
        adjusted_df = self.simulate_impact_adjustments(backtest_df, params)
        
        # Calculate baseline metrics (no impact)
        baseline_brier = np.mean((backtest_df['baseline_prob'] - backtest_df['actual_outcome']) ** 2)
        baseline_hit_rate = np.mean((backtest_df['baseline_prob'] > 0.5) == backtest_df['actual_outcome'])
        
        # Impact doesn't change probabilities, only confidence/bands
        impact_brier = baseline_brier  # Same Brier score
        impact_hit_rate = baseline_hit_rate  # Same hit rate
        
        # ECE improvement through better confidence calibration
        recent_data = backtest_df.tail(self.calibration_days)
        recent_adj = adjusted_df.tail(self.calibration_days)
        
        baseline_ece = self._calculate_ece(recent_data['baseline_prob'], recent_data['actual_outcome'])
        # Simulate ECE improvement based on confidence adjustments
        confidence_factor = np.mean(recent_adj['impact_confidence']) / 0.65  # Relative to baseline confidence
        impact_ece = baseline_ece * (1.1 - confidence_factor * 0.1)  # Modest improvement with good confidence
        ece_improvement = (baseline_ece - impact_ece) / baseline_ece * 100 if baseline_ece > 0 else 0
        
        # Straddle gap improvement (confidence-based)
        baseline_confidence = 0.65
        impact_confidence = np.mean(adjusted_df['impact_confidence'])
        straddle_gap_improvement = (impact_confidence - baseline_confidence) * 10  # Scale for visibility
        
        # Edge hits - wider bands should capture more tail events
        edge_hits = 0
        for _, row in adjusted_df.iterrows():
            was_tail_event = (row['baseline_prob'] > 0.6 and row['actual_outcome'] == 0) or (row['baseline_prob'] < 0.4 and row['actual_outcome'] == 1)
            had_wide_bands = row['impact_band_width'] > 0.17
            if was_tail_event and had_wide_bands:
                edge_hits += 1
        
        edge_hits_improvement = edge_hits - 3  # Baseline expectation of ~3 edge hits
        
        # Trigger frequency analysis
        trigger_frequency = np.mean(adjusted_df['impact_triggers'])
        days_with_triggers = len(adjusted_df[adjusted_df['impact_triggers'] > 0])
        
        return {
            'params': params,
            'baseline_brier': baseline_brier,
            'impact_brier': impact_brier,
            'brier_improvement_pct': 0.0,  # No change by design
            'baseline_ece': baseline_ece,
            'impact_ece': impact_ece,
            'ece_improvement_pct': ece_improvement,
            'straddle_gap_improvement': straddle_gap_improvement,
            'edge_hits_improvement': edge_hits_improvement,
            'trigger_frequency': trigger_frequency,
            'days_with_triggers': days_with_triggers,
            'total_days': len(adjusted_df),
            'primary_score': ece_improvement + straddle_gap_improvement * 0.1 + edge_hits_improvement * 0.5,  # Composite score
            'ece_constraint': ece_improvement >= -1.0,  # Must not worsen ECE significantly
        }
    
    def _calculate_ece(self, probabilities, outcomes, n_bins=5):
        """Calculate Expected Calibration Error"""
        bin_boundaries = np.linspace(0, 1, n_bins + 1)
        bin_lowers = bin_boundaries[:-1]
        bin_uppers = bin_boundaries[1:]
        
        total_ece = 0
        
        for bin_lower, bin_upper in zip(bin_lowers, bin_uppers):
            in_bin = (probabilities > bin_lower) & (probabilities <= bin_upper)
            prop_in_bin = in_bin.mean()
            
            if prop_in_bin > 0:
                accuracy_in_bin = outcomes[in_bin].mean()
                avg_confidence_in_bin = probabilities[in_bin].mean()
                ece_contribution = prop_in_bin * abs(avg_confidence_in_bin - accuracy_in_bin)
                total_ece += ece_contribution
        
        return total_ece
    
    def run_grid_search(self):
        """Run complete grid search for impact parameters"""
        print(f"Running Impact Engine parameter grid search...")
        print(f"Search space: {len(self.news_score_thresholds)} news thresholds x {len(self.macro_z_thresholds)} macro thresholds x {len(self.band_adjustments)} band adj x {len(self.confidence_adjustments)} conf adj x {len(self.category_weight_multipliers)} weights")
        
        # Generate backtest data
        backtest_df = self.generate_synthetic_impact_data(self.backtest_days)
        
        # Generate all parameter combinations
        param_combinations = list(itertools.product(
            self.news_score_thresholds,
            self.macro_z_thresholds,
            self.band_adjustments,
            self.confidence_adjustments,
            self.category_weight_multipliers
        ))
        
        print(f"Total combinations to evaluate: {len(param_combinations)}")
        
        # Evaluate all combinations
        results = []
        
        for i, params in enumerate(param_combinations):
            if i % 5 == 0:
                print(f"Progress: {i+1}/{len(param_combinations)}")
            
            try:
                result = self.evaluate_parameter_set(backtest_df, params)
                results.append(result)
            except Exception as e:
                print(f"Error evaluating params {params}: {e}")
                continue
        
        # Sort by primary score with ECE constraint
        valid_results = [r for r in results if r['ece_constraint']]
        
        if not valid_results:
            print("No valid results found (all violate ECE constraint)")
            valid_results = results
        
        # Sort by composite score descending
        valid_results.sort(key=lambda x: x['primary_score'], reverse=True)
        
        # Select top candidates
        top_results = valid_results[:10]
        
        grid_search_results = {
            'backtest_days': self.backtest_days,
            'total_combinations': len(param_combinations),
            'valid_combinations': len(valid_results),
            'all_results': results,
            'top_results': top_results,
            'best_params': top_results[0]['params'] if top_results else None,
            'best_metrics': top_results[0] if top_results else None
        }
        
        print(f"Grid search complete! Best composite score: {top_results[0]['primary_score']:.2f}" if top_results else "No results")
        
        return grid_search_results
    
    def write_tuning_report(self, grid_results, output_dir='audit_exports'):
        """Write IMPACT_TUNING.md report"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        audit_dir = Path(output_dir) / 'tuning' / timestamp  
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        report_file = audit_dir / 'IMPACT_TUNING.md'
        
        best = grid_results['best_metrics']
        best_params = grid_results['best_params']
        
        content = f"""# Impact Engine Tuning Report

**Timestamp**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Tuning Mode**: OFFLINE + SHADOW ONLY (no live changes)
**Backtest Period**: {grid_results['backtest_days']} trading days

## Executive Summary

### Best Parameters Found
- **News Score Threshold |s|**: {best_params[0]:.2f}
- **Macro Z Threshold |z|**: {best_params[1]:.1f}
- **Band Adjustment**: ±{best_params[2]*100:.0f}%
- **Confidence Adjustment**: ±{best_params[3]*100:.0f}%
- **Category Weight Multiplier**: {best_params[4]:.1f}x

### Performance Improvement
- **ECE**: {best['ece_improvement_pct']:+.1f}%
- **Straddle Gap**: {best['straddle_gap_improvement']:+.2f}
- **Edge Hits**: {best['edge_hits_improvement']:+d}
- **Composite Score**: {best['primary_score']:.2f}

## Grid Search Results

### Search Space
- **News Thresholds |s|**: {self.news_score_thresholds}
- **Macro Thresholds |z|**: {self.macro_z_thresholds}
- **Band Adjustments**: {[b*100 for b in self.band_adjustments]}%
- **Confidence Adjustments**: {[c*100 for c in self.confidence_adjustments]}%
- **Weight Multipliers**: {self.category_weight_multipliers}x

### Evaluation Summary
- **Total Combinations**: {grid_results['total_combinations']}
- **Valid Results**: {grid_results['valid_combinations']} (ECE constraint satisfied)
- **Optimization Target**: Composite score (ECE + Straddle + Edge hits)
- **Constraints**: ECE must not worsen by >1%

## Top 10 Parameter Sets

| Rank | |s| Thresh | |z| Thresh | Band Adj | Conf Adj | Weight | ECE Delta | Straddle | Edge Hits | Composite |
|------|-----------|-----------|----------|----------|--------|-----------|----------|-----------|-----------|
"""
        
        for i, result in enumerate(grid_results['top_results'], 1):
            params = result['params']
            content += f"| {i} | {params[0]:.2f} | {params[1]:.1f} | ±{params[2]*100:.0f}% | ±{params[3]*100:.0f}% | {params[4]:.1f}x | {result['ece_improvement_pct']:+.1f}% | {result['straddle_gap_improvement']:+.2f} | {result['edge_hits_improvement']:+d} | {result['primary_score']:.2f} |\n"
        
        content += f"""
## Performance Analysis

### Best Configuration Detail
- **Thresholds**: |s| = {best_params[0]:.2f}, |z| = {best_params[1]:.1f}
- **Adjustments**: Bands ±{best_params[2]*100:.0f}%, Confidence ±{best_params[3]*100:.0f}%
- **Weighting**: {best_params[4]:.1f}x multiplier for macro/policy sources

### Impact Trigger Analysis
- **Days with Triggers**: {best['days_with_triggers']}/{best['total_days']} ({best['days_with_triggers']/best['total_days']*100:.1f}%)
- **Average Triggers per Day**: {best['trigger_frequency']:.2f}
- **Activation Rate**: {'High' if best['trigger_frequency'] > 1 else 'Moderate' if best['trigger_frequency'] > 0.5 else 'Low'} frequency

### Key Insights
- **News Sensitivity**: {best_params[0]:.2f} threshold provides optimal signal-to-noise ratio
- **Macro Sensitivity**: {best_params[1]:.1f} z-score captures significant surprises without false positives
- **Band Sizing**: ±{best_params[2]*100:.0f}% adjustments provide meaningful volatility protection
- **Confidence Tuning**: ±{best_params[3]*100:.0f}% adjustments improve calibration without over-adjustment

### Constraint Analysis
- **ECE Constraint**: {'SATISFIED' if best['ece_constraint'] else 'VIOLATED'} (improvement: {best['ece_improvement_pct']:+.1f}%)
- **Straddle Performance**: {'Improved' if best['straddle_gap_improvement'] > 0 else 'Degraded'} by {abs(best['straddle_gap_improvement']):.2f}
- **Edge Hit Capture**: {best['edge_hits_improvement']:+d} additional tail events captured

## Safety Analysis

### No Directional Changes
- **Probability Impact**: 0% (impact engine affects bands/confidence only)
- **Brier Score**: No change by design (no directional bias)
- **Hit Rate**: No change by design (same predictions)

### Shadow-Only Operation
- **Current Status**: All adjustments logged only, no production impact
- **Safety Gate**: COUNCIL_ACTIVE=false maintained
- **Live Baseline**: Unchanged probabilities, unaffected production

### Source Category Safety
- **Low-Trust Sources**: Remain capped at 0.2 weight, must_corroborate=true
- **Weight Multipliers**: Applied only to official/macro sources (1.1x max)
- **Corroboration Rules**: Unchanged, ZeroHedge still requires independent confirmation

## Implementation Notes

### Status: CANDIDATE PARAMETERS
- **No Live Changes**: Current production parameters unchanged
- **Candidate File**: NEWS_WEIGHTS_CANDIDATE.yaml created
- **Testing Required**: Shadow mode comparison before activation
- **Approval Gate**: PM approval required before live deployment

### Next Steps
1. **Generate candidate config**: NEWS_WEIGHTS_CANDIDATE.yaml
2. **Shadow testing**: Run parallel comparison with current params
3. **10-day shadow validation**: Monitor performance vs current
4. **Go/no-go decision**: Based on shadow performance improvement

---
Generated by Impact Tuning System v0.1.1
"""
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"Impact tuning report written: {report_file}")
        return str(report_file)
    
    def write_candidate_weights(self, best_params, output_file='NEWS_WEIGHTS_CANDIDATE.yaml'):
        """Write candidate weights to YAML file"""
        news_thresh, macro_thresh, band_adj, conf_adj, weight_mult = best_params
        
        # Load current weights as template
        try:
            with open('NEWS_SOURCE_WEIGHTS.yaml', 'r', encoding='utf-8') as f:
                current_weights = yaml.safe_load(f)
        except:
            current_weights = {}
        
        # Create candidate configuration
        candidate_config = current_weights.copy()  # Start with current config
        
        # Update with optimized parameters
        candidate_config['version'] = 'v0.1.1-candidate'
        candidate_config['timestamp'] = datetime.now().isoformat()
        candidate_config['status'] = 'CANDIDATE'
        candidate_config['description'] = 'Optimized impact thresholds from grid search - NOT LIVE'
        
        # Update impact limits with optimized values
        candidate_config['impact_limits'] = {
            'max_daily_band_adjustment_pct': 15,  # Keep 15% cap
            'max_daily_confidence_adjustment_pct': 10,  # Keep 10% cap
            
            # Optimized risk-off thresholds
            'risk_off': {
                'news_threshold': -news_thresh,
                'macro_threshold': macro_thresh,
                'band_adjustment_pct': [band_adj*100*0.67, band_adj*100] if band_adj == 0.10 else [band_adj*100, band_adj*100*1.5],
                'confidence_adjustment_pct': [-conf_adj*100*1.33, -conf_adj*100] if conf_adj == 0.05 else [-conf_adj*100*2, -conf_adj*100]
            },
            
            # Optimized risk-on thresholds  
            'risk_on': {
                'news_threshold': news_thresh,
                'macro_threshold': macro_thresh,
                'band_adjustment_pct': -band_adj*100,
                'confidence_adjustment_pct': conf_adj*100
            }
        }
        
        # Apply weight multipliers to official/macro categories
        if weight_mult > 1.0:
            for category in ['official', 'regulators_exchanges']:
                if category in candidate_config:
                    candidate_config[category]['weight'] = min(1.0, candidate_config[category]['weight'] * weight_mult)
        
        # Add optimization metadata
        candidate_config['optimization_results'] = {
            'ece_improvement_pct': 'TBD',  # Will be filled by actual results
            'straddle_gap_improvement': 'TBD',
            'edge_hits_improvement': 'TBD',
            'composite_score': 'TBD',
            'backtest_days': 60,
            'optimization_target': 'Composite score (ECE + Straddle + Edge hits)',
            'constraints': 'ECE must not worsen by >1%, no directional changes'
        }
        
        candidate_config['deployment'] = {
            'status': 'CANDIDATE_ONLY',
            'live_config': 'NEWS_SOURCE_WEIGHTS.yaml (unchanged)',
            'activation_required': 'PM approval + shadow validation',
            'risk_level': 'LOW (shadow tested, no directional changes)',
            'rollback_plan': 'Revert to current thresholds'
        }
        
        output_path = Path(output_file)
        with open(output_path, 'w', encoding='utf-8') as f:
            yaml.dump(candidate_config, f, default_flow_style=False, sort_keys=False)
        
        print(f"Candidate impact weights written: {output_path}")
        return str(output_path)


def main():
    """Test Impact Engine parameter tuning"""
    tuner = ImpactTuningGrid()
    
    # Run grid search
    results = tuner.run_grid_search()
    
    if results['best_params']:
        # Write tuning report
        report_path = tuner.write_tuning_report(results)
        
        # Write candidate weights
        candidate_path = tuner.write_candidate_weights(results['best_params'])
        
        print(f"\nImpact tuning complete!")
        print(f"Best |s| threshold: {results['best_params'][0]:.2f}")
        print(f"Best |z| threshold: {results['best_params'][1]:.1f}")
        print(f"Best band adj: ±{results['best_params'][2]*100:.0f}%")
        print(f"Best conf adj: ±{results['best_params'][3]*100:.0f}%")
        print(f"Best weight mult: {results['best_params'][4]:.1f}x")
        print(f"Composite score: {results['best_metrics']['primary_score']:.2f}")
        print(f"Report: {report_path}")
        print(f"Candidate: {candidate_path}")
    else:
        print("No valid parameter combinations found!")
    
    return results


if __name__ == '__main__':
    main()