#!/usr/bin/env python3
"""
Event-Impact Engine v0.1
Computes news score, macro surprise z-scores, and SHADOW-SAFE band/confidence adjustments
"""

import os
import yaml
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys
import math

# Add src to path
sys.path.append(str(Path(__file__).parent))
from news_ingestion import NewsIngestionEngine


class EventImpactEngine:
    """Event-Impact Engine v0.1 with math + guardrails"""
    
    def __init__(self, weights_path="NEWS_SOURCE_WEIGHTS.yaml"):
        self.weights_path = Path(weights_path)
        self.config = self._load_config()
        
        # Time decay parameters
        self.news_tau_hours = self.config.get('time_decay', {}).get('news_tau_hours', 3)
        self.policy_tau_days = self.config.get('time_decay', {}).get('policy_tau_days', 1)
        
        # Impact limits
        self.max_daily_band_pct = self.config.get('impact_limits', {}).get('max_daily_band_adjustment_pct', 15)
        self.max_daily_conf_pct = self.config.get('impact_limits', {}).get('max_daily_confidence_adjustment_pct', 10)
        
        # Thresholds
        self.risk_off_threshold = -0.3
        self.risk_on_threshold = 0.3
        self.macro_significance_threshold = 1.0  # |z| >= 1.0
        
    def _load_config(self):
        """Load configuration from weights YAML"""
        if not self.weights_path.exists():
            return {}
        
        with open(self.weights_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _get_source_weight(self, source_domain):
        """Get weight for source domain from config"""
        source_domain = source_domain.lower().replace('www.', '')
        
        for category, config in self.config.items():
            if isinstance(config, dict) and 'sources' in config:
                if source_domain in config['sources']:
                    weight = config['weight']
                    # Apply weight cap for low-trust sources
                    if config.get('weight_cap'):
                        weight = min(weight, config['weight_cap'])
                    return weight, category
        
        return 0.0, 'unknown'
    
    def _compute_time_decay(self, timestamp, tau_hours=3):
        """Compute time decay factor: exp(-t/τ)"""
        hours_ago = (datetime.now() - timestamp).total_seconds() / 3600
        return math.exp(-hours_ago / tau_hours)
    
    def _get_severity_multiplier(self, severity):
        """Get severity multiplier from config"""
        multipliers = self.config.get('severity_multipliers', {
            'HIGH': 1.0,
            'MEDIUM': 0.6,
            'LOW': 0.3
        })
        return multipliers.get(severity, 0.3)
    
    def compute_news_score(self, news_items, lookback_hours=3):
        """Compute weighted news score s with time decay"""
        if not news_items:
            return {
                'score': 0.0,
                'components': [],
                'total_weight': 0.0,
                'items_count': 0,
                'muted_count': 0
            }
        
        current_time = datetime.now()
        cutoff_time = current_time - timedelta(hours=lookback_hours)
        
        # Filter to recent items
        recent_items = [
            item for item in news_items 
            if item['timestamp'] >= cutoff_time
        ]
        
        score_components = []
        total_weighted_score = 0.0
        total_weight = 0.0
        muted_count = 0
        
        for item in recent_items:
            # Skip muted items (uncorroborated low-trust sources)
            if item.get('status') == 'MUTED':
                muted_count += 1
                continue
            
            # Get source weight
            weight, category = self._get_source_weight(item['source'])
            if weight == 0:
                continue
            
            # Get severity multiplier
            severity_mult = self._get_severity_multiplier(item['severity'])
            
            # Compute time decay
            time_decay = self._compute_time_decay(
                item['timestamp'], 
                tau_hours=self.policy_tau_days * 24 if category == 'official' else self.news_tau_hours
            )
            
            # Compute sentiment score (simplified - in practice would use NLP)
            # For simulation, assign scores based on keywords
            headline = item['headline'].lower()
            base_sentiment = 0.0
            
            # Risk-off keywords
            risk_off_keywords = ['volatility', 'uncertainty', 'concern', 'decline', 'fall', 'crisis', 'negative']
            # Risk-on keywords  
            risk_on_keywords = ['growth', 'positive', 'rise', 'gain', 'strong', 'recovery', 'optimism']
            
            risk_off_score = sum(1 for word in risk_off_keywords if word in headline)
            risk_on_score = sum(1 for word in risk_on_keywords if word in headline)
            
            if risk_off_score > risk_on_score:
                base_sentiment = -0.5  # Risk-off
            elif risk_on_score > risk_off_score:
                base_sentiment = 0.5   # Risk-on
            # else neutral (0.0)
            
            # Apply weights and time decay
            item_score = base_sentiment * weight * severity_mult * time_decay
            total_weighted_score += item_score
            total_weight += weight * severity_mult * time_decay
            
            score_components.append({
                'source': item['source'],
                'category': category,
                'headline': item['headline'][:100] + '...' if len(item['headline']) > 100 else item['headline'],
                'base_sentiment': base_sentiment,
                'weight': weight,
                'severity_mult': severity_mult,
                'time_decay': time_decay,
                'item_score': item_score
            })
        
        # Normalize by total weight to get final score
        final_score = total_weighted_score / total_weight if total_weight > 0 else 0.0
        
        return {
            'score': final_score,
            'components': score_components,
            'total_weight': total_weight,
            'items_count': len(recent_items),
            'muted_count': muted_count,
            'lookback_hours': lookback_hours
        }
    
    def compute_macro_surprise_impact(self, macro_events):
        """Compute aggregate macro surprise z-score"""
        if not macro_events:
            return {
                'aggregate_z': 0.0,
                'high_impact_events': [],
                'event_count': 0
            }
        
        z_scores = []
        high_impact_events = []
        
        for event in macro_events:
            if 'z_score' in event:
                z_score = event['z_score']
                z_scores.append(z_score)
                
                if abs(z_score) >= self.macro_significance_threshold:
                    high_impact_events.append({
                        'event': event['event'],
                        'z_score': z_score,
                        'severity': event['severity'],
                        'surprise_direction': event.get('surprise_direction', 'neutral')
                    })
        
        # Aggregate z-score (simple average for now)
        aggregate_z = np.mean(z_scores) if z_scores else 0.0
        
        return {
            'aggregate_z': aggregate_z,
            'high_impact_events': high_impact_events,
            'event_count': len(macro_events),
            'individual_z_scores': z_scores
        }
    
    def compute_shadow_adjustments(self, news_score, macro_surprise_z):
        """Compute SHADOW-SAFE band and confidence adjustments"""
        adjustments = {
            'band_adjustment_pct': 0.0,
            'confidence_adjustment_pct': 0.0,
            'triggers': [],
            'reasoning': []
        }
        
        # Risk-off conditions: s <= -0.3 OR |z| >= 1 (negative surprise)
        risk_off_triggered = (
            news_score <= self.risk_off_threshold or 
            (abs(macro_surprise_z) >= self.macro_significance_threshold and macro_surprise_z < 0)
        )
        
        # Risk-on conditions: s >= +0.3 OR positive macro surprise
        risk_on_triggered = (
            news_score >= self.risk_on_threshold or
            (abs(macro_surprise_z) >= self.macro_significance_threshold and macro_surprise_z > 0)
        )
        
        if risk_off_triggered:
            # Risk-off: widen bands +10-15%, reduce confidence -5-10%
            risk_off_config = self.config.get('impact_limits', {}).get('risk_off', {})
            
            band_range = risk_off_config.get('band_adjustment_pct', [10, 15])
            conf_range = risk_off_config.get('confidence_adjustment_pct', [-10, -5])
            
            # Use severity to determine within range
            if abs(macro_surprise_z) >= 2.0 or news_score <= -0.5:
                # High severity
                adjustments['band_adjustment_pct'] = band_range[1] if isinstance(band_range, list) else band_range
                adjustments['confidence_adjustment_pct'] = conf_range[0] if isinstance(conf_range, list) else conf_range
            else:
                # Moderate severity
                adjustments['band_adjustment_pct'] = band_range[0] if isinstance(band_range, list) else band_range
                adjustments['confidence_adjustment_pct'] = conf_range[1] if isinstance(conf_range, list) else conf_range
            
            if news_score <= self.risk_off_threshold:
                adjustments['triggers'].append(f"news_risk_off (s={news_score:.3f})")
                adjustments['reasoning'].append(f"Risk-off news sentiment detected (s={news_score:.3f} ≤ {self.risk_off_threshold})")
            
            if abs(macro_surprise_z) >= self.macro_significance_threshold and macro_surprise_z < 0:
                adjustments['triggers'].append(f"macro_negative_surprise (z={macro_surprise_z:.2f})")
                adjustments['reasoning'].append(f"Significant negative macro surprise (z={macro_surprise_z:.2f})")
        
        elif risk_on_triggered:
            # Risk-on: tighten bands -5%, increase confidence +5%
            risk_on_config = self.config.get('impact_limits', {}).get('risk_on', {})
            
            adjustments['band_adjustment_pct'] = risk_on_config.get('band_adjustment_pct', -5)
            adjustments['confidence_adjustment_pct'] = risk_on_config.get('confidence_adjustment_pct', 5)
            
            if news_score >= self.risk_on_threshold:
                adjustments['triggers'].append(f"news_risk_on (s={news_score:.3f})")
                adjustments['reasoning'].append(f"Risk-on news sentiment detected (s={news_score:.3f} ≥ {self.risk_on_threshold})")
            
            if abs(macro_surprise_z) >= self.macro_significance_threshold and macro_surprise_z > 0:
                adjustments['triggers'].append(f"macro_positive_surprise (z={macro_surprise_z:.2f})")
                adjustments['reasoning'].append(f"Significant positive macro surprise (z={macro_surprise_z:.2f})")
        
        # Apply daily limits
        adjustments['band_adjustment_pct'] = np.clip(
            adjustments['band_adjustment_pct'], 
            -self.max_daily_band_pct, 
            self.max_daily_band_pct
        )
        adjustments['confidence_adjustment_pct'] = np.clip(
            adjustments['confidence_adjustment_pct'],
            -self.max_daily_conf_pct,
            self.max_daily_conf_pct
        )
        
        return adjustments
    
    def run_impact_analysis(self, news_items, macro_events):
        """Run complete impact analysis"""
        print("Running Event-Impact Engine v0.1...")
        
        # Compute news score
        news_analysis = self.compute_news_score(news_items)
        news_score = news_analysis['score']
        
        # Compute macro surprise
        macro_analysis = self.compute_macro_surprise_impact(macro_events)
        macro_surprise_z = macro_analysis['aggregate_z']
        
        # Compute shadow adjustments
        adjustments = self.compute_shadow_adjustments(news_score, macro_surprise_z)
        
        # Compile results
        impact_result = {
            'news_analysis': news_analysis,
            'macro_analysis': macro_analysis,
            'adjustments': adjustments,
            'summary': {
                'news_score': news_score,
                'macro_z_score': macro_surprise_z,
                'band_adjustment_pct': adjustments['band_adjustment_pct'],
                'confidence_adjustment_pct': adjustments['confidence_adjustment_pct'],
                'triggers_fired': len(adjustments['triggers'])
            }
        }
        
        print(f"Impact analysis complete:")
        print(f"News Score: {news_score:.3f}")
        print(f"Macro Z-Score: {macro_surprise_z:.2f}")
        print(f"Band Adjustment: {adjustments['band_adjustment_pct']:+.1f}%")
        print(f"Confidence Adjustment: {adjustments['confidence_adjustment_pct']:+.1f}%")
        print(f"Triggers: {adjustments['triggers']}")
        
        return impact_result
    
    def write_impact_artifacts(self, impact_result, output_dir='audit_exports'):
        """Write NEWS_SCORE.md and append to ZEN_COUNCIL_EXPLAIN.md"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        audit_dir = Path(output_dir) / 'daily' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        artifacts = {}
        
        # Write NEWS_SCORE.md
        news_score_file = self._write_news_score_report(impact_result, audit_dir)
        artifacts['news_score'] = news_score_file
        
        # Append to ZEN_COUNCIL_EXPLAIN.md
        explain_file = self._append_to_council_explain(impact_result, output_dir)
        artifacts['council_explain'] = explain_file
        
        return artifacts
    
    def _write_news_score_report(self, impact_result, audit_dir):
        """Write NEWS_SCORE.md report"""
        report_file = audit_dir / 'NEWS_SCORE.md'
        
        news_analysis = impact_result['news_analysis']
        macro_analysis = impact_result['macro_analysis']
        adjustments = impact_result['adjustments']
        
        content = f"""# News Score Analysis

**Date**: {datetime.now().date()}
**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

## Summary

### Final Scores
- **News Score (s)**: {news_analysis['score']:.3f}
- **Macro Surprise (z)**: {macro_analysis['aggregate_z']:.2f}

### Applied Adjustments
- **Band Adjustment**: {adjustments['band_adjustment_pct']:+.1f}%
- **Confidence Adjustment**: {adjustments['confidence_adjustment_pct']:+.1f}%

### Triggers Fired
"""
        
        if adjustments['triggers']:
            for trigger in adjustments['triggers']:
                content += f"- {trigger}\n"
        else:
            content += "- None (neutral conditions)\n"
        
        content += f"""
## News Analysis Detail

### Input Data
- **Items Processed**: {news_analysis['items_count']}
- **Lookback Window**: {news_analysis['lookback_hours']} hours
- **Total Weight**: {news_analysis['total_weight']:.3f}
- **Muted Items**: {news_analysis['muted_count']} (uncorroborated)

### Score Components
"""
        
        if news_analysis['components']:
            for comp in news_analysis['components']:
                content += f"""
#### {comp['source']} ({comp['category']})
- **Headline**: {comp['headline']}
- **Base Sentiment**: {comp['base_sentiment']:+.3f}
- **Source Weight**: {comp['weight']:.2f}
- **Severity Mult**: {comp['severity_mult']:.2f}
- **Time Decay**: {comp['time_decay']:.3f}
- **Item Score**: {comp['item_score']:+.4f}
"""
        else:
            content += "No qualifying news components found.\n"
        
        content += f"""
## Macro Analysis Detail

### Aggregate Impact
- **Events Processed**: {macro_analysis['event_count']}
- **High Impact Events**: {len(macro_analysis['high_impact_events'])}
- **Aggregate Z-Score**: {macro_analysis['aggregate_z']:.2f}

### High Impact Events Detail
"""
        
        if macro_analysis['high_impact_events']:
            for event in macro_analysis['high_impact_events']:
                direction_emoji = "📈" if event['z_score'] > 0 else "📉" if event['z_score'] < 0 else "➖"
                content += f"- **{event['event']}**: {direction_emoji} z={event['z_score']:.2f} ({event['surprise_direction']})\n"
        else:
            content += "No high impact macro events detected.\n"
        
        content += f"""
## Impact Logic

### Decision Rules Applied
"""
        
        if adjustments['reasoning']:
            for reason in adjustments['reasoning']:
                content += f"- {reason}\n"
        else:
            content += "- No impact adjustments triggered (neutral market conditions)\n"
        
        content += f"""
### Thresholds
- **Risk-off News**: s ≤ {self.risk_off_threshold}
- **Risk-on News**: s ≥ {self.risk_on_threshold}  
- **Macro Significance**: |z| ≥ {self.macro_significance_threshold}

### Daily Limits
- **Max Band Adjustment**: ±{self.max_daily_band_pct}%
- **Max Confidence Adjustment**: ±{self.max_daily_conf_pct}%

---
Generated by Event-Impact Engine v0.1
"""
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return str(report_file)
    
    def _append_to_council_explain(self, impact_result, output_dir):
        """Append reasoning to ZEN_COUNCIL_EXPLAIN.md"""
        explain_file = Path(output_dir) / 'ZEN_COUNCIL_EXPLAIN.md'
        
        adjustments = impact_result['adjustments']
        summary = impact_result['summary']
        
        entry = f"""
## Event-Impact Engine Adjustments ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

### Applied Adjustments
- **Band Adjustment**: {adjustments['band_adjustment_pct']:+.1f}% (volatility bands)
- **Confidence Adjustment**: {adjustments['confidence_adjustment_pct']:+.1f}% (forecast confidence)

### Reasoning
"""
        
        if adjustments['reasoning']:
            for reason in adjustments['reasoning']:
                entry += f"- {reason}\n"
        else:
            entry += "- No adjustments applied (neutral market conditions)\n"
        
        entry += f"""
### Market Signals
- News Score: {summary['news_score']:.3f}
- Macro Z-Score: {summary['macro_z_score']:.2f}
- Triggers Fired: {summary['triggers_fired']}

**SHADOW MODE**: These adjustments are logged only and NOT applied to live forecasts.

---
"""
        
        # Append to file (create if doesn't exist)
        with open(explain_file, 'a', encoding='utf-8') as f:
            if not explain_file.exists() or explain_file.stat().st_size == 0:
                f.write("# Zen Council Explanation Log\n\n")
            f.write(entry)
        
        return str(explain_file)


def main():
    """Test event impact engine"""
    # Create ingestion engine and get data
    ingestion = NewsIngestionEngine()
    ingestion_result = ingestion.ingest_daily_news()
    
    # Create impact engine
    impact_engine = EventImpactEngine()
    
    # Run impact analysis
    impact_result = impact_engine.run_impact_analysis(
        ingestion_result['news_items'],
        ingestion_result['macro_events']
    )
    
    # Write artifacts
    artifacts = impact_engine.write_impact_artifacts(impact_result)
    
    print(f"\nEvent-Impact Engine complete!")
    print(f"Artifacts: {list(artifacts.values())}")
    
    return impact_result, artifacts


if __name__ == '__main__':
    main()