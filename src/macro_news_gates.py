#!/usr/bin/env python3
"""
Macro & News Gates: Gate/widen forecasts based on macro events and news sentiment
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
from pathlib import Path


class MacroNewsGates:
    """Macro event and news sentiment gating system"""
    
    def __init__(self):
        self.macro_enabled = os.getenv('MACRO_GATE_ENABLED', 'true').lower() == 'true'
        self.news_enabled = os.getenv('NEWS_ENABLED', 'true').lower() == 'true'
        
        # Environment flags for Event-Impact Engine integration
        self.allowlist_path = os.getenv('NEWS_ALLOWLIST_PATH', 'NEWS_WHITELIST.md')
        self.weights_path = os.getenv('NEWS_WEIGHTS_PATH', 'NEWS_SOURCE_WEIGHTS.yaml')
        
    def load_macro_schedule(self, data_dir='data'):
        """Load macro event schedule from CSV"""
        schedule_path = Path(data_dir) / 'macro_schedule.csv'
        if not schedule_path.exists():
            return pd.DataFrame()
        
        df = pd.read_csv(schedule_path)
        df['date'] = pd.to_datetime(df['date'])
        return df
    
    def load_news_events(self, data_dir='data'):
        """Load news events from CSV"""  
        news_path = Path(data_dir) / 'news_events.csv'
        if not news_path.exists():
            return pd.DataFrame()
        
        df = pd.read_csv(news_path)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    
    def check_macro_gate(self, target_date, schedule_df):
        """Check if macro gate should be applied for target date"""
        if not self.macro_enabled or schedule_df.empty:
            return False, None, {}
        
        # Convert target_date to datetime if it's a string
        if isinstance(target_date, str):
            target_date = pd.to_datetime(target_date).date()
        elif hasattr(target_date, 'date'):
            target_date = target_date.date()
        
        # Filter events for target date
        day_events = schedule_df[schedule_df['date'].dt.date == target_date]
        
        # Check for HIGH severity events at 8:30 ET
        morning_high_events = day_events[
            (day_events['timeET'] == '08:30') & 
            (day_events['severity'] == 'HIGH')
        ]
        
        gate_active = len(morning_high_events) > 0
        
        gate_info = {
            'gate_active': gate_active,
            'events_count': len(day_events),
            'high_events_count': len(morning_high_events),
            'morning_events': morning_high_events['event'].tolist() if gate_active else [],
            'am_send_delay': '9:15 ET' if gate_active else None,
            'band_adjustment': 10.0 if gate_active else 0.0  # +10% widening
        }
        
        return gate_active, morning_high_events, gate_info
    
    def compute_news_score(self, target_datetime, news_df, hours_lookback=3):
        """Compute rolling news sentiment score"""
        if not self.news_enabled or news_df.empty:
            return 0.0, {}
        
        # Convert target_datetime to datetime if needed
        if isinstance(target_datetime, str):
            target_datetime = pd.to_datetime(target_datetime)
        
        # Get news from last N hours
        start_time = target_datetime - timedelta(hours=hours_lookback)
        recent_news = news_df[
            (news_df['timestamp'] >= start_time) & 
            (news_df['timestamp'] <= target_datetime)
        ]
        
        if len(recent_news) == 0:
            return 0.0, {'count': 0, 'mean_score': 0.0}
        
        # Compute rolling mean score
        mean_score = recent_news['score'].mean()
        
        news_info = {
            'count': len(recent_news),
            'mean_score': mean_score,
            'score_range': [recent_news['score'].min(), recent_news['score'].max()],
            'lookback_hours': hours_lookback
        }
        
        return mean_score, news_info
    
    def apply_news_adjustments(self, news_score):
        """Apply news-based forecast adjustments"""
        adjustments = {
            'band_adjustment': 0.0,
            'confidence_adjustment': 0.0,
            'trigger_threshold': 0.3,
            'active_rule': None
        }
        
        if news_score <= -0.3:  # Risk-off sentiment
            adjustments['band_adjustment'] = 10.0  # Widen +10%
            adjustments['confidence_adjustment'] = -5.0  # Reduce confidence -5%
            adjustments['active_rule'] = f"News risk-off (score={news_score:.2f}) -> bands +10%, confidence -5%"
            
        elif news_score >= 0.3:  # Risk-on sentiment  
            adjustments['band_adjustment'] = -5.0  # Tighten -5%
            adjustments['confidence_adjustment'] = 5.0  # Boost confidence +5%
            adjustments['active_rule'] = f"News risk-on (score={news_score:.2f}) -> bands -5%, confidence +5%"
        
        return adjustments
    
    def process_gates(self, target_date, target_time=None):
        """Main gate processing pipeline"""
        if target_time is None:
            target_time = datetime.now()
        
        # Load data
        macro_schedule = self.load_macro_schedule()
        news_events = self.load_news_events()
        
        # Process macro gate
        macro_gate_active, macro_events, macro_info = self.check_macro_gate(target_date, macro_schedule)
        
        # Process news score (use target_time for news lookback)
        news_score, news_info = self.compute_news_score(target_time, news_events)
        news_adjustments = self.apply_news_adjustments(news_score)
        
        # Compile results
        result = {
            'target_date': target_date,
            'target_time': target_time,
            'macro_gate': macro_info,
            'news_analysis': {
                'score': news_score,
                'info': news_info,
                'adjustments': news_adjustments
            },
            'combined_adjustments': {
                'total_band_adjustment': macro_info.get('band_adjustment', 0) + news_adjustments['band_adjustment'],
                'total_confidence_adjustment': news_adjustments['confidence_adjustment'],
                'am_send_delay': macro_info.get('am_send_delay'),
                'active_rules': []
            }
        }
        
        # Compile active rules
        if macro_gate_active:
            events_list = ', '.join(macro_info['morning_events'])
            result['combined_adjustments']['active_rules'].append(
                f"Macro gate: {events_list} at 8:30 ET -> AM send 9:15, bands +10%"
            )
        
        if news_adjustments['active_rule']:
            result['combined_adjustments']['active_rules'].append(news_adjustments['active_rule'])
        
        return result
    
    def write_macro_gate_report(self, result, output_dir):
        """Write MACRO_GATE.md artifact"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        audit_dir = Path(output_dir) / 'daily' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        macro_info = result['macro_gate']
        
        report = f"""# Macro Gate Report

**Date**: {result['target_date']}
**Timestamp**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Gate Status**: {"ACTIVE" if macro_info['gate_active'] else "INACTIVE"}

## Gate Rules
- **Trigger**: HIGH severity events at 8:30 ET
- **Action**: Delay AM send to 9:15 ET, widen bands +10%
- **Enabled**: {self.macro_enabled}

## Today's Events
**Total Events**: {macro_info['events_count']}
**HIGH Severity at 8:30 ET**: {macro_info['high_events_count']}

"""
        
        if macro_info['morning_events']:
            report += "### Triggering Events\n"
            for event in macro_info['morning_events']:
                report += f"- {event} (8:30 ET)\n"
        else:
            report += "### No Triggering Events\n- No HIGH severity events at 8:30 ET today\n"
        
        report += f"""
## Applied Adjustments
- **AM Send Time**: {macro_info.get('am_send_delay', 'Normal (7:00 ET)')}
- **Band Widening**: +{macro_info.get('band_adjustment', 0):.0f}%
- **Gate Active**: {macro_info['gate_active']}

---
Generated by Macro Gate System
"""
        
        report_file = audit_dir / 'MACRO_GATE.md'
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        return str(report_file)
    
    def write_news_score_report(self, result, output_dir):
        """Write NEWS_SCORE.md artifact"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        audit_dir = Path(output_dir) / 'daily' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        news_analysis = result['news_analysis']
        news_info = news_analysis['info']
        adjustments = news_analysis['adjustments']
        
        report = f"""# News Score Report

**Timestamp**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Analysis Period**: {news_info['lookback_hours']}h lookback
**Enabled**: {self.news_enabled}

## Sentiment Analysis
**News Score**: {news_analysis['score']:.3f}
**Event Count**: {news_info['count']} articles/events
**Score Range**: [{news_info.get('score_range', [0, 0])[0]:.2f}, {news_info.get('score_range', [0, 0])[1]:.2f}]

## Score Interpretation
- **< -0.3**: Risk-off sentiment (bearish)
- **-0.3 to 0.3**: Neutral sentiment  
- **> 0.3**: Risk-on sentiment (bullish)
- **Current**: {"Risk-off" if news_analysis['score'] <= -0.3 else "Risk-on" if news_analysis['score'] >= 0.3 else "Neutral"}

## Applied Adjustments
- **Band Adjustment**: {adjustments['band_adjustment']:+.1f}%
- **Confidence Adjustment**: {adjustments['confidence_adjustment']:+.1f}%
- **Trigger Used**: {adjustments['active_rule'] or 'None (score within neutral range)'}

## Adjustment Rules
- **Risk-off** (score ≤ -0.3): Widen bands +10%, reduce confidence -5%
- **Risk-on** (score ≥ 0.3): Tighten bands -5%, boost confidence +5%  
- **Neutral** (-0.3 < score < 0.3): No adjustments

---
Generated by News Scoring System
"""
        
        report_file = audit_dir / 'NEWS_SCORE.md'
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        return str(report_file)


def main():
    """Test run of Macro & News Gates"""
    gates = MacroNewsGates()
    
    # Test with today's date
    target_date = datetime.now().date()
    target_time = datetime.now()
    
    result = gates.process_gates(target_date, target_time)
    
    # Write reports
    output_dir = 'audit_exports'
    macro_report = gates.write_macro_gate_report(result, output_dir) 
    news_report = gates.write_news_score_report(result, output_dir)
    
    print(f"Macro gate active: {result['macro_gate']['gate_active']}")
    print(f"News score: {result['news_analysis']['score']:.3f}")
    print(f"Total band adjustment: {result['combined_adjustments']['total_band_adjustment']:+.1f}%")
    print(f"Active rules: {len(result['combined_adjustments']['active_rules'])}")
    
    return result


if __name__ == '__main__':
    main()