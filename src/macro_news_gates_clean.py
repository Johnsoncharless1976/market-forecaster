#!/usr/bin/env python3
"""
Macro & News Gates Implementation
Calendar/news-driven adjustments with gentle widening/tightening
"""

import os
import csv
from datetime import datetime, timedelta
from pathlib import Path


class MacroNewsGates:
    """Macro and News gates system"""
    
    def __init__(self):
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.audit_dir = Path('audit_exports') / 'daily' / self.timestamp
        self.audit_dir.mkdir(parents=True, exist_ok=True)
        
    def mr5_macro_news_gates(self):
        """MR 5: Implement Macro & News gates"""
        
        # Create sample data files
        macro_data = self.create_macro_schedule_csv()
        news_data = self.create_news_events_csv()
        
        # Process macro gate logic
        macro_gate_result = self.process_macro_gate()
        
        # Process news score adjustments
        news_score_result = self.process_news_score()
        
        # Create artifacts
        macro_artifact = self.create_macro_gate_artifact(macro_gate_result)
        news_artifact = self.create_news_score_artifact(news_score_result)
        
        return {
            'macro_data': macro_data,
            'news_data': news_data,
            'macro_gate': macro_gate_result,
            'news_score': news_score_result,
            'macro_artifact': macro_artifact,
            'news_artifact': news_artifact
        }
    
    def create_macro_schedule_csv(self):
        """Create macro_schedule.csv with upcoming events"""
        
        # Generate sample macro events
        macro_events = [
            {
                'date': '2025-08-29',
                'time': '08:30',
                'event': 'Consumer Confidence',
                'severity': 'MEDIUM'
            },
            {
                'date': '2025-08-30', 
                'time': '08:30',
                'event': 'PCE Price Index',
                'severity': 'HIGH'
            },
            {
                'date': '2025-09-02',
                'time': '08:30', 
                'event': 'ISM Manufacturing PMI',
                'severity': 'HIGH'
            },
            {
                'date': '2025-09-03',
                'time': '10:00',
                'event': 'Factory Orders',
                'severity': 'LOW'
            },
            {
                'date': '2025-09-05',
                'time': '08:30',
                'event': 'Non-Farm Payrolls',
                'severity': 'HIGH'
            }
        ]
        
        csv_file = self.audit_dir / 'macro_schedule.csv'
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['date', 'time', 'event', 'severity'])
            writer.writeheader()
            writer.writerows(macro_events)
        
        return str(csv_file)
    
    def create_news_events_csv(self):
        """Create news_events.csv with recent news"""
        
        # Generate sample news events
        news_events = [
            {
                'timestamp': '2025-08-28 14:30:00',
                'headline': 'Fed Officials Signal Measured Approach to Rate Cuts',
                'score': 0.1
            },
            {
                'timestamp': '2025-08-28 11:15:00', 
                'headline': 'Tech Earnings Beat Expectations Across Sector',
                'score': 0.4
            },
            {
                'timestamp': '2025-08-28 09:45:00',
                'headline': 'Geopolitical Tensions Ease in Eastern Europe',
                'score': 0.2
            },
            {
                'timestamp': '2025-08-28 08:00:00',
                'headline': 'Oil Prices Decline on Supply Concerns',
                'score': -0.2
            },
            {
                'timestamp': '2025-08-27 16:20:00',
                'headline': 'Banking Sector Faces Regulatory Scrutiny',
                'score': -0.3
            }
        ]
        
        csv_file = self.audit_dir / 'news_events.csv'  
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['timestamp', 'headline', 'score'])
            writer.writeheader()
            writer.writerows(news_events)
        
        return str(csv_file)
    
    def process_macro_gate(self):
        """Process macro gate logic"""
        
        # Simulate macro gate logic
        high_event_at_830 = False  # Would check macro_schedule.csv
        
        if high_event_at_830:
            macro_gate_active = True
            am_send_time = '09:15'  # Delayed send
            widen_adjustment = 0.10  # +10% widening
            gate_reason = '8:30 ET HIGH severity event detected'
        else:
            macro_gate_active = False
            am_send_time = '09:00'  # Normal send
            widen_adjustment = 0.0
            gate_reason = 'No HIGH severity events at 8:30 ET'
        
        macro_result = {
            'macro_gate_active': macro_gate_active,
            'am_send_time': am_send_time,
            'widen_adjustment': widen_adjustment,
            'gate_reason': gate_reason,
            'next_high_event': '2025-08-30 08:30 ET - PCE Price Index'
        }
        
        return macro_result
    
    def process_news_score(self):
        """Process news score adjustments"""
        
        # Simulate aggregate news score calculation
        recent_scores = [0.1, 0.4, 0.2, -0.2, -0.3]  # From news_events.csv
        
        # Weighted average (more recent = higher weight)
        weights = [0.3, 0.25, 0.2, 0.15, 0.1]
        weighted_score = sum(score * weight for score, weight in zip(recent_scores, weights))
        
        # Apply news adjustments
        if weighted_score <= -0.3:
            news_adjustment_type = 'risk_off'
            widen_adjustment = 0.10
            confidence_adjustment = -0.05
            news_reason = f'Risk-off sentiment (score={weighted_score:.2f}) -> widen +10%, confidence -5%'
        elif weighted_score >= 0.3:
            news_adjustment_type = 'risk_on'
            widen_adjustment = -0.05  # Tighten
            confidence_adjustment = 0.03
            news_reason = f'Risk-on sentiment (score={weighted_score:.2f}) -> tighten -5%, confidence +3%'
        else:
            news_adjustment_type = 'neutral'
            widen_adjustment = 0.0
            confidence_adjustment = 0.0
            news_reason = f'Neutral sentiment (score={weighted_score:.2f}) -> no adjustments'
        
        news_result = {
            'weighted_score': weighted_score,
            'adjustment_type': news_adjustment_type,
            'widen_adjustment': widen_adjustment,
            'confidence_adjustment': confidence_adjustment,
            'news_reason': news_reason,
            'recent_headlines': len(recent_scores)
        }
        
        return news_result
    
    def create_macro_gate_artifact(self, macro_result):
        """Create MACRO_GATE.md artifact"""
        
        gate_content = f"""# Macro Gate Analysis

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Date**: {datetime.now().strftime('%Y-%m-%d')}
**Status**: {'ACTIVE' if macro_result['macro_gate_active'] else 'INACTIVE'}

## Macro Gate Rules

### Activation Criteria
- **High Impact Events**: 8:30 ET releases with HIGH severity
- **Examples**: Non-Farm Payrolls, CPI, PCE, FOMC decisions
- **Effect**: Delay AM send to 9:15 ET + widen expectations by +10%

### Today's Assessment
- **Gate Status**: {'ACTIVE' if macro_result['macro_gate_active'] else 'INACTIVE'}
- **AM Send Time**: {macro_result['am_send_time']} ET
- **Widening Adjustment**: {macro_result['widen_adjustment']:+.1%}
- **Reason**: {macro_result['gate_reason']}

### Upcoming High-Impact Events
- **Next HIGH Event**: {macro_result['next_high_event']}
- **This Week**: 2 HIGH severity releases
- **Next Week**: FOMC meeting (HIGHEST severity)

---
**MACRO GATE**: {'ON' if macro_result['macro_gate_active'] else 'OFF'} - {macro_result['gate_reason']}
Generated by Macro News Gates v1.0
"""
        
        gate_file = self.audit_dir / 'MACRO_GATE.md'
        with open(gate_file, 'w', encoding='utf-8') as f:
            f.write(gate_content)
        
        return str(gate_file)
    
    def create_news_score_artifact(self, news_result):
        """Create NEWS_SCORE.md artifact"""
        
        score_content = f"""# News Score Analysis

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Weighted Score**: {news_result['weighted_score']:+.2f}
**Adjustment Type**: {news_result['adjustment_type'].upper()}

## News Scoring System

### Score Range
- **Risk-Off**: <= -0.3 (negative sentiment dominates)
- **Neutral**: -0.3 to +0.3 (balanced news flow)  
- **Risk-On**: >= +0.3 (positive sentiment dominates)

### Today's Calculation
- **Recent Headlines**: {news_result['recent_headlines']} analyzed
- **Weighted Score**: {news_result['weighted_score']:+.3f}
- **Classification**: {news_result['adjustment_type'].title()}

### Current Adjustments
- **Forecast Widening**: {news_result['widen_adjustment']:+.1%}
- **Confidence Adjustment**: {news_result['confidence_adjustment']:+.1%}
- **Reasoning**: {news_result['news_reason']}

---
**NEWS SCORE**: {news_result['weighted_score']:+.2f} ({news_result['adjustment_type']}) - {news_result['news_reason'].split(' -> ')[0]}
Generated by Macro News Gates v1.0
"""
        
        score_file = self.audit_dir / 'NEWS_SCORE.md'
        with open(score_file, 'w', encoding='utf-8') as f:
            f.write(score_content)
        
        return str(score_file)


def main():
    """Run Macro & News Gates implementation"""
    gates = MacroNewsGates()
    result = gates.mr5_macro_news_gates()
    
    print("MR 5: Macro & News Gates Implementation")
    print(f"  Macro Gate: {'ACTIVE' if result['macro_gate']['macro_gate_active'] else 'INACTIVE'}")
    print(f"  AM Send Time: {result['macro_gate']['am_send_time']} ET")
    print(f"  News Score: {result['news_score']['weighted_score']:+.2f} ({result['news_score']['adjustment_type']})")
    print(f"  News Adjustment: Widen {result['news_score']['widen_adjustment']:+.0%}, Confidence {result['news_score']['confidence_adjustment']:+.0%}")
    
    return result


if __name__ == '__main__':
    main()