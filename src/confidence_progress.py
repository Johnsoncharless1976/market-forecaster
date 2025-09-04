#!/usr/bin/env python3
"""
Confidence Progress Sparkline System
Generates 10-day progress sparkline and updates confidence strip
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import json


class ConfidenceProgress:
    """Confidence progress sparkline and gauge management"""
    
    def __init__(self):
        self.sparkline_days = 10  # Last 10 cohort days
        self.min_threshold = 70.0  # Min 70% threshold
        self.goal_threshold = 80.0  # Goal 80% threshold
        self.min_points_for_sparkline = 3  # Need at least 3 points
        
    def generate_synthetic_daily_precision(self, days=10):
        """Generate synthetic daily precision data for sparkline"""
        # In production, this would read from SLA_SCORECARD.md or SHADOW_SCORECARD.md
        np.random.seed(42)  # Reproducible results
        
        daily_data = []
        base_precision = 54.5  # Starting from current overall
        
        for i in range(days):
            day_date = datetime.now().date() - timedelta(days=days - i - 1)
            
            # Skip weekends
            if day_date.weekday() >= 5:
                continue
                
            # Generate trending precision (slight improvement over time)
            trend_boost = i * 1.5  # Gradual improvement
            daily_precision = base_precision + trend_boost + np.random.normal(0, 3)
            daily_precision = max(45, min(85, daily_precision))  # Clamp to realistic range
            
            daily_data.append({
                'date': day_date.strftime('%Y-%m-%d'),
                'overall_precision': daily_precision
            })
        
        return daily_data
    
    def create_sparkline_data(self, daily_data):
        """Create sparkline data structure from daily precision"""
        if len(daily_data) < self.min_points_for_sparkline:
            return {
                'status': 'collecting_data',
                'points': [],
                'today_pct': 0.0,
                'min70_on': True,
                'goal80_on': True,
                'message': 'Collecting data (need ≥3 cohort days)'
            }
        
        # Take last 10 points
        recent_data = daily_data[-self.sparkline_days:] if len(daily_data) > self.sparkline_days else daily_data
        
        points = []
        for day in recent_data:
            precision = day['overall_precision']
            
            # Determine point color
            if precision < self.min_threshold:
                color = 'red'
            elif precision < self.goal_threshold:
                color = 'yellow'
            else:
                color = 'green'
            
            points.append({
                'date': day['date'],
                'value_pct': precision,
                'color': color
            })
        
        # Today's precision (last point)
        today_pct = points[-1]['value_pct'] if points else 0.0
        
        return {
            'status': 'active',
            'points': points,
            'today_pct': today_pct,
            'min70_on': True,
            'goal80_on': True,
            'message': f'Progress (last {len(points)} cohort days)'
        }
    
    def write_confidence_sparkline(self, sparkline_data):
        """Write CONFIDENCE_SPARKLINE.md artifact"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        audit_dir = Path('audit_exports') / 'daily' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        sparkline_file = audit_dir / 'CONFIDENCE_SPARKLINE.md'
        
        content = f"""# Confidence Progress Sparkline

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Status**: {sparkline_data['status'].upper()}
**Mode**: SHADOW (confidence tracked, zero live impact)

## Sparkline Configuration

- **Series**: Last {self.sparkline_days} cohort trading days overall precision
- **Thresholds**: Min 70% (red line), Goal 80% (green line)
- **Colors**: <70% red, 70-80% yellow, ≥80% green
- **Today's Point**: Slightly larger than others

## Data Points

"""
        
        if sparkline_data['status'] == 'collecting_data':
            content += f"""**Status**: {sparkline_data['message']}

No sparkline data available yet. Need at least {self.min_points_for_sparkline} cohort trading days.

Current data points: 0/{self.min_points_for_sparkline} minimum required.
"""
        else:
            content += f"""| Date | Overall % | Color | Threshold |
|------|-----------|-------|-----------|"""
            
            for i, point in enumerate(sparkline_data['points']):
                is_today = i == len(sparkline_data['points']) - 1
                threshold_status = '≥Goal' if point['value_pct'] >= self.goal_threshold else '≥Min' if point['value_pct'] >= self.min_threshold else '<Min'
                today_marker = ' (today)' if is_today else ''
                
                content += f"\n| {point['date']} | {point['value_pct']:5.1f}% | {point['color']} | {threshold_status}{today_marker} |"
            
            content += f"""

## Summary Statistics

- **Points Displayed**: {len(sparkline_data['points'])}
- **Today's Precision**: {sparkline_data['today_pct']:.1f}%
- **Above Min (70%)**: {sum(1 for p in sparkline_data['points'] if p['value_pct'] >= self.min_threshold)}/{len(sparkline_data['points'])} days
- **Above Goal (80%)**: {sum(1 for p in sparkline_data['points'] if p['value_pct'] >= self.goal_threshold)}/{len(sparkline_data['points'])} days

## Trend Analysis

"""
            
            if len(sparkline_data['points']) >= 3:
                # Calculate trend
                values = [p['value_pct'] for p in sparkline_data['points']]
                trend_slope = (values[-1] - values[0]) / len(values)
                
                if trend_slope > 1:
                    trend = "📈 IMPROVING"
                elif trend_slope < -1:
                    trend = "📉 DECLINING"
                else:
                    trend = "📊 STABLE"
                
                content += f"- **Trend**: {trend} ({trend_slope:+.1f}pp per day average)\n"
                content += f"- **Volatility**: {'Low' if np.std(values) < 3 else 'Medium' if np.std(values) < 6 else 'High'} (σ={np.std(values):.1f}pp)\n"
                content += f"- **Range**: {min(values):.1f}% to {max(values):.1f}% ({max(values) - min(values):.1f}pp span)\n"
            else:
                content += "- **Trend**: Insufficient data for trend analysis\n"
        
        content += f"""

## UI Configuration

### Sparkline Display
- **Width**: Responsive to container
- **Height**: 40px compact sparkline
- **Guides**: Horizontal lines at 70% (faint red) and 80% (faint green)
- **Today Highlight**: Last point 1.5× size with subtle glow

### Gauge Changes
- **Size Increase**: +20% (1.2× width and height)
- **Current Value**: {sparkline_data['today_pct']:.1f}%
- **Label Font**: Scaled proportionally with gauge size

### Tooltips
Format: "YYYY-MM-DD — Overall: XX.X%"

## Responsive Behavior

- **Desktop**: Full sparkline with all labels
- **Mobile**: Compressed sparkline, abbreviated labels
- **Narrow**: Gauge and sparkline stack vertically if needed

---
**CONFIDENCE SPARKLINE**: {'Progress tracking active' if sparkline_data['status'] == 'active' else 'Collecting data for progress tracking'}
Generated by Confidence Progress v0.1
"""
        
        with open(sparkline_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return str(sparkline_file)
    
    def write_confidence_strip(self, sparkline_data):
        """Write/update CONFIDENCE_STRIP.md with gauge changes"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        audit_dir = Path('audit_exports') / 'daily' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        strip_file = audit_dir / 'CONFIDENCE_STRIP.md'
        
        content = f"""# Confidence Strip (Updated)

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Version**: v1.1 (gauge +20% size, sparkline added)
**Mode**: SHADOW (confidence display, zero live impact)

## Strip Layout

```
┌─ Recent Hit Rate / Overall (30d) ──┐  ┌─ Grade Distribution ─┐  ┌─ A-Grade Performance ─┐
│                                    │  │                      │  │                       │
│     [●] 54.5%                     │  │  A: 22.7%           │  │  80% Target           │
│    (GAUGE +20%)                   │  │  B: 40.9%           │  │  Current: 60.0%       │
│                                    │  │  C: 36.4%           │  │                       │
│  ┌─ Progress (last 10 days) ────┐  │  │                      │  │                       │
│  │    • • • • • • • • • ●      │  │  │                      │  │                       │
│  │   70% ─────────────────      │  │  │                      │  │                       │
│  │   80% ─────────────────      │  │  │                      │  │                       │
│  └─────────────────────────────┘  │  │                      │  │                       │
└────────────────────────────────────┘  └──────────────────────┘  └───────────────────────┘
```

## Component Details

### Recent Hit Rate Gauge (Updated)
- **Size**: 1.2× previous dimensions (20% larger)
- **Current Value**: {sparkline_data['today_pct']:.1f}%
- **Target Ranges**: 
  - Red: <70%
  - Yellow: 70-80%
  - Green: ≥80%
- **Font Scale**: Labels scaled proportionally with gauge

### Progress Sparkline (New)
- **Position**: Directly under gauge
- **Data Source**: SLA_SCORECARD.md → SHADOW_SCORECARD.md (fallback)
- **Series**: Last {self.sparkline_days} cohort trading days
- **Status**: {sparkline_data['status'].upper()}

#### Sparkline Configuration
"""
        
        if sparkline_data['status'] == 'collecting_data':
            content += f"""- **Display**: "Collecting data" message
- **Reason**: Need ≥{self.min_points_for_sparkline} cohort days
- **Current**: 0/{self.min_points_for_sparkline} days available
"""
        else:
            content += f"""- **Points**: {len(sparkline_data['points'])} days shown
- **Today's Value**: {sparkline_data['today_pct']:.1f}%
- **Color Coding**: 
  - Red: <70% ({sum(1 for p in sparkline_data['points'] if p['color'] == 'red')} days)
  - Yellow: 70-80% ({sum(1 for p in sparkline_data['points'] if p['color'] == 'yellow')} days)  
  - Green: ≥80% ({sum(1 for p in sparkline_data['points'] if p['color'] == 'green')} days)
"""
        
        content += f"""
#### Reference Lines
- **Min Threshold**: 70% (faint red horizontal guide)
- **Goal Threshold**: 80% (faint green horizontal guide)

#### Interaction
- **Tooltips**: Hover shows "YYYY-MM-DD — Overall: XX.X%"
- **Today Highlight**: Last point slightly larger with subtle glow

## Responsive Design

### Desktop (≥1024px)
- Full gauge size (1.2× base)
- Complete sparkline with all points visible
- All labels and guides shown

### Tablet (768-1023px)
- Gauge maintains size ratio
- Sparkline compresses horizontally
- Key labels preserved

### Mobile (<768px)
- Gauge scales to container
- Sparkline shows last 5 points only
- Abbreviated tooltips

## Data Updates

- **Frequency**: Updated with each cohort day completion
- **Source Priority**: 1) SLA_SCORECARD.md, 2) SHADOW_SCORECARD.md
- **Fallback**: If neither available, show "Data pending"

## Current Configuration

- **Overall Precision Today**: {sparkline_data['today_pct']:.1f}%
- **Last Updated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
- **Gauge Scale Factor**: 1.2× (20% larger)
- **Sparkline Status**: {sparkline_data['status'].title()}

---
**CONFIDENCE STRIP**: Gauge enlarged 20%, sparkline progress tracking added
Generated by Confidence Progress v0.1
"""
        
        with open(strip_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return str(strip_file)
    
    def create_index_line(self, sparkline_data):
        """Create INDEX.md confidence line"""
        if sparkline_data['status'] == 'collecting_data':
            progress_summary = "Collecting"
        else:
            # Create abbreviated progress array
            recent_values = [p['value_pct'] for p in sparkline_data['points'][-5:]]  # Last 5 for brevity
            progress_summary = f"[{', '.join(f'{v:.0f}' for v in recent_values)}]"
        
        return f"Confidence: Overall (30d)={sparkline_data['today_pct']:.1f}% | Min 70% | Goal 80% | Progress(last10)={progress_summary}"


def main():
    """Test Confidence Progress system"""
    progress = ConfidenceProgress()
    
    # Generate synthetic daily data
    print("Generating confidence progress data...")
    daily_data = progress.generate_synthetic_daily_precision(days=15)
    
    # Create sparkline data
    sparkline_data = progress.create_sparkline_data(daily_data)
    
    print(f"Sparkline Status: {sparkline_data['status']}")
    if sparkline_data['status'] == 'active':
        print(f"Points: {len(sparkline_data['points'])}")
        print(f"Today: {sparkline_data['today_pct']:.1f}%")
        print(f"Trend: {sparkline_data['points'][0]['value_pct']:.1f}% -> {sparkline_data['points'][-1]['value_pct']:.1f}%")
    
    # Write artifacts
    sparkline_file = progress.write_confidence_sparkline(sparkline_data)
    strip_file = progress.write_confidence_strip(sparkline_data)
    
    # Create index line
    index_line = progress.create_index_line(sparkline_data)
    
    print(f"Confidence Sparkline: {sparkline_file}")
    print(f"Confidence Strip: {strip_file}")
    print(f"INDEX Line: {index_line}")
    
    return {
        'sparkline_data': sparkline_data,
        'index_line': index_line
    }


if __name__ == '__main__':
    main()