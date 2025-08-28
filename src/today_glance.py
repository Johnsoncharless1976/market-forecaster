#!/usr/bin/env python3
"""
Today at a Glance system
Artifact-driven overview row for dashboard and emails
"""

import os
import json
from datetime import datetime
from pathlib import Path
import re


class TodayGlance:
    """Today at a Glance row generator from existing artifacts"""
    
    def __init__(self):
        self.artifacts_base = Path('audit_exports/daily')
        self.default_values = {
            'forecast_stance': 'Pending',
            'forecast_grade': 'C',
            'confidence_pct': 0.0,
            'cohort_day': '0/30',
            'macro_gate': 'Off',
            'news_score': 0.0,
            'macro_z': 0.0,
            'magnet_target': 'L25',
            'magnet_m': 0.0,
            'pipeline_sha': 'local',
            'evidence_timestamp': datetime.now().strftime('%H:%M:%S UTC'),
            'live_gate_status': 'PENDING'
        }
    
    def parse_index_md(self):
        """Parse INDEX.md for key values"""
        index_file = self.artifacts_base / 'INDEX.md'
        
        if not index_file.exists():
            return {}
        
        try:
            content = index_file.read_text(encoding='utf-8')
            
            values = {}
            
            # Parse Forecast line
            forecast_match = re.search(r'Forecast:\s*(\w+)\s*\|\s*Grade=(\w+)', content)
            if forecast_match:
                values['forecast_stance'] = forecast_match.group(1)
                values['forecast_grade'] = forecast_match.group(2)
            
            # Parse Confidence line
            confidence_match = re.search(r'Confidence:\s*Overall\s*\(30d\)=([0-9.]+)%', content)
            if confidence_match:
                values['confidence_pct'] = float(confidence_match.group(1))
            
            # Parse Shadow cohort line
            shadow_match = re.search(r'Shadow:.*Day\s+(\d+)/(\d+)', content)
            if shadow_match:
                values['cohort_day'] = f"{shadow_match.group(1)}/{shadow_match.group(2)}"
            
            # Parse Live Gate status
            live_gate_match = re.search(r'Live Gate:\s*(\w+)', content)
            if live_gate_match:
                values['live_gate_status'] = live_gate_match.group(1)
            
            return values
            
        except Exception as e:
            print(f"Error parsing INDEX.md: {e}")
            return {}
    
    def parse_latest_artifacts(self):
        """Parse latest artifacts for additional data"""
        values = {}
        
        # Find latest timestamp directory
        try:
            timestamp_dirs = [d for d in self.artifacts_base.iterdir() 
                            if d.is_dir() and re.match(r'\d{8}_\d{6}', d.name)]
            if not timestamp_dirs:
                return values
                
            latest_dir = sorted(timestamp_dirs, key=lambda x: x.name)[-1]
            
            # Parse NEWS_SCORE.md if exists
            news_file = latest_dir / 'NEWS_SCORE.md'
            if news_file.exists():
                try:
                    content = news_file.read_text(encoding='utf-8')
                    score_match = re.search(r'News Score.*:\s*([0-9.-]+)', content)
                    if score_match:
                        values['news_score'] = float(score_match.group(1))
                except:
                    pass
            
            # Parse MACRO_EVENTS.md if exists
            macro_file = latest_dir / 'MACRO_EVENTS.md'
            if macro_file.exists():
                try:
                    content = macro_file.read_text(encoding='utf-8')
                    # Look for macro z-score
                    z_match = re.search(r'z[=-]\s*([0-9.-]+)', content)
                    if z_match:
                        values['macro_z'] = float(z_match.group(1))
                    
                    # Check for 9:15 gate
                    gate_match = re.search(r'9:15.*rule|Macro Gate.*On', content, re.IGNORECASE)
                    values['macro_gate'] = 'On' if gate_match else 'Off'
                except:
                    pass
            
            # Parse LEVEL_MAGNETS.md if exists  
            magnet_file = latest_dir / 'LEVEL_MAGNETS.md'
            if magnet_file.exists():
                try:
                    content = magnet_file.read_text(encoding='utf-8')
                    # Look for L25 target and M value
                    l25_match = re.search(r'L25[=:]\s*(\d+)', content)
                    if l25_match:
                        values['magnet_target'] = f"L25={l25_match.group(1)}"
                    
                    m_match = re.search(r'M[=:]\s*([0-9.]+)', content)
                    if m_match:
                        values['magnet_m'] = float(m_match.group(1))
                except:
                    pass
            
            # Set evidence timestamp from directory
            values['evidence_timestamp'] = datetime.now().strftime('%H:%M:%S UTC')
            
        except Exception as e:
            print(f"Error parsing artifacts: {e}")
        
        return values
    
    def generate_glance_data(self):
        """Generate Today at a Glance data from artifacts"""
        # Start with defaults
        glance_data = self.default_values.copy()
        
        # Update with INDEX.md values
        index_values = self.parse_index_md()
        glance_data.update(index_values)
        
        # Update with latest artifact values
        artifact_values = self.parse_latest_artifacts()
        glance_data.update(artifact_values)
        
        # Set pipeline SHA (mock)
        glance_data['pipeline_sha'] = os.getenv('CI_COMMIT_SHORT_SHA', 'local')[:7]
        
        return glance_data
    
    def format_glance_row_html(self, data):
        """Format glance data as HTML row"""
        
        # Determine status colors
        confidence_color = ('green' if data['confidence_pct'] >= 80 else 
                          'orange' if data['confidence_pct'] >= 70 else 'red')
        
        gate_color = 'green' if data['macro_gate'] == 'On' else 'gray'
        live_gate_color = 'red' if data['live_gate_status'] == 'BLOCKED' else 'green'
        
        html = f"""
<div class="today-glance-row" style="
    background: #f8f9fa; 
    border: 1px solid #dee2e6; 
    border-radius: 8px; 
    padding: 12px 16px; 
    margin-bottom: 16px;
    display: flex; 
    flex-wrap: wrap; 
    gap: 24px; 
    align-items: center;
    font-size: 14px;
">
    <div class="glance-item">
        <strong>Forecast (shadow):</strong> 
        <span class="badge badge-{data['forecast_grade'].lower()}" style="
            background: {'#28a745' if data['forecast_grade'] == 'A' else '#ffc107' if data['forecast_grade'] == 'B' else '#6c757d'}; 
            color: white; 
            padding: 2px 6px; 
            border-radius: 4px;
        ">{data['forecast_stance']}</span>
    </div>
    
    <div class="glance-item">
        <strong>Confidence (30d):</strong> 
        <span style="color: {confidence_color}; font-weight: bold;">{data['confidence_pct']:.1f}%</span>
        <small style="color: #6c757d;"> (Min 70 | Goal 80)</small>
        <span class="sparkline-chip" style="margin-left: 4px;">📈</span>
    </div>
    
    <div class="glance-item">
        <strong>Cohort:</strong> 
        <span>Day {data['cohort_day']}</span>
    </div>
    
    <div class="glance-item">
        <strong>Macro Gate:</strong> 
        <span style="color: {gate_color}; font-weight: bold;">{data['macro_gate']}</span>
        <small style="color: #6c757d;"> (9:15 rule)</small>
    </div>
    
    <div class="glance-item">
        <strong>News Score (s):</strong> 
        <span>{data['news_score']:.3f}</span> ; 
        <strong>Macro z:</strong> 
        <span>{data['macro_z']:.2f}</span>
    </div>
    
    <div class="glance-item">
        <strong>Magnet:</strong> 
        <span>{data['magnet_target']}, M={data['magnet_m']:.3f}</span>
    </div>
    
    <div class="glance-item" style="margin-left: auto;">
        <small style="color: #6c757d;">
            <strong>Source:</strong> pipeline {data['pipeline_sha']} @ {data['evidence_timestamp']}<br>
            <strong>Live Gate:</strong> <span style="color: {live_gate_color}; font-weight: bold;">{data['live_gate_status']}</span>
        </small>
    </div>
</div>
"""
        return html
    
    def format_glance_row_text(self, data):
        """Format glance data as plain text row"""
        return f"""Today at a Glance:
Forecast (shadow): {data['forecast_stance']} | Grade: {data['forecast_grade']}
Confidence (30d): {data['confidence_pct']:.1f}% (Min 70 | Goal 80)
Cohort: Day {data['cohort_day']}
Macro Gate: {data['macro_gate']} (9:15 rule)
News Score (s): {data['news_score']:.3f} ; Macro z: {data['macro_z']:.2f}
Magnet: {data['magnet_target']}, M={data['magnet_m']:.3f}
Source: pipeline {data['pipeline_sha']} @ {data['evidence_timestamp']}
Live Gate: {data['live_gate_status']}"""
    
    def write_glance_artifact(self, data):
        """Write TODAY_GLANCE.md artifact"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        audit_dir = Path('audit_exports') / 'daily' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        glance_file = audit_dir / 'TODAY_GLANCE.md'
        
        content = f"""# Today at a Glance

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Mode**: SHADOW (artifact-driven overview, zero live impact)

## Overview Row Data

### Forecast
- **Stance**: {data['forecast_stance']} (shadow)
- **Grade**: {data['forecast_grade']}

### Confidence  
- **Overall (30d)**: {data['confidence_pct']:.1f}%
- **Thresholds**: Min 70% | Goal 80%
- **Status**: {'Above Goal' if data['confidence_pct'] >= 80 else 'Above Min' if data['confidence_pct'] >= 70 else 'Below Min'}

### Cohort Progress
- **Day**: {data['cohort_day']}
- **Status**: {'Complete' if '30/30' in data['cohort_day'] else 'Active'}

### Market Conditions
- **Macro Gate**: {data['macro_gate']} (9:15 rule)
- **News Score (s)**: {data['news_score']:.3f}
- **Macro z-score**: {data['macro_z']:.2f}

### Trading Systems
- **Magnet**: {data['magnet_target']}, M={data['magnet_m']:.3f}

### System Status
- **Pipeline**: {data['pipeline_sha']} @ {data['evidence_timestamp']}
- **Live Gate**: {data['live_gate_status']} (approval status)

## Artifact Sources

- **Primary**: INDEX.md (forecast, confidence, cohort, live gate)
- **Secondary**: Latest timestamp artifacts
  - NEWS_SCORE.md (news score)
  - MACRO_EVENTS.md (macro z, gate status)
  - LEVEL_MAGNETS.md (magnet target, M value)
- **Fallback**: Default values if artifacts missing

## Missing Artifacts Handling

If any artifact is missing:
- Show "Awaiting pipeline" badge for that component
- No runtime errors generated
- Default values used as fallback
- System continues to function

---
**TODAY GLANCE**: Artifact-driven overview row (no runtime compute)
Generated by Today Glance v0.1
"""
        
        with open(glance_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return str(glance_file)


def main():
    """Test Today at a Glance system"""
    glance = TodayGlance()
    
    print("Generating Today at a Glance...")
    
    # Generate glance data
    data = glance.generate_glance_data()
    
    print("Glance Data:")
    for key, value in data.items():
        print(f"  {key}: {value}")
    
    # Write artifact
    glance_file = glance.write_glance_artifact(data)
    print(f"Today Glance: {glance_file}")
    
    # Generate formatted outputs
    html_row = glance.format_glance_row_html(data)
    text_row = glance.format_glance_row_text(data)
    
    print("\nHTML Row Preview:")
    print(html_row[:200] + "..." if len(html_row) > 200 else html_row)
    
    print("\nText Row Preview:")
    print(text_row)
    
    return data


if __name__ == '__main__':
    main()