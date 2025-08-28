#!/usr/bin/env python3
"""
Integrated Council Engine: Combines Zen Council + Event-Impact Engine for complete forecast adjustment
SHADOW SAFE - applies adjustments only when COUNCIL_ACTIVE=true
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add src to path
sys.path.append(str(Path(__file__).parent))
from zen_council import ZenCouncil
from macro_news_gates import MacroNewsGates  
from news_ingestion import NewsIngestionEngine
from event_impact_engine import EventImpactEngine


class IntegratedCouncilEngine:
    """Integrated engine combining all Council components with safety controls"""
    
    def __init__(self):
        # Initialize component engines
        self.zen_council = ZenCouncil()
        self.macro_gates = MacroNewsGates()
        self.news_ingestion = NewsIngestionEngine()
        self.impact_engine = EventImpactEngine()
        
        # Safety flags
        self.council_active = os.getenv('COUNCIL_ACTIVE', 'false').lower() == 'true'
        self.news_enabled = os.getenv('NEWS_ENABLED', 'true').lower() == 'true'
        
        print(f"Integrated Council Engine initialized:")
        print(f"Council Active: {self.council_active}")
        print(f"News Enabled: {self.news_enabled}")
    
    def process_complete_forecast(self, p_baseline, symbol="^GSPC", target_date=None):
        """Process complete forecast with all engines"""
        if target_date is None:
            target_date = datetime.now().date()
        
        print(f"Processing forecast for {target_date} (baseline: {p_baseline:.3f})")
        
        result = {
            'p_baseline': p_baseline,
            'target_date': target_date,
            'timestamp': datetime.now(),
            'council_active': self.council_active,
            'news_enabled': self.news_enabled,
            'components': {}
        }
        
        # Step 1: Zen Council adjustment (always compute for shadow mode)
        print("Running Zen Council adjustment...")
        zen_result = self.zen_council.adjust_forecast(p_baseline, symbol)
        result['components']['zen_council'] = zen_result
        result['p_zen_adjusted'] = zen_result['p_final']
        
        # Step 2: Event-Impact Engine (if news enabled)
        if self.news_enabled:
            print("Running Event-Impact Engine...")
            
            # Get news and macro data
            ingestion_result = self.news_ingestion.ingest_daily_news(target_date)
            
            # Run impact analysis
            impact_result = self.impact_engine.run_impact_analysis(
                ingestion_result['news_items'],
                ingestion_result['macro_events']
            )
            
            result['components']['news_ingestion'] = ingestion_result
            result['components']['impact_engine'] = impact_result
            
            # Apply impact adjustments to bands and confidence
            impact_adjustments = impact_result['adjustments']
            
            # Modify confidence based on impact
            base_confidence = zen_result.get('conf_reduction_pct', 0)  # Zen Council confidence reduction
            impact_conf_adj = impact_adjustments['confidence_adjustment_pct']
            total_conf_reduction = base_confidence + abs(impact_conf_adj) if impact_conf_adj < 0 else max(0, base_confidence - impact_conf_adj)
            
            # Modify band widening based on impact
            base_band_widen = zen_result.get('band_widen_pct', 0)  # Zen Council band widening
            impact_band_adj = impact_adjustments['band_adjustment_pct']
            total_band_widen = max(0, base_band_widen + impact_band_adj)
            
            result['impact_adjusted_confidence_reduction'] = total_conf_reduction
            result['impact_adjusted_band_widen'] = total_band_widen
            result['impact_triggers'] = impact_adjustments['triggers']
            
        else:
            # No impact adjustments
            result['impact_adjusted_confidence_reduction'] = zen_result.get('conf_reduction_pct', 0)
            result['impact_adjusted_band_widen'] = zen_result.get('band_widen_pct', 0)
            result['impact_triggers'] = []
        
        # Step 3: Traditional Macro Gates (legacy compatibility)
        print("Running traditional macro gates...")
        gates_result = self.macro_gates.process_gates(target_date)
        result['components']['macro_gates'] = gates_result
        
        # Step 4: Final decision logic
        if self.council_active:
            # LIVE MODE: Apply all adjustments
            result['p_final'] = zen_result['p_final']
            result['confidence_reduction_pct'] = result['impact_adjusted_confidence_reduction']
            result['band_widen_pct'] = result['impact_adjusted_band_widen'] 
            result['mode'] = 'LIVE'
            result['decision'] = 'council_with_impact'
            print(f"LIVE MODE: Final probability {result['p_final']:.3f} (from baseline {p_baseline:.3f})")
        else:
            # SHADOW MODE: Log adjustments but keep baseline live
            result['p_final'] = p_baseline  # Keep baseline as live decision
            result['p_council_suggestion'] = zen_result['p_final']
            result['confidence_reduction_pct'] = 0  # No actual reduction in shadow mode
            result['band_widen_pct'] = 0  # No actual widening in shadow mode
            result['mode'] = 'SHADOW'
            result['decision'] = 'baseline_live_council_shadow'
            print(f"SHADOW MODE: Live {p_baseline:.3f}, Council suggests {zen_result['p_final']:.3f}")
        
        # Compile summary
        result['summary'] = {
            'baseline_prob': p_baseline,
            'zen_adjusted_prob': zen_result['p_final'],
            'final_prob': result['p_final'],
            'mode': result['mode'],
            'zen_rules_applied': len(zen_result['active_rules']),
            'impact_triggers_fired': len(result['impact_triggers']),
            'news_enabled': self.news_enabled,
            'council_active': self.council_active,
            'total_band_adjustment': result['impact_adjusted_band_widen'],
            'total_confidence_adjustment': result['impact_adjusted_confidence_reduction']
        }
        
        return result
    
    def write_comprehensive_artifacts(self, forecast_result, output_dir='audit_exports'):
        """Write comprehensive artifacts from integrated forecast"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        audit_dir = Path(output_dir) / 'daily' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        artifacts = {}
        
        # Write integrated forecast report
        integrated_report = self._write_integrated_report(forecast_result, audit_dir)
        artifacts['integrated_report'] = integrated_report
        
        # Write individual component artifacts if they exist
        if 'news_ingestion' in forecast_result['components']:
            news_artifacts = self.news_ingestion.write_daily_artifacts(
                forecast_result['components']['news_ingestion'], 
                output_dir
            )
            artifacts.update(news_artifacts)
        
        if 'impact_engine' in forecast_result['components']:
            impact_artifacts = self.impact_engine.write_impact_artifacts(
                forecast_result['components']['impact_engine'],
                output_dir
            )
            artifacts.update(impact_artifacts)
        
        return artifacts
    
    def _write_integrated_report(self, forecast_result, audit_dir):
        """Write integrated forecast report"""
        report_file = audit_dir / 'INTEGRATED_COUNCIL_REPORT.md'
        
        summary = forecast_result['summary']
        mode = forecast_result['mode']
        
        content = f"""# Integrated Council Forecast Report

**Date**: {forecast_result['target_date']}
**Timestamp**: {forecast_result['timestamp'].strftime('%Y-%m-%d %H:%M:%S UTC')}
**Mode**: {mode} ({'Council adjustments applied' if mode == 'LIVE' else 'Baseline live, Council shadow logged'})

## Executive Summary

### Probability Flow
- **Baseline (p0)**: {summary['baseline_prob']:.3f} ← Original forecast
- **Zen Adjusted**: {summary['zen_adjusted_prob']:.3f} ← Council mathematical adjustment  
- **Final Decision**: {summary['final_prob']:.3f} ← {"Live probability" if mode == 'LIVE' else "Baseline (shadow mode)"}

### System Status
- **Council Active**: {summary['council_active']} ({'Live adjustments' if summary['council_active'] else 'Shadow logging only'})
- **News Engine**: {summary['news_enabled']} ({'Enabled' if summary['news_enabled'] else 'Disabled'})

## Component Analysis

### Zen Council v0.1
"""
        
        zen_result = forecast_result['components']['zen_council']
        content += f"""- **Rules Applied**: {len(zen_result['active_rules'])}
- **Probability Adjustment**: {zen_result['p_final'] - zen_result['p_baseline']:+.3f}
- **Base Confidence Reduction**: {zen_result.get('conf_reduction_pct', 0):.1f}%
- **Base Band Widening**: {zen_result.get('band_widen_pct', 0):.1f}%

**Active Rules:**
"""
        
        if zen_result['active_rules']:
            for rule in zen_result['active_rules']:
                content += f"- {rule}\n"
        else:
            content += "- No rules triggered\n"
        
        if summary['news_enabled']:
            impact_result = forecast_result['components']['impact_engine']
            content += f"""
### Event-Impact Engine v0.1
- **News Score**: {impact_result['summary']['news_score']:.3f}
- **Macro Z-Score**: {impact_result['summary']['macro_z_score']:.2f}
- **Impact Triggers**: {len(forecast_result['impact_triggers'])}
- **Band Impact**: {impact_result['adjustments']['band_adjustment_pct']:+.1f}%
- **Confidence Impact**: {impact_result['adjustments']['confidence_adjustment_pct']:+.1f}%

**Impact Triggers:**
"""
            
            if forecast_result['impact_triggers']:
                for trigger in forecast_result['impact_triggers']:
                    content += f"- {trigger}\n"
            else:
                content += "- No impact triggers fired\n"
        else:
            content += f"""
### Event-Impact Engine v0.1
- **Status**: DISABLED (NEWS_ENABLED=false)
- **Impact**: No news/macro adjustments applied
"""
        
        content += f"""
## Final Adjustments Applied

### {"Live Adjustments" if mode == 'LIVE' else "Shadow Adjustments (Not Applied)"}
- **Total Band Widening**: {forecast_result['impact_adjusted_band_widen']:+.1f}%
- **Total Confidence Reduction**: {forecast_result['impact_adjusted_confidence_reduction']:+.1f}%
- **Decision Logic**: {forecast_result['decision']}

### Safety Controls
- **Council Gate**: {'OPEN' if forecast_result['council_active'] else 'CLOSED'} (COUNCIL_ACTIVE={forecast_result['council_active']})
- **News Gate**: {'OPEN' if forecast_result['news_enabled'] else 'CLOSED'} (NEWS_ENABLED={forecast_result['news_enabled']})
- **Mode**: {mode} ({'Production impact' if mode == 'LIVE' else 'Shadow logging only'})

## Audit Trail

### System Components
1. **Zen Council**: Mathematical forecast calibration and adjustment
2. **Event-Impact Engine**: News sentiment and macro surprise analysis  
3. **Macro Gates**: Traditional high-impact event detection
4. **Safety Controls**: SHADOW mode and activation gates

### Artifacts Generated
- INTEGRATED_COUNCIL_REPORT.md (this file)
- NEWS_FEED_LOG.md (if news enabled)
- MACRO_EVENTS.md (if news enabled)  
- NEWS_SCORE.md (if news enabled)
- ZEN_COUNCIL_EXPLAIN.md (appended)

---
Generated by Integrated Council Engine v0.1
**SHADOW SAFE**: {f"All adjustments applied to live forecast" if mode == 'LIVE' else "Adjustments logged only - baseline probability remains live"}
"""
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return str(report_file)


def main():
    """Test integrated council engine"""
    engine = IntegratedCouncilEngine()
    
    # Test forecast
    p_baseline = 0.58
    
    # Process complete forecast
    result = engine.process_complete_forecast(p_baseline)
    
    # Write artifacts
    artifacts = engine.write_comprehensive_artifacts(result)
    
    print(f"\nIntegrated Council Engine complete!")
    print(f"Mode: {result['mode']}")
    print(f"Baseline: {result['p_baseline']:.3f}")
    print(f"Final: {result['p_final']:.3f}")
    print(f"Artifacts: {list(artifacts.values())}")
    
    return result, artifacts


if __name__ == '__main__':
    main()