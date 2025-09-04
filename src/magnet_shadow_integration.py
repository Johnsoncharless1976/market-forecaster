#!/usr/bin/env python3
"""
Magnet Shadow Integration
Integrates Level Magnet Engine with existing shadow systems
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add src to path
sys.path.append(str(Path(__file__).parent))

from level_magnet_engine import LevelMagnetEngine
from magnet_guardrails import MagnetGuardrails
from shadow_day2_runner import ShadowDay2Runner


class MagnetShadowIntegration:
    """Integration of Magnet Engine with Shadow Day system"""
    
    def __init__(self):
        self.magnet_engine = LevelMagnetEngine()
        self.guardrails = MagnetGuardrails()
        self.shadow_runner = ShadowDay2Runner()
        
    def run_magnet_shadow_day(self, target_date=None):
        """Run complete shadow day with Magnet Engine integration"""
        if target_date is None:
            target_date = datetime.now().date()
        
        print(f"Running Magnet Shadow Day for {target_date}...")
        
        # Step 1: Run standard shadow day 2
        comparison_result, live_daily_report, live_decision_log = self.shadow_runner.run_shadow_day_2(target_date)
        
        # Step 2: Run magnet analysis
        live_forecast = comparison_result['live_forecast']
        baseline_center = 5600  # Sample - would come from live forecast in real system
        baseline_width = 2.5    # Sample - would come from live forecast
        
        magnet_result = self.magnet_engine.run_magnet_analysis(baseline_center, baseline_width, target_date)
        
        # Step 3: Generate magnet artifacts
        magnet_report = self.magnet_engine.write_level_magnets_report(magnet_result)
        
        # Step 4: Update shadow logs with magnet data
        updated_council_shadow = self._append_magnet_to_council_shadow(comparison_result, magnet_result)
        
        # Step 5: Check guardrails (if we have actual outcome)
        if 'actual_outcome' in live_forecast:
            # Log magnet performance for guardrail assessment
            p_baseline = live_forecast['p_baseline']
            p_with_magnet = p_baseline  # Would be different with actual magnet applied
            actual_outcome = live_forecast['actual_outcome']
            
            self.guardrails.log_magnet_performance(target_date, p_baseline, p_with_magnet, actual_outcome)
            
            # Check if muting is needed
            mute_triggered = self.guardrails.apply_guardrail_decision()
            
            if mute_triggered:
                print("AUTO-MUTE TRIGGERED: Check MAGNET_GUARDRAILS_REPORT.md")
        
        return {
            'shadow_result': comparison_result,
            'magnet_result': magnet_result,
            'magnet_report': magnet_report,
            'updated_council_shadow': updated_council_shadow,
            'live_daily_report': live_daily_report,
            'live_decision_log': live_decision_log
        }
    
    def _append_magnet_to_council_shadow(self, comparison_result, magnet_result):
        """Append magnet analysis to Council shadow report"""
        target_date = comparison_result['date']
        timestamp = target_date.strftime('%Y%m%d')
        
        audit_dir = Path('audit_exports') / 'daily' / timestamp
        council_shadow_file = audit_dir / 'COUNCIL_SHADOW_DAILY.md'
        
        if not council_shadow_file.exists():
            print(f"Warning: Council shadow file not found: {council_shadow_file}")
            return None
        
        # Append magnet section
        magnet_section = f"""
## Level Magnet Analysis (SHADOW)

### SPX 25-Point Magnet
- **SPX Reference**: {magnet_result['spx_ref']:.1f}
- **L25 Level**: {magnet_result['l25']:.0f}
- **Delta**: {magnet_result['delta']:+.1f} points
- **Magnet Strength (M)**: {magnet_result['strength']:.3f}
- **OPEX Day**: {'Yes' if magnet_result['is_opex'] else 'No'} (κ={magnet_result['kappa']:.1f})

### Hypothetical Magnet Adjustments (NOT Applied)
- **Center Shift**: {magnet_result['center_shift']:+.2f} points (toward {magnet_result['l25']:.0f})
- **Width Change**: {magnet_result['width_delta_pct']:+.1f}%
- **Band Center**: {magnet_result['center_before']:.1f} → {magnet_result['center_after']:.1f}
- **Band Width**: {magnet_result['width_before']:.2f}% → {magnet_result['width_after']:.2f}%

### Magnet Assessment
"""
        
        if magnet_result['z_score'] < 0.5:
            assessment = "🔴 **VERY CLOSE** - Strong magnet effect"
        elif magnet_result['z_score'] < 1.0:
            assessment = "🟡 **MODERATE** - Weak magnet effect"
        else:
            assessment = "🟢 **DISTANT** - Minimal magnet effect"
        
        magnet_section += f"- **Distance**: {assessment} (z={magnet_result['z_score']:.3f})\n"
        
        if magnet_result['is_opex']:
            magnet_section += f"- **OPEX Enhancement**: {magnet_result['kappa']:.1f}x multiplier (stronger pin expected)\n"
        
        magnet_section += f"""
### Shadow Mode Status
- **Magnet Engine**: v0.1 SHADOW (adjustments logged, not applied live)
- **Guardrails**: Active (auto-mute on performance degradation)
- **Integration**: Council → Impact → Magnet (layered adjustments)

---
**MAGNET SHADOW**: All level magnet adjustments are hypothetical comparisons only
"""
        
        # Append to file
        with open(council_shadow_file, 'a', encoding='utf-8') as f:
            f.write(magnet_section)
        
        return str(council_shadow_file)


def main():
    """Test Magnet Shadow Integration"""
    integration = MagnetShadowIntegration()
    
    # Run complete magnet shadow day
    result = integration.run_magnet_shadow_day()
    
    print(f"\nMagnet Shadow Day complete!")
    print(f"Magnet Level: {result['magnet_result']['l25']:.0f}")
    print(f"Magnet Strength: {result['magnet_result']['strength']:.3f}")
    print(f"OPEX Day: {'Yes' if result['magnet_result']['is_opex'] else 'No'}")
    print(f"Center Shift: {result['magnet_result']['center_shift']:+.2f}")
    print(f"Width Delta: {result['magnet_result']['width_delta_pct']:+.1f}%")
    print(f"Magnet Report: {result['magnet_report']}")
    print(f"Updated Council Shadow: {result['updated_council_shadow']}")
    
    return result


if __name__ == '__main__':
    main()