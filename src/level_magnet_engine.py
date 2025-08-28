#!/usr/bin/env python3
"""
Level Magnet Engine v0.1
SPX 25-point magnet levels with OPEX awareness (SHADOW-only)
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import math
import yfinance as yf


class LevelMagnetEngine:
    """SPX 25-point magnet engine with OPEX awareness"""
    
    def __init__(self):
        self.tau = 0.5  # Strength decay parameter
        self.gamma = 0.30  # Center nudge strength
        self.beta = 0.07  # Width tightening strength
        self.max_center_shift_atr = 0.10  # Max center shift as fraction of ATR
        self.max_width_tighten_pct = 0.10  # Max width tightening (10%)
        
        # OPEX detection
        self.opex_multiplier = 1.5  # Stronger on OPEX days
        
        # Auto-mute parameters
        self.mute_brier_threshold = 0.02  # 2% Brier degradation
        self.shadow_period_days = 10
        
    def get_spx_reference_price(self, target_date=None):
        """Get SPX reference price (08:45 ET ES price or prev close)"""
        if target_date is None:
            target_date = datetime.now().date()
        
        try:
            # Try to get ES futures price first (ES=F)
            es = yf.Ticker("ES=F")
            es_data = es.history(period="2d")
            
            if not es_data.empty:
                # Use latest close as reference (08:45 ET equivalent)
                spx_ref = es_data['Close'].iloc[-1]
                data_source = "ES_futures"
            else:
                # Fallback to SPX
                spx = yf.Ticker("^GSPC")
                spx_data = spx.history(period="2d")
                spx_ref = spx_data['Close'].iloc[-1]
                data_source = "SPX_close"
                
        except Exception as e:
            print(f"Error fetching SPX/ES data: {e}")
            # Fallback to approximate current level
            spx_ref = 5600.0  # Reasonable default for 2025
            data_source = "fallback"
        
        return float(spx_ref), data_source
    
    def get_atr14(self, target_date=None):
        """Get 14-day ATR for SPX"""
        if target_date is None:
            target_date = datetime.now().date()
        
        try:
            spx = yf.Ticker("^GSPC")
            # Get 20 days to calculate 14-day ATR
            spx_data = spx.history(period="20d")
            
            if len(spx_data) < 14:
                return 50.0  # Fallback ATR
            
            # Calculate True Range
            spx_data['prev_close'] = spx_data['Close'].shift(1)
            spx_data['tr1'] = spx_data['High'] - spx_data['Low']
            spx_data['tr2'] = abs(spx_data['High'] - spx_data['prev_close'])
            spx_data['tr3'] = abs(spx_data['Low'] - spx_data['prev_close'])
            spx_data['true_range'] = spx_data[['tr1', 'tr2', 'tr3']].max(axis=1)
            
            # 14-day ATR
            atr14 = spx_data['true_range'].rolling(14).mean().iloc[-1]
            
            return float(atr14)
            
        except Exception as e:
            print(f"Error calculating ATR: {e}")
            return 50.0  # Fallback
    
    def is_opex_day(self, target_date):
        """Check if target date is an OPEX day (Mon/Wed/Fri SPX weeklies or 3rd Friday monthly)"""
        if target_date.weekday() in [0, 2, 4]:  # Mon, Wed, Fri
            return True
        
        # Check 3rd Friday of month
        if target_date.weekday() == 4:  # Friday
            # Find 3rd Friday of the month
            first_day = target_date.replace(day=1)
            first_friday = first_day + timedelta(days=(4 - first_day.weekday()) % 7)
            third_friday = first_friday + timedelta(days=14)
            
            if target_date == third_friday:
                return True
        
        return False
    
    def calculate_magnet_level(self, spx_ref):
        """Calculate nearest 25-point level"""
        return 25 * round(spx_ref / 25)
    
    def calculate_magnet_strength(self, delta, atr14):
        """Calculate magnet strength M = exp(-z/tau) where z = |delta|/ATR14"""
        z = abs(delta) / atr14
        strength = math.exp(-z / self.tau)
        return strength, z
    
    def apply_magnet_adjustments(self, center, width, spx_ref, l25, atr14, is_opex):
        """Apply magnet adjustments to band center and width"""
        # Calculate magnet parameters
        delta = spx_ref - l25
        strength, z_score = self.calculate_magnet_strength(delta, atr14)
        kappa = self.opex_multiplier if is_opex else 1.0
        
        # Store originals
        center_before = center
        width_before = width
        
        # Center nudge toward L25 (negative delta means nudge up)
        center_shift = (-delta) * self.gamma * strength * kappa
        
        # Cap center shift
        max_shift = self.max_center_shift_atr * atr14
        center_shift = max(min(center_shift, max_shift), -max_shift)
        
        center_after = center + center_shift
        
        # Width tightening near strong magnet
        width_tighten_factor = self.beta * strength * kappa
        width_tighten_factor = min(width_tighten_factor, self.max_width_tighten_pct)
        
        width_after = width * (1 - width_tighten_factor)
        width_delta_pct = (width_after - width_before) / width_before * 100
        
        return {
            'center_before': center_before,
            'center_after': center_after,
            'center_shift': center_shift,
            'width_before': width_before,
            'width_after': width_after,
            'width_delta_pct': width_delta_pct,
            'l25': l25,
            'delta': delta,
            'z_score': z_score,
            'strength': strength,
            'kappa': kappa,
            'is_opex': is_opex
        }
    
    def run_magnet_analysis(self, baseline_center, baseline_width, target_date=None):
        """Run complete magnet analysis for a given date"""
        if target_date is None:
            target_date = datetime.now().date()
        
        # Get SPX reference and ATR
        spx_ref, data_source = self.get_spx_reference_price(target_date)
        atr14 = self.get_atr14(target_date)
        
        # Calculate magnet level
        l25 = self.calculate_magnet_level(spx_ref)
        
        # Check OPEX
        is_opex = self.is_opex_day(target_date)
        
        # Apply magnet adjustments
        adjustments = self.apply_magnet_adjustments(
            baseline_center, baseline_width, spx_ref, l25, atr14, is_opex
        )
        
        # Compile result
        result = {
            'date': target_date,
            'spx_ref': spx_ref,
            'data_source': data_source,
            'atr14': atr14,
            **adjustments
        }
        
        return result
    
    def write_level_magnets_report(self, magnet_result, output_dir='audit_exports'):
        """Write LEVEL_MAGNETS.md report"""
        target_date = magnet_result['date']
        timestamp = target_date.strftime('%Y%m%d')
        
        audit_dir = Path(output_dir) / 'daily' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        report_file = audit_dir / 'LEVEL_MAGNETS.md'
        
        content = f"""# Level Magnet Engine Report v0.1

**Date**: {target_date}
**Mode**: SHADOW (adjustments logged, not applied to live)
**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

## SPX 25-Point Magnet Analysis

### Reference Data
- **SPX Reference**: {magnet_result['spx_ref']:.1f} (source: {magnet_result['data_source']})
- **ATR(14)**: {magnet_result['atr14']:.2f}
- **Nearest L25 Level**: {magnet_result['l25']:.0f}
- **Delta (SPX - L25)**: {magnet_result['delta']:+.1f} points

### Magnet Strength
- **Distance (z-score)**: {magnet_result['z_score']:.3f} (|Δ|/ATR)
- **Raw Strength (M)**: {magnet_result['strength']:.3f} (exp(-z/τ), τ={self.tau})
- **OPEX Flag**: {'Yes' if magnet_result['is_opex'] else 'No'}
- **OPEX Multiplier (κ)**: {magnet_result['kappa']:.1f}
- **Effective Strength**: {magnet_result['strength'] * magnet_result['kappa']:.3f}

### Shadow Adjustments (NOT Applied Live)

#### Band Center Nudge
- **Center Before**: {magnet_result['center_before']:.1f}
- **Center Shift**: {magnet_result['center_shift']:+.2f} points (toward L25)
- **Center After**: {magnet_result['center_after']:.1f}
- **Shift Formula**: (-Δ) × γ × M × κ, γ={self.gamma}, cap ≤{self.max_center_shift_atr}×ATR

#### Band Width Tightening
- **Width Before**: {magnet_result['width_before']:.2f}%
- **Width After**: {magnet_result['width_after']:.2f}%
- **Width Delta**: {magnet_result['width_delta_pct']:+.1f}%
- **Tighten Formula**: w × (1 - β×M×κ), β={self.beta}, cap ≤{self.max_width_tighten_pct*100:.0f}%

## Magnet Assessment

### Distance Analysis
"""
        
        if magnet_result['z_score'] < 0.5:
            assessment = "🔴 **VERY CLOSE** - Strong magnet effect expected"
        elif magnet_result['z_score'] < 1.0:
            assessment = "🟡 **MODERATE** - Weak magnet effect"
        else:
            assessment = "🟢 **DISTANT** - Minimal magnet effect"
        
        content += f"- **Assessment**: {assessment}\n"
        
        if magnet_result['is_opex']:
            content += f"- **OPEX Enhancement**: {magnet_result['kappa']:.1f}x multiplier applied (stronger pin expected)\n"
        
        content += f"""
### Impact Summary
- **Net Center Move**: {magnet_result['center_shift']:+.2f} points toward {magnet_result['l25']:.0f}
- **Net Width Change**: {magnet_result['width_delta_pct']:+.1f}% (tighter bands)
- **Risk Profile**: {'Enhanced OPEX pinning' if magnet_result['is_opex'] else 'Standard magnet attraction'}

## Technical Parameters

### Magnet Engine Settings
- **Decay Constant (τ)**: {self.tau} (strength decay rate)
- **Center Nudge (γ)**: {self.gamma} (attraction strength)
- **Width Tighten (β)**: {self.beta} (band compression)
- **Max Center Shift**: ±{self.max_center_shift_atr*100:.0f}% ATR
- **Max Width Tighten**: {self.max_width_tighten_pct*100:.0f}%

### OPEX Calendar
- **Weekly OPEX**: Monday, Wednesday, Friday (SPX weeklies)
- **Monthly OPEX**: 3rd Friday of month
- **Today's Status**: {'OPEX Day' if magnet_result['is_opex'] else 'Non-OPEX Day'}

---
**SHADOW MODE**: All magnet adjustments are hypothetical and NOT applied to live forecasts
Generated by Level Magnet Engine v0.1
"""
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return str(report_file)


def main():
    """Test Level Magnet Engine"""
    engine = LevelMagnetEngine()
    
    # Sample baseline parameters (would come from Council/Impact in real use)
    baseline_center = 5620.0  # Sample SPX center
    baseline_width = 2.5  # Sample width percentage
    
    # Run magnet analysis
    magnet_result = engine.run_magnet_analysis(baseline_center, baseline_width)
    
    # Write report
    report_file = engine.write_level_magnets_report(magnet_result)
    
    print(f"Magnet analysis complete!")
    print(f"SPX Reference: {magnet_result['spx_ref']:.1f}")
    print(f"L25 Level: {magnet_result['l25']:.0f}")
    print(f"Magnet Strength: {magnet_result['strength']:.3f}")
    print(f"OPEX Day: {'Yes' if magnet_result['is_opex'] else 'No'}")
    print(f"Center Shift: {magnet_result['center_shift']:+.2f}")
    print(f"Width Delta: {magnet_result['width_delta_pct']:+.1f}%")
    print(f"Report: {report_file}")
    
    return magnet_result


if __name__ == '__main__':
    main()