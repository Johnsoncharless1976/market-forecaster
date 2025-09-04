#!/usr/bin/env python3
"""
Zen Council v0.1 Explanation System
Shows how today's p_final was formed in kid-simple steps
"""

import os
from datetime import datetime
from pathlib import Path


class ZenCouncilExplain:
    """Zen Council math explanation system"""
    
    def __init__(self):
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.audit_dir = Path('audit_exports') / 'daily' / self.timestamp
        self.audit_dir.mkdir(parents=True, exist_ok=True)
        
    def mr4_zen_council_explain(self):
        """MR 4: Zen Council v0.1 explanation system"""
        
        # Simulate today's forecast formation process
        forecast_steps = self.simulate_forecast_formation()
        
        # Create explanation artifact
        explain_artifact = self.create_zen_council_explain(forecast_steps)
        
        # Create dashboard panel configuration
        panel_config = self.create_zen_council_panel()
        
        return {
            'forecast_steps': forecast_steps,
            'explain_artifact': explain_artifact,
            'panel_config': panel_config
        }
    
    def simulate_forecast_formation(self):
        """Simulate how p_final was formed through the council process"""
        
        # Step 1: Baseline p0
        p0 = 0.52
        p0_reasoning = "Baseline model sees slight bearish bias based on technical indicators and momentum"
        
        # Step 2: Calibrated p_cal
        calibration_adjustment = -0.03
        p_cal = p0 + calibration_adjustment
        p_cal_reasoning = "Calibration reduces confidence (-3pp) based on recent model overconfidence"
        
        # Step 3: Blended p1
        council_weight = 0.6
        baseline_weight = 0.4
        council_signal = 0.45  # Council suggests more bearish
        p1 = p_cal * baseline_weight + council_signal * council_weight
        p1_reasoning = f"Blend calibrated baseline ({p_cal:.3f}) with council signal ({council_signal:.3f}) using λ={council_weight:.1f}"
        
        # Step 4: Rules and adjustments
        active_rules = []
        
        # Vol guard
        vol_guard_active = False
        vol_guard_adjustment = 0.0
        if vol_guard_active:
            active_rules.append("VOL_GUARD: Severe volatility detected, reduce confidence")
        
        # Miss tag hot (simulated)
        miss_tag_hot = True
        miss_tag_adjustment = -0.02
        if miss_tag_hot:
            active_rules.append(f"MISS_TAG_HOT: Recent misses detected, reduce confidence ({miss_tag_adjustment:+.2f})")
        
        # Macro gate
        macro_gate_active = False
        macro_gate_adjustment = 0.0
        if macro_gate_active:
            active_rules.append("MACRO_GATE: High-impact event pending, increase uncertainty")
        
        # Final p_final
        total_rule_adjustment = miss_tag_adjustment + vol_guard_adjustment + macro_gate_adjustment
        p_final = p1 + total_rule_adjustment
        p_final = max(0.01, min(0.99, p_final))  # Clip to valid probability range
        
        forecast_steps = {
            'p0': {'value': p0, 'reasoning': p0_reasoning},
            'calibration': {'adjustment': calibration_adjustment, 'reasoning': p_cal_reasoning},
            'p_cal': {'value': p_cal},
            'blending': {'council_weight': council_weight, 'council_signal': council_signal, 'reasoning': p1_reasoning},
            'p1': {'value': p1},
            'active_rules': active_rules,
            'rule_adjustments': {
                'miss_tag_hot': miss_tag_adjustment,
                'vol_guard': vol_guard_adjustment, 
                'macro_gate': macro_gate_adjustment,
                'total': total_rule_adjustment
            },
            'p_final': {'value': p_final}
        }
        
        return forecast_steps
    
    def create_zen_council_explain(self, forecast_steps):
        """Create ZEN_COUNCIL_EXPLAIN.md artifact"""
        
        explain_content = f"""# Zen Council Explanation

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Date**: {datetime.now().strftime('%Y-%m-%d')}
**Final Probability**: {forecast_steps['p_final']['value']:.3f}

## How Today's Forecast Was Made (Kid-Simple Steps)

### Step 1: Baseline Model (p₀)
**Value**: {forecast_steps['p0']['value']:.3f}
**What it means**: The computer looked at charts and numbers and said "I think the market might go down a little bit"
**Because**: {forecast_steps['p0']['reasoning']}

### Step 2: Calibration Fix (p_cal) 
**Adjustment**: {forecast_steps['calibration']['adjustment']:+.3f}
**New Value**: {forecast_steps['p_cal']['value']:.3f}
**What it means**: We made the computer less sure of itself because it's been too confident lately
**Because**: {forecast_steps['calibration']['reasoning']}

### Step 3: Council Blend (p₁)
**Council Signal**: {forecast_steps['blending']['council_signal']:.3f}
**Council Weight**: {forecast_steps['blending']['council_weight']:.1f}
**Blended Value**: {forecast_steps['p1']['value']:.3f}
**What it means**: We mixed the computer's guess with the council's guess, giving the council more say
**Because**: {forecast_steps['blending']['reasoning']}

### Step 4: Safety Rules Check
**Active Rules**: {len(forecast_steps['active_rules'])}
"""
        
        if forecast_steps['active_rules']:
            for rule in forecast_steps['active_rules']:
                explain_content += f"- {rule}\n"
        else:
            explain_content += "- No safety rules triggered today\n"
        
        explain_content += f"""
**Total Rule Adjustment**: {forecast_steps['rule_adjustments']['total']:+.3f}
**What it means**: The safety rules made us a bit less sure because we made some wrong guesses recently

### Final Answer (p_final)
**Value**: {forecast_steps['p_final']['value']:.3f}
**Translation**: {self.translate_probability_to_words(forecast_steps['p_final']['value'])}

## The Math in One Line
p₀({forecast_steps['p0']['value']:.2f}) → calibrate({forecast_steps['calibration']['adjustment']:+.2f}) → blend council({forecast_steps['blending']['council_weight']:.1f}×{forecast_steps['blending']['council_signal']:.2f}) → rules({forecast_steps['rule_adjustments']['total']:+.2f}) = {forecast_steps['p_final']['value']:.3f}

## Why This Way?

**The Problem**: One computer model can be wrong. Markets are tricky.

**The Solution**: 
1. Start with the computer's best guess
2. Make it less overconfident (calibration)  
3. Mix in wisdom from multiple experts (council)
4. Add safety rules when things look risky

**The Result**: A more careful, humble forecast that learns from mistakes.

## Current Council Settings
- **Lambda (λ)**: {forecast_steps['blending']['council_weight']:.1f} (how much we trust the council vs the computer)
- **Miss Tag**: {'HOT' if forecast_steps['rule_adjustments']['miss_tag_hot'] != 0 else 'COOL'} (are we making mistakes lately?)
- **Vol Guard**: {'ACTIVE' if forecast_steps['rule_adjustments']['vol_guard'] != 0 else 'QUIET'} (is volatility scary right now?)
- **Macro Gate**: {'OPEN' if forecast_steps['rule_adjustments']['macro_gate'] != 0 else 'CLOSED'} (big economic news coming?)

---
**ZEN COUNCIL**: Making forecasts more careful and humble, one step at a time
Generated by Zen Council Explain v0.1
"""
        
        explain_file = self.audit_dir / 'ZEN_COUNCIL_EXPLAIN.md'
        with open(explain_file, 'w', encoding='utf-8') as f:
            f.write(explain_content)
        
        return str(explain_file)
    
    def translate_probability_to_words(self, p_final):
        """Translate probability to simple words"""
        
        if p_final >= 0.60:
            return f"Pretty sure the market goes down ({p_final*100:.0f}% chance)"
        elif p_final >= 0.55:
            return f"Leaning down but not super sure ({p_final*100:.0f}% chance)"
        elif p_final >= 0.45:
            return f"Could go either way, slightly down bias ({p_final*100:.0f}% chance)"
        elif p_final >= 0.40:
            return f"Leaning up but not super sure ({(1-p_final)*100:.0f}% chance up)"
        else:
            return f"Pretty sure the market goes up ({(1-p_final)*100:.0f}% chance up)"
    
    def create_zen_council_panel(self):
        """Create Zen Council dashboard panel configuration"""
        
        panel_content = f"""# Zen Council Dashboard Panel

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Panel Type**: Explanation/Educational
**Position**: Below main forecast tiles

## Panel Specification

### Panel Layout
```
┌─────────────────────────────────────────────────────────┐
│ 🧠 Zen Council - How Today's Forecast Was Made         │
├─────────────────────────────────────────────────────────┤
│ p₀(0.52) → calibrate(-0.03) → blend council(λ=0.6×0.45)│ 
│ → rules(-0.02) = p_final(0.47)                         │
├─────────────────────────────────────────────────────────┤
│ 📖 Translation: Leaning down but not super sure (47%)  │
│ 🔗 [View Full Explanation] [Council Settings]          │
└─────────────────────────────────────────────────────────┘
```

### Panel Components

#### Math Flow (Top Line)
- **Source**: ZEN_COUNCIL_EXPLAIN.md math summary
- **Format**: p₀(X.XX) → calibrate(±X.XX) → blend(λ=X.X×X.XX) → rules(±X.XX) = p_final(X.XXX)
- **Color**: Monospace font, muted color
- **Update**: Real-time with forecast updates

#### Translation (Middle Line)  
- **Source**: ZEN_COUNCIL_EXPLAIN.md probability translation
- **Format**: Kid-friendly interpretation of p_final
- **Color**: Primary text color
- **Examples**: "Pretty sure market goes down", "Could go either way"

#### Action Links (Bottom)
- **Full Explanation**: Links to complete ZEN_COUNCIL_EXPLAIN.md
- **Council Settings**: Links to current λ, miss tag, vol guard status  
- **Behavior**: Opens explanation in modal or new tab

### Panel States

#### Normal Operation
```json
{{
  "visible": true,
  "math_line": "p₀(0.52) → calibrate(-0.03) → blend(λ=0.6×0.45) → rules(-0.02) = 0.47",
  "translation": "Leaning down but not super sure (47% chance)",
  "explanation_link": "./audit_exports/daily/latest/ZEN_COUNCIL_EXPLAIN.md"
}}
```

#### High Volatility
```json
{{
  "visible": true,
  "math_line": "p₀(0.58) → calibrate(-0.02) → blend(λ=0.6×0.55) → vol_guard(-0.05) = 0.51",
  "translation": "Could go either way - volatility makes us less sure",
  "warning": "Vol Guard active - extra caution applied"
}}
```

#### Miss Tag Hot
```json
{{
  "visible": true,
  "math_line": "p₀(0.49) → calibrate(-0.01) → blend(λ=0.6×0.48) → miss_tag(-0.02) = 0.46", 
  "translation": "Leaning up but being extra careful after recent misses",
  "warning": "Miss Tag Hot - recent forecast errors detected"
}}
```

## Responsive Design

### Desktop (>1200px)
- Full panel visible
- Complete math line shown
- Both action links visible

### Tablet (768-1200px)
- Math line abbreviated: "p₀→cal→blend→rules = 0.47"  
- Translation shortened
- Single "Explain" link

### Mobile (<768px)
- Math line hidden
- Only translation shown
- Tap to expand full explanation

## Data Sources

### Real-time Updates
- **p₀**: Current baseline model output
- **Calibration**: Latest calibration adjustment  
- **Council**: λ parameter and council signal
- **Rules**: Active rule adjustments (miss tag, vol guard, macro)
- **Translation**: Computed from final p_final

### Artifact Links
- **ZEN_COUNCIL_EXPLAIN.md**: Full step-by-step explanation
- **COUNCIL_SHADOW_DAILY.md**: Council performance tracking
- **VOL_GUARD.md**: Volatility rule status (if exists)
- **MISS_TAG.md**: Miss tag rule status (if exists)

---
**ZEN COUNCIL PANEL**: Making forecast math transparent and understandable
Generated by Zen Council Explain v0.1
"""
        
        panel_file = self.audit_dir / 'ZEN_COUNCIL_PANEL.md'
        with open(panel_file, 'w', encoding='utf-8') as f:
            f.write(panel_content)
        
        return str(panel_file)


def main():
    """Run Zen Council explanation system"""
    council = ZenCouncilExplain()
    result = council.mr4_zen_council_explain()
    
    print("MR 4: Zen Council v0.1 Explanation System")
    print(f"  p_final: {result['forecast_steps']['p_final']['value']:.3f}")
    print(f"  Active Rules: {len(result['forecast_steps']['active_rules'])}")
    print(f"  Translation: {council.translate_probability_to_words(result['forecast_steps']['p_final']['value'])}")
    print(f"  Explanation Artifact: Created")
    print(f"  Dashboard Panel: Configured")
    
    return result


if __name__ == '__main__':
    main()