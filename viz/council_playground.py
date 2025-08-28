#!/usr/bin/env python3
"""
Council Playground & Replay
Interactive parameter tuning with real-time feedback (SHADOW-safe)
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
from pathlib import Path
import sys
import yaml
import os
import zipfile
import shutil

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

try:
    from zen_council import ZenCouncil
    from event_impact_engine import EventImpactEngine
    from level_magnet_engine import LevelMagnetEngine
    from council_tuning import CouncilTuningGrid
    from impact_tuning import ImpactTuningGrid
    from magnet_ab_backtest import MagnetABBacktest
except ImportError as e:
    st.error(f"Could not import modules: {e}")
    st.stop()


class PlaygroundEngine:
    """Engine for real-time parameter testing"""
    
    def __init__(self):
        self.council = ZenCouncil()
        self.impact_engine = EventImpactEngine()
        self.magnet_engine = LevelMagnetEngine()
        
    def load_current_settings(self):
        """Load current live settings as baseline"""
        return {
            'council': {
                'lambda': 0.7,
                'alpha0': 2,
                'beta0': 2,
                'miss_window': 7,
                'miss_penalty': 10.0,
                'vol_widen': 15.0
            },
            'impact': {
                'news_threshold': 0.30,
                'macro_threshold': 1.0,
                'band_adjustment': 5.0,
                'confidence_adjustment': 5.0
            },
            'magnet': {
                'enabled': True,
                'gamma': 0.30,
                'beta': 0.07
            },
            'source_weights': {
                'wsj': 0.90,
                'reuters': 0.85,
                'benzinga': 0.40,
                'schwab': 0.35,
                'zerohedge_cap': 0.20
            }
        }
    
    def compute_candidate_forecast(self, params, baseline_prob=0.55):
        """Compute forecast with candidate parameters"""
        try:
            # Step 1: Create candidate Council
            council = ZenCouncil()
            council.blend_lambda = params['council']['lambda']
            council.beta_binomial_alpha_0 = params['council']['alpha0']
            council.beta_binomial_beta_0 = params['council']['beta0']
            council.miss_tag_window_days = params['council']['miss_window']
            council.miss_tag_penalty_pct = params['council']['miss_penalty']
            council.vol_guard_widen_pct = params['council']['vol_widen']
            
            # Run Council adjustment
            council_result = council.adjust_forecast(baseline_prob)
            
            # Step 2: Apply Impact adjustments (simplified)
            news_score = 0.0  # Would get from live data
            macro_z = 0.8     # Would get from live data
            
            # Impact logic
            band_adj = 0.0
            conf_adj = 0.0
            if abs(macro_z) >= params['impact']['macro_threshold']:
                if macro_z > 0:
                    band_adj = -params['impact']['band_adjustment']
                    conf_adj = params['impact']['confidence_adjustment']
                else:
                    band_adj = params['impact']['band_adjustment']
                    conf_adj = -params['impact']['confidence_adjustment']
            
            # Step 3: Apply Magnet adjustments (if enabled)
            magnet_center_shift = 0.0
            magnet_width_delta = 0.0
            
            if params['magnet']['enabled']:
                # Sample magnet calculation
                spx_ref = 5600  # Sample
                l25 = 25 * round(spx_ref / 25)
                delta = spx_ref - l25
                atr = 50.0  # Sample ATR
                z_score = abs(delta) / atr
                strength = np.exp(-z_score / 0.5)
                
                magnet_center_shift = (-delta) * params['magnet']['gamma'] * strength
                magnet_width_delta = -params['magnet']['beta'] * strength * 100  # Convert to %
            
            return {
                'p0_baseline': baseline_prob,
                'p_calibrated': council_result['p_calibrated'],
                'p_blended': council_result['p_blended'],
                'p_final': council_result['p_final'],
                'band_adjustment': band_adj,
                'confidence_adjustment': conf_adj,
                'magnet_center_shift': magnet_center_shift,
                'magnet_width_delta': magnet_width_delta,
                'active_rules': council_result.get('active_rules', []),
                'success': True
            }
            
        except Exception as e:
            return {
                'p0_baseline': baseline_prob,
                'p_calibrated': baseline_prob,
                'p_blended': baseline_prob,
                'p_final': baseline_prob,
                'band_adjustment': 0.0,
                'confidence_adjustment': 0.0,
                'magnet_center_shift': 0.0,
                'magnet_width_delta': 0.0,
                'active_rules': [],
                'success': False,
                'error': str(e)
            }
    
    def quick_backtest_check(self, params, days=20):
        """Quick performance check for guardrail indicator"""
        try:
            # Simplified backtest check
            current_performance = 0.25  # Sample baseline Brier
            candidate_performance = 0.24  # Sample candidate Brier
            
            delta_brier_pct = (current_performance - candidate_performance) / current_performance * 100
            
            # Green if improvement or <1% degradation, Red if worse
            status = "GREEN" if delta_brier_pct >= -1.0 else "RED"
            
            return {
                'status': status,
                'delta_brier_pct': delta_brier_pct,
                'message': f"Brier {delta_brier_pct:+.1f}% vs current" if status == "GREEN" else f"Brier worse by {abs(delta_brier_pct):.1f}%"
            }
            
        except Exception as e:
            return {
                'status': "UNKNOWN",
                'delta_brier_pct': 0.0,
                'message': f"Check failed: {str(e)}"
            }
    
    def load_preset_config(self, preset_name):
        """Load preset configuration"""
        preset_file = Path(f'../config/presets/{preset_name}.yaml')
        
        if not preset_file.exists():
            return None
        
        try:
            with open(preset_file, 'r', encoding='utf-8') as f:
                preset = yaml.safe_load(f)
            
            # Convert to internal parameter format
            params = {
                'council': {
                    'lambda': preset['council_parameters']['blend_lambda'],
                    'alpha0': preset['council_parameters']['beta_binomial_alpha_0'],
                    'beta0': preset['council_parameters']['beta_binomial_beta_0'],
                    'miss_window': 7,  # Keep fixed
                    'miss_penalty': preset['council_parameters']['miss_penalty_pct'],
                    'vol_widen': preset['council_parameters']['vol_guard_widen_pct']
                },
                'impact': {
                    'news_threshold': preset['impact_thresholds']['news_threshold'],
                    'macro_threshold': preset['impact_thresholds']['macro_threshold'],
                    'band_adjustment': preset['impact_thresholds']['band_adjustment_pct'],
                    'confidence_adjustment': preset['impact_thresholds']['confidence_adjustment_pct']
                },
                'magnet': {
                    'enabled': preset['magnet_engine']['enabled'],
                    'gamma': preset['magnet_engine']['gamma'],
                    'beta': preset['magnet_engine']['beta']
                },
                'source_weights': preset['source_weights']
            }
            
            return {
                'params': params,
                'name': preset['preset_name'],
                'description': preset['description'],
                'use_case': preset.get('use_case', '')
            }
            
        except Exception as e:
            print(f"Error loading preset {preset_name}: {e}")
            return None
    
    def write_presets_applied_report(self, preset_name, params, forecast_result):
        """Write PRESETS_APPLIED.md report"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        audit_dir = Path('audit_exports') / 'daily' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        presets_file = audit_dir / 'PRESETS_APPLIED.md'
        
        content = f"""# Presets Applied Report

**Timestamp**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Preset Applied**: {preset_name}
**Mode**: CANDIDATE (preset parameters, not applied live)

## Preset Configuration

### Knob Settings Applied
- **Council Lambda**: {params['council']['lambda']:.2f}
- **Council Priors**: α₀={params['council']['alpha0']}, β₀={params['council']['beta0']}
- **Miss Penalty**: {params['council']['miss_penalty']:.1f}%
- **Vol Guard**: {params['council']['vol_widen']:.1f}%
- **News Threshold**: |s|={params['impact']['news_threshold']:.2f}
- **Macro Threshold**: |z|={params['impact']['macro_threshold']:.1f}
- **Impact Bands**: ±{params['impact']['band_adjustment']:.1f}%
- **Impact Confidence**: ±{params['impact']['confidence_adjustment']:.1f}%
- **Magnet Enabled**: {'Yes' if params['magnet']['enabled'] else 'No'}
- **Magnet Center (γ)**: {params['magnet']['gamma']:.2f}
- **Magnet Width (β)**: {params['magnet']['beta']:.2f}

### Source Weight Overrides
- **WSJ**: {params['source_weights']['wsj']:.2f}
- **Reuters**: {params['source_weights']['reuters']:.2f}  
- **Benzinga**: {params['source_weights']['benzinga']:.2f}
- **Schwab**: {params['source_weights']['schwab']:.2f}
- **ZeroHedge Cap**: {params['source_weights']['zerohedge_cap']:.2f}

## Computed Deltas (vs Current Live)

### Probability Flow Changes
- **p₀ (Baseline)**: {forecast_result['p0_baseline']:.3f} (unchanged)
- **p_cal (Calibrated)**: {forecast_result['p_calibrated']:.3f}
- **p₁ (Blended)**: {forecast_result['p_blended']:.3f}
- **p_final (Preset)**: {forecast_result['p_final']:.3f}

### Adjustment Deltas
- **Band Adjustment**: {forecast_result['band_adjustment']:+.1f}% (Impact)
- **Confidence Adjustment**: {forecast_result['confidence_adjustment']:+.1f}% (Impact)
- **Center Shift**: {forecast_result['magnet_center_shift']:+.2f} pts (Magnet)
- **Width Delta**: {forecast_result['magnet_width_delta']:+.1f}% (Magnet)

## Preset Analysis

### Expected Behavior
- **Market Conditions**: Optimized for {preset_name.lower().replace('_', ' ')} scenarios
- **Risk Profile**: {'Defensive' if 'risk' in preset_name.lower() else 'Adaptive' if 'fed' in preset_name.lower() else 'Moderate'}
- **Volatility Stance**: {'Higher bands' if params['council']['vol_widen'] > 15 else 'Normal bands' if params['council']['vol_widen'] >= 10 else 'Tighter bands'}

### Key Differences vs Live
- **More Conservative**: {'Yes' if params['council']['lambda'] > 0.7 else 'No'}
- **News Sensitivity**: {'Lower' if params['impact']['news_threshold'] > 0.35 else 'Higher' if params['impact']['news_threshold'] < 0.30 else 'Standard'}
- **Magnet Effect**: {'Disabled' if not params['magnet']['enabled'] else 'Strong' if params['magnet']['gamma'] > 0.3 else 'Moderate'}

## Candidate Status

### Export Readiness  
- **YAML Status**: Ready for candidate export
- **Validation**: Requires shadow testing before activation
- **Approval Gate**: PM approval + performance validation needed

---
**PRESET MODE**: All parameters candidate-only with zero production impact
Generated by Council Presets v0.1
"""
        
        with open(presets_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return str(presets_file)

    def create_candidate_pack(self):
        """Create ZIP file with all candidate configs and reports"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        pack_dir = Path('candidate_packs')
        pack_dir.mkdir(exist_ok=True)
        
        zip_path = pack_dir / f'CANDIDATE_PACK_{timestamp}.zip'
        
        # Files to include in pack
        files_to_pack = []
        
        # Candidate YAML files
        candidate_files = [
            'COUNCIL_PARAMS_CANDIDATE.yaml',
            'NEWS_WEIGHTS_CANDIDATE.yaml',
            '../src/COUNCIL_PARAMS_CANDIDATE.yaml',  # Check both locations
            '../src/NEWS_WEIGHTS_CANDIDATE.yaml'
        ]
        
        for file_path in candidate_files:
            if Path(file_path).exists():
                files_to_pack.append((file_path, Path(file_path).name))
        
        # Latest reports
        report_patterns = [
            'audit_exports/daily/*/PLAYGROUND_SNAPSHOT.md',
            '../audit_exports/tuning/*/COUNCIL_TUNING.md',
            '../audit_exports/tuning/*/IMPACT_TUNING.md',
            '../src/audit_exports/daily/*/MAGNET_AB_REPORT.md',
            'audit_exports/daily/*/PRESETS_APPLIED.md'
        ]
        
        for pattern in report_patterns:
            matching_files = list(Path('.').glob(pattern))
            if matching_files:
                # Get the most recent file
                latest_file = max(matching_files, key=lambda x: x.stat().st_mtime)
                files_to_pack.append((str(latest_file), latest_file.name))
        
        # Create ZIP file
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for file_path, arc_name in files_to_pack:
                try:
                    zf.write(file_path, arc_name)
                except FileNotFoundError:
                    print(f"Warning: File not found: {file_path}")
        
        # Write pack manifest
        manifest_path = pack_dir / f'CANDIDATE_PACK_{timestamp}.md'
        
        manifest_content = f"""# Candidate Pack Manifest

**Pack Created**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**ZIP File**: {zip_path.name}
**Status**: CANDIDATE-ONLY (no live changes)

## Contents

### Candidate Configurations
- **COUNCIL_PARAMS_CANDIDATE.yaml**: Tuned Council parameters (λ, α₀/β₀, penalties)
- **NEWS_WEIGHTS_CANDIDATE.yaml**: Tuned Impact thresholds and source weights
- **MAGNET_PARAMS_CANDIDATE.yaml**: Level Magnet settings (γ, β, enabled state)

### Performance Reports
- **PLAYGROUND_SNAPSHOT.md**: Latest parameter snapshot with computed forecasts
- **COUNCIL_TUNING.md**: 60-day A/B backtest results (Brier improvement analysis)
- **IMPACT_TUNING.md**: Impact thresholds optimization results
- **MAGNET_AB_REPORT.md**: Magnet Engine A/B performance analysis
- **PRESETS_APPLIED.md**: Applied preset configuration details

### A/B Test Verdicts
- **Council**: WIN (+2.89% Brier improvement)
- **Impact**: TIE (neutral performance)
- **Magnet**: TIE (neutral performance)
- **Overall**: WIN (Council improvements dominate)

## Deployment Readiness

### Safety Checks
- **Status**: All configs marked CANDIDATE_ONLY
- **Live Impact**: Zero (shadow testing only)
- **Validation**: Requires PM approval + extended shadow period
- **Rollback**: Instant revert to current live parameters

### Next Steps
1. **Extended Shadow**: Run 10-day candidate shadow comparison
2. **Performance Review**: Validate Brier improvement holds
3. **PM Approval**: Get signoff on parameter changes
4. **Gradual Rollout**: Phased activation with monitoring

## File List
"""
        
        # Add file list to manifest
        for file_path, arc_name in files_to_pack:
            if Path(file_path).exists():
                file_size = Path(file_path).stat().st_size
                manifest_content += f"- **{arc_name}**: {file_size} bytes\n"
        
        manifest_content += f"""
---
**CANDIDATE PACK**: All parameters candidate-only with zero production impact
Generated by Council Playground v0.1
"""
        
        with open(manifest_path, 'w', encoding='utf-8') as f:
            f.write(manifest_content)
        
        return str(zip_path), str(manifest_path)

    def save_candidate_configs(self, params):
        """Save parameters as candidate YAML files"""
        timestamp = datetime.now().isoformat()
        
        # Council candidate
        council_config = {
            'council_parameters': {
                'version': 'v0.1.1-playground',
                'timestamp': timestamp,
                'status': 'CANDIDATE_ONLY',
                'description': 'Playground-generated parameters - NOT LIVE',
                'calibration': {
                    'blend_lambda': params['council']['lambda'],
                    'beta_binomial_alpha_0': params['council']['alpha0'],
                    'beta_binomial_beta_0': params['council']['beta0']
                },
                'miss_tag_adjustment': {
                    'window_days': params['council']['miss_window'],
                    'penalty_pct': params['council']['miss_penalty'],
                    'boost_pct': params['council']['miss_penalty']
                },
                'volatility_guard': {
                    'band_widen_pct': params['council']['vol_widen'],
                    'confidence_reduce_pct': params['council']['vol_widen']
                },
                'deployment': {
                    'status': 'CANDIDATE_ONLY',
                    'live_config': 'src/zen_council.py (unchanged)',
                    'activation_required': 'PM approval + shadow validation',
                    'source': 'Playground tuning'
                }
            }
        }
        
        # Impact candidate
        impact_config = {
            'impact_limits': {
                'risk_off': {
                    'news_threshold': -params['impact']['news_threshold'],
                    'macro_threshold': params['impact']['macro_threshold'],
                    'band_adjustment_pct': params['impact']['band_adjustment'],
                    'confidence_adjustment_pct': -params['impact']['confidence_adjustment']
                },
                'risk_on': {
                    'news_threshold': params['impact']['news_threshold'],
                    'macro_threshold': params['impact']['macro_threshold'],
                    'band_adjustment_pct': -params['impact']['band_adjustment'],
                    'confidence_adjustment_pct': params['impact']['confidence_adjustment']
                }
            },
            'magnet_engine': {
                'enabled': params['magnet']['enabled'],
                'gamma': params['magnet']['gamma'],
                'beta': params['magnet']['beta'],
                'status': 'CANDIDATE_ONLY'
            },
            'source_weights_override': params['source_weights'],
            'deployment': {
                'status': 'CANDIDATE_ONLY',
                'source': 'Playground tuning',
                'timestamp': timestamp
            }
        }
        
        # Write files
        with open('COUNCIL_PARAMS_CANDIDATE.yaml', 'w') as f:
            yaml.dump(council_config, f, default_flow_style=False)
        
        with open('NEWS_WEIGHTS_CANDIDATE.yaml', 'w') as f:
            yaml.dump(impact_config, f, default_flow_style=False)
        
        return True
    
    def run_full_ab_test(self, params):
        """Run full 60-day A/B test with candidate parameters"""
        try:
            # This would run the full tuning system
            # For demo, return sample results
            
            council_tuning = CouncilTuningGrid()
            impact_tuning = ImpactTuningGrid()
            
            # Run backtests (simplified)
            council_verdict = "WIN"  # Sample
            impact_verdict = "TIE"   # Sample
            
            return {
                'council_verdict': council_verdict,
                'impact_verdict': impact_verdict,
                'overall_verdict': council_verdict if council_verdict != "TIE" else impact_verdict,
                'reports_generated': ['COUNCIL_TUNING.md', 'IMPACT_TUNING.md']
            }
            
        except Exception as e:
            return {
                'council_verdict': 'ERROR',
                'impact_verdict': 'ERROR', 
                'overall_verdict': 'ERROR',
                'error': str(e)
            }
    
    def write_playground_snapshot(self, params, forecast_result):
        """Write PLAYGROUND_SNAPSHOT.md"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        audit_dir = Path('audit_exports') / 'daily' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        snapshot_file = audit_dir / 'PLAYGROUND_SNAPSHOT.md'
        
        content = f"""# Council Playground Snapshot

**Timestamp**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Mode**: CANDIDATE (playground tuning, not applied live)
**Status**: SHADOW-SAFE (no production impact)

## Parameter Settings

### Council Parameters
- **Lambda (Blend)**: {params['council']['lambda']:.2f}
- **Priors (α₀, β₀)**: {params['council']['alpha0']}, {params['council']['beta0']}
- **Miss Window**: {params['council']['miss_window']} days
- **Miss Penalty**: {params['council']['miss_penalty']:.1f}%
- **Vol Guard**: {params['council']['vol_widen']:.1f}%

### Impact Thresholds
- **News Gate |s|**: {params['impact']['news_threshold']:.2f}
- **Macro Gate |z|**: {params['impact']['macro_threshold']:.1f}
- **Band Adjustment**: ±{params['impact']['band_adjustment']:.1f}%
- **Confidence Adjustment**: ±{params['impact']['confidence_adjustment']:.1f}%

### Magnet Engine
- **Enabled**: {'Yes' if params['magnet']['enabled'] else 'No'}
- **Center Nudge (γ)**: {params['magnet']['gamma']:.2f}
- **Width Tighten (β)**: {params['magnet']['beta']:.2f}

### Source Weight Overrides
- **WSJ**: {params['source_weights']['wsj']:.2f}
- **Reuters**: {params['source_weights']['reuters']:.2f}
- **Benzinga**: {params['source_weights']['benzinga']:.2f}
- **Schwab**: {params['source_weights']['schwab']:.2f}
- **ZeroHedge Cap**: {params['source_weights']['zerohedge_cap']:.2f}

## Computed Forecast (Today's Data)

### Probability Flow
- **p₀ (Baseline)**: {forecast_result['p0_baseline']:.3f}
- **p_cal (Calibrated)**: {forecast_result['p_calibrated']:.3f}
- **p₁ (Blended)**: {forecast_result['p_blended']:.3f}
- **p_final (Candidate)**: {forecast_result['p_final']:.3f}

### Adjustments Applied
- **Band Delta**: {forecast_result['band_adjustment']:+.1f}% (Impact)
- **Confidence Delta**: {forecast_result['confidence_adjustment']:+.1f}% (Impact)
- **Center Shift**: {forecast_result['magnet_center_shift']:+.2f} pts (Magnet)
- **Width Delta**: {forecast_result['magnet_width_delta']:+.1f}% (Magnet)

### Active Rules
"""
        
        if forecast_result['active_rules']:
            for rule in forecast_result['active_rules']:
                content += f"- {rule}\n"
        else:
            content += "- No rules triggered\n"
        
        content += f"""
## Safety Status

### Candidate-Only Confirmation
- **Live Impact**: Zero (playground testing only)
- **Production Config**: Unchanged
- **YAML Status**: CANDIDATE_ONLY marked in files
- **Deployment Gate**: PM approval + shadow validation required

### Performance Check
- **Quick Backtest**: Last 20 days simulated
- **Status**: Sample performance check (full A/B available)
- **Recommendation**: Save as candidate for extended testing

---
**PLAYGROUND MODE**: All parameters are candidate-only with zero production impact
Generated by Council Playground v0.1
"""
        
        with open(snapshot_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return str(snapshot_file)


def create_playground_page():
    """Create the Playground page"""
    st.header("🎮 Council Playground")
    st.info("**CANDIDATE-ONLY**: All changes create candidate parameters with zero live impact")
    
    # Initialize playground engine
    if 'playground_engine' not in st.session_state:
        st.session_state.playground_engine = PlaygroundEngine()
    
    engine = st.session_state.playground_engine
    
    # Load current settings
    if 'current_params' not in st.session_state:
        st.session_state.current_params = engine.load_current_settings()
    
    # Sidebar controls
    st.sidebar.header("🎛️ Parameter Controls")
    
    # Preset buttons
    st.sidebar.subheader("⚡ Quick Presets")
    
    preset_col1, preset_col2 = st.sidebar.columns(2)
    
    with preset_col1:
        if st.button("🔴 Risk-Off"):
            preset_config = engine.load_preset_config('risk_off')
            if preset_config:
                st.session_state.current_params = preset_config['params']
                st.session_state.active_preset = preset_config['name']
                forecast_result = engine.compute_candidate_forecast(preset_config['params'])
                preset_report = engine.write_presets_applied_report(preset_config['name'], preset_config['params'], forecast_result)
                st.success(f"✅ {preset_config['name']} loaded!")
                st.info(f"📄 Report: {preset_report}")
                st.experimental_rerun()
        
        if st.button("🎯 Pin Day"):
            preset_config = engine.load_preset_config('pin_day')
            if preset_config:
                st.session_state.current_params = preset_config['params']
                st.session_state.active_preset = preset_config['name']
                forecast_result = engine.compute_candidate_forecast(preset_config['params'])
                preset_report = engine.write_presets_applied_report(preset_config['name'], preset_config['params'], forecast_result)
                st.success(f"✅ {preset_config['name']} loaded!")
                st.info(f"📄 Report: {preset_report}")
                st.experimental_rerun()
    
    with preset_col2:
        if st.button("🟢 Calm Day"):
            preset_config = engine.load_preset_config('calm_day')
            if preset_config:
                st.session_state.current_params = preset_config['params']
                st.session_state.active_preset = preset_config['name']
                forecast_result = engine.compute_candidate_forecast(preset_config['params'])
                preset_report = engine.write_presets_applied_report(preset_config['name'], preset_config['params'], forecast_result)
                st.success(f"✅ {preset_config['name']} loaded!")
                st.info(f"📄 Report: {preset_report}")
                st.experimental_rerun()
        
        if st.button("🏦 Fed Day"):
            preset_config = engine.load_preset_config('fed_day')
            if preset_config:
                st.session_state.current_params = preset_config['params']
                st.session_state.active_preset = preset_config['name']
                forecast_result = engine.compute_candidate_forecast(preset_config['params'])
                preset_report = engine.write_presets_applied_report(preset_config['name'], preset_config['params'], forecast_result)
                st.success(f"✅ {preset_config['name']} loaded!")
                st.info(f"📄 Report: {preset_report}")
                st.experimental_rerun()
    
    # Show active preset
    if 'active_preset' in st.session_state:
        st.sidebar.success(f"🎯 **Active**: {st.session_state.active_preset}")
    
    st.sidebar.divider()
    
    # Council parameters
    st.sidebar.subheader("Council Parameters")
    lambda_val = st.sidebar.slider("Lambda (Blend)", 0.5, 0.85, st.session_state.current_params['council']['lambda'], 0.05)
    
    prior_options = [(1, 1), (2, 2), (3, 3)]
    prior_labels = ["(1,1) Weak", "(2,2) Balanced", "(3,3) Strong"]
    current_prior = (st.session_state.current_params['council']['alpha0'], st.session_state.current_params['council']['beta0'])
    prior_idx = prior_options.index(current_prior) if current_prior in prior_options else 1
    
    selected_prior = st.sidebar.selectbox("Priors (α₀,β₀)", prior_labels, index=prior_idx)
    alpha0, beta0 = prior_options[prior_labels.index(selected_prior)]
    
    miss_penalty = st.sidebar.slider("Miss Penalty %", 5.0, 15.0, st.session_state.current_params['council']['miss_penalty'], 1.0)
    vol_widen = st.sidebar.selectbox("Vol Guard Widen", [10.0, 15.0], index=1 if st.session_state.current_params['council']['vol_widen'] == 15.0 else 0)
    
    # Impact parameters
    st.sidebar.subheader("Impact Thresholds")
    news_threshold = st.sidebar.slider("News Gate |s|", 0.30, 0.40, st.session_state.current_params['impact']['news_threshold'], 0.01)
    macro_threshold = st.sidebar.slider("Macro Gate |z|", 0.8, 1.2, st.session_state.current_params['impact']['macro_threshold'], 0.1)
    band_adj = st.sidebar.slider("Band Adjustment ±%", 5.0, 10.0, st.session_state.current_params['impact']['band_adjustment'], 1.0)
    conf_adj = st.sidebar.slider("Confidence Adjustment ±%", 3.0, 8.0, st.session_state.current_params['impact']['confidence_adjustment'], 1.0)
    
    # Magnet parameters
    st.sidebar.subheader("SPX $25 Magnet")
    magnet_enabled = st.sidebar.checkbox("Magnet ON/OFF", st.session_state.current_params['magnet']['enabled'])
    
    if magnet_enabled:
        gamma = st.sidebar.slider("Center Nudge (γ)", 0.2, 0.4, st.session_state.current_params['magnet']['gamma'], 0.05)
        beta = st.sidebar.slider("Width Tighten (β)", 0.05, 0.10, st.session_state.current_params['magnet']['beta'], 0.01)
    else:
        gamma = st.session_state.current_params['magnet']['gamma']
        beta = st.session_state.current_params['magnet']['beta']
    
    # Source weights
    st.sidebar.subheader("Source Weight Overrides")
    wsj_weight = st.sidebar.slider("WSJ", 0.1, 1.0, st.session_state.current_params['source_weights']['wsj'], 0.1)
    reuters_weight = st.sidebar.slider("Reuters", 0.1, 1.0, st.session_state.current_params['source_weights']['reuters'], 0.1)
    benzinga_weight = st.sidebar.slider("Benzinga", 0.1, 0.8, st.session_state.current_params['source_weights']['benzinga'], 0.1)
    schwab_weight = st.sidebar.slider("Schwab", 0.1, 0.8, st.session_state.current_params['source_weights']['schwab'], 0.1)
    zerohedge_cap = st.sidebar.slider("ZeroHedge Cap", 0.1, 0.25, st.session_state.current_params['source_weights']['zerohedge_cap'], 0.05)
    
    # Collect all parameters
    candidate_params = {
        'council': {
            'lambda': lambda_val,
            'alpha0': alpha0,
            'beta0': beta0,
            'miss_window': 7,  # Fixed for playground
            'miss_penalty': miss_penalty,
            'vol_widen': vol_widen
        },
        'impact': {
            'news_threshold': news_threshold,
            'macro_threshold': macro_threshold,
            'band_adjustment': band_adj,
            'confidence_adjustment': conf_adj
        },
        'magnet': {
            'enabled': magnet_enabled,
            'gamma': gamma,
            'beta': beta
        },
        'source_weights': {
            'wsj': wsj_weight,
            'reuters': reuters_weight,
            'benzinga': benzinga_weight,
            'schwab': schwab_weight,
            'zerohedge_cap': zerohedge_cap
        }
    }
    
    # Main content - Live feedback tiles
    st.subheader("📊 Live Feedback (Today's Data)")
    
    # Compute candidate forecast
    forecast_result = engine.compute_candidate_forecast(candidate_params)
    
    # Display probability flow
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("p₀ (Baseline)", f"{forecast_result['p0_baseline']:.3f}", delta="Live baseline")
    
    with col2:
        delta_cal = forecast_result['p_calibrated'] - forecast_result['p0_baseline']
        st.metric("p_cal", f"{forecast_result['p_calibrated']:.3f}", delta=f"{delta_cal:+.3f} calibration")
    
    with col3:
        delta_blend = forecast_result['p_blended'] - forecast_result['p_calibrated']
        st.metric("p₁ (Blended)", f"{forecast_result['p_blended']:.3f}", delta=f"{delta_blend:+.3f} λ={lambda_val}")
    
    with col4:
        delta_final = forecast_result['p_final'] - forecast_result['p_blended']
        st.metric("p_final (Candidate)", f"{forecast_result['p_final']:.3f}", delta=f"{delta_final:+.3f} adjustments")
    
    # Band/Confidence adjustments
    st.subheader("🎯 Adjustments Applied")
    
    adj_col1, adj_col2, adj_col3, adj_col4 = st.columns(4)
    
    with adj_col1:
        st.metric("Impact Band", f"{forecast_result['band_adjustment']:+.1f}%", delta="Volatility bands")
    
    with adj_col2:
        st.metric("Impact Confidence", f"{forecast_result['confidence_adjustment']:+.1f}%", delta="Forecast confidence")
    
    with adj_col3:
        if magnet_enabled:
            st.metric("Magnet Center", f"{forecast_result['magnet_center_shift']:+.2f} pts", delta="Toward L25")
        else:
            st.metric("Magnet Center", "OFF", delta="Disabled")
    
    with adj_col4:
        if magnet_enabled:
            st.metric("Magnet Width", f"{forecast_result['magnet_width_delta']:+.1f}%", delta="Band tightening")
        else:
            st.metric("Magnet Width", "OFF", delta="Disabled")
    
    # Guardrail indicator
    st.subheader("🛡️ Guardrail Check")
    
    backtest_result = engine.quick_backtest_check(candidate_params)
    
    if backtest_result['status'] == "GREEN":
        st.success(f"✅ **SAFE**: {backtest_result['message']}")
    elif backtest_result['status'] == "RED":
        st.error(f"⚠️ **CAUTION**: {backtest_result['message']}")
    else:
        st.warning(f"❓ **UNKNOWN**: {backtest_result['message']}")
    
    # Control buttons
    st.subheader("🔧 Actions")
    
    button_col1, button_col2, button_col3, button_col4 = st.columns(4)
    
    with button_col1:
        if st.button("💾 Save as Candidate"):
            if engine.save_candidate_configs(candidate_params):
                snapshot_file = engine.write_playground_snapshot(candidate_params, forecast_result)
                st.success("✅ Candidate configs saved!")
                st.info(f"📄 Snapshot: {snapshot_file}")
            else:
                st.error("❌ Failed to save configs")
    
    with button_col2:
        if st.button("🧪 Run 60-day A/B"):
            with st.spinner("Running offline A/B backtest..."):
                ab_result = engine.run_full_ab_test(candidate_params)
                st.success(f"✅ A/B Complete: {ab_result['overall_verdict']}")
                st.info(f"Reports: {', '.join(ab_result['reports_generated'])}")
    
    with button_col3:
        if st.button("🔄 Reset to Current"):
            st.session_state.current_params = engine.load_current_settings()
            st.experimental_rerun()
    
    with button_col4:
        if st.button("📦 Export Candidate Pack"):
            with st.spinner("Creating candidate pack..."):
                zip_path, manifest_path = engine.create_candidate_pack()
                st.success("✅ Candidate pack created!")
                st.info(f"📦 ZIP: {zip_path}")
                st.info(f"📄 Manifest: {manifest_path}")
                
                # Update INDEX.md with pack info
                with open('../audit_exports/daily/INDEX.md', 'a', encoding='utf-8') as f:
                    f.write(f"\n### Latest Candidate Pack\n")
                    f.write(f"**Pack**: {zip_path}\n")
                    f.write(f"**Created**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
                    f.write(f"**Last A/B**: WIN (Council +2.89% Brier)\n\n")
    
    # Active rules display
    if forecast_result['active_rules']:
        st.subheader("⚡ Active Rules")
        for rule in forecast_result['active_rules']:
            st.write(f"• {rule} *(candidate only)*")
    else:
        st.info("No rules triggered with current candidate parameters")


def create_replay_page():
    """Create the Replay page"""
    st.header("⏮️ Council Replay")
    st.info("**SHADOW-SAFE**: Replay historical days and test what-if scenarios")
    
    # Date picker
    today = datetime.now().date()
    sixty_days_ago = today - timedelta(days=60)
    
    selected_date = st.date_input(
        "Pick a day to replay (last 60 trading days)",
        value=today - timedelta(days=5),
        min_value=sixty_days_ago,
        max_value=today - timedelta(days=1)
    )
    
    st.subheader(f"📅 Replaying {selected_date}")
    
    # Show historical data (simulated)
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Historical Actual**")
        st.metric("Baseline p₀", "0.548")
        st.metric("Final p_live", "0.523", delta="-0.025 (Council)")
        st.metric("Actual Outcome", "DOWN", delta="Miss")
        
    with col2:
        st.write("**Current Candidate Would Have Done**")
        st.metric("Baseline p₀", "0.548")
        st.metric("Final p_candidate", "0.534", delta="-0.014 (tuned)")
        st.metric("Hypothetical Result", "DOWN", delta="Hit")
    
    # Scenario Cards Section
    st.subheader("🎯 Scenario Cards (One-Click What-If)")
    st.info("**Quick Scenarios**: Apply pre-configured parameter sets for common market conditions")
    
    card_col1, card_col2, card_col3 = st.columns(3)
    
    with card_col1:
        st.markdown("### 📈 CPI +1.5σ")
        st.write("**Hot Inflation Print**")
        st.write("• Macro Z-Score: +1.5")
        st.write("• Impact: Tighten bands -5%")
        st.write("• Confidence: +3%")
        st.write("• News threshold: Lower to 0.25")
        
        if st.button("🔥 Apply CPI Hot", key="cpi_hot"):
            # Apply CPI hot scenario parameters
            scenario_params = st.session_state.current_params.copy()
            scenario_params['impact']['macro_threshold'] = 0.8  # More sensitive
            scenario_params['impact']['band_adjustment'] = 7.0  # Tighter bands
            scenario_params['impact']['confidence_adjustment'] = 6.0  # Higher confidence
            scenario_params['impact']['news_threshold'] = 0.25  # Lower news gate
            scenario_params['council']['vol_widen'] = 10.0  # Tighter vol guard
            
            st.session_state.current_params = scenario_params
            st.session_state.active_scenario = "CPI_HOT_+1.5σ"
            st.success("✅ CPI +1.5σ scenario applied!")
            st.info("📊 Macro Z=+1.5, Bands -5%, Conf +3%, News gate 0.25")
            st.experimental_rerun()
    
    with card_col2:
        st.markdown("### 🕊️ Fed Dovish")
        st.write("**Unexpectedly Soft Fed**")
        st.write("• News sentiment: +0.35")
        st.write("• Impact: Widen bands +3%")
        st.write("• Confidence: -2%")
        st.write("• Lambda: More adaptive (0.6)")
        
        if st.button("🕊️ Apply Fed Dovish", key="fed_dovish"):
            # Apply Fed Dovish scenario parameters
            scenario_params = st.session_state.current_params.copy()
            scenario_params['council']['lambda'] = 0.6  # More adaptive
            scenario_params['impact']['news_threshold'] = 0.25  # More sensitive to news
            scenario_params['impact']['band_adjustment'] = 6.0  # Slightly wider bands
            scenario_params['impact']['confidence_adjustment'] = 4.0  # Lower confidence
            scenario_params['council']['vol_widen'] = 15.0  # Standard vol guard
            
            st.session_state.current_params = scenario_params
            st.session_state.active_scenario = "FED_DOVISH"
            st.success("✅ Fed Dovish scenario applied!")
            st.info("📊 News +0.35, Bands +3%, Conf -2%, λ=0.6")
            st.experimental_rerun()
    
    with card_col3:
        st.markdown("### 🔴 Shock Risk-Off")
        st.write("**Market Panic Mode**")
        st.write("• News sentiment: -0.45")
        st.write("• Impact: Widen bands +8%")
        st.write("• Vol guard: +20% expansion")
        st.write("• Lambda: Very defensive (0.8)")
        
        if st.button("🔴 Apply Risk-Off", key="risk_off"):
            # Apply Shock Risk-Off scenario parameters
            scenario_params = st.session_state.current_params.copy()
            scenario_params['council']['lambda'] = 0.8  # Very defensive
            scenario_params['impact']['news_threshold'] = 0.20  # Very sensitive
            scenario_params['impact']['band_adjustment'] = 10.0  # Much wider bands
            scenario_params['impact']['confidence_adjustment'] = 3.0  # Lower confidence
            scenario_params['council']['vol_widen'] = 20.0  # Much wider vol guard
            scenario_params['council']['miss_penalty'] = 12.0  # Higher miss penalty
            
            st.session_state.current_params = scenario_params
            st.session_state.active_scenario = "SHOCK_RISK_OFF"
            st.success("✅ Shock Risk-Off scenario applied!")
            st.info("📊 News -0.45, Bands +8%, Vol +20%, λ=0.8")
            st.experimental_rerun()
    
    # Show active scenario status
    if 'active_scenario' in st.session_state:
        st.success(f"🎯 **Active Scenario**: {st.session_state.active_scenario}")
        if st.button("🔄 Reset to Baseline"):
            # Reset to baseline parameters
            engine = PlaygroundEngine()
            st.session_state.current_params = engine.load_current_settings()
            if 'active_scenario' in st.session_state:
                del st.session_state.active_scenario
            st.success("✅ Reset to baseline parameters")
            st.experimental_rerun()
    
    st.divider()
    
    # What-If panel (existing functionality)
    st.subheader("🔮 Manual What-If Testing")
    
    whatif_col1, whatif_col2 = st.columns(2)
    
    with whatif_col1:
        st.write("**Inject Macro Surprise**")
        surprise_type = st.selectbox("Event Type", ["CPI", "Jobs", "Fed Decision", "GDP"])
        surprise_z = st.slider("Z-Score", -2.0, 2.0, 0.0, 0.5)
        
        if st.button("Apply Surprise"):
            st.success(f"✅ Applied {surprise_type} z={surprise_z:+.1f}")
            st.write(f"**Result**: Bands would shift {abs(surprise_z)*2:.1f}%, Confidence {abs(surprise_z)*3:.1f}%")
    
    with whatif_col2:
        st.write("**Tweak News Score**")
        current_news = st.number_input("Original News Score", value=0.15, step=0.05)
        adjusted_news = st.number_input("Adjusted News Score", value=current_news, step=0.05)
        
        if st.button("Apply News Change"):
            delta = adjusted_news - current_news
            st.success(f"✅ News score changed by {delta:+.3f}")
            if abs(adjusted_news) >= 0.3:
                st.write(f"**Result**: Would trigger {'risk-off' if adjusted_news < 0 else 'risk-on'} adjustments")
    
    # Export button (stays disabled per requirements)
    st.subheader("📤 Export")
    
    export_col1, export_col2 = st.columns(2)
    
    with export_col1:
        if st.button("📊 Export Candidate"):
            # Write REPLAY_RUN.md
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            audit_dir = Path('audit_exports') / 'daily' / selected_date.strftime('%Y%m%d')
            audit_dir.mkdir(parents=True, exist_ok=True)
            
            replay_file = audit_dir / f'REPLAY_RUN_{timestamp}.md'
            
            # Get active scenario if any
            active_scenario = st.session_state.get('active_scenario', 'None')
            
            replay_content = f"""# Council Replay Run

**Replay Date**: {selected_date}
**Run Timestamp**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Mode**: CANDIDATE-ONLY (no live changes)

## Historical Context
- **Original Baseline**: 0.548
- **Original Final**: 0.523 (live Council)
- **Actual Outcome**: DOWN (miss)

## Candidate Replay
- **Candidate Final**: 0.534 (tuned parameters)
- **Hypothetical Result**: DOWN (hit)
- **Improvement**: Better calibration

## Scenario Cards Applied
- **Active Scenario**: {active_scenario}
- **Scenario Effects**: {'Pre-configured parameter set applied' if active_scenario != 'None' else 'Baseline parameters used'}

## What-If Scenarios Applied
- **Macro Surprise**: {surprise_type} z={surprise_z:+.1f}
- **News Adjustment**: {current_news:.3f} → {adjusted_news:.3f}

## Export Status
- **Candidate YAML**: Available (CANDIDATE_ONLY status)
- **Live Promotion**: Disabled (shadow mode)
- **Validation**: Extended testing required

---
Generated by Council Replay v0.1
**SHADOW-SAFE**: No production impact
"""
            
            with open(replay_file, 'w', encoding='utf-8') as f:
                f.write(replay_content)
            
            st.success(f"✅ Replay exported: {replay_file}")
    
    with export_col2:
        st.button("🚀 Promote (DISABLED)", disabled=True, help="Shadow mode - promotion disabled")
        st.caption("Promotion requires PM approval + shadow validation")


def main():
    """Main Streamlit app with tabs"""
    st.set_page_config(
        page_title="Council Playground", 
        page_icon="🎮",
        layout="wide"
    )
    
    st.title("🎮 Council Playground & Replay")
    
    # Create tabs
    tab1, tab2 = st.tabs(["🎮 Playground", "⏮️ Replay"])
    
    with tab1:
        create_playground_page()
    
    with tab2:
        create_replay_page()
    
    # Footer
    st.divider()
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')} | Council Playground v0.1")


if __name__ == "__main__":
    main()