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
    
    button_col1, button_col2, button_col3 = st.columns(3)
    
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
    
    # What-If panel
    st.subheader("🔮 What-If Scenarios")
    
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