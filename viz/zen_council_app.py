#!/usr/bin/env python3
"""
Zen Council Streamlit Dashboard
Real-time brain visualization for market forecasting
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
from pathlib import Path
import sys
import os

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

try:
    from zen_council import ZenCouncil
    from macro_news_gates import MacroNewsGates
    from news_ingestion import NewsIngestionEngine
    from event_impact_engine import EventImpactEngine
    from impact_ab_backtest import ImpactABBacktest
    from impact_guardrails import ImpactGuardrails
    from level_magnet_engine import LevelMagnetEngine
    from magnet_ab_backtest import MagnetABBacktest
    from win_conditions_gate import WinConditionsGate
    from shadow_scorecard import ShadowScorecard
except ImportError:
    st.error("Could not import Zen Council modules. Check src/ directory.")
    st.stop()


def load_latest_zen_council_data():
    """Load latest Zen Council analysis"""
    council = ZenCouncil()
    
    # Check if shadow mode is active
    shadow_active = os.getenv('COUNCIL_ACTIVE', 'false').lower() != 'true'
    
    # Run analysis with sample data
    p_baseline = 0.58  # Sample baseline
    result = council.adjust_forecast(p_baseline)
    result['shadow_mode_active'] = shadow_active
    
    return result


def load_latest_gates_data():
    """Load latest macro and news gates data"""
    gates = MacroNewsGates()
    
    target_date = datetime.now().date()
    target_time = datetime.now()
    
    result = gates.process_gates(target_date, target_time)
    return result


def load_latest_impact_data():
    """Load latest Event-Impact Engine data"""
    # Check if NEWS_ENABLED and NEWS_IMPACT_MUTED
    news_enabled = os.getenv('NEWS_ENABLED', 'true').lower() == 'true'
    impact_muted = os.getenv('NEWS_IMPACT_MUTED', 'false').lower() == 'true'
    
    if not news_enabled:
        return {
            'enabled': False,
            'muted': False,
            'news_analysis': {'score': 0.0, 'items_count': 0, 'muted_count': 0},
            'macro_analysis': {'aggregate_z': 0.0, 'high_impact_events': [], 'event_count': 0},
            'adjustments': {'band_adjustment_pct': 0.0, 'confidence_adjustment_pct': 0.0, 'triggers': []},
            'summary': {'news_score': 0.0, 'macro_z_score': 0.0, 'triggers_fired': 0}
        }
    
    if impact_muted:
        return {
            'enabled': True,
            'muted': True,
            'mute_reason': 'Performance guardrails triggered',
            'news_analysis': {'score': 0.0, 'items_count': 0, 'muted_count': 0},
            'macro_analysis': {'aggregate_z': 0.0, 'high_impact_events': [], 'event_count': 0},
            'adjustments': {'band_adjustment_pct': 0.0, 'confidence_adjustment_pct': 0.0, 'triggers': []},
            'summary': {'news_score': 0.0, 'macro_z_score': 0.0, 'triggers_fired': 0}
        }
    
    try:
        # Create engines
        ingestion = NewsIngestionEngine()
        impact_engine = EventImpactEngine()
        
        # Get ingestion data
        ingestion_result = ingestion.ingest_daily_news()
        
        # Run impact analysis
        impact_result = impact_engine.run_impact_analysis(
            ingestion_result['news_items'],
            ingestion_result['macro_events']
        )
        
        # Add metadata
        impact_result['enabled'] = True
        impact_result['muted'] = False
        impact_result['sources_used'] = ingestion_result['sources_used']
        
        return impact_result
        
    except Exception as e:
        st.error(f"Error loading impact data: {e}")
        return {
            'enabled': False,
            'muted': False,
            'error': str(e),
            'news_analysis': {'score': 0.0, 'items_count': 0, 'muted_count': 0},
            'macro_analysis': {'aggregate_z': 0.0, 'high_impact_events': [], 'event_count': 0},
            'adjustments': {'band_adjustment_pct': 0.0, 'confidence_adjustment_pct': 0.0, 'triggers': []},
            'summary': {'news_score': 0.0, 'macro_z_score': 0.0, 'triggers_fired': 0}
        }


def load_magnet_data():
    """Load latest Magnet Engine data"""
    # Check if MAGNET_MUTED
    magnet_muted = os.getenv('MAGNET_MUTED', 'false').lower() == 'true'
    
    if magnet_muted:
        return {
            'enabled': False,
            'muted': True,
            'mute_reason': 'Performance guardrails triggered',
            'l25_level': 0,
            'strength': 0.0,
            'is_opex': False,
            'center_shift': 0.0,
            'width_delta': 0.0
        }
    
    try:
        engine = LevelMagnetEngine()
        
        # Sample baseline parameters for demo
        baseline_center = 5620.0
        baseline_width = 2.5
        
        # Run magnet analysis
        result = engine.run_magnet_analysis(baseline_center, baseline_width)
        
        return {
            'enabled': True,
            'muted': False,
            'l25_level': result['l25'],
            'strength': result['strength'],
            'is_opex': result['is_opex'],
            'center_shift': result['center_shift'],
            'width_delta': result['width_delta_pct'],
            'spx_ref': result['spx_ref'],
            'delta': result['delta'],
            'z_score': result['z_score'],
            'kappa': result['kappa']
        }
        
    except Exception as e:
        return {
            'enabled': False,
            'muted': False,
            'error': str(e),
            'l25_level': 0,
            'strength': 0.0,
            'is_opex': False,
            'center_shift': 0.0,
            'width_delta': 0.0
        }


def load_magnet_ab_results():
    """Load latest Magnet A/B backtest results"""
    try:
        backtest = MagnetABBacktest()
        results = backtest.run_magnet_ab_backtest(days=60)
        return results
    except Exception as e:
        return {
            'verdict': 'ERROR',
            'metrics': {
                'brier_improvement_pct': 0.0,
                'ece_improvement_pct': 0.0,
                'straddle_improvement': 0.0,
                'edge_hits_improvement': 0
            },
            'error': str(e)
        }


def load_win_conditions():
    """Load Win Conditions Gate assessment"""
    try:
        gate = WinConditionsGate()
        assessment = gate.assess_win_conditions()
        
        # Write WIN_GATE.md report
        gate_report = gate.write_win_gate_report(assessment)
        assessment['report_file'] = gate_report
        
        return assessment
    except Exception as e:
        return {
            'ready': False,
            'conditions': {
                'brier_gate': {'value': 0.0, 'pass': False},
                'ece_gate': {'value': 0.0, 'pass': False}, 
                'straddle_gate': {'value': 0.0, 'pass': False},
                'streak_gate': {'value': 0, 'pass': False}
            },
            'summary': {'gates_passed': 0, 'total_gates': 4, 'pass_rate': 0.0},
            'error': str(e)
        }


def load_shadow_scorecard():
    """Load 30-day Shadow Scorecard"""
    try:
        scorecard = ShadowScorecard()
        
        # Get cohort progress
        cohort = scorecard.get_cohort_day()
        
        # Generate synthetic shadow data
        shadow_df = scorecard.generate_synthetic_shadow_data()
        
        # Calculate metrics
        metrics = scorecard.calculate_scorecard_metrics(shadow_df)
        
        # Write scorecard report
        scorecard_file = scorecard.write_shadow_scorecard(metrics)
        metrics['report_file'] = scorecard_file
        metrics['cohort'] = cohort
        
        return metrics
    except Exception as e:
        return {
            'trading_days': 0,
            'brier_improvement_pct': 0.0,
            'ece_improvement_pct': 0.0,
            'straddle_improvement_pct': 0.0,
            'shadow_streak': 0,
            'cohort': {'day': 1, 'total': 30, 'start_date': '2025-08-28', 'status': 'ACTIVE'},
            'error': str(e)
        }


def load_pilot_mode_status():
    """Check pilot mode status and toggle"""
    pilot_mode = os.getenv('PILOT_MODE', 'false').lower() == 'true'
    return {
        'enabled': pilot_mode,
        'log_file': 'audit_exports/daily/PILOT_DECISION_LOG.csv'
    }


def write_pilot_decision_log(decision_data):
    """Write decision to pilot log"""
    try:
        pilot_log = Path('audit_exports') / 'daily' / 'PILOT_DECISION_LOG.csv'
        pilot_log.parent.mkdir(parents=True, exist_ok=True)
        
        # Check if file exists to determine if we need headers
        write_headers = not pilot_log.exists()
        
        import csv
        with open(pilot_log, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            if write_headers:
                writer.writerow(['date', 'p0', 'p_final', 'band_adjustment_pct', 'confidence_adjustment_pct', 'outcome', 'note'])
            
            writer.writerow([
                decision_data.get('date', datetime.now().strftime('%Y-%m-%d')),
                f"{decision_data.get('p0', 0.50):.3f}",
                f"{decision_data.get('p_final', 0.50):.3f}",
                f"{decision_data.get('band_adjustment_pct', 0.0):+.1f}",
                f"{decision_data.get('confidence_adjustment_pct', 0.0):+.1f}",
                decision_data.get('outcome', '*'),  # * = pending
                'PILOT'
            ])
            
        return str(pilot_log)
    except Exception as e:
        print(f"Error writing pilot log: {e}")
        return None


def load_live_gate_status():
    """Check PM approval gate status"""
    try:
        import hashlib
        
        # Check environment variable
        council_live_approved = os.getenv('COUNCIL_LIVE_APPROVED', 'false').lower() == 'true'
        
        # Check approval file
        approval_file = Path('governance') / 'PM_APPROVAL.md'
        if not approval_file.exists():
            return {
                'status': 'BLOCKED',
                'reason': 'approval_file_missing',
                'message': 'PM approval file not found'
            }
        
        # Extract fields from approval file
        try:
            content = approval_file.read_text(encoding='utf-8')
            
            approved_by = ""
            date = ""
            target_sha = ""
            approval_hash = ""
            
            for line in content.split('\n'):
                if line.startswith('APPROVED_BY='):
                    approved_by = line.split('=', 1)[1].strip()
                elif line.startswith('DATE='):
                    date = line.split('=', 1)[1].strip()
                elif line.startswith('TARGET_SHA='):
                    target_sha = line.split('=', 1)[1].strip()
                elif line.startswith('APPROVAL_HASH='):
                    approval_hash = line.split('=', 1)[1].strip()
            
            # Check if all fields present
            if not all([approved_by, date, target_sha, approval_hash]):
                return {
                    'status': 'BLOCKED',
                    'reason': 'approval_missing',
                    'message': 'PM approval fields incomplete'
                }
            
            # Verify hash
            approval_string = f"{approved_by}|{date}|{target_sha}"
            computed_hash = hashlib.sha256(approval_string.encode()).hexdigest()
            
            if computed_hash != approval_hash:
                return {
                    'status': 'BLOCKED', 
                    'reason': 'hash_mismatch',
                    'message': 'PM approval hash invalid'
                }
            
            # Check environment variable
            if not council_live_approved:
                return {
                    'status': 'BLOCKED',
                    'reason': 'var_false', 
                    'message': 'COUNCIL_LIVE_APPROVED not set to true'
                }
            
            return {
                'status': 'UNBLOCKED',
                'reason': 'ready_to_flip',
                'message': 'All approvals verified',
                'approved_by': approved_by,
                'date': date,
                'target_sha': target_sha
            }
            
        except Exception as e:
            return {
                'status': 'BLOCKED',
                'reason': 'file_error',
                'message': f'Error reading approval file: {e}'
            }
            
    except Exception as e:
        return {
            'status': 'BLOCKED',
            'reason': 'system_error',
            'message': f'Gate check failed: {e}'
        }


def load_impact_ab_results():
    """Load latest Impact A/B backtest results"""
    try:
        backtest = ImpactABBacktest()
        results = backtest.run_impact_ab_backtest(days=60)
        return results
    except Exception as e:
        return {
            'verdict': 'ERROR',
            'metrics': {
                'ece_improvement_pct': 0.0,
                'straddle_improvement': 0.0,
                'edge_hits_improvement': 0
            },
            'error': str(e)
        }


def create_probability_flow_chart(zen_data):
    """Create probability flow visualization"""
    stages = ['Baseline', 'Calibrated', 'Blended', 'Final']
    values = [
        zen_data['p_baseline'],
        zen_data['p_calibrated'], 
        zen_data['p_blended'],
        zen_data['p_final']
    ]
    
    fig = go.Figure()
    
    # Add line chart
    fig.add_trace(go.Scatter(
        x=stages,
        y=values,
        mode='lines+markers',
        line=dict(color='#1f77b4', width=3),
        marker=dict(size=10),
        name='Probability Flow'
    ))
    
    # Add value labels
    for i, (stage, value) in enumerate(zip(stages, values)):
        fig.add_annotation(
            x=stage,
            y=value,
            text=f"{value:.3f}",
            showarrow=False,
            yshift=15,
            font=dict(size=12, color='black')
        )
    
    fig.update_layout(
        title="Zen Council Probability Flow",
        xaxis_title="Processing Stage",
        yaxis_title="Probability",
        yaxis=dict(range=[0, 1]),
        height=300
    )
    
    return fig


def create_calibration_gauge(zen_data):
    """Create hit rate gauge"""
    hit_rate = zen_data['calibration_data']['hits'] / zen_data['calibration_data']['total_days']
    
    fig = go.Figure(go.Indicator(
        mode = "gauge+number",
        value = hit_rate * 100,
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': "Recent Hit Rate"},
        gauge = {
            'axis': {'range': [None, 100]},
            'bar': {'color': "darkgreen"},
            'steps': [
                {'range': [0, 50], 'color': "lightgray"},
                {'range': [50, 70], 'color': "yellow"},
                {'range': [70, 100], 'color': "lightgreen"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 80
            }
        }
    ))
    
    fig.update_layout(height=200)
    return fig


def main():
    """Main Streamlit app"""
    st.set_page_config(
        page_title="Zen Council Dashboard", 
        page_icon="🧠",
        layout="wide"
    )
    
    # Quick Links Top Bar
    link_col1, link_col2, link_col3, link_col4 = st.columns(4)
    with link_col1:
        st.markdown("🏠 [Live Dashboard](http://localhost:8501)")
    with link_col2:
        st.markdown("🎮 [Playground](http://localhost:8502)")
    with link_col3:
        st.markdown("⏮️ [Replay](http://localhost:8502)")
    with link_col4:
        st.markdown("📋 [Evidence](./audit_exports/daily/)")
    
    # Header with pipeline source
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.title("🧠 Zen Council v0.1")
    with header_col2:
        try:
            pipeline_sha = os.getenv('CI_COMMIT_SHORT_SHA', 'local')
            pipeline_time = datetime.now().strftime('%H:%M:%S UTC')
            st.caption(f"**Source**: pipeline {pipeline_sha} @ {pipeline_time}")
        except:
            st.caption("**Source**: local development")
    
    # Load data first to check shadow mode
    try:
        zen_data = load_latest_zen_council_data()
        shadow_mode = zen_data.get('shadow_mode_active', True)
    except:
        shadow_mode = True
    
    if shadow_mode:
        st.markdown("*Making the Brain Real - **SHADOW MODE ACTIVE** (Council suggestions logged, Baseline live)*")
        st.warning("⚠️ **SHADOW MODE**: Council adjustments are being logged but NOT applied to live forecasts")
    else:
        st.markdown("*Making the Brain Real - Live Forecast Intelligence*")
        st.success("✅ **LIVE MODE**: Council adjustments are active and applied to forecasts")
    
    # Load gates, impact, magnet, win conditions, shadow scorecard, live gate, pilot mode, and A/B data (zen_data already loaded above)
    try:
        gates_data = load_latest_gates_data()
        impact_data = load_latest_impact_data()
        magnet_data = load_magnet_data()
        win_conditions = load_win_conditions()
        shadow_scorecard = load_shadow_scorecard()
        live_gate = load_live_gate_status()
        pilot_mode = load_pilot_mode_status()
        ab_results = load_impact_ab_results()
        magnet_ab_results = load_magnet_ab_results()
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return
    
    # Live Gate Header Badge
    if live_gate['status'] == 'BLOCKED':
        st.error(f"🔒 **Approval Required**: {live_gate['message']} ({live_gate['reason']})")
        st.info("**Live deployment blocked** until PM approval gate passes. See governance/PM_APPROVAL.md for instructions.")
    
    # Pilot Mode Interface
    pilot_col1, pilot_col2 = st.columns([3, 1])
    
    with pilot_col2:
        # PM-only toggle for pilot mode
        current_pilot = pilot_mode['enabled']
        st.write("**PM Controls:**")
        
        if st.button(f"{'🟣 PILOT ON' if current_pilot else '⚫ PILOT OFF'}", key="pilot_toggle"):
            # Toggle pilot mode (in a real app, this would set an env var or session state)
            st.info("**Pilot Mode Toggle**: In production, this would toggle PILOT_MODE environment variable")
            st.experimental_rerun()
    
    with pilot_col1:
        if pilot_mode['enabled']:
            st.success("**Mode: PILOT (shadow only)** - Live-looking interface with zero production impact")
            st.markdown("""
            <div style="background: linear-gradient(45deg, #7c3aed, #a855f7); color: white; padding: 10px; border-radius: 5px; text-align: center; margin: 10px 0; border: 2px solid #6d28d9;">
                <strong>🎭 SIMULATION — NO PRODUCTION IMPACT</strong><br>
                <small>All decisions logged to PILOT_DECISION_LOG.csv only</small>
            </div>
            """, unsafe_allow_html=True)
        elif shadow_mode:
            st.warning("**Mode: SHADOW** - Candidate adjustments logged for evaluation")
        else:
            st.success("**Mode: LIVE** - Active forecast adjustments")
    
    # Pilot Decision Logging
    if pilot_mode['enabled']:
        try:
            # Log current decision to pilot log
            pilot_decision = {
                'date': datetime.now().strftime('%Y-%m-%d'),
                'p0': zen_data.get('p_baseline', 0.50),
                'p_final': zen_data.get('p_final', 0.50), 
                'band_adjustment_pct': impact_data.get('adjustments', {}).get('band_adjustment_pct', 0.0),
                'confidence_adjustment_pct': impact_data.get('adjustments', {}).get('confidence_adjustment_pct', 0.0),
                'outcome': '*'  # Pending outcome
            }
            
            pilot_log_file = write_pilot_decision_log(pilot_decision)
            if pilot_log_file:
                st.info(f"📝 **Pilot Decision Logged**: {pilot_log_file}")
        except Exception as e:
            st.warning(f"⚠️ Pilot logging error: {e}")
    
    # Update INDEX.md with pilot status
    try:
        index_file = Path('audit_exports') / 'daily' / 'INDEX.md'
        if index_file.exists():
            content = index_file.read_text(encoding='utf-8')
            
            pilot_line = f"Pilot: {'ON | log=PILOT_DECISION_LOG.csv' if pilot_mode['enabled'] else 'OFF'}"
            
            if 'Pilot:' in content:
                # Replace existing pilot line
                lines = content.split('\n')
                for i, line in enumerate(lines):
                    if line.strip().startswith('Pilot:'):
                        lines[i] = pilot_line
                        break
                content = '\n'.join(lines)
            else:
                # Add after Live Gate line
                content = content.replace('Live Gate:', f'Live Gate:')
                if 'Live Gate:' in content:
                    content = content.replace('Live Gate: BLOCKED', f'Live Gate: BLOCKED\n{pilot_line}')
                    content = content.replace('Live Gate: UNBLOCKED', f'Live Gate: UNBLOCKED\n{pilot_line}')
            
            index_file.write_text(content, encoding='utf-8')
    except Exception as e:
        pass  # Silent fail for INDEX update
    
    # Overview tiles
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if shadow_mode:
            st.metric(
                "Live Probability (Baseline)", 
                f"{zen_data['p_baseline']:.3f}",
                delta="Live decision",
                help="Current live forecast (Council suggestions are shadow-logged only)"
            )
        else:
            st.metric(
                "Final Probability", 
                f"{zen_data['p_final']:.3f}",
                delta=f"{zen_data['p_final'] - zen_data['p_baseline']:+.3f}"
            )
    
    with col2:
        st.metric(
            "Hit Rate (20d)",
            f"{zen_data['calibration_data']['hits']}/{zen_data['calibration_data']['total_days']}",
            delta=f"{zen_data['calibration_data']['hits']/zen_data['calibration_data']['total_days']*100:.1f}%"
        )
    
    with col3:
        if shadow_mode:
            st.metric(
                "Council Suggestion",
                f"{zen_data['p_final']:.3f}",
                delta=f"{zen_data['p_final'] - zen_data['p_baseline']:+.3f} shadow",
                help="What Council would suggest (not applied to live forecast)"
            )
        else:
            st.metric(
                "Active Rules",
                len(zen_data['active_rules']),
                delta="rules applied" if zen_data['active_rules'] else "no rules"
            )
    
    with col4:
        if shadow_mode:
            st.metric(
                "Shadow Mode",
                "10-DAY TEST",
                delta="Day N/10",
                help="Shadow testing phase - Council logged only, not live"
            )
        else:
            macro_status = "ACTIVE" if gates_data['macro_gate']['gate_active'] else "INACTIVE"
            st.metric(
                "Macro Gate",
                macro_status,
                delta=f"{gates_data['news_analysis']['score']:+.2f} news"
            )
    
    # Main charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.plotly_chart(create_probability_flow_chart(zen_data), use_container_width=True)
    
    with col2:
        st.plotly_chart(create_calibration_gauge(zen_data), use_container_width=True)
    
    # Win Conditions Tile
    st.subheader("🏆 Win Conditions Gate")
    
    win_col1, win_col2, win_col3, win_col4, win_col5 = st.columns(5)
    
    with win_col1:
        brier_value = win_conditions['conditions']['brier_gate']['value']
        brier_status = "✅" if win_conditions['conditions']['brier_gate']['pass'] else "❌"
        st.metric("ΔBrier(60d)", f"{brier_value:+.2f}%", delta=f"{brier_status} Improvement")
    
    with win_col2:
        ece_value = win_conditions['conditions']['ece_gate']['value'] 
        ece_status = "✅" if win_conditions['conditions']['ece_gate']['pass'] else "❌"
        st.metric("ΔECE(20d)", f"{ece_value:+.2f}%", delta=f"{ece_status} Calibration")
    
    with win_col3:
        straddle_value = win_conditions['conditions']['straddle_gate']['value']
        straddle_status = "✅" if win_conditions['conditions']['straddle_gate']['pass'] else "❌" 
        st.metric("ΔStraddle", f"{straddle_value:+.2f}%", delta=f"{straddle_status} Confidence")
    
    with win_col4:
        streak_value = win_conditions['conditions']['streak_gate']['value']
        streak_status = "✅" if win_conditions['conditions']['streak_gate']['pass'] else "❌"
        st.metric("Shadow Streak", f"{streak_value}d", delta=f"{streak_status} Consistency")
    
    with win_col5:
        if win_conditions['ready']:
            st.success("🎯 **READY**")
            st.caption(f"{win_conditions['summary']['gates_passed']}/4 gates pass")
        else:
            st.error("⏸️ **NOT READY**")  
            st.caption(f"{win_conditions['summary']['gates_passed']}/4 gates pass")
    
    # Shadow Scorecard Tile
    st.subheader("📊 Shadow Scorecard (30-day)")
    
    scorecard_col1, scorecard_col2, scorecard_col3, scorecard_col4, scorecard_col5 = st.columns(5)
    
    with scorecard_col1:
        st.metric("ΔBrier", f"{shadow_scorecard['brier_improvement_pct']:+.2f}%", delta="30-day avg")
    
    with scorecard_col2:
        st.metric("ΔECE", f"{shadow_scorecard['ece_improvement_pct']:+.2f}%", delta="20-day avg")
    
    with scorecard_col3:
        st.metric("ΔStraddle", f"{shadow_scorecard['straddle_improvement_pct']:+.2f}%", delta="Confidence")
    
    with scorecard_col4:
        st.metric("Streak", f"{shadow_scorecard['shadow_streak']}d", delta="Consecutive")
    
    with scorecard_col5:
        cohort = shadow_scorecard.get('cohort', {'day': 1, 'total': 30})
        st.info(f"**Day {cohort['day']}/{cohort['total']}**")
        st.caption("Shadow Cohort")
    
    # Magnet Engine Chip
    if magnet_data['enabled'] and not magnet_data.get('muted', False):
        magnet_chip_text = f"→ {magnet_data['l25_level']:.0f} (M={magnet_data['strength']:.2f}, OPEX={'Yes' if magnet_data['is_opex'] else 'No'})"
        st.info(f"🧲 **Level Magnet**: {magnet_chip_text}")
    elif magnet_data.get('muted', False):
        st.warning(f"🧲 **Level Magnet**: MUTED - {magnet_data.get('mute_reason', 'Performance guardrails')}")
    else:
        st.info("🧲 **Level Magnet**: v0.1 (SHADOW MODE)")
    
    # Impact Engine Status
    st.subheader("📊 Event-Impact Engine v0.1")
    
    # Impact Mode Tiles (A/B Results)
    st.write("**Impact Mode: SHADOW**")
    impact_mode_col1, impact_mode_col2, impact_mode_col3, impact_mode_col4 = st.columns(4)
    
    with impact_mode_col1:
        ece_improvement = ab_results['metrics']['ece_improvement_pct']
        st.metric(
            "ΔBrier(60d)",
            f"{ece_improvement:+.1f}%",
            delta=f"ECE improvement" if ece_improvement > 0 else "ECE degradation" if ece_improvement < 0 else "No change"
        )
    
    with impact_mode_col2:
        ece_improvement = ab_results['metrics']['ece_improvement_pct']
        st.metric(
            "ΔECE(20d)", 
            f"{ece_improvement:+.1f}%",
            delta="Calibration" if ece_improvement != 0 else "No change"
        )
    
    with impact_mode_col3:
        straddle_improvement = ab_results['metrics']['straddle_improvement']
        st.metric(
            "ΔStraddle",
            f"{straddle_improvement:+.2f}%",
            delta="Vol gap" if straddle_improvement != 0 else "No change"
        )
    
    with impact_mode_col4:
        edge_hits = ab_results['metrics']['edge_hits_improvement']
        st.metric(
            "EdgeHits",
            f"{edge_hits:+d}",
            delta="Tail captures" if edge_hits != 0 else "No change"
        )
    
    # Tuning Mode Display
    st.write("**Tuning Mode: OFFLINE** | Best λ=0.5, α₀/β₀=1/1, |s| gate=0.30, |z| gate=0.8 (candidate only)")
    tuning_col1, tuning_col2, tuning_col3 = st.columns(3)
    with tuning_col1:
        st.markdown("📊 [COUNCIL_TUNING.md](../audit_exports/tuning/)")
    with tuning_col2:
        st.markdown("🎯 [IMPACT_TUNING.md](../audit_exports/tuning/)")
    with tuning_col3:
        st.markdown("🎮 [Playground](http://localhost:8502) - Live tuning")
    
    # Mute Status Check
    if impact_data.get('muted', False):
        st.error("⚠️ **IMPACT MUTED**: Performance guardrails triggered")
        st.write(f"**Reason**: {impact_data.get('mute_reason', 'Unknown')}")
        st.write("**Status**: News ingestion continues, impact adjustments disabled")
        st.write("**To re-enable**: Set NEWS_IMPACT_MUTED=false after addressing performance issues")
    
    if not impact_data['enabled']:
        if 'error' in impact_data:
            st.error(f"❌ **NEWS DISABLED**: {impact_data['error']}")
        else:
            st.warning("❌ **NEWS DISABLED**: Set NEWS_ENABLED=true to activate")
        
        st.info("**Impact Engine Status**: Sources parsed = 0, no adjustments applied")
    elif not impact_data.get('muted', False):
        # Impact Tiles
        impact_col1, impact_col2, impact_col3, impact_col4 = st.columns(4)
        
        with impact_col1:
            news_score = impact_data['summary']['news_score']
            score_color = "🔴" if news_score <= -0.3 else "🟢" if news_score >= 0.3 else "🟡"
            st.metric(
                "News Score (s)",
                f"{news_score:.3f}",
                delta=f"{score_color} {'Risk-off' if news_score <= -0.3 else 'Risk-on' if news_score >= 0.3 else 'Neutral'}"
            )
        
        with impact_col2:
            macro_z = impact_data['summary']['macro_z_score']
            z_color = "📉" if macro_z < -1 else "📈" if macro_z > 1 else "➖"
            st.metric(
                "Macro Surprise (z)",
                f"{macro_z:.2f}",
                delta=f"{z_color} {'Significant' if abs(macro_z) >= 1 else 'Normal'}"
            )
        
        with impact_col3:
            band_adj = impact_data['adjustments']['band_adjustment_pct']
            st.metric(
                "Band Adjustment",
                f"{band_adj:+.1f}%",
                delta="Volatility bands" if band_adj != 0 else "No change"
            )
        
        with impact_col4:
            conf_adj = impact_data['adjustments']['confidence_adjustment_pct']
            st.metric(
                "Confidence Adjustment", 
                f"{conf_adj:+.1f}%",
                delta="Forecast confidence" if conf_adj != 0 else "No change"
            )
        
        # Sources Panel
        st.write("**📰 Sources Used Today**")
        sources_col1, sources_col2, sources_col3 = st.columns(3)
        
        with sources_col1:
            st.write(f"**Total Sources**: {impact_data.get('sources_used', 0)}")
            st.write(f"**News Items**: {impact_data['news_analysis']['items_count']}")
        
        with sources_col2:
            st.write(f"**Macro Events**: {impact_data['macro_analysis']['event_count']}")
            st.write(f"**Triggers Fired**: {impact_data['summary']['triggers_fired']}")
        
        with sources_col3:
            muted_count = impact_data['news_analysis']['muted_count']
            if muted_count > 0:
                st.warning(f"**Muted Sources**: {muted_count} (uncorroborated)")
            else:
                st.success("**All Sources Verified**: ✓")
        
        # High Impact Events
        high_impact = impact_data['macro_analysis']['high_impact_events']
        if high_impact:
            st.write("**⚡ High Impact Events**")
            for event in high_impact:
                direction_emoji = "📈" if event['z_score'] > 0 else "📉"
                st.write(f"• {event['event']}: {direction_emoji} z={event['z_score']:.2f}")
        
        # Applied Triggers
        triggers = impact_data['adjustments']['triggers']
        if triggers:
            st.write("**🎯 Active Triggers**")
            for trigger in triggers:
                st.write(f"• {trigger}")
        else:
            st.write("**🎯 No Active Triggers** (neutral market conditions)")
    
    # Why Section
    if shadow_mode:
        st.subheader("🎯 Shadow Analysis: What Council Would Do")
        st.info("**SHADOW MODE**: These adjustments are logged for evaluation but NOT applied to live forecasts")
    else:
        st.subheader("🎯 Why These Adjustments?")
    
    if zen_data['active_rules']:
        for rule in zen_data['active_rules']:
            if shadow_mode:
                st.write(f"• {rule} *(shadow logged only)*")
            else:
                st.write(f"• {rule}")
    else:
        st.write("• All thresholds below triggers - no adjustments applied")
    
    # Add Magnet explanation
    if magnet_data['enabled'] and not magnet_data.get('muted', False):
        magnet_explanation = f"Magnet toward {magnet_data['l25_level']:.0f}: center {magnet_data['center_shift']:+.1f} pts, width {magnet_data['width_delta']:+.1f}%"
        if shadow_mode:
            st.write(f"• {magnet_explanation} *(shadow logged only)*")
        else:
            st.write(f"• {magnet_explanation}")
    elif magnet_data.get('muted', False):
        if shadow_mode:
            st.write(f"• Level Magnet: MUTED ({magnet_data.get('mute_reason', 'guardrails')}) *(shadow mode)*")
        else:
            st.write(f"• Level Magnet: MUTED ({magnet_data.get('mute_reason', 'guardrails')})")
    
    # Gates Section
    st.subheader("🚦 Gates & Guards")
    
    gate_col1, gate_col2, gate_col3 = st.columns(3)
    
    with gate_col1:
        st.write("**Macro Gate**")
        macro_status = "🔴 ACTIVE" if gates_data['macro_gate']['gate_active'] else "🟢 INACTIVE"
        st.write(macro_status)
        if gates_data['macro_gate']['gate_active']:
            st.write(f"Events: {gates_data['macro_gate']['high_events_count']}")
    
    with gate_col2:
        st.write("**News Score**")
        news_score = gates_data['news_analysis']['score']
        if news_score <= -0.3:
            sentiment = "🔴 Risk-off"
        elif news_score >= 0.3:
            sentiment = "🟢 Risk-on"
        else:
            sentiment = "🟡 Neutral"
        st.write(f"{sentiment} ({news_score:.3f})")
    
    with gate_col3:
        st.write("**Vol Guard**")
        vol_guard = zen_data['band_widen_pct'] > 0
        vol_status = "🔴 ACTIVE" if vol_guard else "🟢 INACTIVE"
        st.write(vol_status)
        if vol_guard:
            st.write(f"Bands +{zen_data['band_widen_pct']:.0f}%")
    
    # Evidence Section
    st.subheader("📋 Evidence & Artifacts")
    
    if impact_data['enabled'] and not impact_data.get('muted', False):
        evidence_tabs = st.tabs(["Zen Council", "Impact Engine", "Impact A/B Report", "News Sources", "Macro Events"])
    elif impact_data['enabled'] and impact_data.get('muted', False):
        evidence_tabs = st.tabs(["Zen Council", "Impact Engine (MUTED)", "Impact A/B Report", "Guardrails"])
    else:
        evidence_tabs = st.tabs(["Zen Council", "Macro Gate", "News Score (Disabled)"])
    
    with evidence_tabs[0]:
        st.write("**Probability Breakdown**")
        breakdown_df = pd.DataFrame({
            'Stage': ['Baseline (p0)', 'Calibrated (p_cal)', 'Blended (p1)', 'Final (p_final)'],
            'Probability': [zen_data['p_baseline'], zen_data['p_calibrated'], 
                           zen_data['p_blended'], zen_data['p_final']],
            'Formula': [
                'From forecast engine',
                f"Beta-binomial: {zen_data['calibration_data']['hits']}H + {zen_data['calibration_data']['misses']}M",
                f"λ=0.7 blend: 0.7×p0 + 0.3×p_cal", 
                f"Clipped adjustment: A={zen_data['miss_tag_adjustment']:.3f}"
            ]
        })
        st.dataframe(breakdown_df, hide_index=True)
    
    if impact_data['enabled'] and not impact_data.get('muted', False):
        with evidence_tabs[1]:
            st.write("**Event-Impact Analysis**")
            st.write(f"**News Score**: {impact_data['summary']['news_score']:.3f}")
            st.write(f"**Macro Z-Score**: {impact_data['summary']['macro_z_score']:.2f}")
            st.write(f"**Band Adjustment**: {impact_data['adjustments']['band_adjustment_pct']:+.1f}%")
            st.write(f"**Confidence Adjustment**: {impact_data['adjustments']['confidence_adjustment_pct']:+.1f}%")
            
            if impact_data['adjustments']['triggers']:
                st.write("**Active Triggers**:")
                for trigger in impact_data['adjustments']['triggers']:
                    st.write(f"• {trigger}")
            else:
                st.write("**No Active Triggers** (neutral conditions)")
        
        with evidence_tabs[2]:
            st.write("**Impact A/B Backtest (60 days)**")
            st.write(f"**Verdict**: {ab_results['verdict']}")
            st.write(f"**ECE Improvement**: {ab_results['metrics']['ece_improvement_pct']:+.1f}%")
            st.write(f"**Straddle Improvement**: {ab_results['metrics']['straddle_improvement']:+.2f}%")
            st.write(f"**Edge Hits**: {ab_results['metrics']['edge_hits_improvement']:+d}")
            
            st.write("**Artifact Links**:")
            st.write("• IMPACT_AB_REPORT.md")
            st.write("• IMPACT_AB_REPORT.csv")
        
        with evidence_tabs[3]:
            st.write("**News Sources Breakdown**")
            news_analysis = impact_data['news_analysis']
            st.write(f"**Total Items**: {news_analysis['items_count']}")
            st.write(f"**Muted (Uncorroborated)**: {news_analysis['muted_count']}")
            st.write(f"**Total Weight**: {news_analysis['total_weight']:.3f}")
            st.write(f"**Lookback**: {news_analysis['lookback_hours']} hours")
            
            if news_analysis['components']:
                st.write("**Score Components**:")
                for comp in news_analysis['components'][:5]:  # Show top 5
                    st.write(f"• {comp['source']} ({comp['category']}): {comp['item_score']:+.4f}")
            else:
                st.write("No qualifying news components")
            
            st.write("**Artifact Links**:")
            st.write("• NEWS_FEED_LOG.md")
            st.write("• NEWS_WHITELIST.md") 
            st.write("• NEWS_SOURCE_WEIGHTS.yaml")
        
        with evidence_tabs[4]:
            st.write("**Macro Events Detail**")
            macro_analysis = impact_data['macro_analysis']
            st.write(f"**Events Processed**: {macro_analysis['event_count']}")
            st.write(f"**High Impact**: {len(macro_analysis['high_impact_events'])}")
            st.write(f"**Aggregate Z-Score**: {macro_analysis['aggregate_z']:.2f}")
            
            if macro_analysis['high_impact_events']:
                st.write("**High Impact Events**:")
                for event in macro_analysis['high_impact_events']:
                    direction = "Positive" if event['z_score'] > 0 else "Negative"
                    st.write(f"• {event['event']}: {direction} (z={event['z_score']:.2f})")
            else:
                st.write("No high impact macro events detected")
            
            st.write("**Artifact Links**:")
            st.write("• MACRO_EVENTS.md")
            st.write("• NEWS_SCORE.md")
    
    elif impact_data['enabled'] and impact_data.get('muted', False):
        with evidence_tabs[1]:
            st.warning("**Impact Engine MUTED**")
            st.write(f"**Reason**: {impact_data.get('mute_reason', 'Unknown')}")
            st.write("**Status**: News ingestion continues, adjustments disabled")
            st.write("**Current Adjustments**: 0% (muted)")
        
        with evidence_tabs[2]:
            st.write("**Impact A/B Backtest (60 days)**")
            st.write(f"**Verdict**: {ab_results['verdict']}")
            st.write(f"**ECE Improvement**: {ab_results['metrics']['ece_improvement_pct']:+.1f}%")
            st.write(f"**Straddle Improvement**: {ab_results['metrics']['straddle_improvement']:+.2f}%")
            st.write(f"**Edge Hits**: {ab_results['metrics']['edge_hits_improvement']:+d}")
            
            st.write("**Performance Issue**: Impact adjustments currently disabled due to guardrail triggers")
        
        with evidence_tabs[3]:
            st.write("**Guardrails Status**")
            st.write("**AUTO-MUTE TRIGGERED**: Performance guardrails activated")
            st.write("**Effect**: Impact adjustments disabled, news ingestion continues")
            st.write("**Resolution**: Review IMPACT_GUARDRAILS_REPORT.md")
            st.write("**Re-enable**: Set NEWS_IMPACT_MUTED=false after fixes")
    
    else:
        with evidence_tabs[1]:
            st.write("**Macro Events Today**")
            if gates_data['macro_gate']['morning_events']:
                for event in gates_data['macro_gate']['morning_events']:
                    st.write(f"• {event} (8:30 ET)")
            else:
                st.write("No HIGH severity events at 8:30 ET")
            
            st.write(f"**Band Adjustment**: +{gates_data['macro_gate'].get('band_adjustment', 0):.0f}%")
            st.write(f"**AM Send**: {gates_data['macro_gate'].get('am_send_delay', 'Normal (7:00 ET)')}")
        
        with evidence_tabs[2]:
            st.warning("**News scoring disabled** (NEWS_ENABLED=false)")
            st.write("**To enable**: Set NEWS_ENABLED=true environment variable")
            st.write("**Sources parsed**: 0")
            st.write("**Adjustments applied**: None")
    
    # Footer
    st.divider()
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')} | Zen Council v0.1")


if __name__ == "__main__":
    main()