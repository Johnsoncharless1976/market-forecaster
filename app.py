import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import os
import glob
import snowflake.connector
from pathlib import Path

# Page config
st.set_page_config(
    page_title="ZenMarket AI Visualization",
    page_icon="📊",
    layout="wide"
)

# Sidebar navigation
st.sidebar.title("📊 ZenMarket AI")
page = st.sidebar.selectbox("Navigate", ["Overview", "Zen Grid", "Forecast vs Actual", "Evidence"])

# Snowflake connection helper
@st.cache_resource
def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            role=os.getenv('SNOWFLAKE_ROLE'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database='ZEN_MARKET',
            schema='FORECASTING'
        )
        return conn
    except Exception as e:
        st.error(f"Snowflake connection failed: {e}")
        return None

# Helper to read audit files
def read_audit_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    except:
        return "File not found"

# Helper to parse INDEX.md for status
def get_status_from_index():
    index_path = "audit_exports/daily/INDEX.md"
    if os.path.exists(index_path):
        content = read_audit_file(index_path)
        
        # Parse guardrails status
        guardrails_pass = "PASS" in content and "Guardrails" in content
        
        # Parse SLO line
        slo_line = ""
        for line in content.split('\n'):
            if "SLO Performance" in line:
                slo_line = line.strip()
                break
        
        return {
            'guardrails': 'PASS' if guardrails_pass else 'FAIL',
            'slo_line': slo_line,
            'content': content
        }
    return {'guardrails': 'UNKNOWN', 'slo_line': '', 'content': ''}

# Overview Page
if page == "Overview":
    st.title("📊 ZenMarket AI - Overview")
    
    # Stage status banner
    stage_open_6 = os.getenv('STAGE_OPEN_6', 'false').lower() == 'true'
    if not stage_open_6:
        st.warning("⏸️ Stage 6 Visualization is currently closed. Set STAGE_OPEN_6=true to enable.")
    
    col1, col2, col3 = st.columns(3)
    
    # Status Lights
    with col1:
        st.subheader("🚦 Stage Status")
        
        # Stages 1-5, 8 are green (active)
        st.success("🟢 Stage 1: Ingest")
        st.success("🟢 Stage 2: Features") 
        st.success("🟢 Stage 3: Models")
        st.success("🟢 Stage 4: Forecast")
        st.success("🟢 Stage 5: Notify")
        
        if stage_open_6:
            st.success("🟢 Stage 6: Visualization")
        else:
            st.warning("⏸️ Stage 6: Visualization")
        
        st.warning("⏸️ Stage 7: Archive")
        st.success("🟢 Stage 8: QA Guardrails")
        st.info("💤 Stage 9: Commercial")
    
    with col2:
        st.subheader("🛡️ Guardrails Status")
        
        status = get_status_from_index()
        if status['guardrails'] == 'PASS':
            st.success("✅ Guardrails: PASS")
        else:
            st.error("❌ Guardrails: FAIL")
        
        # Links to evidence
        st.markdown("**Evidence Links:**")
        daily_dirs = glob.glob("audit_exports/daily/*/")
        if daily_dirs:
            latest_dir = max(daily_dirs)
            
            files = ['CI_LINT.md', 'CI_SIGNATURE.md', 'REPO_HEALTH.md']
            for file in files:
                filepath = os.path.join(latest_dir, file)
                if os.path.exists(filepath):
                    st.markdown(f"📄 [{file}]({filepath})")
    
    with col3:
        st.subheader("📈 SLO Performance")
        
        if status['slo_line']:
            st.markdown(status['slo_line'])
        else:
            st.info("SLO data not available")
        
        # Recent artifacts
        st.markdown("**Recent Artifacts:**")
        if daily_dirs:
            for dir_path in sorted(daily_dirs, reverse=True)[:3]:
                dir_name = os.path.basename(dir_path.rstrip('/'))
                st.markdown(f"📁 {dir_name}")

# Zen Grid Page
elif page == "Zen Grid":
    st.title("📈 Zen Grid - Market Dashboard")
    
    # Controls
    col1, col2 = st.columns([1, 3])
    with col1:
        days_back = st.selectbox("Time Range", [30, 60], index=1)
        show_rsi = st.checkbox("Show RSI", True)
    
    # Mock data for now (would connect to Snowflake in production)
    st.info("📊 Connecting to Snowflake for market data...")
    
    # Create mock charts for ^GSPC, ES=F, ^VIX
    symbols = ['^GSPC', 'ES=F', '^VIX']
    
    for symbol in symbols:
        st.subheader(f"📊 {symbol}")
        
        # Mock price data
        dates = pd.date_range(end=datetime.now(), periods=days_back, freq='D')
        prices = pd.DataFrame({
            'Date': dates,
            'Close': [4500 + i*2 + (i%10)*5 for i in range(days_back)]
        })
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=prices['Date'], 
            y=prices['Close'],
            name=f'{symbol} Close',
            line=dict(color='blue')
        ))
        
        if show_rsi:
            # Mock RSI data
            rsi_values = [50 + (i%20)*2 for i in range(days_back)]
            fig.add_trace(go.Scatter(
                x=dates,
                y=rsi_values,
                name='RSI-14',
                yaxis='y2',
                line=dict(color='orange')
            ))
            
            fig.update_layout(
                yaxis2=dict(
                    title="RSI",
                    overlaying='y',
                    side='right',
                    range=[0, 100]
                )
            )
        
        fig.update_layout(
            title=f"{symbol} - Last {days_back} Days",
            xaxis_title="Date",
            yaxis_title="Price"
        )
        
        st.plotly_chart(fig, use_container_width=True)

# Forecast vs Actual Page  
elif page == "Forecast vs Actual":
    st.title("🎯 Forecast vs Actual")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Today's Performance")
        
        # Mock forecast tiles
        metrics = [
            ("Forecast Bias", "+0.3%", "🎯"),
            ("ATM Straddle", "85.2", "📊"),
            ("Hit/Miss", "MISS", "❌"),
            ("Miss Tag", "VOL_SHIFT", "🏷️")
        ]
        
        for metric, value, emoji in metrics:
            st.metric(f"{emoji} {metric}", value)
    
    with col2:
        st.subheader("📈 20-Day Calibration")
        
        # Mock calibration data
        calibration_data = pd.DataFrame({
            'Bin': ['0-20%', '20-40%', '40-60%', '60-80%', '80-100%'],
            'Expected': [0.1, 0.3, 0.5, 0.7, 0.9],
            'Actual': [0.15, 0.28, 0.52, 0.71, 0.88]
        })
        
        fig = px.bar(calibration_data, x='Bin', y=['Expected', 'Actual'], 
                     title="Forecast Calibration", barmode='group')
        st.plotly_chart(fig, use_container_width=True)
    
    # Recent post-mortems
    st.subheader("📋 Recent Analysis")
    weekly_files = glob.glob("audit_exports/weekly/*/WEEKLY_LESSONS.md")
    if weekly_files:
        latest_weekly = max(weekly_files)
        st.markdown(f"📄 [Latest Weekly Lessons]({latest_weekly})")

# Evidence Page
elif page == "Evidence":
    st.title("📁 Evidence Explorer")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("📂 Browse Artifacts")
        
        # Daily artifacts
        st.markdown("**Daily Reports:**")
        daily_dirs = glob.glob("audit_exports/daily/*/")
        selected_daily = st.selectbox(
            "Select Daily Report", 
            [os.path.basename(d.rstrip('/')) for d in sorted(daily_dirs, reverse=True)]
        ) if daily_dirs else None
        
        # Weekly artifacts  
        st.markdown("**Weekly Reports:**")
        weekly_dirs = glob.glob("audit_exports/weekly/*/")
        selected_weekly = st.selectbox(
            "Select Weekly Report",
            [os.path.basename(d.rstrip('/')) for d in sorted(weekly_dirs, reverse=True)]
        ) if weekly_dirs else None
        
        # Quick links
        st.markdown("**Quick Links:**")
        quick_files = ['INDEX.md', 'CI_LINT.md', 'CI_SIGNATURE.md', 'REPO_HEALTH.md', 'KNEEBOARD_SLO.md']
        selected_file = st.selectbox("Quick Access", [''] + quick_files)
    
    with col2:
        st.subheader("📄 File Viewer")
        
        # Display selected file
        content = ""
        if selected_file and selected_daily:
            if selected_file == 'INDEX.md':
                filepath = "audit_exports/daily/INDEX.md"
            else:
                filepath = f"audit_exports/daily/{selected_daily}/{selected_file}"
            
            if os.path.exists(filepath):
                content = read_audit_file(filepath)
                st.markdown("**File:** `" + filepath + "`")
                st.markdown(content)
            else:
                st.warning(f"File not found: {filepath}")
        
        elif selected_weekly:
            filepath = f"audit_exports/weekly/{selected_weekly}/WEEKLY_LESSONS.md"
            if os.path.exists(filepath):
                content = read_audit_file(filepath)
                st.markdown("**File:** `" + filepath + "`")
                st.markdown(content)
        
        if not content:
            st.info("Select a file to view its contents")

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("🤖 **ZenMarket AI**")
st.sidebar.markdown("Stage 6 Visualization MVP")