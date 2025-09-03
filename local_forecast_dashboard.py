#!/usr/bin/env python3
"""
ZenMarket Forecaster Performance Dashboard
Shows forecast data directly from Snowflake database
NO API CALLS - Pure data visualization
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os
from datetime import datetime, timedelta

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

try:
    from snowflake_conn import get_snowflake_connection
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    st.error("Snowflake connection not available")

st.set_page_config(
    page_title="ZenMarket Forecaster Dashboard",
    page_icon=":chart_with_upwards_trend:",
    layout="wide"
)

def load_forecast_data():
    """Load forecast data from database"""
    
    if not SNOWFLAKE_AVAILABLE:
        # Fallback: Create sample data for demo
        dates = pd.date_range(start='2025-08-20', end='2025-09-03', freq='D')
        return pd.DataFrame({
            'FORECAST_DATE': dates,
            'BULL_TARGET': [5450 + i*10 for i in range(len(dates))],
            'BEAR_TARGET': [5350 + i*8 for i in range(len(dates))],
            'CHOP_LOWER': [5380 + i*9 for i in range(len(dates))],
            'CHOP_UPPER': [5420 + i*9 for i in range(len(dates))],
            'CONFIDENCE': [0.88, 0.92, 0.85, 0.90, 0.87, 0.89, 0.91, 0.86, 0.88, 0.90, 0.89, 0.87, 0.92, 0.88, 0.85],
            'ACTUAL_DIRECTION': ['UP', 'UP', 'DOWN', 'UP', 'CHOP', 'UP', 'UP', 'DOWN', 'UP', 'CHOP', 'UP', 'DOWN', 'UP', 'UP', None]
        })
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Get recent forecast data
        query = """
        SELECT 
            FORECAST_DATE,
            BULL_TARGET,
            BEAR_TARGET, 
            CHOP_LOWER,
            CHOP_UPPER,
            CONFIDENCE,
            ACTUAL_DIRECTION,
            CREATED_AT
        FROM FORECAST_DAILY 
        WHERE FORECAST_DATE >= CURRENT_DATE() - 30
        ORDER BY FORECAST_DATE DESC
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        if results:
            columns = ['FORECAST_DATE', 'BULL_TARGET', 'BEAR_TARGET', 'CHOP_LOWER', 'CHOP_UPPER', 'CONFIDENCE', 'ACTUAL_DIRECTION', 'CREATED_AT']
            df = pd.DataFrame(results, columns=columns)
            df['FORECAST_DATE'] = pd.to_datetime(df['FORECAST_DATE'])
            return df
        else:
            st.warning("No forecast data found in database")
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()

def load_market_data():
    """Load market OHLCV data"""
    
    if not SNOWFLAKE_AVAILABLE:
        return pd.DataFrame()
        
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT 
            DATE,
            SYMBOL,
            OPEN,
            HIGH,
            LOW,
            CLOSE,
            VOLUME
        FROM MARKET_OHLCV 
        WHERE DATE >= CURRENT_DATE() - 30
        AND SYMBOL IN ('^GSPC', '^VIX', 'ES=F')
        ORDER BY DATE DESC, SYMBOL
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        if results:
            columns = ['DATE', 'SYMBOL', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']
            df = pd.DataFrame(results, columns=columns)
            df['DATE'] = pd.to_datetime(df['DATE'])
            return df
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Market data error: {e}")
        return pd.DataFrame()

def calculate_accuracy_metrics(df):
    """Calculate forecast accuracy from data"""
    
    if df.empty or 'ACTUAL_DIRECTION' not in df.columns:
        return {}
        
    # Filter for forecasts with actual outcomes
    completed = df[df['ACTUAL_DIRECTION'].notna()]
    
    if completed.empty:
        return {"message": "No completed forecasts to analyze"}
    
    total_forecasts = len(completed)
    
    # Calculate directional accuracy (simplified)
    # This would need your specific logic for determining if forecast was correct
    accuracy_data = {
        "total_forecasts": total_forecasts,
        "avg_confidence": completed['CONFIDENCE'].mean() if 'CONFIDENCE' in completed.columns else 0,
        "date_range": f"{completed['FORECAST_DATE'].min().date()} to {completed['FORECAST_DATE'].max().date()}"
    }
    
    return accuracy_data

def main():
    """Main dashboard"""
    
    st.title("ZenMarket Forecaster Dashboard")
    st.markdown("**Real-time forecast performance - No API costs**")
    
    # Sidebar
    st.sidebar.title("Controls")
    
    # Auto-refresh checkbox
    auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=False)
    if auto_refresh:
        st_autorefresh(interval=30000, key="datarefresh")
    
    # Manual refresh button
    if st.sidebar.button("Refresh Data"):
        st.rerun()
    
    # Main content
    col1, col2, col3 = st.columns(3)
    
    # Load data
    forecast_data = load_forecast_data()
    market_data = load_market_data()
    
    # Key metrics
    if not forecast_data.empty:
        accuracy_metrics = calculate_accuracy_metrics(forecast_data)
        
        with col1:
            st.metric("Total Forecasts", 
                     accuracy_metrics.get("total_forecasts", "N/A"))
        
        with col2:
            avg_conf = accuracy_metrics.get("avg_confidence", 0)
            if avg_conf > 0:
                st.metric("Avg Confidence", f"{avg_conf:.1%}")
            else:
                st.metric("Avg Confidence", "N/A")
        
        with col3:
            st.metric("Data Range", accuracy_metrics.get("date_range", "N/A"))
    
    # Forecast targets chart
    if not forecast_data.empty:
        st.subheader("Forecast Targets")
        
        fig = go.Figure()
        
        # Add forecast bands
        fig.add_trace(go.Scatter(
            x=forecast_data['FORECAST_DATE'],
            y=forecast_data['BULL_TARGET'],
            mode='lines',
            name='Bull Target',
            line=dict(color='green')
        ))
        
        fig.add_trace(go.Scatter(
            x=forecast_data['FORECAST_DATE'],
            y=forecast_data['BEAR_TARGET'], 
            mode='lines',
            name='Bear Target',
            line=dict(color='red')
        ))
        
        fig.add_trace(go.Scatter(
            x=forecast_data['FORECAST_DATE'],
            y=forecast_data['CHOP_UPPER'],
            mode='lines',
            name='Chop Upper',
            line=dict(color='orange', dash='dash')
        ))
        
        fig.add_trace(go.Scatter(
            x=forecast_data['FORECAST_DATE'],
            y=forecast_data['CHOP_LOWER'],
            mode='lines',
            name='Chop Lower', 
            line=dict(color='orange', dash='dash')
        ))
        
        fig.update_layout(
            title="Forecast Target Levels",
            xaxis_title="Date",
            yaxis_title="Price Level",
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Market data overlay
    if not market_data.empty:
        spx_data = market_data[market_data['SYMBOL'] == '^GSPC']
        
        if not spx_data.empty:
            st.subheader("SPX vs Forecast Levels")
            
            # Create subplot with SPX price and forecast levels
            fig2 = make_subplots(specs=[[{"secondary_y": False}]])
            
            # Add SPX close price
            fig2.add_trace(go.Scatter(
                x=spx_data['DATE'],
                y=spx_data['CLOSE'],
                mode='lines',
                name='SPX Close',
                line=dict(color='blue', width=2)
            ))
            
            # Overlay forecast levels if dates match
            if not forecast_data.empty:
                # Simple overlay - you'd want to match dates properly
                recent_forecasts = forecast_data.tail(10)
                
                for _, row in recent_forecasts.iterrows():
                    fig2.add_hline(
                        y=row['BULL_TARGET'], 
                        line_dash="dot",
                        line_color="green",
                        annotation_text=f"Bull: {row['BULL_TARGET']}"
                    )
            
            fig2.update_layout(
                title="SPX Price vs Forecast Levels", 
                xaxis_title="Date",
                yaxis_title="Price"
            )
            
            st.plotly_chart(fig2, use_container_width=True)
    
    # Raw data tables
    st.subheader("Raw Data")
    
    tab1, tab2 = st.tabs(["Forecast Data", "Market Data"])
    
    with tab1:
        if not forecast_data.empty:
            st.dataframe(forecast_data, use_container_width=True)
        else:
            st.info("No forecast data available")
    
    with tab2:
        if not market_data.empty:
            st.dataframe(market_data, use_container_width=True)
        else:
            st.info("No market data available")
    
    # System status
    st.subheader("System Status")
    
    status_col1, status_col2 = st.columns(2)
    
    with status_col1:
        db_status = "Connected" if SNOWFLAKE_AVAILABLE else "Disconnected"
        st.write(f"Database: {db_status}")
        
        data_status = "Available" if not forecast_data.empty else "No Data"
        st.write(f"Forecast Data: {data_status}")
    
    with status_col2:
        last_update = datetime.now().strftime("%H:%M:%S")
        st.write(f"Last Update: {last_update}")
        
        st.write("Cost: $0.00 (No API calls)")

# Add auto-refresh component if available
try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    # Define dummy function if not available
    def st_autorefresh(**kwargs):
        pass

if __name__ == "__main__":
    main()
