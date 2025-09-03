import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime

def create_connection():
    """Create a fresh Snowflake connection"""
    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            client_session_keep_alive=True
        )
        return conn
    except Exception as e:
        st.error(f"Connection failed: {str(e)}")
        return None

def load_data():
    """Load all data in one connection"""
    conn = create_connection()
    if not conn:
        return None, None, None
    
    try:
        # Load forecast postmortem data
        forecast_query = """
        SELECT 
            DATE,
            INDEX as SYMBOL,
            FORECAST_BIAS,
            ACTUAL_CLOSE,
            HIT,
            LOAD_TS
        FROM FORECAST_POSTMORTEM 
        ORDER BY DATE DESC
        LIMIT 50
        """
        forecast_df = pd.read_sql(forecast_query, conn)
        
        # Load market data
        market_query = """
        SELECT 
            DATE,
            SPY_CLOSE,
            ES_CLOSE,
            VIX_CLOSE,
            VVIX_CLOSE
        FROM DAILY_MARKET_DATA 
        ORDER BY DATE DESC
        LIMIT 50
        """
        market_df = pd.read_sql(market_query, conn)
        
        # Load forecast summary
        summary_query = """
        SELECT 
            DATE,
            INDEX as SYMBOL,
            FORECAST_BIAS,
            SUPPORTS,
            RESISTANCES,
            ATM_STRADDLE,
            NOTES
        FROM FORECAST_SUMMARY 
        ORDER BY DATE DESC
        LIMIT 30
        """
        summary_df = pd.read_sql(summary_query, conn)
        
        return forecast_df, market_df, summary_df
        
    except Exception as e:
        st.error(f"Data loading failed: {str(e)}")
        return None, None, None
    finally:
        if conn:
            conn.close()

def main():
    st.set_page_config(
        page_title="Zen Grid Market Forecaster",
        page_icon="🎯",
        layout="wide"
    )
    
    st.title("🎯 Zen Grid Market Forecaster Dashboard")
    st.markdown("**Real-time analysis of your 88% accuracy forecasting system**")
    
    # Add refresh button
    if st.button("🔄 Refresh Data"):
        st.experimental_rerun()
    
    # Load all data
    with st.spinner("Loading data from Snowflake..."):
        forecast_df, market_df, summary_df = load_data()
    
    if forecast_df is None:
        st.error("Failed to load data. Please check your Snowflake connection.")
        return
    
    if len(forecast_df) == 0:
        st.warning("No forecast data found in FORECAST_POSTMORTEM table")
        return
    
    # Calculate metrics
    total_forecasts = len(forecast_df)
    hits = forecast_df['HIT'].sum()
    accuracy = (hits / total_forecasts) * 100 if total_forecasts > 0 else 0
    misses = total_forecasts - hits
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Overall Accuracy", f"{accuracy:.1f}%")
    with col2:
        st.metric("Total Forecasts", total_forecasts)
    with col3:
        st.metric("Hits", hits)
    with col4:
        st.metric("Misses", misses)
    
    # Main content
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.subheader("📊 Forecast Performance")
        
        if len(forecast_df) > 0:
            # Performance chart
            fig = go.Figure()
            
            hits_data = forecast_df[forecast_df['HIT'] == True]
            misses_data = forecast_df[forecast_df['HIT'] == False]
            
            if len(hits_data) > 0:
                fig.add_trace(go.Scatter(
                    x=hits_data['DATE'],
                    y=hits_data['ACTUAL_CLOSE'],
                    mode='markers',
                    marker=dict(color='green', size=12),
                    name='Hits',
                    text=hits_data['FORECAST_BIAS']
                ))
            
            if len(misses_data) > 0:
                fig.add_trace(go.Scatter(
                    x=misses_data['DATE'],
                    y=misses_data['ACTUAL_CLOSE'],
                    mode='markers',
                    marker=dict(color='red', size=12),
                    name='Misses',
                    text=misses_data['FORECAST_BIAS']
                ))
            
            fig.update_layout(
                title="Forecast Results Over Time",
                xaxis_title="Date",
                yaxis_title="Actual Close Price"
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🎯 Bias Analysis")
        
        # Bias breakdown
        if len(forecast_df) > 0:
            bias_stats = forecast_df.groupby('FORECAST_BIAS').agg({
                'HIT': ['count', 'sum']
            }).round(2)
            bias_stats.columns = ['Total', 'Hits']
            bias_stats['Accuracy %'] = (bias_stats['Hits'] / bias_stats['Total'] * 100).round(1)
            
            st.dataframe(bias_stats)
    
    # Recent forecasts table
    st.subheader("📋 Recent Forecast Results")
    
    if len(forecast_df) > 0:
        display_df = forecast_df[['DATE', 'SYMBOL', 'FORECAST_BIAS', 'ACTUAL_CLOSE', 'HIT']].head(10)
        
        # Apply styling
        def color_results(val):
            if val == True:
                return 'background-color: #d4edda'
            elif val == False:
                return 'background-color: #f8d7da'
            return ''
        
        styled_df = display_df.style.applymap(color_results, subset=['HIT'])
        st.dataframe(styled_df, use_container_width=True)
    
    # Data info
    st.subheader("📊 Data Summary")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.write("**Forecast Data**")
        if forecast_df is not None:
            st.write(f"Rows: {len(forecast_df)}")
            if len(forecast_df) > 0:
                st.write(f"Date range: {forecast_df['DATE'].min()} to {forecast_df['DATE'].max()}")
    
    with col2:
        st.write("**Market Data**") 
        if market_df is not None:
            st.write(f"Rows: {len(market_df)}")
            if len(market_df) > 0:
                st.write(f"Date range: {market_df['DATE'].min()} to {market_df['DATE'].max()}")
    
    with col3:
        st.write("**Summary Data**")
        if summary_df is not None:
            st.write(f"Rows: {len(summary_df)}")
            if len(summary_df) > 0:
                st.write(f"Date range: {summary_df['DATE'].min()} to {summary_df['DATE'].max()}")

if __name__ == "__main__":
    main()