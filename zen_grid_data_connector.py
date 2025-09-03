import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from datetime import datetime, timedelta

# Snowflake connection
@st.cache_resource
def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    )

@st.cache_data
def load_forecast_data():
    """Load forecast performance data"""
    conn = get_snowflake_connection()
    
    # Get forecast vs actual data from postmortem table
    query = """
    SELECT 
        DATE,
        INDEX as SYMBOL,
        FORECAST_BIAS,
        ACTUAL_CLOSE,
        HIT,
        LOAD_TS
    FROM FORECAST_POSTMORTEM 
    ORDER BY DATE DESC
    LIMIT 100
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df

@st.cache_data
def load_market_data():
    """Load market price data"""
    conn = get_snowflake_connection()
    
    query = """
    SELECT 
        DATE,
        SPY_CLOSE,
        ES_CLOSE,
        VIX_CLOSE,
        VVIX_CLOSE
    FROM DAILY_MARKET_DATA 
    ORDER BY DATE DESC
    LIMIT 100
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_forecast_summary():
    """Load forecast summaries"""
    conn = get_snowflake_connection()
    
    try:
        query = """
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
        LIMIT 50
        """
        
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()

def calculate_accuracy_metrics(forecast_df):
    """Calculate accuracy metrics from forecast data"""
    if len(forecast_df) == 0:
        return {}
    
    total_forecasts = len(forecast_df)
    hits = forecast_df['HIT'].sum()
    accuracy = (hits / total_forecasts) * 100 if total_forecasts > 0 else 0
    
    # Accuracy by bias type
    bias_accuracy = forecast_df.groupby('FORECAST_BIAS')['HIT'].agg(['count', 'sum'])
    bias_accuracy['accuracy'] = (bias_accuracy['sum'] / bias_accuracy['count'] * 100).round(1)
    
    return {
        'overall_accuracy': accuracy,
        'total_forecasts': total_forecasts,
        'hits': hits,
        'misses': total_forecasts - hits,
        'bias_breakdown': bias_accuracy
    }

def main():
    st.set_page_config(
        page_title="Zen Grid Market Forecaster",
        page_icon="🎯",
        layout="wide"
    )
    
    st.title("🎯 Zen Grid Market Forecaster Dashboard")
    st.markdown("**Real-time analysis of your 88% accuracy forecasting system**")
    
    # Load data
    try:
        forecast_data = load_forecast_data()
        market_data = load_market_data()
        forecast_summary = load_forecast_summary()
        
        if len(forecast_data) == 0:
            st.warning("No forecast data found in FORECAST_POSTMORTEM table")
            return
            
        # Calculate metrics
        metrics = calculate_accuracy_metrics(forecast_data)
        
        # Metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Overall Accuracy", 
                f"{metrics['overall_accuracy']:.1f}%",
                delta=None
            )
        
        with col2:
            st.metric(
                "Total Forecasts", 
                metrics['total_forecasts']
            )
        
        with col3:
            st.metric(
                "Hits", 
                metrics['hits']
            )
        
        with col4:
            st.metric(
                "Misses", 
                metrics['misses']
            )
        
        # Main dashboard
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader("📊 Forecast Performance Over Time")
            
            # Create performance chart
            fig = go.Figure()
            
            # Add hit/miss scatter
            hits_data = forecast_data[forecast_data['HIT'] == True]
            misses_data = forecast_data[forecast_data['HIT'] == False]
            
            fig.add_trace(go.Scatter(
                x=hits_data['DATE'],
                y=hits_data['ACTUAL_CLOSE'],
                mode='markers',
                marker=dict(color='green', size=10),
                name='Hits',
                text=hits_data['FORECAST_BIAS'],
                hovertemplate='<b>%{text}</b><br>Date: %{x}<br>Price: %{y}<br>Result: HIT<extra></extra>'
            ))
            
            fig.add_trace(go.Scatter(
                x=misses_data['DATE'],
                y=misses_data['ACTUAL_CLOSE'],
                mode='markers',
                marker=dict(color='red', size=10),
                name='Misses',
                text=misses_data['FORECAST_BIAS'],
                hovertemplate='<b>%{text}</b><br>Date: %{x}<br>Price: %{y}<br>Result: MISS<extra></extra>'
            ))
            
            fig.update_layout(
                title="Forecast Results by Date",
                xaxis_title="Date",
                yaxis_title="Actual Close Price",
                hovermode='closest'
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("🎯 Accuracy Breakdown")
            
            # Bias accuracy breakdown
            if 'bias_breakdown' in metrics and len(metrics['bias_breakdown']) > 0:
                bias_df = metrics['bias_breakdown'].reset_index()
                
                fig_pie = px.pie(
                    bias_df, 
                    values='count', 
                    names='FORECAST_BIAS',
                    title="Forecasts by Bias Type"
                )
                st.plotly_chart(fig_pie, use_container_width=True)
                
                # Accuracy table
                st.subheader("Accuracy by Bias")
                display_df = bias_df[['FORECAST_BIAS', 'count', 'sum', 'accuracy']].copy()
                display_df.columns = ['Bias', 'Total', 'Hits', 'Accuracy %']
                st.dataframe(display_df, use_container_width=True)
        
        # Recent forecasts table
        st.subheader("📋 Recent Forecast Results")
        display_columns = ['DATE', 'SYMBOL', 'FORECAST_BIAS', 'ACTUAL_CLOSE', 'HIT']
        recent_forecasts = forecast_data[display_columns].head(10)
        
        # Style the dataframe
        def highlight_results(row):
            if row['HIT']:
                return ['background-color: #d4edda'] * len(row)
            else:
                return ['background-color: #f8d7da'] * len(row)
        
        st.dataframe(
            recent_forecasts.style.apply(highlight_results, axis=1),
            use_container_width=True
        )
        
        # Raw data expanders
        with st.expander("🔍 View Raw Data"):
            tab1, tab2, tab3 = st.tabs(["Forecast Data", "Market Data", "Forecast Summary"])
            
            with tab1:
                st.write("FORECAST_POSTMORTEM Table")
                st.dataframe(forecast_data)
            
            with tab2:
                st.write("DAILY_MARKET_DATA Table")
                st.dataframe(market_data)
            
            with tab3:
                st.write("FORECAST_SUMMARY Table")
                st.dataframe(forecast_summary)
    
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        st.write("Please check your Snowflake connection and table structures.")

if __name__ == "__main__":
    main()