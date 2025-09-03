# zen_grid_with_backtest.py
import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import os
from datetime import datetime
import numpy as np

def create_connection():
    """Create a fresh Snowflake connection using Streamlit secrets"""
    try:
        conn = snowflake.connector.connect(
            account=st.secrets["snowflake"]["account"],
            user=st.secrets["snowflake"]["user"],
            password=st.secrets["snowflake"]["password"],
            database=st.secrets["snowflake"]["database"],
            schema=st.secrets["snowflake"]["schema"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            client_session_keep_alive=True
        )
        return conn
    except Exception as e:
        st.error(f"Connection failed: {str(e)}")
        return None

def load_backtest_results():
    """Load backtest results if available"""
    try:
        # Look for most recent backtest file
        import glob
        backtest_files = glob.glob("historical_backtest_results_*.csv")
        if backtest_files:
            latest_file = max(backtest_files, key=os.path.getctime)
            return pd.read_csv(latest_file), latest_file
        return None, None
    except Exception as e:
        st.error(f"Error loading backtest results: {str(e)}")
        return None, None

def load_learning_corpus():
    """Load learning corpus if available"""
    try:
        import glob
        corpus_files = glob.glob("learning_corpus_*.json")
        if corpus_files:
            latest_file = max(corpus_files, key=os.path.getctime)
            with open(latest_file, 'r') as f:
                return json.load(f), latest_file
        return None, None
    except Exception as e:
        st.error(f"Error loading learning corpus: {str(e)}")
        return None, None

def load_news_analysis():
    """Load news analysis results"""
    try:
        import glob
        news_files = glob.glob("comprehensive_news_analysis_*.json")
        if news_files:
            latest_file = max(news_files, key=os.path.getctime)
            with open(latest_file, 'r') as f:
                return json.load(f), latest_file
        return None, None
    except Exception as e:
        st.error(f"Error loading news analysis: {str(e)}")
        return None, None

def load_live_forecast_data():
    """Load current live forecast data from Snowflake"""
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
        FROM ZEN_MARKET.FORECASTING.FORECAST_POSTMORTEM 
        ORDER BY DATE DESC
        LIMIT 100
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
        FROM ZEN_MARKET.FORECASTING.DAILY_MARKET_DATA 
        ORDER BY DATE DESC
        LIMIT 100
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
        FROM ZEN_MARKET.FORECASTING.FORECAST_SUMMARY 
        ORDER BY DATE DESC
        LIMIT 50
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
        page_title="Zen Grid Market Forecaster - Enhanced",
        page_icon="🎯",
        layout="wide"
    )
    
    st.title("🎯 Zen Grid Market Forecaster Dashboard")
    st.markdown("**Enhanced with Backtesting & Adaptive Learning**")
    st.markdown("*SPX 0DTE Specialization - Real-time analysis of your 88% accuracy forecasting system*")
    
    # Sidebar with system status
    with st.sidebar:
        st.subheader("🔧 System Status")
        
        # Connection status
        conn = create_connection()
        if conn:
            st.success("✅ Snowflake Connected")
            conn.close()
        else:
            st.error("❌ Snowflake Disconnected")
        
        # Backtest status
        backtest_df, backtest_file = load_backtest_results()
        if backtest_df is not None:
            st.success(f"✅ Backtest Data Loaded")
            st.caption(f"Records: {len(backtest_df)}")
        else:
            st.warning("⏳ No Backtest Data")
            st.caption("Run historical_backtest_generator.py")
        
        # Learning corpus status
        corpus_data, corpus_file = load_learning_corpus()
        if corpus_data:
            st.success(f"✅ Learning Corpus Ready")
            st.caption(f"Records: {len(corpus_data)}")
        else:
            st.warning("⏳ No Learning Corpus")
        
        # News analysis status
        news_data, news_file = load_news_analysis()
        if news_data:
            st.success(f"✅ News Sources Analyzed")
            successful_feeds = len([f for f in news_data if f.get('status') == 'SUCCESS'])
            st.caption(f"Working feeds: {successful_feeds}")
        else:
            st.warning("⏳ No News Analysis")
    
    # Main dashboard tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Live Performance", "🔄 Backtesting Results", "📰 News Analysis", "🧠 Learning Progress"])
    
    with tab1:
        st.subheader("Live Forecast Performance")
        
        # Load live data
        forecast_df, market_df, summary_df = load_live_forecast_data()
        
        if forecast_df is not None and len(forecast_df) > 0:
            # Current metrics
            total_forecasts = len(forecast_df)
            hits = forecast_df['HIT'].sum()
            accuracy = (hits / total_forecasts) * 100 if total_forecasts > 0 else 0
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Live Accuracy", f"{accuracy:.1f}%")
            with col2:
                st.metric("Total Forecasts", total_forecasts)
            with col3:
                st.metric("Hits", hits)
            with col4:
                st.metric("Misses", total_forecasts - hits)
            
            # Recent forecasts table
            st.subheader("Recent Forecast Results")
            display_df = forecast_df[['DATE', 'SYMBOL', 'FORECAST_BIAS', 'ACTUAL_CLOSE', 'HIT']].head(10)
            display_df['HIT'] = display_df['HIT'].apply(lambda x: "✅ HIT" if x else "❌ MISS")
            st.dataframe(display_df, use_container_width=True)
        else:
            st.info("No live forecast data available yet. System will populate after tomorrow's 8:40 ET execution.")
    
    with tab2:
        st.subheader("Historical Backtesting Results")
        
        if backtest_df is not None:
            # Backtest performance metrics
            if 'forecast_hit' in backtest_df.columns:
                backtest_accuracy = backtest_df['forecast_hit'].mean() * 100
                total_backtest = len(backtest_df)
                backtest_hits = backtest_df['forecast_hit'].sum()
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Backtest Accuracy", f"{backtest_accuracy:.1%}")
                with col2:
                    st.metric("Historical Forecasts", total_backtest)
                with col3:
                    st.metric("Historical Hits", backtest_hits)
                with col4:
                    st.metric("Data Period", f"{backtest_df['DATE'].min()} to {backtest_df['DATE'].max()}")
                
                # Performance over time chart
                if len(backtest_df) > 20:
                    st.subheader("📈 Historical Performance Trend")
                    
                    # Rolling accuracy calculation
                    backtest_df['rolling_accuracy'] = backtest_df['forecast_hit'].rolling(window=30).mean() * 100
                    
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=pd.to_datetime(backtest_df['DATE']),
                        y=backtest_df['rolling_accuracy'],
                        mode='lines',
                        name='30-Day Rolling Accuracy',
                        line=dict(color='blue', width=2)
                    ))
                    
                    fig.add_hline(y=88, line_dash="dash", line_color="red", 
                                 annotation_text="Target: 88%")
                    
                    fig.update_layout(
                        title="Historical Forecast Accuracy Trend",
                        xaxis_title="Date",
                        yaxis_title="Accuracy (%)",
                        yaxis=dict(range=[0, 100])
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                
                # Bias performance breakdown
                if 'forecast_bias' in backtest_df.columns:
                    st.subheader("🎯 Performance by Forecast Bias")
                    
                    bias_performance = backtest_df.groupby('forecast_bias').agg({
                        'forecast_hit': ['count', 'sum', 'mean']
                    }).round(3)
                    bias_performance.columns = ['Total', 'Hits', 'Accuracy']
                    bias_performance['Accuracy'] = bias_performance['Accuracy'] * 100
                    
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.dataframe(bias_performance, use_container_width=True)
                    
                    with col2:
                        # Pie chart of bias distribution
                        fig_pie = px.pie(
                            bias_performance.reset_index(),
                            values='Total',
                            names='forecast_bias',
                            title='Forecast Bias Distribution'
                        )
                        st.plotly_chart(fig_pie, use_container_width=True)
            
            # File info
            st.caption(f"Data source: {backtest_file}")
        else:
            st.info("No backtest results available. Run the historical backtesting system to generate performance data.")
            st.code("python historical_backtest_generator.py")
    
    with tab3:
        st.subheader("News Source Analysis")
        
        if news_data:
            # Successful feeds
            successful_feeds = [f for f in news_data if f.get('status') == 'SUCCESS']
            failed_feeds = [f for f in news_data if f.get('status') != 'SUCCESS']
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Working Feeds", len(successful_feeds))
            with col2:
                st.metric("Failed Feeds", len(failed_feeds))
            
            if successful_feeds:
                st.subheader("📈 Top Performing News Sources")
                
                # Sort by average impact score
                feed_performance = []
                for feed in successful_feeds:
                    if feed.get('analysis'):
                        analysis = feed['analysis']
                        feed_performance.append({
                            'Source': feed['name'],
                            'Avg Impact': analysis.get('avg_impact_score', 0),
                            'High Impact Articles': analysis.get('high_impact_articles', 0),
                            'Total Articles': analysis.get('total_articles', 0),
                            'Categories': ', '.join(list(analysis.get('category_distribution', {}).keys())[:3])
                        })
                
                if feed_performance:
                    performance_df = pd.DataFrame(feed_performance).sort_values('Avg Impact', ascending=False)
                    st.dataframe(performance_df, use_container_width=True)
                    
                    # Top 5 sources chart
                    top_5 = performance_df.head(5)
                    fig = px.bar(
                        top_5,
                        x='Source',
                        y='Avg Impact',
                        title='Top 5 News Sources by Impact Score',
                        color='High Impact Articles'
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            st.caption(f"Analysis from: {news_file}")
        else:
            st.info("No news analysis available. Run the comprehensive news expansion to analyze sources.")
            st.code("python comprehensive_news_expansion.py")
    
    with tab4:
        st.subheader("Adaptive Learning Progress")
        
        if corpus_data:
            st.success(f"Learning corpus contains {len(corpus_data)} historical forecasts")
            
            # Learning readiness check
            if len(corpus_data) >= 50:
                st.success("✅ Sufficient data for adaptive learning")
                st.info("Ready to run adaptive parameter discovery")
                st.code("python adaptive_news_attribution_learning.py")
            else:
                st.warning(f"⏳ Need {50 - len(corpus_data)} more records for optimal learning")
            
            # Sample learning data
            st.subheader("📋 Sample Learning Records")
            sample_df = pd.DataFrame(corpus_data[:10])
            if not sample_df.empty:
                display_cols = ['DATE', 'FORECAST_BIAS', 'HIT', 'PRICE_CHANGE_PCT', 'LEVEL_BREACH']
                available_cols = [col for col in display_cols if col in sample_df.columns]
                if available_cols:
                    st.dataframe(sample_df[available_cols], use_container_width=True)
        else:
            st.info("No learning corpus available. Generate historical data first.")
            
        # Learning status summary
        st.subheader("🎯 System Readiness")
        
        readiness_items = [
            ("Historical Data", backtest_df is not None),
            ("Learning Corpus", corpus_data is not None),
            ("News Sources", news_data is not None),
            ("Live Data Pipeline", forecast_df is not None and len(forecast_df) > 0)
        ]
        
        for item, ready in readiness_items:
            if ready:
                st.success(f"✅ {item}")
            else:
                st.warning(f"⏳ {item}")
    
    # Footer
    st.markdown("---")
    st.markdown("🎯 **Zen Council Market Forecaster** | Enhanced Dashboard with Backtesting & Learning")
    
    # Auto-refresh option
    if st.checkbox("Auto-refresh every 30 seconds"):
        import time
        time.sleep(30)
        st.experimental_rerun()

if __name__ == "__main__":
    main()