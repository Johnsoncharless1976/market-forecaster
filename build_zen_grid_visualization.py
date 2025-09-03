#!/usr/bin/env python3
"""
Zen Grid Visualization System Builder
Creates comprehensive forecast analysis and pattern memory dashboard
"""

import os
from datetime import datetime

class ZenGridBuilder:
    def __init__(self):
        pass
        
    def create_zen_grid_dashboard(self):
        """Create the main Zen Grid visualization dashboard"""
        
        dashboard_code = '''#!/usr/bin/env python3
"""
Zen Grid Visualization Dashboard
Comprehensive forecast analysis with pattern recognition
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
import sys
import os
from datetime import datetime, timedelta
import json

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

try:
    from snowflake_conn import get_snowflake_connection
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

st.set_page_config(
    page_title="Zen Grid - Pattern Analysis",
    page_icon=":chart_with_upwards_trend:",
    layout="wide"
)

def load_forecast_data():
    """Load forecast and performance data"""
    
    if not SNOWFLAKE_AVAILABLE:
        # Generate comprehensive sample data for visualization
        dates = pd.date_range(start=datetime.now() - timedelta(days=30), periods=30, freq='D')
        data = []
        
        for i, date in enumerate(dates):
            # Vary accuracy to show patterns
            if i < 10:
                accuracy = 0.9  # Good period
            elif i < 20:
                accuracy = 0.6  # Poor period
            else:
                accuracy = 0.85  # Recovery
            
            predicted = ['BULL', 'CHOP', 'BEAR'][i % 3]
            actual = predicted if np.random.random() < accuracy else ['BULL', 'CHOP', 'BEAR'][np.random.randint(0, 3)]
            
            data.append({
                'FORECAST_DATE': date,
                'BULL_TARGET': 5500 + i*2,
                'BEAR_TARGET': 5400 + i*2,
                'CHOP_LOWER': 5440 + i*2,
                'CHOP_UPPER': 5460 + i*2,
                'PREDICTED_DIRECTION': predicted,
                'ACTUAL_DIRECTION': actual,
                'CONFIDENCE': 0.75 + (i * 0.005),
                'ACTUAL_CLOSE': 5450 + i*3 + np.random.randint(-20, 20),
                'ACCURACY': 1.0 if predicted == actual else 0.0
            })
        
        return pd.DataFrame(data)
    
    # Database query would go here
    return pd.DataFrame()

def load_miss_analysis():
    """Load post-mortem miss analysis data"""
    
    miss_files = []
    if os.path.exists("output/postmortem"):
        import glob
        miss_files = glob.glob("output/postmortem/miss_analysis_*.json")
    
    all_misses = []
    for file_path in miss_files:
        try:
            with open(file_path, 'r') as f:
                misses = json.load(f)
                all_misses.extend(misses)
        except:
            continue
    
    return pd.DataFrame(all_misses) if all_misses else pd.DataFrame()

def create_forecast_accuracy_grid(df):
    """Create the main Zen Grid showing forecast accuracy patterns"""
    
    if df.empty:
        return go.Figure()
    
    # Create accuracy rolling window
    df = df.sort_values('FORECAST_DATE')
    df['ROLLING_ACCURACY'] = df['ACCURACY'].rolling(window=5, min_periods=1).mean()
    
    # Create the grid visualization
    fig = make_subplots(
        rows=3, cols=1,
        subplot_titles=('Forecast Accuracy Over Time', 'Confidence vs Performance', 'Target Levels vs Actual'),
        vertical_spacing=0.08,
        row_heights=[0.4, 0.3, 0.3]
    )
    
    # Row 1: Accuracy trend with confidence bands
    fig.add_trace(
        go.Scatter(
            x=df['FORECAST_DATE'],
            y=df['ROLLING_ACCURACY'] * 100,
            mode='lines+markers',
            name='5-Day Rolling Accuracy',
            line=dict(color='blue', width=3),
            marker=dict(size=8)
        ), row=1, col=1
    )
    
    # Add individual accuracy points
    colors = ['green' if acc == 1.0 else 'red' for acc in df['ACCURACY']]
    fig.add_trace(
        go.Scatter(
            x=df['FORECAST_DATE'],
            y=df['ACCURACY'] * 100,
            mode='markers',
            name='Individual Forecasts',
            marker=dict(color=colors, size=12, symbol='diamond'),
            opacity=0.6
        ), row=1, col=1
    )
    
    # Row 2: Confidence vs Performance scatter
    fig.add_trace(
        go.Scatter(
            x=df['CONFIDENCE'],
            y=df['ACCURACY'],
            mode='markers',
            name='Confidence vs Accuracy',
            marker=dict(
                size=10,
                color=df['ACCURACY'],
                colorscale='RdYlGn',
                showscale=True,
                colorbar=dict(title="Accuracy", y=0.3)
            ),
            text=[f"{row['FORECAST_DATE'].strftime('%m/%d')}<br>{row['PREDICTED_DIRECTION']}" 
                  for _, row in df.iterrows()],
            hovertemplate='Confidence: %{x:.2f}<br>Accuracy: %{y}<br>%{text}<extra></extra>'
        ), row=2, col=1
    )
    
    # Row 3: Forecast bands vs actual closes
    fig.add_trace(
        go.Scatter(
            x=df['FORECAST_DATE'],
            y=df['BULL_TARGET'],
            mode='lines',
            name='Bull Target',
            line=dict(color='green', dash='dash'),
            opacity=0.7
        ), row=3, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=df['FORECAST_DATE'],
            y=df['BEAR_TARGET'],
            mode='lines',
            name='Bear Target',
            line=dict(color='red', dash='dash'),
            opacity=0.7
        ), row=3, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=df['FORECAST_DATE'],
            y=df['ACTUAL_CLOSE'],
            mode='lines+markers',
            name='Actual Close',
            line=dict(color='blue', width=2),
            marker=dict(size=6)
        ), row=3, col=1
    )
    
    # Update layout
    fig.update_layout(
        height=1000,
        title_text="Zen Grid: Forecast Pattern Analysis",
        title_x=0.5,
        showlegend=True
    )
    
    fig.update_yaxes(title_text="Accuracy %", row=1, col=1)
    fig.update_yaxes(title_text="Accuracy", row=2, col=1)
    fig.update_yaxes(title_text="Price Level", row=3, col=1)
    
    fig.update_xaxes(title_text="Date", row=3, col=1)
    fig.update_xaxes(title_text="Confidence Level", row=2, col=1)
    
    return fig

def create_miss_pattern_analysis(miss_df):
    """Analyze patterns in forecast misses"""
    
    if miss_df.empty:
        return go.Figure(), pd.DataFrame()
    
    # Convert forecast_date strings to datetime if needed
    if 'forecast_date' in miss_df.columns:
        miss_df['forecast_date'] = pd.to_datetime(miss_df['forecast_date'])
    
    # Pattern analysis
    cause_counts = miss_df['primary_cause'].value_counts() if 'primary_cause' in miss_df.columns else pd.Series()
    
    # Create pie chart of miss causes
    fig = go.Figure(data=[go.Pie(
        labels=cause_counts.index,
        values=cause_counts.values,
        hole=0.4,
        textinfo='label+percent',
        textposition='outside'
    )])
    
    fig.update_layout(
        title="Miss Cause Distribution",
        height=400
    )
    
    return fig, miss_df

def create_performance_metrics(df):
    """Calculate and display key performance metrics"""
    
    if df.empty:
        return {}
    
    total_forecasts = len(df)
    correct_forecasts = df['ACCURACY'].sum()
    accuracy_rate = correct_forecasts / total_forecasts if total_forecasts > 0 else 0
    
    avg_confidence = df['CONFIDENCE'].mean()
    confidence_accuracy_corr = df['CONFIDENCE'].corr(df['ACCURACY'])
    
    # Recent performance (last 7 days)
    recent_df = df.tail(7)
    recent_accuracy = recent_df['ACCURACY'].mean() if len(recent_df) > 0 else 0
    
    # Streak analysis
    current_streak = 0
    for accuracy in reversed(df['ACCURACY'].tolist()):
        if accuracy == 1.0:
            current_streak += 1
        else:
            break
    
    return {
        'total_forecasts': total_forecasts,
        'accuracy_rate': accuracy_rate,
        'recent_accuracy': recent_accuracy,
        'avg_confidence': avg_confidence,
        'confidence_correlation': confidence_accuracy_corr,
        'current_streak': current_streak
    }

def main():
    """Main dashboard application"""
    
    st.title("Zen Grid - Pattern Analysis Dashboard")
    st.markdown("**Visual analysis of forecast performance and learning patterns**")
    
    # Sidebar controls
    st.sidebar.title("Analysis Controls")
    
    # Date range selector
    end_date = st.sidebar.date_input("End Date", datetime.now().date())
    days_back = st.sidebar.slider("Days to Analyze", 7, 90, 30)
    
    # Refresh button
    if st.sidebar.button("Refresh Analysis"):
        st.rerun()
    
    # Load data
    with st.spinner("Loading forecast data..."):
        forecast_df = load_forecast_data()
        miss_df = load_miss_analysis()
    
    if forecast_df.empty:
        st.error("No forecast data available. Check database connection or run forecast system.")
        return
    
    # Calculate metrics
    metrics = create_performance_metrics(forecast_df)
    
    # Display key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Overall Accuracy",
            f"{metrics.get('accuracy_rate', 0):.1%}",
            delta=f"{(metrics.get('recent_accuracy', 0) - metrics.get('accuracy_rate', 0)):.1%}"
        )
    
    with col2:
        st.metric(
            "Total Forecasts",
            f"{metrics.get('total_forecasts', 0):,}"
        )
    
    with col3:
        st.metric(
            "Current Streak",
            f"{metrics.get('current_streak', 0)} hits"
        )
    
    with col4:
        st.metric(
            "Avg Confidence",
            f"{metrics.get('avg_confidence', 0):.2f}"
        )
    
    # Main Zen Grid visualization
    st.subheader("Zen Grid: Pattern Analysis")
    with st.spinner("Generating Zen Grid..."):
        zen_grid_fig = create_forecast_accuracy_grid(forecast_df)
        st.plotly_chart(zen_grid_fig, use_container_width=True)
    
    # Miss pattern analysis
    if not miss_df.empty:
        st.subheader("Miss Pattern Analysis")
        col_left, col_right = st.columns([1, 1])
        
        with col_left:
            miss_fig, _ = create_miss_pattern_analysis(miss_df)
            st.plotly_chart(miss_fig, use_container_width=True)
        
        with col_right:
            st.subheader("Recent Miss Details")
            if len(miss_df) > 0:
                recent_misses = miss_df.head(5)
                for _, miss in recent_misses.iterrows():
                    with st.expander(f"Miss: {miss.get('forecast_date', 'Unknown Date')}"):
                        st.write(f"**Type:** {miss.get('miss_type', 'Unknown')}")
                        st.write(f"**Cause:** {miss.get('primary_cause', 'Unknown')}")
                        st.write(f"**Details:** {miss.get('cause_details', 'No details')}")
            else:
                st.info("No miss analysis data available")
    
    # Performance trends
    st.subheader("Performance Trends")
    
    if len(forecast_df) > 5:
        # Rolling accuracy chart
        fig_trend = px.line(
            forecast_df, 
            x='FORECAST_DATE', 
            y=forecast_df['ACCURACY'].rolling(window=5).mean() * 100,
            title='5-Day Rolling Accuracy Trend'
        )
        fig_trend.add_hline(y=88, line_dash="dot", annotation_text="Target: 88%")
        st.plotly_chart(fig_trend, use_container_width=True)
    
    # Data tables
    st.subheader("Recent Forecast Data")
    
    # Display recent forecasts
    display_df = forecast_df.tail(10).copy()
    if not display_df.empty:
        display_df['FORECAST_DATE'] = display_df['FORECAST_DATE'].dt.strftime('%Y-%m-%d')
        display_df['ACCURACY'] = display_df['ACCURACY'].apply(lambda x: "✅ HIT" if x == 1.0 else "❌ MISS")
        
        st.dataframe(
            display_df[['FORECAST_DATE', 'PREDICTED_DIRECTION', 'ACTUAL_DIRECTION', 
                       'CONFIDENCE', 'ACCURACY']],
            use_container_width=True
        )
    
    # Footer info
    st.markdown("---")
    st.markdown(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.markdown(f"**Data Source:** {'Database' if SNOWFLAKE_AVAILABLE else 'Sample Data'}")

if __name__ == "__main__":
    main()
'''

        with open("zen_grid_dashboard.py", "w", encoding='utf-8') as f:
            f.write(dashboard_code)
            
        print("Generated Zen Grid dashboard: zen_grid_dashboard.py")
        
    def create_pattern_memory_system(self):
        """Create the pattern memory and learning system"""
        
        pattern_code = '''#!/usr/bin/env python3
"""
Pattern Memory System - Council's Playbook
Tracks recurring patterns in forecast performance
"""

import pandas as pd
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List

class PatternMemory:
    def __init__(self):
        self.patterns = {}
        self.pattern_file = "output/patterns/zen_patterns.json"
        
    def identify_patterns(self, forecast_df: pd.DataFrame, miss_df: pd.DataFrame) -> Dict:
        """Identify recurring patterns in forecasts and misses"""
        
        patterns = {
            'accuracy_patterns': self._find_accuracy_patterns(forecast_df),
            'miss_patterns': self._find_miss_patterns(miss_df),
            'confidence_patterns': self._find_confidence_patterns(forecast_df),
            'temporal_patterns': self._find_temporal_patterns(forecast_df)
        }
        
        return patterns
    
    def _find_accuracy_patterns(self, df: pd.DataFrame) -> Dict:
        """Find patterns in accuracy over time"""
        
        if df.empty:
            return {}
        
        # Weekly accuracy patterns
        df['weekday'] = df['FORECAST_DATE'].dt.day_name()
        weekday_accuracy = df.groupby('weekday')['ACCURACY'].mean().to_dict()
        
        # Confidence level accuracy
        df['confidence_bucket'] = pd.cut(df['CONFIDENCE'], 
                                       bins=[0, 0.7, 0.8, 0.9, 1.0], 
                                       labels=['Low', 'Med', 'High', 'Very High'])
        confidence_accuracy = df.groupby('confidence_bucket')['ACCURACY'].mean().to_dict()
        
        return {
            'weekday_performance': weekday_accuracy,
            'confidence_performance': {str(k): v for k, v in confidence_accuracy.items()}
        }
    
    def _find_miss_patterns(self, miss_df: pd.DataFrame) -> Dict:
        """Find patterns in forecast misses"""
        
        if miss_df.empty:
            return {}
        
        # Miss cause patterns
        cause_patterns = miss_df['primary_cause'].value_counts().to_dict() if 'primary_cause' in miss_df.columns else {}
        
        # Temporal miss patterns
        miss_df['weekday'] = pd.to_datetime(miss_df['forecast_date']).dt.day_name()
        weekday_misses = miss_df['weekday'].value_counts().to_dict()
        
        return {
            'cause_frequency': cause_patterns,
            'weekday_misses': weekday_misses
        }
    
    def _find_confidence_patterns(self, df: pd.DataFrame) -> Dict:
        """Find patterns in confidence vs performance"""
        
        if df.empty:
            return {}
        
        # Overconfidence detection
        high_conf_misses = df[(df['CONFIDENCE'] > 0.85) & (df['ACCURACY'] == 0)]
        overconfidence_rate = len(high_conf_misses) / len(df[df['CONFIDENCE'] > 0.85]) if len(df[df['CONFIDENCE'] > 0.85]) > 0 else 0
        
        return {
            'overconfidence_rate': overconfidence_rate,
            'high_confidence_accuracy': df[df['CONFIDENCE'] > 0.85]['ACCURACY'].mean()
        }
    
    def _find_temporal_patterns(self, df: pd.DataFrame) -> Dict:
        """Find time-based patterns"""
        
        if df.empty:
            return {}
        
        # Month-over-month trends
        df['month'] = df['FORECAST_DATE'].dt.month
        monthly_accuracy = df.groupby('month')['ACCURACY'].mean().to_dict()
        
        return {
            'monthly_performance': monthly_accuracy
        }
    
    def save_patterns(self, patterns: Dict):
        """Save identified patterns to file"""
        
        os.makedirs("output/patterns", exist_ok=True)
        
        # Add timestamp
        patterns['analysis_date'] = datetime.now().isoformat()
        patterns['version'] = '1.0'
        
        with open(self.pattern_file, 'w') as f:
            json.dump(patterns, f, indent=2, default=str)
        
        print(f"Patterns saved to {self.pattern_file}")
    
    def load_patterns(self) -> Dict:
        """Load previously saved patterns"""
        
        if os.path.exists(self.pattern_file):
            with open(self.pattern_file, 'r') as f:
                return json.load(f)
        return {}

def main():
    """Run pattern analysis"""
    
    print("=== PATTERN MEMORY ANALYSIS ===")
    
    memory = PatternMemory()
    
    # This would load actual data
    print("Pattern memory system initialized")
    print("Ready to analyze forecast patterns when connected to data")
    
    return 0

if __name__ == "__main__":
    exit(main())
'''

        with open("src/pattern_memory.py", "w", encoding='utf-8') as f:
            f.write(pattern_code)
            
        print("Generated pattern memory system: src/pattern_memory.py")
        
    def create_zen_launcher(self):
        """Create launcher for Zen Grid dashboard"""
        
        launcher_code = '''#!/usr/bin/env python3
"""
Zen Grid Dashboard Launcher
"""

import subprocess
import sys
import webbrowser
import time

def main():
    print("Zen Grid - Pattern Analysis Dashboard")
    print("=" * 50)
    print("Visual forecast analysis and pattern recognition")
    print("Starting dashboard...")
    
    try:
        # Start streamlit dashboard
        process = subprocess.Popen([
            sys.executable, "-m", "streamlit", "run", 
            "zen_grid_dashboard.py", "--server.port", "8506"
        ])
        
        # Wait then open browser
        time.sleep(3)
        webbrowser.open("http://localhost:8506")
        
        print("Dashboard running at http://localhost:8506")
        print("Press Ctrl+C to stop")
        
        # Wait for process
        process.wait()
        
    except KeyboardInterrupt:
        print("\\nStopping dashboard...")
        process.terminate()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
'''

        with open("launch_zen_grid.py", "w", encoding='utf-8') as f:
            f.write(launcher_code)
            
        print("Generated Zen Grid launcher: launch_zen_grid.py")

def main():
    """Build complete Zen Grid visualization system"""
    print("=== ZEN GRID VISUALIZATION BUILDER ===")
    print("Building comprehensive pattern analysis dashboard...")
    
    builder = ZenGridBuilder()
    
    # Create visualization components
    builder.create_zen_grid_dashboard()
    builder.create_pattern_memory_system()
    builder.create_zen_launcher()
    
    print("=== ZEN GRID BUILD COMPLETE ===")
    print("Generated files:")
    print("- zen_grid_dashboard.py (main visualization dashboard)")
    print("- src/pattern_memory.py (pattern analysis system)")
    print("- launch_zen_grid.py (dashboard launcher)")
    
    print("\\nTo launch Zen Grid dashboard:")
    print("python launch_zen_grid.py")
    print("\\nDashboard URL: http://localhost:8506")
    
    print("\\nVisualization features:")
    print("1. Forecast accuracy trends and patterns")
    print("2. Confidence vs performance analysis")
    print("3. Target levels vs actual price overlay")
    print("4. Miss pattern categorization")
    print("5. Performance metrics and streaks")
    print("6. Interactive pattern exploration")
    
    return 0

if __name__ == "__main__":
    exit(main())