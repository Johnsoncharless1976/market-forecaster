#!/usr/bin/env python3
"""
Stage-3 Forecast Audit - Score Forecasts vs Actuals
Reads FORECAST_DAILY and FEATURES_DAILY, calculates hit-rate and MAE, emits forecast_audit_summary.csv
"""

import os
import sys
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    stream=sys.stdout
)

def load_env_vars() -> dict:
    """Load Snowflake connection parameters from environment variables."""
    load_dotenv()
    
    required_vars = [
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_WAREHOUSE',
        'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA'
    ]
    
    env_vars = {}
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            raise ValueError(f"Missing required environment variable: {var}")
        env_vars[var] = value
    
    # Password authentication
    password = os.getenv('SNOWFLAKE_PASSWORD')
    if not password:
        raise ValueError("Missing SNOWFLAKE_PASSWORD environment variable")
    env_vars['SNOWFLAKE_PASSWORD'] = password
    
    # Optional role
    role = os.getenv('SNOWFLAKE_ROLE')
    if role:
        env_vars['SNOWFLAKE_ROLE'] = role
        
    return env_vars

def create_snowflake_engine(env_vars: dict) -> Engine:
    """Create Snowflake SQLAlchemy engine."""
    connection_string = (
        f"snowflake://{env_vars['SNOWFLAKE_USER']}:{env_vars['SNOWFLAKE_PASSWORD']}"
        f"@{env_vars['SNOWFLAKE_ACCOUNT']}/{env_vars['SNOWFLAKE_DATABASE']}/{env_vars['SNOWFLAKE_SCHEMA']}"
        f"?warehouse={env_vars['SNOWFLAKE_WAREHOUSE']}"
    )
    
    if 'SNOWFLAKE_ROLE' in env_vars:
        connection_string += f"&role={env_vars['SNOWFLAKE_ROLE']}"
    
    return create_engine(connection_string)

def fetch_forecast_vs_actual_data(engine: Engine, lookback_days: int = 30) -> pd.DataFrame:
    """
    Fetch forecasts and corresponding actuals for scoring.
    Join FORECAST_DAILY with FEATURES_DAILY on (SYMBOL, TRADE_DATE).
    """
    cutoff_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
    
    # First, check if FORECAST_DAILY has any data
    count_query = text("SELECT COUNT(*) FROM FORECAST_DAILY")
    with engine.connect() as conn:
        result = conn.execute(count_query)
        forecast_count = result.fetchone()[0]
        
    if forecast_count == 0:
        logging.warning("FORECAST_DAILY table is empty - no forecasts to audit")
        return pd.DataFrame()
    
    # Updated query to match actual table schema
    query = text(f"""
    SELECT 
        f.SYMBOL,
        f.TRADE_DATE as FORECAST_DATE,
        f.PREDICTION,
        f.PROB_UP,
        f.PROB_DOWN,
        f.CONFIDENCE,
        f.CREATED_AT as FORECAST_CREATED_AT,
        a.CLOSE as ACTUAL_CLOSE,
        a.ADJ_CLOSE as ACTUAL_ADJ_CLOSE,
        a.RETURN_1D as ACTUAL_RETURN_1D
    FROM FORECAST_DAILY f
    INNER JOIN FEATURES_DAILY a 
        ON f.SYMBOL = a.SYMBOL 
        AND f.TRADE_DATE = a.TRADE_DATE
    WHERE f.TRADE_DATE >= '{cutoff_date}'
        AND a.CLOSE IS NOT NULL
        AND f.PREDICTION IS NOT NULL
    ORDER BY f.SYMBOL, f.TRADE_DATE DESC
    """)
    
    logging.info(f"Fetching forecast vs actual data for last {lookback_days} days...")
    df = pd.read_sql(query, engine)
    logging.info(f"Retrieved {len(df)} forecast-actual pairs")
    
    return df

def calculate_forecast_metrics(df: pd.DataFrame) -> dict:
    """Calculate forecast accuracy metrics for directional predictions."""
    if len(df) == 0:
        return {
            'total_forecasts': 0,
            'hit_rate_directional': 0.0,
            'avg_confidence': 0.0,
            'avg_prob_up': 0.0,
            'avg_prob_down': 0.0,
            'status': 'Yellow',
            'status_reason': 'No forecast data available for audit'
        }
    
    # Convert predictions to boolean (UP = True, DOWN = False)
    df['predicted_up'] = df['PREDICTION'].str.upper() == 'UP'
    df['actual_up'] = df['ACTUAL_RETURN_1D'] > 0
    
    # Calculate directional hit rate
    hits = df['predicted_up'] == df['actual_up']
    hit_rate = hits.mean() * 100
    
    # Calculate average metrics
    avg_confidence = df['CONFIDENCE'].mean() * 100 if 'CONFIDENCE' in df.columns else 0
    avg_prob_up = df['PROB_UP'].mean() * 100 if 'PROB_UP' in df.columns else 0
    avg_prob_down = df['PROB_DOWN'].mean() * 100 if 'PROB_DOWN' in df.columns else 0
    
    # Determine status based on hit rate
    if hit_rate >= 60:
        status = 'Green'
        reason = f'Directional accuracy {hit_rate:.1f}% meets performance threshold'
    elif hit_rate >= 50:
        status = 'Yellow'
        reason = f'Directional accuracy {hit_rate:.1f}% needs improvement'
    else:
        status = 'Red'
        reason = f'Directional accuracy {hit_rate:.1f}% below acceptable threshold'
    
    return {
        'total_forecasts': len(df),
        'hit_rate_directional': hit_rate,
        'avg_confidence': avg_confidence,
        'avg_prob_up': avg_prob_up,
        'avg_prob_down': avg_prob_down,
        'status': status,
        'status_reason': reason
    }

def create_audit_summary(metrics: dict, output_dir: Path) -> str:
    """Create forecast_audit_summary.csv with STATUS rules."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    summary_path = output_dir / f"forecast_audit_summary_{timestamp}.csv"
    
    # Create summary data
    summary_data = [
        ['metric', 'value', 'status'],
        ['total_forecasts', metrics['total_forecasts'], 'INFO'],
        ['hit_rate_directional_pct', f"{metrics['hit_rate_directional']:.2f}", 'PASS' if metrics['hit_rate_directional'] >= 50 else 'FAIL'],
        ['avg_confidence_pct', f"{metrics['avg_confidence']:.2f}", 'INFO'],
        ['avg_prob_up_pct', f"{metrics['avg_prob_up']:.2f}", 'INFO'],
        ['avg_prob_down_pct', f"{metrics['avg_prob_down']:.2f}", 'INFO'],
        ['overall_status', metrics['status'], metrics['status'].upper()]
    ]
    
    # Write CSV
    df_summary = pd.DataFrame(summary_data[1:], columns=summary_data[0])
    df_summary.to_csv(summary_path, index=False)
    
    logging.info(f"Forecast audit summary saved to: {summary_path}")
    return str(summary_path)

def create_console_report(metrics: dict) -> None:
    """Generate console report of forecast audit results."""
    print("\n" + "="*60)
    print("FORECAST AUDIT SUMMARY")
    print("="*60)
    print(f"Total Forecasts Evaluated: {metrics['total_forecasts']}")
    print(f"Directional Hit Rate:      {metrics['hit_rate_directional']:.2f}%")
    print(f"Average Confidence:        {metrics['avg_confidence']:.2f}%")
    print(f"Average Prob Up:           {metrics['avg_prob_up']:.2f}%")
    print(f"Average Prob Down:         {metrics['avg_prob_down']:.2f}%")
    print()
    print(f"STATUS: {metrics['status']}")
    print(f"REASON: {metrics['status_reason']}")
    print("="*60)

def main():
    """Main forecast audit execution."""
    try:
        logging.info("Starting Stage-3 Forecast Audit")
        
        # Load environment
        env_vars = load_env_vars()
        engine = create_snowflake_engine(env_vars)
        
        # Create audit exports directory
        project_root = Path(__file__).parent.parent.parent.parent
        audit_dir = project_root / "audit_exports"
        audit_dir.mkdir(exist_ok=True)
        
        # Fetch and analyze forecast data
        df = fetch_forecast_vs_actual_data(engine)
        metrics = calculate_forecast_metrics(df)
        
        # Generate outputs
        summary_path = create_audit_summary(metrics, audit_dir)
        create_console_report(metrics)
        
        # Log audit completion
        logging.info(f"Forecast audit completed with status: {metrics['status']}")
        logging.info(f"Summary saved to: {summary_path}")
        
        # Exit with appropriate code for CI/CD
        if metrics['status'] == 'Red':
            sys.exit(1)
        elif metrics['status'] == 'Yellow':
            sys.exit(2)
        else:
            sys.exit(0)
            
    except Exception as e:
        logging.error(f"Forecast audit failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()