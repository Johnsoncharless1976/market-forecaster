#!/usr/bin/env python3
"""
Stage-3 Forecast Writer - Baseline (Persistence)
Reads FEATURES_DAILY from Snowflake, generates persistence forecasts, writes to FORECAST_DAILY
"""

import os
import sys
import logging
from datetime import datetime, date, timedelta
from typing import Optional

import pandas as pd
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
    
    # Password or private key authentication
    password = os.getenv('SNOWFLAKE_PASSWORD')
    private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
    
    if password:
        env_vars['SNOWFLAKE_PASSWORD'] = password
    elif private_key_path:
        env_vars['SNOWFLAKE_PRIVATE_KEY_PATH'] = private_key_path
        env_vars['SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'] = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', '')
    else:
        raise ValueError("Must provide either SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH")
    
    # Optional role
    role = os.getenv('SNOWFLAKE_ROLE')
    if role:
        env_vars['SNOWFLAKE_ROLE'] = role
    
    return env_vars

def create_snowflake_engine(env_vars: dict) -> Engine:
    """Create Snowflake SQLAlchemy engine from environment variables."""
    
    # Build connection string
    user = env_vars['SNOWFLAKE_USER']
    account = env_vars['SNOWFLAKE_ACCOUNT']
    warehouse = env_vars['SNOWFLAKE_WAREHOUSE']
    database = env_vars['SNOWFLAKE_DATABASE']
    schema = env_vars['SNOWFLAKE_SCHEMA']
    
    if 'SNOWFLAKE_PASSWORD' in env_vars:
        password = env_vars['SNOWFLAKE_PASSWORD']
        conn_str = f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}"
    else:
        # Private key authentication would require additional setup
        raise NotImplementedError("Private key authentication not implemented in this baseline version")
    
    if 'SNOWFLAKE_ROLE' in env_vars:
        conn_str += f"&role={env_vars['SNOWFLAKE_ROLE']}"
    
    return create_engine(conn_str)

def get_latest_features(engine: Engine) -> pd.DataFrame:
    """Get the most recent features data for all symbols."""
    
    # First check what columns are available
    try:
        schema_query = text("DESC TABLE FEATURES_DAILY")
        with engine.connect() as conn:
            schema_result = conn.execute(schema_query)
            columns = [row[0] for row in schema_result]
            logging.info(f"FEATURES_DAILY columns: {columns}")
    except Exception as e:
        logging.warning(f"Could not describe table: {e}")
    
    # Use a simple query to get recent data - using correct column name TRADE_DATE
    query = text("""
    SELECT SYMBOL, TRADE_DATE, CLOSE, ADJ_CLOSE 
    FROM FEATURES_DAILY 
    ORDER BY TRADE_DATE DESC 
    LIMIT 100
    """)
    
    df = pd.read_sql(query, engine)
    logging.info(f"Retrieved {len(df)} feature records")
    logging.info(f"Columns in result: {list(df.columns)}")
    
    if df.empty:
        return df
        
    # Get the latest date and filter to that date only
    latest_date = df['trade_date'].max()
    df_latest = df[df['trade_date'] == latest_date]
    
    logging.info(f"Retrieved {len(df_latest)} feature records for forecast generation on {latest_date}")
    
    return df_latest

def generate_persistence_forecasts(features_df: pd.DataFrame) -> pd.DataFrame:
    """Generate persistence forecasts: forecast_{t+1} = close_t"""
    
    if features_df.empty:
        return pd.DataFrame()
    
    # Use the correct column names from FEATURES_DAILY
    date_col = 'trade_date'
    symbol_col = 'symbol' 
    close_col = 'close'
    
    logging.info(f"Using columns - Date: {date_col}, Symbol: {symbol_col}, Close: {close_col}")
    
    # Calculate next business day (forecast date)
    latest_date = features_df[date_col].iloc[0]
    if isinstance(latest_date, str):
        latest_date = pd.to_datetime(latest_date).date()
    elif hasattr(latest_date, 'date'):
        latest_date = latest_date.date()
    
    # Add one day for forecast (simplified - in production would handle weekends/holidays)
    forecast_date = latest_date + timedelta(days=1)
    
    # Create forecast dataframe to match FORECAST_DAILY schema
    forecast_df = pd.DataFrame({
        'SYMBOL': features_df[symbol_col],
        'TRADE_DATE': forecast_date,
        'PREDICTION': 'FLAT',  # Baseline persistence prediction
        'PROB_UP': 0.3333,    # Neutral baseline probabilities  
        'PROB_DOWN': 0.3333,
        'CONFIDENCE': 0.5,    # Low confidence for baseline
        'MODEL_VERSION': 'baseline_persistence_v1.0',
        'NOTES': f'Persistence forecast: close={features_df[close_col].iloc[0]:.2f}',
        'CREATED_AT': datetime.utcnow()
    })
    
    logging.info(f"Generated {len(forecast_df)} persistence forecasts for {forecast_date}")
    
    return forecast_df

def upsert_forecasts(engine: Engine, forecast_df: pd.DataFrame) -> int:
    """Upsert forecasts to FORECAST_DAILY table using idempotent MERGE."""
    
    if forecast_df.empty:
        logging.warning("No forecasts to upsert")
        return 0
    
    # Create temporary table name
    temp_table = f"TEMP_FORECAST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        # Write to temporary table first
        forecast_df.to_sql(temp_table, engine, if_exists='replace', index=False, method='multi')
        logging.info(f"Created temporary table {temp_table} with {len(forecast_df)} records")
        
        # Execute MERGE statement using correct FORECAST_DAILY schema
        merge_sql = text(f"""
        MERGE INTO FORECAST_DAILY AS target
        USING {temp_table} AS source
        ON target.SYMBOL = source.SYMBOL AND target.TRADE_DATE = source.TRADE_DATE
        WHEN MATCHED THEN UPDATE SET
            PREDICTION = source.PREDICTION,
            PROB_UP = source.PROB_UP,
            PROB_DOWN = source.PROB_DOWN,
            CONFIDENCE = source.CONFIDENCE,
            MODEL_VERSION = source.MODEL_VERSION,
            NOTES = source.NOTES,
            CREATED_AT = source.CREATED_AT
        WHEN NOT MATCHED THEN INSERT (
            SYMBOL, TRADE_DATE, PREDICTION, PROB_UP, PROB_DOWN, CONFIDENCE, MODEL_VERSION, NOTES, CREATED_AT
        ) VALUES (
            source.SYMBOL, source.TRADE_DATE, source.PREDICTION, source.PROB_UP, source.PROB_DOWN, 
            source.CONFIDENCE, source.MODEL_VERSION, source.NOTES, source.CREATED_AT
        )
        """)
        
        with engine.connect() as conn:
            result = conn.execute(merge_sql)
            rows_affected = result.rowcount if hasattr(result, 'rowcount') else len(forecast_df)
        
        logging.info(f"MERGE completed successfully")
        
        return rows_affected
        
    except Exception as e:
        logging.error(f"Error during MERGE operation: {e}")
        raise
    finally:
        # Clean up temporary table
        try:
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
            logging.info(f"Cleaned up temporary table {temp_table}")
        except Exception as e:
            logging.warning(f"Failed to clean up temporary table {temp_table}: {e}")

def main():
    """Main forecast writer function."""
    try:
        logging.info("Starting Stage-3 Forecast Writer (Baseline Persistence)")
        
        # Load environment variables
        env_vars = load_env_vars()
        logging.info("Environment variables loaded successfully")
        
        # Create Snowflake engine
        engine = create_snowflake_engine(env_vars)
        logging.info("Snowflake connection established")
        
        # Get latest features
        features_df = get_latest_features(engine)
        
        if features_df.empty:
            logging.error("No feature data found - cannot generate forecasts")
            sys.exit(1)
        
        # Generate forecasts
        forecast_df = generate_persistence_forecasts(features_df)
        
        # Upsert to database
        rows_upserted = upsert_forecasts(engine, forecast_df)
        
        # Final success message
        model_version = "baseline_persistence_v1.0"
        print(f"FORECAST: rows_upserted={rows_upserted} model_version={model_version}")
        
        logging.info("Forecast writer completed successfully")
        
    except Exception as e:
        logging.error(f"Forecast writer failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()