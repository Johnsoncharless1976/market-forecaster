# vscode_snowflake_starter\src\forecast\forecast_writer.py
import os
import pandas as pd
from datetime import datetime, date
from typing import Tuple
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def get_snowflake_engine():
    """Build Snowflake SQLAlchemy engine from environment variables."""
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    passphrase = os.getenv("SNOWFLAKE_PASSPHRASE")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    role = os.getenv("SNOWFLAKE_ROLE")
    
    if not all([account, user, warehouse, database, schema]):
        raise ValueError("Missing required Snowflake environment variables")
    
    if private_key_path:
        # Use private key authentication
        conn_str = f"snowflake://{user}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}&private_key_path={private_key_path}"
        if passphrase:
            conn_str += f"&private_key_passphrase={passphrase}"
    else:
        # Use password authentication
        if not password:
            raise ValueError("SNOWFLAKE_PASSWORD required when SNOWFLAKE_PRIVATE_KEY_PATH not provided")
        conn_str = f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}"
    
    print(f"INFO: Connecting to Snowflake: {account}/{database}/{schema}")
    return create_engine(conn_str)

def load_features_daily(engine) -> pd.DataFrame:
    """Read latest FEATURES_DAILY rows for forecasting."""
    query = """
    SELECT symbol, trade_date as date, close
    FROM FEATURES_DAILY 
    WHERE trade_date = (SELECT MAX(trade_date) FROM FEATURES_DAILY)
    ORDER BY symbol
    """
    print("INFO: Loading latest features from FEATURES_DAILY")
    df = pd.read_sql_query(query, engine)
    print(f"INFO: Loaded {len(df)} feature rows for forecasting")
    return df

def create_forecast_daily_table(engine):
    """Create FORECAST_DAILY_V2 table for numeric forecasts."""
    create_sql = """
    CREATE TABLE IF NOT EXISTS FORECAST_DAILY_V2 (
        symbol STRING,
        forecast_date DATE,
        forecast FLOAT,
        model_version STRING,
        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """
    print("INFO: Ensuring FORECAST_DAILY_V2 table exists")
    with engine.connect() as conn:
        conn.execute(text(create_sql))
        conn.commit()

def compute_baseline_forecast(features_df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """Compute baseline persistence forecast: forecast_{t+1} = close_t."""
    if features_df.empty:
        print("INFO: No features data - returning empty forecast")
        return pd.DataFrame(), "persistence_v1.0"
    
    # Create next-day forecast dataframe
    forecast_df = features_df.copy()
    
    # Add one day to get forecast date
    forecast_df['date'] = pd.to_datetime(forecast_df['date']) + pd.Timedelta(days=1)
    
    # Baseline model: next day forecast = current close price
    forecast_df['forecast'] = forecast_df['close']
    forecast_df['model_version'] = 'persistence_v1.0'
    forecast_df['created_at'] = datetime.now()
    
    # Rename date to forecast_date to avoid reserved word conflicts  
    forecast_df = forecast_df.rename(columns={'date': 'forecast_date'})
    
    # Keep only required columns for MERGE
    forecast_df = forecast_df[['symbol', 'forecast_date', 'forecast', 'model_version', 'created_at']]
    
    print(f"INFO: Computed {len(forecast_df)} baseline forecasts using persistence model")
    return forecast_df, 'persistence_v1.0'

def merge_forecasts(forecast_df: pd.DataFrame, engine) -> int:
    """MERGE forecasts into FORECAST_DAILY table (idempotent upsert)."""
    if forecast_df.empty:
        return 0
    
    merge_sql = """
    MERGE INTO FORECAST_DAILY_V2 AS target
    USING (VALUES {values_clause}) AS source (symbol, forecast_date, forecast, model_version, created_at)
    ON target.symbol = source.symbol AND target.forecast_date = source.forecast_date
    WHEN MATCHED THEN 
        UPDATE SET 
            forecast = source.forecast,
            model_version = source.model_version,
            created_at = source.created_at
    WHEN NOT MATCHED THEN 
        INSERT (symbol, forecast_date, forecast, model_version, created_at)
        VALUES (source.symbol, source.forecast_date, source.forecast, source.model_version, source.created_at)
    """
    
    # Build VALUES clause
    values_list = []
    for _, row in forecast_df.iterrows():
        values_list.append(f"('{row['symbol']}', '{row['forecast_date'].strftime('%Y-%m-%d')}', {row['forecast']}, '{row['model_version']}', '{row['created_at'].strftime('%Y-%m-%d %H:%M:%S')}')")
    
    values_clause = ', '.join(values_list)
    final_sql = merge_sql.format(values_clause=values_clause)
    
    print(f"INFO: Merging {len(forecast_df)} forecasts into FORECAST_DAILY_V2")
    
    with engine.connect() as conn:
        result = conn.execute(text(final_sql))
        conn.commit()
        return len(forecast_df)  # Return number of processed rows

def smoke_test():
    """In-memory smoke test of forecast logic."""
    print("INFO: Running smoke test")
    
    # Create test dataframe
    test_features = pd.DataFrame({
        'symbol': ['AAPL', 'MSFT'],
        'date': ['2024-01-15', '2024-01-15'],
        'close': [150.0, 300.0]
    })
    test_features['date'] = pd.to_datetime(test_features['date'])
    
    # Test forecast computation
    forecast_df, model_version = compute_baseline_forecast(test_features)
    
    # Verify results
    assert len(forecast_df) == 2
    assert model_version == 'persistence_v1.0'
    assert forecast_df['forecast'].iloc[0] == 150.0
    assert forecast_df['forecast'].iloc[1] == 300.0
    assert forecast_df['date'].iloc[0] == pd.Timestamp('2024-01-16')
    
    print("SMOKE TEST PASS")

def main():
    """Main forecast writer entry point."""
    try:
        # Check if smoke test requested FIRST (before imports)
        if os.getenv("FORECAST_SMOKE_TEST", "0") == "1":
            smoke_test()
            return
        
        print("INFO: Starting Stage-3 Forecast Writer")
        
        # Build Snowflake connection
        engine = get_snowflake_engine()
        
        # Ensure target table exists
        create_forecast_daily_table(engine)
        
        # Load latest features
        features_df = load_features_daily(engine)
        
        # Compute forecasts
        forecast_df, model_version = compute_baseline_forecast(features_df)
        
        # Merge into target table
        rows_upserted = merge_forecasts(forecast_df, engine)
        
        # Success summary
        print(f"FORECAST: rows_upserted={rows_upserted} model_version={model_version}")
        
    except Exception as e:
        print(f"ERROR: Forecast writer failed: {e}")
        raise

if __name__ == "__main__":
    main()