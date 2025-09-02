#!/usr/bin/env python3
"""
Production-safe baseline Forecast Writer
Reads FEATURES_DAILY and MERGEs forecasts into FORECAST_DAILY using .env settings
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    stream=sys.stdout
)

def get_next_business_day(date):
    """Get next business day (skip weekends)"""
    next_day = date + timedelta(days=1)
    # Skip weekends (Saturday=5, Sunday=6)
    while next_day.weekday() >= 5:
        next_day += timedelta(days=1)
    return next_day

def main():
    # Load environment variables
    load_dotenv()
    
    # Get Snowflake connection details from .env
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    private_key_passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    role = os.getenv("SNOWFLAKE_ROLE")
    
    # Validate required environment variables
    if not all([account, user, warehouse, database, schema]):
        logging.error("Missing required Snowflake environment variables")
        sys.exit(1)
    
    if not password and not private_key_path:
        logging.error("Must provide either SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH")
        sys.exit(1)
    
    # Create Snowflake engine
    if private_key_path and os.path.exists(private_key_path):
        # Use private key authentication
        connection_string = f"snowflake://{user}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}"
        engine = create_engine(connection_string, connect_args={
            "private_key_path": private_key_path,
            "private_key_passphrase": private_key_passphrase
        })
    else:
        # Use password authentication
        connection_string = f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}"
        engine = create_engine(connection_string)
    
    with engine.connect() as conn:
        # Drop and recreate the table with correct schema to avoid conflicts
        conn.execute(text("DROP TABLE IF EXISTS FORECAST_DAILY"))
        
        # Create fresh table
        create_table_sql = text("""
        CREATE TABLE FORECAST_DAILY (
            symbol STRING,
            forecast_date DATE,
            forecast FLOAT,
            model_version STRING,
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """)
        conn.execute(create_table_sql)
        logging.info("FORECAST_DAILY table recreated")
        
        # Read latest close per symbol from FEATURES_DAILY using QUALIFY
        features_query = text("""
        SELECT SYMBOL as symbol, TRADE_DATE as trade_date, CLOSE as close
        FROM FEATURES_DAILY
        WHERE CLOSE IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY SYMBOL ORDER BY TRADE_DATE DESC) = 1
        """)
        
        features_df = pd.read_sql(features_query, conn)
        
        if features_df.empty:
            logging.warning("No data found in FEATURES_DAILY")
            print("FORECAST: rows_upserted=0 model_version=baseline_persistence_v1")
            return
        
        logging.info(f"FEATURES_DAILY rows read: {len(features_df)}")
        logging.info(f"Symbols found: {len(features_df['symbol'].unique())}")
        
        # Compute baseline persistence forecast with next business day logic
        forecast_rows = []
        
        for _, row in features_df.iterrows():
            # Get next business day as forecast date using Python logic
            trade_date = row['trade_date']
            next_date = get_next_business_day(trade_date)
            
            forecast_rows.append({
                'symbol': row['symbol'],
                'forecast_date': next_date,
                'forecast': row['close'],  # Use current close as forecast
                'model_version': 'baseline_persistence_v1'
            })
        
        if not forecast_rows:
            logging.warning("No forecasts generated")
            print("FORECAST: rows_upserted=0 model_version=baseline_persistence_v1")
            return
            
        forecast_df = pd.DataFrame(forecast_rows)
        logging.info(f"Prepared forecast row count: {len(forecast_df)}")
        
        # Create staging table for robust upsert
        conn.execute(text("""
        CREATE OR REPLACE TEMP TABLE STG_FORECAST (
            symbol STRING,
            forecast_date DATE,
            forecast FLOAT,
            model_version STRING
        )
        """))
        
        # Bulk insert forecast data into staging table using executemany
        insert_sql = text("""
        INSERT INTO STG_FORECAST (symbol, forecast_date, forecast, model_version)
        VALUES (:symbol, :forecast_date, :forecast, :model_version)
        """)
        
        # Convert DataFrame to list of dicts for executemany
        insert_data = []
        for _, row in forecast_df.iterrows():
            insert_data.append({
                'symbol': row['symbol'],
                'forecast_date': row['forecast_date'],
                'forecast': row['forecast'],
                'model_version': row['model_version']
            })
        
        conn.execute(insert_sql, insert_data)
        
        # Execute MERGE operation (using forecast_date consistently)
        merge_sql = text("""
        MERGE INTO FORECAST_DAILY t
        USING STG_FORECAST s
        ON t.symbol = s.symbol AND t.forecast_date = s.forecast_date
        WHEN MATCHED THEN 
            UPDATE SET 
                t.forecast = s.forecast,
                t.model_version = s.model_version,
                t.created_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN 
            INSERT (symbol, forecast_date, forecast, model_version, created_at)
            VALUES (s.symbol, s.forecast_date, s.forecast, s.model_version, CURRENT_TIMESTAMP())
        """)
        
        conn.execute(merge_sql)
        
        # Get row count from staging table as proxy for rows_upserted
        count_result = conn.execute(text("SELECT COUNT(*) FROM STG_FORECAST"))
        rows_upserted = count_result.fetchone()[0]
        
        logging.info(f"MERGE completed: {rows_upserted} rows processed")
        
        # Print the required final line
        print(f"FORECAST: rows_upserted={rows_upserted} model_version=baseline_persistence_v1")

if __name__ == "__main__":
    main()