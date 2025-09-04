# check_historical_data.py
#!/usr/bin/env python3
"""
Check what historical data is actually available for learning
"""

import pandas as pd
import snowflake.connector
import os

def connect_to_snowflake():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    )

def check_available_data():
    conn = connect_to_snowflake()
    
    print("HISTORICAL DATA AVAILABILITY CHECK")
    print("=" * 50)
    
    # Check each table separately
    tables_to_check = [
        'FORECAST_POSTMORTEM',
        'DAILY_MARKET_DATA', 
        'FORECAST_SUMMARY'
    ]
    
    for table in tables_to_check:
        try:
            query = f"SELECT COUNT(*) as count FROM ZEN_MARKET.FORECASTING.{table}"
            df = pd.read_sql(query, conn)
            count = df.iloc[0]['COUNT']
            print(f"✅ {table}: {count} records")
            
            if count > 0:
                # Show sample data
                sample_query = f"SELECT * FROM ZEN_MARKET.FORECASTING.{table} LIMIT 3"
                sample_df = pd.read_sql(sample_query, conn)
                print(f"   Sample columns: {list(sample_df.columns)}")
                print(f"   Sample data:")
                print(sample_df.to_string(index=False))
                print()
        except Exception as e:
            print(f"❌ {table}: Error - {str(e)}")
    
    # Try the complex join query to see what fails
    print("TESTING COMPLEX QUERY:")
    print("-" * 30)
    
    try:
        complex_query = """
        SELECT 
            fp.DATE,
            fp.INDEX as symbol,
            fp.FORECAST_BIAS,
            fp.ACTUAL_CLOSE,
            fp.HIT
        FROM ZEN_MARKET.FORECASTING.FORECAST_POSTMORTEM fp
        LIMIT 5
        """
        df = pd.read_sql(complex_query, conn)
        print(f"✅ Complex query successful: {len(df)} records")
        if len(df) > 0:
            print(df.to_string(index=False))
    except Exception as e:
        print(f"❌ Complex query failed: {str(e)}")
    
    conn.close()

if __name__ == "__main__":
    check_available_data()