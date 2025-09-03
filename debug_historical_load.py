# debug_historical_load.py
#!/usr/bin/env python3
"""
Debug the historical data loading issue
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

def debug_data_loading():
    conn = connect_to_snowflake()
    
    print("DEBUGGING HISTORICAL DATA LOADING")
    print("=" * 50)
    
    # Test 1: Simple count from SPX_HISTORICAL
    print("Test 1: Count records in SPX_HISTORICAL")
    try:
        count_query = "SELECT COUNT(*) as total_records FROM ZEN_MARKET.FORECASTING.SPX_HISTORICAL"
        count_df = pd.read_sql(count_query, conn)
        total_records = count_df.iloc[0]['TOTAL_RECORDS']
        print(f"✅ Total SPX records: {total_records}")
    except Exception as e:
        print(f"❌ Count query failed: {str(e)}")
        conn.close()
        return
    
    # Test 2: Get date range 
    print("\nTest 2: Check date range")
    try:
        range_query = "SELECT MIN(DATE) as min_date, MAX(DATE) as max_date FROM ZEN_MARKET.FORECASTING.SPX_HISTORICAL"
        range_df = pd.read_sql(range_query, conn)
        min_date = range_df.iloc[0]['MIN_DATE']
        max_date = range_df.iloc[0]['MAX_DATE']
        print(f"✅ Date range: {min_date} to {max_date}")
    except Exception as e:
        print(f"❌ Range query failed: {str(e)}")
    
    # Test 3: Simple select with recent date
    print("\nTest 3: Simple SELECT with recent date")
    try:
        simple_query = """
        SELECT 
            DATE,
            CLOSE as spx_close
        FROM ZEN_MARKET.FORECASTING.SPX_HISTORICAL
        WHERE DATE >= '2023-01-01'
        ORDER BY DATE DESC
        LIMIT 5
        """
        simple_df = pd.read_sql(simple_query, conn)
        print(f"✅ Simple query returned {len(simple_df)} rows")
        if len(simple_df) > 0:
            print("Sample data:")
            print(simple_df.to_string(index=False))
        else:
            print("❌ No data returned")
    except Exception as e:
        print(f"❌ Simple query failed: {str(e)}")
    
    # Test 4: Test the JOIN query
    print("\nTest 4: Test JOIN query")
    try:
        join_query = """
        SELECT 
            s.DATE,
            s.CLOSE as spx_close,
            v.CLOSE as vix_close
        FROM ZEN_MARKET.FORECASTING.SPX_HISTORICAL s
        LEFT JOIN ZEN_MARKET.FORECASTING.VIX_HISTORICAL v ON s.DATE = v.DATE
        WHERE s.DATE >= '2023-01-01'
        ORDER BY s.DATE DESC
        LIMIT 5
        """
        join_df = pd.read_sql(join_query, conn)
        print(f"✅ JOIN query returned {len(join_df)} rows")
        if len(join_df) > 0:
            print("Sample joined data:")
            print(join_df.to_string(index=False))
            print(f"Columns: {list(join_df.columns)}")
        else:
            print("❌ JOIN returned no data")
    except Exception as e:
        print(f"❌ JOIN query failed: {str(e)}")
    
    # Test 5: Check for data in 2024
    print("\nTest 5: Check for 2024 data (backtest range)")
    try:
        recent_query = """
        SELECT COUNT(*) as count_2024
        FROM ZEN_MARKET.FORECASTING.SPX_HISTORICAL 
        WHERE DATE >= '2024-01-01'
        """
        recent_df = pd.read_sql(recent_query, conn)
        count_2024 = recent_df.iloc[0]['COUNT_2024']
        print(f"Records in 2024+: {count_2024}")
        
        if count_2024 > 0:
            # Show sample 2024 data
            sample_2024_query = """
            SELECT DATE, CLOSE 
            FROM ZEN_MARKET.FORECASTING.SPX_HISTORICAL 
            WHERE DATE >= '2024-01-01' 
            ORDER BY DATE 
            LIMIT 5
            """
            sample_2024_df = pd.read_sql(sample_2024_query, conn)
            print("Sample 2024 data:")
            print(sample_2024_df.to_string(index=False))
    except Exception as e:
        print(f"❌ 2024 data check failed: {str(e)}")
    
    conn.close()

if __name__ == "__main__":
    debug_data_loading()