# check_historical_tables.py
#!/usr/bin/env python3
"""
Check what historical tables and columns are actually available
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

def check_historical_data_structure():
    conn = connect_to_snowflake()
    
    print("HISTORICAL DATA STRUCTURE CHECK")
    print("=" * 50)
    
    # Check all tables with HISTORICAL in name
    try:
        tables_query = "SHOW TABLES LIKE '%HISTORICAL%'"
        tables_df = pd.read_sql(tables_query, conn)
        
        print(f"Found {len(tables_df)} historical tables:")
        for _, row in tables_df.iterrows():
            table_name = row['name']
            print(f"\n📊 TABLE: {table_name}")
            
            # Get column structure
            try:
                describe_query = f"DESCRIBE TABLE ZEN_MARKET.FORECASTING.{table_name}"
                cols_df = pd.read_sql(describe_query, conn)
                print("   COLUMNS:")
                for _, col_row in cols_df.iterrows():
                    print(f"      {col_row['name']} ({col_row['type']})")
                
                # Get sample data
                sample_query = f"SELECT * FROM ZEN_MARKET.FORECASTING.{table_name} LIMIT 3"
                sample_df = pd.read_sql(sample_query, conn)
                print(f"   SAMPLE DATA ({len(sample_df)} rows):")
                if len(sample_df) > 0:
                    print(sample_df.to_string(index=False, max_cols=8))
                else:
                    print("      No data found")
                    
            except Exception as e:
                print(f"   ❌ Error accessing {table_name}: {str(e)}")
    
    except Exception as e:
        print(f"❌ Error checking tables: {str(e)}")
    
    # Also check if you have your 5 years of data in different tables
    print("\n" + "=" * 50)
    print("CHECKING OTHER POTENTIAL DATA SOURCES")
    print("=" * 50)
    
    # Check all tables to see what data you actually have
    try:
        all_tables_query = "SHOW TABLES"
        all_tables_df = pd.read_sql(all_tables_query, conn)
        
        market_tables = []
        for _, row in all_tables_df.iterrows():
            table_name = row['name'].upper()
            if any(keyword in table_name for keyword in ['SPX', 'SPY', 'VIX', 'MARKET', 'PRICE', 'OHLC']):
                market_tables.append(row['name'])
        
        if market_tables:
            print("Potential market data tables found:")
            for table in market_tables:
                print(f"  - {table}")
                
                # Quick row count
                try:
                    count_query = f"SELECT COUNT(*) as count FROM ZEN_MARKET.FORECASTING.{table}"
                    count_df = pd.read_sql(count_query, conn)
                    count = count_df.iloc[0]['count']
                    print(f"    ({count} records)")
                except:
                    print("    (could not count records)")
        else:
            print("No obvious market data tables found")
    
    except Exception as e:
        print(f"❌ Error checking all tables: {str(e)}")
    
    conn.close()

if __name__ == "__main__":
    check_historical_data_structure()