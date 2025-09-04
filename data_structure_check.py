import snowflake.connector
import pandas as pd
import os

def check_data_structures():
    print("=== DATA STRUCTURE ANALYSIS ===")
    
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    )
    
    # Key tables for Zen Grid
    key_tables = [
        'FORECAST_DAILY_V2',
        'DAILY_MARKET_DATA', 
        'FORECAST_POSTMORTEM',
        'FORECAST_SUMMARY'
    ]
    
    for table in key_tables:
        print(f"\n📊 TABLE: {table}")
        try:
            # Get column structure
            query = f"DESCRIBE TABLE {table}"
            df = pd.read_sql(query, conn)
            print("   COLUMNS:")
            for _, row in df.iterrows():
                print(f"      {row['name']} ({row['type']})")
            
            # Get sample data
            sample_query = f"SELECT * FROM {table} ORDER BY 1 DESC LIMIT 3"
            sample_df = pd.read_sql(sample_query, conn)
            print(f"   SAMPLE DATA ({len(sample_df)} rows):")
            if len(sample_df) > 0:
                print(sample_df.to_string(index=False, max_cols=10))
            else:
                print("      No data found")
                
        except Exception as e:
            print(f"   ❌ Error accessing {table}: {str(e)}")
    
    # Check date ranges
    print(f"\n📅 DATE RANGES:")
    date_tables = [
        ('FORECAST_DAILY_V2', 'FORECAST_DATE'),
        ('DAILY_MARKET_DATA', 'DATE'),
        ('FORECAST_POSTMORTEM', 'FORECAST_DATE')
    ]
    
    for table, date_col in date_tables:
        try:
            query = f"SELECT MIN({date_col}) as min_date, MAX({date_col}) as max_date, COUNT(*) as records FROM {table}"
            df = pd.read_sql(query, conn)
            if len(df) > 0:
                row = df.iloc[0]
                print(f"   {table}: {row['MIN_DATE']} to {row['MAX_DATE']} ({row['RECORDS']} records)")
        except Exception as e:
            print(f"   ❌ {table}: {str(e)}")
    
    conn.close()

if __name__ == "__main__":
    check_data_structures()