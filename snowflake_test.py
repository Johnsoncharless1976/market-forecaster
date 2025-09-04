import snowflake.connector
import os
from datetime import datetime

def test_connection():
    print("=== SNOWFLAKE CONNECTION TEST ===")
    
    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
        )
        print("✅ Connection successful")
        
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()
        print(f"✅ Snowflake version: {version[0]}")
        
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        if tables:
            print(f"✅ Found {len(tables)} tables:")
            for table in tables[:10]:
                print(f"   - {table[1]}")
            if len(tables) > 10:
                print(f"   ... and {len(tables) - 10} more")
        else:
            print("⚠️ No tables found")
        
        # Look for market data tables
        market_keywords = ['SPX', 'VIX', 'MARKET', 'PRICE', 'DATA', 'FORECAST']
        market_tables = []
        for table in tables:
            table_name = table[1].upper()
            if any(keyword in table_name for keyword in market_keywords):
                market_tables.append(table[1])
        
        if market_tables:
            print(f"✅ Market data tables: {', '.join(market_tables)}")
        else:
            print("⚠️ No obvious market data tables found")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        error_str = str(e).lower()
        if 'authentication' in error_str or 'login' in error_str:
            print("💡 Authentication issue - check credentials")
        elif 'network' in error_str or 'timeout' in error_str:
            print("💡 Network issue - check account name/firewall")
        elif 'warehouse' in error_str:
            print("💡 Warehouse issue - check warehouse name/permissions")

if __name__ == "__main__":
    test_connection()