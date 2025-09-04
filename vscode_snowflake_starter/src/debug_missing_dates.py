#!/usr/bin/env python3
"""
Debug Missing Weekdays - Show specific dates with no market data
Helps identify if gaps are legitimate market closures (holidays)
"""

import os, sys
from datetime import datetime
sys.path.append(os.path.dirname(__file__))
from snowflake_conn import get_conn

MISSING_DATES_SQL = """
WITH
rng AS (
  SELECT SYMBOL, MIN(TRADE_DATE) AS MIN_DATE, MAX(TRADE_DATE) AS MAX_DATE
  FROM MARKET_OHLCV GROUP BY SYMBOL
),
cal AS (
  SELECT r.SYMBOL, DATEADD(day, seq4(), r.MIN_DATE) AS D
  FROM rng r, TABLE(GENERATOR(ROWCOUNT => 20000))
  WHERE DATEADD(day, seq4(), r.MIN_DATE) <= r.MAX_DATE
    AND DAYOFWEEKISO(DATEADD(day, seq4(), r.MIN_DATE)) BETWEEN 1 AND 5
),
missing AS (
  SELECT c.SYMBOL, c.D as MISSING_DATE
  FROM cal c
  LEFT JOIN MARKET_OHLCV m
    ON m.SYMBOL = c.SYMBOL AND m.TRADE_DATE = c.D
  WHERE m.TRADE_DATE IS NULL
)
SELECT 
  TO_VARCHAR(MISSING_DATE, 'YYYY-MM-DD') AS MISSING_DATE_STR,
  DAYNAME(MISSING_DATE) AS DAY_OF_WEEK,
  COUNT(DISTINCT SYMBOL) AS SYMBOLS_MISSING
FROM missing 
GROUP BY MISSING_DATE, DAYNAME(MISSING_DATE)
ORDER BY MISSING_DATE
LIMIT 30;
"""

def main():
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(MISSING_DATES_SQL)
            rows = cur.fetchall()
            
            print("Missing Market Data Dates:")
            print("=" * 50)
            print(f"{'Date':<12} {'Day':<10} {'Symbols Missing':<15}")
            print("-" * 50)
            
            for date_str, day, count in rows:
                print(f"{date_str:<12} {day:<10} {count:<15}")
                
            print(f"\nTotal missing weekdays: {len(rows)}")
            
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())