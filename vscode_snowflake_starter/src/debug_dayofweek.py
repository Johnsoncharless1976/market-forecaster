#!/usr/bin/env python3
"""
Debug DAYOFWEEKISO behavior to understand weekend inclusion
"""

import os, sys
sys.path.append(os.path.dirname(__file__))
from snowflake_conn import get_conn

TEST_SQL = """
SELECT 
  '2025-01-20'::date AS test_date,
  DAYOFWEEKISO('2025-01-20'::date) AS iso_weekday,
  DAYNAME('2025-01-20'::date) AS day_name,
  CASE WHEN DAYOFWEEKISO('2025-01-20'::date) BETWEEN 1 AND 5 THEN 'WEEKDAY' ELSE 'WEEKEND' END AS classification
UNION ALL
SELECT 
  '2025-01-25'::date AS test_date,
  DAYOFWEEKISO('2025-01-25'::date) AS iso_weekday,
  DAYNAME('2025-01-25'::date) AS day_name,
  CASE WHEN DAYOFWEEKISO('2025-01-25'::date) BETWEEN 1 AND 5 THEN 'WEEKDAY' ELSE 'WEEKEND' END AS classification
UNION ALL  
SELECT 
  '2025-01-26'::date AS test_date,
  DAYOFWEEKISO('2025-01-26'::date) AS iso_weekday,
  DAYNAME('2025-01-26'::date) AS day_name,
  CASE WHEN DAYOFWEEKISO('2025-01-26'::date) BETWEEN 1 AND 5 THEN 'WEEKDAY' ELSE 'WEEKEND' END AS classification
ORDER BY test_date;
"""

def main():
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(TEST_SQL)
            rows = cur.fetchall()
            
            print("DAYOFWEEKISO Test:")
            print("=" * 60)
            print(f"{'Date':<12} {'ISO Day':<8} {'Day Name':<10} {'Classification':<12}")
            print("-" * 60)
            
            for date, iso_day, day_name, classification in rows:
                print(f"{date:<12} {iso_day:<8} {day_name:<10} {classification:<12}")
                
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())