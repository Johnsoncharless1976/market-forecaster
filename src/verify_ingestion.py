import snowflake.connector

conn = snowflake.connector.connect(
    user="JOHNSONCHARLESS",
    password="s7AfXRG7krgnh3H",
    account="GFXGPHR-HXC94041",
    warehouse="COMPUTE_WH",
    database="ZEN_MARKET",
    schema="FORECASTING"
)
cur = conn.cursor()
cur.execute("USE WAREHOUSE COMPUTE_WH;")

query = """
(
  SELECT 'SPX' AS symbol, DATE, CLOSE
  FROM SPX_HISTORICAL
  ORDER BY DATE DESC
  LIMIT 1
)
UNION ALL
(
  SELECT 'SPY', DATE, CLOSE
  FROM SPY_HISTORICAL
  ORDER BY DATE DESC
  LIMIT 1
)
UNION ALL
(
  SELECT 'VIX', DATE, CLOSE
  FROM VIX_HISTORICAL
  ORDER BY DATE DESC
  LIMIT 1
)
UNION ALL
(
  SELECT 'VVIX', DATE, CLOSE
  FROM VVIX_HISTORICAL
  ORDER BY DATE DESC
  LIMIT 1
)
UNION ALL
(
  SELECT 'ES', DATE, CLOSE
  FROM ES_HISTORICAL
  ORDER BY DATE DESC
  LIMIT 1
);
"""

cur.execute(query)

rows = cur.fetchall()
print("=== Latest Market Data Ingestion Check ===")
for row in rows:
    print(f"{row[0]} | {row[1]} | {row[2]}")

cur.close()
conn.close()
