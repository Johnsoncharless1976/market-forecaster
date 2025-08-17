import snowflake.connector

conn = snowflake.connector.connect(
    user="JOHNSONCHARLESS",
    password="s7AfXRG7krgnh3H",
    account="GFXGPHR-HXC94041",
    warehouse="XS_WH",
    database="ZEN_MARKET",
    schema="FORECASTING"
)
cur = conn.cursor()

# Stage files (assumes snowsql already PUT them, or use Python's PUT if needed)
cur.execute("""
    COPY INTO SPX_HISTORICAL
    FROM @FORECASTING_STAGE/spx.csv.gz
    FILE_FORMAT = (FORMAT_NAME = FORECASTING_CSV)
""")
cur.execute("""
    COPY INTO VIX_HISTORICAL
    FROM @FORECASTING_STAGE/vix.csv.gz
    FILE_FORMAT = (FORMAT_NAME = FORECASTING_CSV)
""")
cur.execute("""
    COPY INTO VVIX_HISTORICAL
    FROM @FORECASTING_STAGE/vvix.csv.gz
    FILE_FORMAT = (FORMAT_NAME = FORECASTING_CSV)
""")

cur.close()
conn.close()
