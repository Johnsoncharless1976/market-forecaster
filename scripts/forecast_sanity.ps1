# File: scripts/forecast_sanity.ps1
# Prints row counts and latest dates for FEATURES_DAILY, ACTUALS_DAILY, FORECAST_DAILY

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Info($msg) { Write-Host "[INFO] $msg" -ForegroundColor Green }

try {
    Write-Info "Starting Snowflake data sanity check"
    
    # Activate venv
    if (Test-Path ".\.venv\Scripts\Activate.ps1") { 
        . .\.venv\Scripts\Activate.ps1 
        Write-Info "Activated virtual environment"
    }

    # Create temporary Python script
    $tempScript = [System.IO.Path]::GetTempFileName() + ".py"
    
    $pythonCode = @"
import os
import sys
from dotenv import load_dotenv

# Install dependencies if needed
try:
    from sqlalchemy import create_engine, text
    import pandas as pd
except ImportError:
    print('[INFO] Installing dependencies...')
    import subprocess
    subprocess.run([sys.executable, '-m', 'pip', 'install', '-q', 'python-dotenv', 'SQLAlchemy', 'snowflake-connector-python', 'snowflake-sqlalchemy', 'pandas'])
    from sqlalchemy import create_engine, text
    import pandas as pd

load_dotenv()

# Get Snowflake credentials
acct = os.getenv('SNOWFLAKE_ACCOUNT')
user = os.getenv('SNOWFLAKE_USER') 
pwd = os.getenv('SNOWFLAKE_PASSWORD')
wh = os.getenv('SNOWFLAKE_WAREHOUSE')
db = os.getenv('SNOWFLAKE_DATABASE')
sch = os.getenv('SNOWFLAKE_SCHEMA')
role = os.getenv('SNOWFLAKE_ROLE')

if not all([acct, user, pwd, wh, db, sch]):
    raise SystemExit('[ERROR] Missing Snowflake env vars. Check .env.')

# Create engine
engine = create_engine(f'snowflake://{user}:{pwd}@{acct}/{db}/{sch}?warehouse={wh}&role={role}')

def check_table(con, table_name):
    try:
        query = f'''
        SELECT '{table_name}' AS table_name,
               COUNT(*) AS row_count,
               TO_VARCHAR(MAX(TRADE_DATE)) AS max_date,
               TO_VARCHAR(MIN(TRADE_DATE)) AS min_date
        FROM {table_name}
        '''
        result = con.execute(text(query)).mappings().all()
        if result:
            rec = dict(result[0])
            print(f"{rec['table_name']}: rows={rec['row_count']} min_date={rec['min_date']} max_date={rec['max_date']}")
        return True
    except Exception as e:
        print(f'{table_name}: [ERROR] {e}')
        return False

with engine.connect() as con:
    # Check main tables
    for table in ['FEATURES_DAILY', 'FORECAST_DAILY']:
        check_table(con, table)
    
    # Try to check ACTUALS_DAILY (might not exist)
    check_table(con, 'ACTUALS_DAILY')
    
    # Sample FORECAST_DAILY
    try:
        result = con.execute(text('SELECT * FROM FORECAST_DAILY ORDER BY TRADE_DATE DESC, SYMBOL LIMIT 5')).mappings().all()
        if result:
            print('FORECAST_DAILY sample:')
            for row in result:
                print(dict(row))
        else:
            print('FORECAST_DAILY sample: (no rows)')
    except Exception as e:
        print(f'FORECAST_DAILY sample error: {e}')
"@
    
    $pythonCode | Out-File -FilePath $tempScript -Encoding UTF8
    
    # Run the Python script
    python $tempScript
    
    # Cleanup
    Remove-Item $tempScript -ErrorAction SilentlyContinue
    
} catch {
    Write-Error "Sanity check failed: $($_.Exception.Message)"
    exit 1
}