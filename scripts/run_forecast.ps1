# scripts\run_forecast.ps1
# ZenMarket AI - Stage-3 Forecast Writer Runner
# Ensures Python environment and dependencies, then runs forecast writer

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Info($msg) { Write-Host "[INFO] $msg" -ForegroundColor Green }
function Write-Warn($msg) { Write-Host "[WARN] $msg" -ForegroundColor Yellow }
function Write-Error($msg) { Write-Host "[ERROR] $msg" -ForegroundColor Red }

try {
    Write-Info "Starting ZenMarket Forecast Writer"
    
    # Ensure we're in the project root
    if (-not (Test-Path "vscode_snowflake_starter\src\forecast\forecast_writer.py")) {
        throw "forecast_writer.py not found - ensure you're running from project root"
    }
    
    # 1. Ensure virtual environment exists
    if (-not (Test-Path ".venv")) {
        Write-Info "Creating Python virtual environment..."
        python -m venv .venv
        if ($LASTEXITCODE -ne 0) { throw "Failed to create virtual environment" }
    }
    
    # 2. Activate virtual environment
    Write-Info "Activating virtual environment..."
    if (Test-Path ".venv\Scripts\Activate.ps1") {
        & ".venv\Scripts\Activate.ps1"
    } else {
        throw "Virtual environment activation script not found"
    }
    
    # 3. Check and install required packages
    Write-Info "Checking Python dependencies..."
    $required_packages = @(
        "snowflake-connector-python",
        "snowflake-sqlalchemy", 
        "SQLAlchemy",
        "python-dotenv",
        "pandas"
    )
    
    # Check if packages are installed by trying to import them
    $missing_packages = @()
    foreach ($package in $required_packages) {
        $import_name = $package
        if ($package -eq "snowflake-connector-python") { $import_name = "snowflake.connector" }
        if ($package -eq "snowflake-sqlalchemy") { $import_name = "snowflake.sqlalchemy" }
        if ($package -eq "python-dotenv") { $import_name = "dotenv" }
        
        $result = python -c "
try:
    import $import_name
    print('OK')
except ImportError:
    print('MISSING')
" 2>$null
        
        if ($result -ne "OK") {
            $missing_packages += $package
        }
    }
    
    # Install missing packages
    if ($missing_packages.Count -gt 0) {
        Write-Info "Installing missing packages: $($missing_packages -join ', ')"
        pip install $missing_packages
        if ($LASTEXITCODE -ne 0) { throw "Failed to install required packages" }
    } else {
        Write-Info "All required packages are installed"
    }
    
    # 4. Run the forecast writer
    Write-Info "Running forecast writer..."
    python "vscode_snowflake_starter\src\forecast\forecast_writer.py"
    
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Forecast writer failed with exit code $LASTEXITCODE"
        exit $LASTEXITCODE
    }
    
    Write-Info "Forecast writer completed successfully"
}
catch {
    Write-Error "Runner failed: $($_.Exception.Message)"
    exit 1
}
finally {
    # Deactivate virtual environment if it was activated
    if (Get-Command "deactivate" -ErrorAction SilentlyContinue) {
        deactivate
    }
}