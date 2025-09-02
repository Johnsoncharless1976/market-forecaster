# ============================================
# File: scripts\run_forecast_audit.ps1
# Title: Stage-3 Forecast Audit Runner
# Description: Sets up Python environment and executes forecast audit
# ============================================

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Info($msg) { Write-Host "[INFO] $msg" -ForegroundColor Green }
function Write-Warn($msg) { Write-Host "[WARN] $msg" -ForegroundColor Yellow }
function Write-Error($msg) { Write-Host "[ERROR] $msg" -ForegroundColor Red }

try {
    Write-Info "Starting Stage-3 Forecast Audit Runner"
    
    # Get script directory and project root
    $ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $ProjectRoot = Split-Path -Parent $ScriptDir
    Set-Location $ProjectRoot
    
    Write-Info "Project root: $ProjectRoot"
    
    # Check if Python is available
    try {
        $pythonVersion = python --version 2>&1
        Write-Info "Python found: $pythonVersion"
    } catch {
        Write-Error "Python not found in PATH. Please install Python 3.10+ and add to PATH."
        exit 1
    }
    
    # Create .venv if it doesn't exist
    $venvPath = Join-Path $ProjectRoot ".venv"
    if (-not (Test-Path $venvPath)) {
        Write-Info "Creating Python virtual environment at $venvPath"
        python -m venv .venv
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Failed to create virtual environment"
            exit 1
        }
    } else {
        Write-Info "Virtual environment already exists at $venvPath"
    }
    
    # Activate virtual environment
    $activateScript = Join-Path $venvPath "Scripts\Activate.ps1"
    if (-not (Test-Path $activateScript)) {
        Write-Error "Virtual environment activation script not found: $activateScript"
        exit 1
    }
    
    Write-Info "Activating virtual environment"
    & $activateScript
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to activate virtual environment"
        exit 1
    }
    
    # Check and install required packages
    $requiredPackages = @(
        "python-dotenv",
        "SQLAlchemy",
        "snowflake-connector-python",
        "snowflake-sqlalchemy", 
        "pandas",
        "numpy"
    )
    
    Write-Info "Installing required Python packages"
    foreach ($package in $requiredPackages) {
        Write-Info "Installing $package"
        pip install $package --quiet
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Failed to install $package"
            exit 1
        }
    }
    
    Write-Info "All dependencies installed successfully"
    
    # Run the forecast audit
    $forecastAudit = Join-Path $ProjectRoot "vscode_snowflake_starter\src\forecast\forecast_audit.py"
    if (-not (Test-Path $forecastAudit)) {
        Write-Error "Forecast audit script not found: $forecastAudit"
        exit 1
    }
    
    Write-Info "Running forecast audit: $forecastAudit"
    python $forecastAudit
    
    $exitCode = $LASTEXITCODE
    if ($exitCode -eq 0) {
        Write-Info "Forecast audit completed successfully with Green status"
    } elseif ($exitCode -eq 2) {
        Write-Warn "Forecast audit completed with Yellow status - needs attention"
    } else {
        Write-Error "Forecast audit failed with Red status (exit code $exitCode)"
        exit $exitCode
    }
    
} catch {
    Write-Error "Runner script failed: $($_.Exception.Message)"
    Write-Error "Stack trace: $($_.ScriptStackTrace)"
    exit 1
}