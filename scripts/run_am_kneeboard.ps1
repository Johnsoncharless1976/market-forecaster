# ============================================
# File: scripts\run_am_kneeboard.ps1
# Title: AM Kneeboard Runner
# Description: Sets up Python environment and executes AM kneeboard generator
# ============================================

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Info($msg) { Write-Host "[INFO] $msg" -ForegroundColor Green }
function Write-Warn($msg) { Write-Host "[WARN] $msg" -ForegroundColor Yellow }
function Write-Error($msg) { Write-Host "[ERROR] $msg" -ForegroundColor Red }

try {
    Write-Info "Starting AM Kneeboard Runner"
    
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
    
    # Install required packages
    $requiredPackages = @(
        "python-dotenv",
        "SQLAlchemy",
        "snowflake-connector-python",
        "snowflake-sqlalchemy",
        "pandas",
        "requests"
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
    
    # Create notifiers directory if it doesn't exist
    $notifiersDir = Join-Path $ProjectRoot "vscode_snowflake_starter\src\notifiers"
    if (-not (Test-Path $notifiersDir)) {
        New-Item -ItemType Directory -Path $notifiersDir -Force | Out-Null
        Write-Info "Created notifiers directory: $notifiersDir"
    }
    
    # Run the AM kneeboard generator
    $amKneeboard = Join-Path $ProjectRoot "vscode_snowflake_starter\src\notifiers\am_kneeboard.py"
    if (-not (Test-Path $amKneeboard)) {
        Write-Error "AM kneeboard script not found: $amKneeboard"
        exit 1
    }
    
    Write-Info "Running AM kneeboard generator: $amKneeboard"
    python $amKneeboard
    
    $exitCode = $LASTEXITCODE
    if ($exitCode -eq 0) {
        Write-Info "AM kneeboard completed successfully"
    } else {
        Write-Error "AM kneeboard failed with exit code $exitCode"
        exit $exitCode
    }
    
} catch {
    Write-Error "Runner script failed: $($_.Exception.Message)"
    Write-Error "Stack trace: $($_.ScriptStackTrace)"
    exit 1
}