# ============================================
# File: start-zenfactory.ps1
# Title: ZenFactory Entry Point
# Description: Runs the 10-step roadmap with AUTO_NEXT enabled (Haiku-only)
# ============================================

# Runs the 10-step roadmap with AUTO_NEXT enabled (Haiku-only)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Push-Location $PSScriptRoot
try {
    $env:ZENFACTORY_MODE  = "ROADMAP"
    $env:MAX_STEPS        = "10"
    $env:AUTO_NEXT        = "1"
    .\zenfactory-10x.ps1
} finally {
    Pop-Location
}