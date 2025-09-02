# ============================================
# File: setup-tasks.ps1  
# Title: Windows Task Scheduler Setup
# Description: Register ZenFactory scheduled tasks
# ============================================

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ps = "$env:WINDIR\System32\WindowsPowerShell\v1.0\powershell.exe"
$scriptPath = Join-Path $PSScriptRoot "start-zenfactory.ps1"
$cmd = "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`""

Write-Host "[INFO] Setting up ZenFactory scheduled tasks..."
Write-Host "[INFO] PowerShell: $ps"
Write-Host "[INFO] Script: $scriptPath"
Write-Host "[INFO] Command: $cmd"

# Register AM task (8:40am ET weekdays)
Write-Host "[INFO] Creating AM Roadmap task (8:40am weekdays)..."
$result1 = schtasks /Create /TN "ZenFactory AM Roadmap" /TR "$ps $cmd" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 08:40 /RL HIGHEST /F
Write-Host $result1

# Register Midday task (12:30pm ET weekdays) 
Write-Host "[INFO] Creating Midday Roadmap task (12:30pm weekdays)..."
$result2 = schtasks /Create /TN "ZenFactory Midday Roadmap" /TR "$ps $cmd" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 12:30 /RL HIGHEST /F
Write-Host $result2

Write-Host "[INFO] Tasks created successfully!"
Write-Host "[INFO] To view tasks: schtasks /Query /TN `"ZenFactory*`""
Write-Host "[INFO] To run manually: schtasks /Run /TN `"ZenFactory AM Roadmap`""