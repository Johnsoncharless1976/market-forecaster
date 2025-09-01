# File: stage1_weekday.ps1
# Title: Stage 1 – Weekday Ingest + Metrics + Audit CSV (with audit events)
# Commit Notes:
# - Skips weekends (US/Eastern).
# - On success: logs OK with context.
# - On failure: logs FAIL with error message.

param([int]$Days = 7)

$ErrorActionPreference = "Stop"

# US/Eastern weekday guard
$tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
$nowET = [System.TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $tz)
if ($nowET.DayOfWeek -in 'Saturday','Sunday') {
  Write-Host "Market closed (ET weekend) — skipping ingest."
  exit 0
}

# Pick python (prefer venv)
$pyCmd = Get-Command "$PSScriptRoot\.venv\Scripts\python.exe" -ErrorAction SilentlyContinue
if (-not $pyCmd) { $pyCmd = Get-Command "python" -ErrorAction Stop }
$py = $pyCmd.Path

# Job tag for this run
$job = "stage1_weekday_auto_{0}" -f (Get-Date -Format "yyyyMMdd")

try {
  # Ingest
  & $py "$PSScriptRoot\vscode_snowflake_starter\src\ingest_yahoo_to_market_ohlcv.py" --symbols "^VIX,^VVIX,^GSPC,ES=F" --days $Days --job $job

  # Metrics
  & $py "$PSScriptRoot\vscode_snowflake_starter\src\show_metrics.py"

  # Export audit rollup CSV
  $exportJson = & $py "$PSScriptRoot\vscode_snowflake_starter\src\export_audit_rollup_csv.py" --days 30
  # Prepare context for audit event
  $ctx = @{ days = $Days; export = $exportJson } | ConvertTo-Json -Compress

  # Success audit
  & $py "$PSScriptRoot\vscode_snowflake_starter\src\log_audit_event.py" `
       --stage "Stage 1: Runner" --job $job --status OK `
       --message "Ingest+metrics+export OK" `
       --context $ctx

} catch {
  $err = $_.Exception.Message
  $ctx = @{ days = $Days } | ConvertTo-Json -Compress
  & $py "$PSScriptRoot\vscode_snowflake_starter\src\log_audit_event.py" `
       --stage "Stage 1: Runner" --job $job --status FAIL `
       --message "Runner failed" --error $err `
       --context $ctx
  throw
}
