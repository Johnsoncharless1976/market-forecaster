param(
    [string]$UserPrompt = "Summarize the latest Stage-1 audit."
)

# Locate the newest Stage-1 export folder
$latest = Get-ChildItem .\vscode_snowflake_starter\audit_exports\stage1_exec_* -Directory |
  Sort-Object Name -Descending | Select-Object -First 1

# Build paths
$report  = Join-Path $latest.FullName 'REPORT_EXEC.md'
$summary = Join-Path $latest.FullName 'summary.csv'

# Compose final prompt
$prompt = @"
You are the ZenMarket CI auditor. Analyze the latest Stage-1 audit.

User request: $UserPrompt

--- REPORT_EXEC.md ---
$(Get-Content $report -Raw)

--- summary.csv ---
$(Get-Content $summary -Raw)
"@

# Run Claude Runner
python .\claude_runner\claude_runner.py $prompt
