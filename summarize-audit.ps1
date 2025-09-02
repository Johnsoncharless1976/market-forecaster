<# summarize-audit.ps1
    Usage:
      .\summarize-audit.ps1
      .\summarize-audit.ps1 -MaxTokens 800 -Temperature 0.3
#>

[CmdletBinding()]
param(
  [int]$MaxTokens = 800,       # safe for Haiku (limit 4096)
  [double]$Temperature = 0.3,
  [string]$Model = $env:ANTHROPIC_MODEL
)

$ErrorActionPreference = "Stop"

# Load .env if loader exists (same one you used earlier)
$dotenv = Join-Path $PSScriptRoot "load-dotenv.ps1"
if (Test-Path $dotenv) { & $dotenv | Out-Null }

if (-not $env:ANTHROPIC_API_KEY) { throw "ANTHROPIC_API_KEY is not set." }
if (-not $Model -or [string]::IsNullOrWhiteSpace($Model)) { $Model = "claude-3-haiku-20240307" }

# Resolve audit dir (prefer env from CI; else find newest locally)
function Get-LatestAuditDir {
  if ($env:AUDIT_DIR -and (Test-Path $env:AUDIT_DIR)) { return $env:AUDIT_DIR }
  $root = Join-Path $PSScriptRoot "vscode_snowflake_starter/audit_exports"
  if (-not (Test-Path $root)) { throw "Audit exports folder not found: $root" }
  $candidate = Get-ChildItem -Path $root -Directory -Filter "stage1_exec_*" |
               Sort-Object LastWriteTime -Descending | Select-Object -First 1
  if (-not $candidate) { throw "No stage1_exec_* directories found in $root" }
  return $candidate.FullName
}

$AuditDir = Get-LatestAuditDir
$summaryCsv = Join-Path $AuditDir "summary.csv"
$reportMd   = Join-Path $AuditDir "REPORT_EXEC.md"

if (-not (Test-Path $summaryCsv)) { throw "Missing $summaryCsv" }
if (-not (Test-Path $reportMd))   { throw "Missing $reportMd" }

# Load files
$summaryRows = Import-Csv -Path $summaryCsv
$summaryJson = $summaryRows | ConvertTo-Json -Depth 4
$reportHead  = (Get-Content $reportMd -Raw)

# Build a useful prompt for Claude
$proj  = $env:CI_PROJECT_PATH
$pipe  = $env:CI_PIPELINE_ID
$job   = $env:CI_JOB_URL

$prompt = @"
You are the CI data-audit assistant. Summarize and assess the Stage 1 Audit execution below.

Context:
- CI project: ${proj}
- Pipeline: ${pipe}
- Job URL: ${job}
- Audit dir: ${AuditDir}

summary.csv (parsed):
$summaryJson

REPORT_EXEC.md (full text):
$reportHead

Please provide:
1) A crisp executive summary (1-2 sentences).
2) A bullet list of failing checks (if any) with counts.
3) Whether failures are likely calendar/holiday artifacts vs data quality.
4) A concrete, prioritized remediation plan (3-5 bullets).
5) A single STATUS label: Green / Yellow / Red.

Keep it compact and actionable.
"@

# Call Anthropic API directly (same pattern as your working claude.ps1)
$body = @{
  model       = $Model
  max_tokens  = $MaxTokens
  temperature = $Temperature
  messages    = @(@{ role = "user"; content = $prompt })
} | ConvertTo-Json -Depth 6

$response = Invoke-RestMethod `
  -Uri "https://api.anthropic.com/v1/messages" `
  -Headers @{
    "x-api-key"         = $env:ANTHROPIC_API_KEY
    "anthropic-version" = "2023-06-01"
    "Content-Type"      = "application/json"
  } `
  -Method Post `
  -Body $body

if ($response -and $response.content -and $response.content.Count -gt 0) {
  $response.content[0].text
} else {
  Write-Host "No content returned." -ForegroundColor Yellow
}
