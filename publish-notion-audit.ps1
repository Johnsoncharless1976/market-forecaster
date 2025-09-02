[CmdletBinding()]
param(
  # Optional: point to a specific audit dir; if omitted we auto-pick the newest stage1_exec_* folder
  [string]$AuditDir
)

$ErrorActionPreference = "Stop"

function Require-Env([string[]]$Names) {
  foreach ($n in $Names) {
    if (-not $env:$n -or [string]::IsNullOrWhiteSpace($env:$n)) {
      throw "Missing required environment variable: $n"
    }
  }
}

function Get-LatestAuditDir {
  $base = Join-Path -Path $PSScriptRoot -ChildPath "vscode_snowflake_starter/audit_exports"
  if (-not (Test-Path $base)) { return $null }

  Get-ChildItem -Path $base -Directory -Filter "stage1_exec_*" |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1 |
    ForEach-Object { $_.FullName }
}

function Read-SummaryCsv([string]$Dir) {
  $csvPath = Join-Path -Path $Dir -ChildPath "summary.csv"
  if (-not (Test-Path $csvPath)) { throw "summary.csv not found: $csvPath" }
  $rows = Import-Csv -Path $csvPath
  if (-not $rows) { throw "summary.csv is empty: $csvPath" }

  # convert to hashtable like: check -> @{ violations = N; status = "PASS|FAIL" }
  $map = @{}
  foreach ($r in $rows) {
    $check = $r.check
    $viol  = [int]$r.violations
    $stat  = $r.status
    $map[$check] = @{ violations = $viol; status = $stat }
  }
  return $map
}

function Get-NotionTitleProp([string]$DatabaseId, [string]$Token) {
  $db = Invoke-RestMethod `
    -Method Get `
    -Uri ("https://api.notion.com/v1/databases/{0}" -f $DatabaseId) `
    -Headers @{
      "Authorization"    = "Bearer $Token"
      "Notion-Version"   = "2022-06-28"
      "Content-Type"     = "application/json"
    }

  foreach ($k in $db.properties.PSObject.Properties.Name) {
    $p = $db.properties.$k
    if ($p.type -eq "title") { return $k }
  }
  # Fallback
  return "Name"
}

function New-NotionAuditPage(
  [string]$DatabaseId,
  [string]$Token,
  [string]$TitleProp,
  [string]$TitleText,
  [string]$BodyText
) {
  $payload = @{
    parent = @{ database_id = $DatabaseId }
    properties = @{
      $TitleProp = @{
        title = @(@{
          type = "text"
          text = @{ content = $TitleText }
        })
      }
    }
    children = @(
      @{
        object = "block"
        type   = "paragraph"
        paragraph = @{
          rich_text = @(
            @{ type = "text"; text = @{ content = $BodyText } }
          )
        }
      }
    )
  } | ConvertTo-Json -Depth 10

  $resp = Invoke-RestMethod `
    -Method Post `
    -Uri "https://api.notion.com/v1/pages" `
    -Headers @{
      "Authorization"    = "Bearer $Token"
      "Notion-Version"   = "2022-06-28"
      "Content-Type"     = "application/json"
    } `
    -Body $payload

  return $resp
}

# --------------------
# 1) Load .env
# --------------------
& "$PSScriptRoot\load-dotenv.ps1" | Out-Null

# --------------------
# 2) Verify required env vars
# --------------------
Require-Env @(
  "NOTION_TOKEN",
  "NOTION_AUDIT_DB_ID"
)

# --------------------
# 3) Resolve AuditDir
# --------------------
if (-not $AuditDir -or [string]::IsNullOrWhiteSpace($AuditDir)) {
  $AuditDir = Get-LatestAuditDir
  if (-not $AuditDir) {
    throw "Could not auto-detect latest audit dir. Pass -AuditDir <path>."
  }
}

Write-Host "Using AUDIT_DIR: $AuditDir"

# --------------------
# 4) Read summary.csv and build body
# --------------------
$summaryMap = Read-SummaryCsv -Dir $AuditDir
$summaryJson = $summaryMap | ConvertTo-Json -Depth 10

$nowUtc = [DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
$project = $env:CI_PROJECT_PATH
$pipeline = $env:CI_PIPELINE_ID
$jobUrl = $env:CI_JOB_URL

$bodyLines = @()
if ($project)  { $bodyLines += "*CI project:* $project" }
if ($pipeline) { $bodyLines += "*Pipeline:* $pipeline" }
if ($jobUrl)   { $bodyLines += "*Job URL:* $jobUrl" }
$bodyLines += "*Audit dir:* $AuditDir"
$bodyLines += ""
$bodyLines += "```json"
$bodyLines += $summaryJson
$bodyLines += "```"

$bodyText  = ($bodyLines -join "`n")
$titleText = "Stage 1 Audit — $nowUtc"

# --------------------
# 5) Discover title property and create page
# --------------------
$titleProp = Get-NotionTitleProp -DatabaseId $env:NOTION_AUDIT_DB_ID -Token $env:NOTION_TOKEN
Write-Host "Detected Notion title property: $titleProp"

$page = New-NotionAuditPage `
  -DatabaseId $env:NOTION_AUDIT_DB_ID `
  -Token $env:NOTION_TOKEN `
  -TitleProp $titleProp `
  -TitleText $titleText `
  -BodyText $bodyText

$createdUrl = $page.url
Write-Host "✅ Notion page created:"
Write-Host "   $createdUrl"
