<# claude.ps1 — minimal, reliable Claude caller
   Usage:
     .\claude.ps1 "hello world"
     .\claude.ps1 -Prompt "build me a script" -Model "claude-3-haiku-20240307" -MaxTokens 512 -Temperature 0.5
#>

[CmdletBinding()]
param(
  [Parameter(Position=0, Mandatory=$false)]
  [string]$Prompt,

  [string]$Model = $env:ANTHROPIC_MODEL,        # falls back to env
  [int]$MaxTokens = 512,                         # safe for Haiku (limit 4096)
  [double]$Temperature = 0.7
)

$ErrorActionPreference = "Stop"

# Optional: auto-load .env if you have load-dotenv.ps1 next to this script
$dotenv = Join-Path $PSScriptRoot "load-dotenv.ps1"
if (Test-Path $dotenv) { & $dotenv | Out-Null }

if (-not $env:ANTHROPIC_API_KEY) { throw "ANTHROPIC_API_KEY is not set." }
if (-not $Model -or [string]::IsNullOrWhiteSpace($Model)) { $Model = "claude-3-haiku-20240307" }

# If no -Prompt was passed, read from prompt.md (common dev flow)
if (-not $Prompt -or [string]::IsNullOrWhiteSpace($Prompt)) {
  $pm = Join-Path $PSScriptRoot "prompt.md"
  if (-not (Test-Path $pm)) { throw "No prompt provided and prompt.md not found." }
  $Prompt = Get-Content $pm -Raw
}

# Build request
$body = @{
  model       = $Model
  max_tokens  = $MaxTokens
  temperature = $Temperature
  messages    = @(@{ role = "user"; content = $Prompt })
} | ConvertTo-Json -Depth 6

# Call Claude
$response = Invoke-RestMethod `
  -Uri "https://api.anthropic.com/v1/messages" `
  -Headers @{
    "x-api-key"         = $env:ANTHROPIC_API_KEY
    "anthropic-version" = "2023-06-01"
    "Content-Type"      = "application/json"
  } `
  -Method Post `
  -Body $body

# Print first text chunk
if ($response -and $response.content -and $response.content.Count -gt 0) {
  $response.content[0].text
} else {
  Write-Host "No content returned." -ForegroundColor Yellow
}
