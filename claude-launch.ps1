# ============================================
# File: claude-launch.ps1
# Title: Claude Launcher — Send Prompt to Anthropic API and Save Response
# ============================================

param(
    [string]$Prompt,
    [string]$PromptFile,
    [string]$Model = "claude-3-haiku-20240307",
    [int]$MaxTokens = 1800,
    [double]$Temperature = 0.2
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Info($msg) { Write-Host "[INFO] $msg" -ForegroundColor Green }
function Write-Warn($msg) { Write-Host "[WARN] $msg" -ForegroundColor Yellow }
function Write-Fail($msg) { Write-Host "[FAIL] $msg" -ForegroundColor Red }

function Load-DotEnv([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) {
        Write-Warn ".env not found at: $Path"
        return
    }
    Get-Content -LiteralPath $Path | ForEach-Object {
        $line = $_.Trim()
        if ($line -eq "" -or $line.StartsWith("#")) { return }
        $pair = $line -split "=", 2
        if ($pair.Count -eq 2) {
            $name = $pair[0].Trim()
            $value = $pair[1].Trim().Trim('"').Trim("'")
            Set-Item -Path "env:$name" -Value $value
        }
    }
}

function Invoke-Claude([string]$InputPrompt, [string]$ModelName, [int]$MaxToks, [double]$Temp) {
    $apiKey = $env:ANTHROPIC_API_KEY
    if (-not $apiKey) { $apiKey = $env:CLAUDE_API_KEY }
    if (-not $apiKey) { throw "Missing ANTHROPIC_API_KEY in environment." }

    $body = @{
        model       = $ModelName
        max_tokens  = $MaxToks
        temperature = $Temp
        messages    = @(@{ role = "user"; content = $InputPrompt })
    } | ConvertTo-Json -Depth 6

    $headers = @{
        "x-api-key"         = $apiKey
        "anthropic-version" = "2023-06-01"
        "content-type"      = "application/json"
    }

    Write-Info "Posting to Claude model: $ModelName"
    $resp = Invoke-RestMethod -Method Post -Uri "https://api.anthropic.com/v1/messages" -Headers $headers -Body $body
    $text = ($resp.content | ForEach-Object { $_.text }) -join "`n"
    return @{ text = $text.Trim(); model = $ModelName }
}

# -------- Main --------

try {
    # Load .env if available
    $envPath = ".env"
    if (Test-Path $envPath) { 
        Load-DotEnv $envPath 
        Write-Info "Loaded environment variables from .env"
    }

    # Determine prompt source
    $finalPrompt = ""
    if ($PromptFile -and (Test-Path $PromptFile)) {
        $finalPrompt = Get-Content $PromptFile -Raw
        Write-Info "Loaded prompt from file: $PromptFile"
    } elseif ($Prompt) {
        $finalPrompt = $Prompt
        Write-Info "Using provided prompt parameter"
    } else {
        throw "No prompt provided. Use -Prompt 'text' or -PromptFile 'path'"
    }

    # Create timestamped output directory
    $timestamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
    $outputDir = "claude_outputs\claude_$timestamp"
    New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
    Write-Info "Created output directory: $outputDir"

    # Save the prompt
    $finalPrompt | Out-File -FilePath "$outputDir\prompt.txt" -Encoding UTF8
    
    # Call Claude
    Write-Info "Sending request to Claude API..."
    $result = Invoke-Claude -InputPrompt $finalPrompt -ModelName $Model -MaxToks $MaxTokens -Temp $Temperature
    
    # Save the response
    $result.text | Out-File -FilePath "$outputDir\response.txt" -Encoding UTF8
    
    # Display response
    Write-Host "`n===== CLAUDE RESPONSE =====" -ForegroundColor Cyan
    Write-Host $result.text -ForegroundColor White
    Write-Host "===========================`n" -ForegroundColor Cyan
    
    # Save metadata
    @{
        timestamp = $timestamp
        model = $result.model
        max_tokens = $MaxTokens
        temperature = $Temperature
        prompt_chars = $finalPrompt.Length
        response_chars = $result.text.Length
    } | ConvertTo-Json | Out-File -FilePath "$outputDir\metadata.json" -Encoding UTF8
    
    Write-Info "All files saved to: $outputDir"
    Write-Info "Claude launch completed successfully"
    
} catch {
    Write-Fail $_.Exception.Message
    exit 1
}