# ============================================
# File: sonnet-audit.ps1
# Title: Stage-1 Exec Audit → Claude (Sonnet) Summarizer + Slack (multi-channel) + Notion
# Commit Notes:
# - Multi-Slack support: posts to #zen-forecaster-ops, #zen-forecaster-incidents, #zen-forecaster-mr
# - Slack webhooks hardwired from env for safety, or override below
# - Audit summarization unchanged (Console, Notion still work as before)
# ============================================

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Info($msg) { Write-Host "[INFO] $msg" }
function Write-Warn($msg) { Write-Warning $msg }
function Write-Fail($msg) { Write-Error $msg }

function Load-DotEnv([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) {
        Write-Warn ".env not found at: $Path"
        return
    }
    
    # Clear any existing SLACK_WEBHOOK_URL* variables first
    Remove-Item env:SLACK_WEBHOOK_URL* -ErrorAction SilentlyContinue
    
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
    Write-Info "Loaded environment from $Path"
}

function Find-LatestStage1ExecAudit {
    $root = if ($PSScriptRoot) { $PSScriptRoot } else { (Get-Location).Path }
    $candidates = @()

    $preferredBase = Join-Path $root "vscode_snowflake_starter\audit_exports"
    if (Test-Path -LiteralPath $preferredBase) {
        $candidates += Get-ChildItem -LiteralPath $preferredBase -Directory -Filter "stage1_exec_*" -ErrorAction SilentlyContinue
    }

    if ($candidates.Count -eq 0) { throw "No stage1_exec_* audit directories found." }

    $latest = $candidates | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    Write-Info "Found latest audit: $($latest.FullName)"
    return $latest.FullName
}

function Build-ClaudePrompt([string]$CsvRaw) {
@"
You are a senior QA auditor. Summarize the Stage-1 (Data Ingestion) EXEC audit results contained in summary.csv below.

REQUIRED OUTPUT (plain text, concise):
1) Five bullets: the clearest facts from the data (use strong, specific language).
2) STATUS: One of [Green | Yellow | Red] with a one-line reason.
3) Remediation Plan: up to 5 numbered steps, actionable and verifiable.
4) Metrics: call out any non-zero gap/duplicate/reject counts; note holiday_weekdays vs true_weekday_gaps specifically.
5) One-sentence executive headline (<= 25 words).

Constraints:
- Use ONLY the provided CSV; do not speculate.
- Keep total output under ~250 words.
- If true_weekday_gaps = 0, consider Green status even with holiday_weekdays > 0.
- If true_weekday_gaps > 0, this indicates Red status (missing actual trading day data).

--- BEGIN summary.csv ---
$CsvRaw
--- END summary.csv ---
"@
}

function Invoke-Claude([string]$Prompt, [string]$Model = $null) {
    $apiKey = $env:ANTHROPIC_API_KEY
    if (-not $apiKey) { $apiKey = $env:CLAUDE_API_KEY }
    if (-not $apiKey) { throw "Missing ANTHROPIC_API_KEY in environment." }

    if (-not $Model) {
        $Model = if ($env:ANTHROPIC_MODEL) { $env:ANTHROPIC_MODEL } else { "claude-3-sonnet-20240229" }
    }

    $body = @{
        model       = $Model
        max_tokens  = 1800
        temperature = 0.2
        messages    = @(@{ role = "user"; content = $Prompt })
    } | ConvertTo-Json -Depth 6

    $headers = @{
        "x-api-key"         = $apiKey
        "anthropic-version" = "2023-06-01"
        "content-type"      = "application/json"
    }

    Write-Info "Posting to Claude model: $Model"
    $resp = Invoke-RestMethod -Method Post -Uri "https://api.anthropic.com/v1/messages" -Headers $headers -Body $body
    $text = ($resp.content | ForEach-Object { $_.text }) -join "`n"
    return @{ text = $text.Trim(); model = $Model }
}

# --- Slack Multi-Channel (clean) ---
function Get-SlackHooks {
    @(
        $env:SLACK_WEBHOOK_URL,
        $env:SLACK_WEBHOOK_URL1,
        $env:SLACK_WEBHOOK_URL2,
        $env:SLACK_WEBHOOK_URL3
    ) | Where-Object { $_ -and ($_ -match '^https://hooks\.slack\.com/services/') }
}

function Mask-Hook([string]$u){
    if (-not $u) { return "" }
    # keep team/channel ids, mask token
    $parts = $u -split '/'
    if ($parts.Count -ge 7) { return "$($parts[0..5] -join '/')/******" }
    return "hook:******"
}

function Post-MultiSlack([hashtable]$Payload) {
    $hooks = Get-SlackHooks
    if (-not $hooks -or @($hooks).Count -eq 0) {
        Write-Warning "No Slack webhooks in env; skipping Slack post."
        return
    }
    $json = $Payload | ConvertTo-Json -Depth 12
    foreach ($h in $hooks) {
        try {
            Write-Host "[INFO] Posting to Slack: $(Mask-Hook $h)"
            Invoke-RestMethod -Method Post -Uri $h -Body $json -ContentType "application/json" | Out-Null
        } catch {
            Write-Warning "Slack post failed ($(Mask-Hook $h)): $($_.Exception.Message)"
        }
    }
}

function Post-NotionAudit([string]$SummaryText, [string]$AuditDir, [string]$Model) {
    if (-not ($env:NOTION_TOKEN -and $env:NOTION_AUDIT_DB_ID)) {
        Write-Warn "NOTION_TOKEN/NOTION_AUDIT_DB_ID not set - skipping Notion publish."
        return
    }

    $titleProp = if ($env:NOTION_AUDIT_TITLE_PROP) { $env:NOTION_AUDIT_TITLE_PROP } else { "Name" }
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm")
    $title = "[Stage-1 EXEC] Audit Summary - $ts"

    $payload = @{
        parent = @{ database_id = $env:NOTION_AUDIT_DB_ID }
        properties = @{
            $titleProp = @{ title = @(@{ type = "text"; text = @{ content = $title } }) }
        }
        children = @(
            @{ object = "block"; type = "heading_2"; heading_2 = @{ rich_text = @(@{ type = "text"; text = @{ content = "Stage-1 Exec Audit Summary" } }) } },
            @{ object = "block"; type = "paragraph"; paragraph = @{ rich_text = @(@{ type = "text"; text = @{ content = "Model: $Model | Dir: $AuditDir" } }) } },
            @{ object = "block"; type = "code"; code = @{ language = "plain text"; rich_text = @(@{ type = "text"; text = @{ content = $SummaryText } }) } }
        )
    } | ConvertTo-Json -Depth 10

    $headers = @{
        "Authorization"  = "Bearer $($env:NOTION_TOKEN)"
        "Notion-Version" = "2022-06-28"
        "Content-Type"   = "application/json"
    }

    Write-Info "Creating Notion page..."
    $resp = Invoke-RestMethod -Method Post -Uri "https://api.notion.com/v1/pages" -Headers $headers -Body $payload
    Write-Info "Notion page created: $($resp.id)"
}

# -------- Main --------

try {
    # 1) Load .env
    $envPath = Join-Path (Get-Location).Path ".env"
    if (Test-Path $envPath) { Load-DotEnv $envPath }

    # 2) Find latest audit
    $latestDir = Find-LatestStage1ExecAudit

    # 3) Read summary.csv
    $summaryPath = Join-Path $latestDir "summary.csv"
    if (-not (Test-Path $summaryPath)) { throw "summary.csv not found in: $latestDir" }
    Write-Info "Reading $summaryPath"
    $csvRaw = Get-Content $summaryPath -Raw

    # 4) Invoke Claude
    $prompt = Build-ClaudePrompt -CsvRaw $csvRaw
    $claude = Invoke-Claude -Prompt $prompt
    $summary = $claude.text
    $model = $claude.model

    # 5) Console output
    ""
    "===== CLAUDE (SONNET) AUDIT SUMMARY ====="
    $summary
    "=========================================="
    ""

    # 6) Multi-channel Slack post
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    $payload = @{
        blocks = @(
            @{ type = "header"; text = @{ type = "plain_text"; text = "ZenMarket Audit Summary"; emoji = $true } },
            @{ type = "section"; text = @{ type = "mrkdwn"; text = ":mag: Stage-1 EXEC Audit Summary" } },
            @{ type = "section"; text = @{ type = "mrkdwn"; text = "``````$summary``````" } },
            @{ type = "context"; elements = @(@{ type = "mrkdwn"; text = "_Dir_: $latestDir | _Model_: $model | _When_: $ts" }) }
        )
    }
    Post-MultiSlack -Payload $payload

    # 7) Notion publish
    Post-NotionAudit -SummaryText $summary -AuditDir $latestDir -Model $model

    Write-Info "Pipeline complete: Console -> Multi-Slack -> Notion"
}
catch {
    Write-Fail $_.Exception.Message
    exit 1
}