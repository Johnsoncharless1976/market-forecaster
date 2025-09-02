# ============================================
# File: zenfactory-10x.ps1
# Title: Hands-Free Orchestrator — ROADMAP or RULE (up to 10 steps)
# ============================================

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" }
function Warn($m){ Write-Warning $m }
function Fail($m){ Write-Error $m }

function Load-DotEnv([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) { return }
    Get-Content -LiteralPath $Path | ForEach-Object {
        $line = $_.Trim()
        if ($line -eq "" -or $line.StartsWith("#")) { return }
        $kv = $line -split "=", 2
        if ($kv.Count -eq 2) {
            $k = $kv[0].Trim(); $v = $kv[1].Trim().Trim('"').Trim("'")
            if ($k) { Set-Item -Path "env:$k" -Value $v }
        }
    }
}

function Parse-StatusAndHeadline([string]$SummaryText) {
    $status=$null;$reason=$null;$headline=$null
    foreach ($line in ($SummaryText -split "(`r`n|`n)")) {
        $m = [regex]::Match($line,'^\s*STATUS:\s*(?<s>\w+)\s*-\s*(?<r>.+)$','IgnoreCase')
        if ($m.Success) { $status=$m.Groups['s'].Value; $reason=$m.Groups['r'].Value; break }
    }
    $lines = ($SummaryText -split "(`r`n|`n)") | Where-Object { $_.Trim() -ne "" }
    if ($lines.Count -gt 0) {
        for ($i=$lines.Count-1; $i -ge 0; $i--) {
            $ln = $lines[$i].Trim()
            if ($ln -match '^\s*STATUS:' -or $ln -match '^\s*\d+\)') { continue }
            $headline = $ln; break
        }
    }
    return @{ "status"=$status; "reason"=$reason; "headline"=$headline }
}

function Build-SlackPayload([string]$Title,[string]$Body,[string]$Context){
    return @{
        "blocks" = @(
            @{ "type"="header"; "text"=@{ "type"="plain_text"; "text"=$Title; "emoji"=$true } },
            @{ "type"="section"; "text"=@{ "type"="mrkdwn"; "text"="```$Body```" } },
            @{ "type"="context"; "elements"=@(@{ "type"="mrkdwn"; "text"=$Context }) }
        )
    }
}

function Post-Slack([hashtable]$Payload){
    $hooks = @(
        $env:SLACK_WEBHOOK_URL,
        $env:SLACK_WEBHOOK_URL1,
        $env:SLACK_WEBHOOK_URL2,
        $env:SLACK_WEBHOOK_URL3
    ) | Where-Object { $_ -and ($_ -match '^https://hooks\.slack\.com/services/') }
    if (-not $hooks -or $hooks.Count -eq 0) { return }
    $json = $Payload | ConvertTo-Json -Depth 12
    foreach ($h in $hooks) {
        try { Invoke-RestMethod -Method Post -Uri $h -Body $json -ContentType "application/json" | Out-Null }
        catch { Warn "Slack post failed ($h): $($_.Exception.Message)" }
    }
}

function NotionHeaders {
    if (-not $env:NOTION_TOKEN) { throw "NOTION_TOKEN not set." }
    return @{
        "Authorization"  = "Bearer $env:NOTION_TOKEN"
        "Notion-Version" = "2022-06-28"
        "Content-Type"   = "application/json"
    }
}

function NotionNewPage([string]$DatabaseId,[string]$TitleProp,[string]$Title,[array]$Blocks,[hashtable]$ExtraProps=$null){
    $props = @{ $TitleProp = @{ "title"=@(@{ "type"="text"; "text"=@{ "content"=$Title } }) } }
    if ($ExtraProps){ foreach($k in $ExtraProps.Keys){ $props[$k]=$ExtraProps[$k] } }
    $payload = @{ "parent"=@{ "database_id"=$DatabaseId }; "properties"=$props; "children"=$Blocks } | ConvertTo-Json -Depth 20
    Invoke-RestMethod -Method Post -Uri "https://api.notion.com/v1/pages" -Headers (NotionHeaders) -Body $payload | Out-Null
}

function NotionBlocks([string]$H,[string]$Body,[string]$Note){
    return @(
        @{ "object"="block"; "type"="heading_2"; "heading_2"=@{ "rich_text"=@(@{ "type"="text"; "text"=@{ "content"=$H }}) } },
        @{ "object"="block"; "type"="paragraph"; "paragraph"=@{ "rich_text"=@(@{ "type"="text"; "text"=@{ "content"=$Note }}) } },
        @{ "object"="block"; "type"="code"; "code"=@{ "language"="plain text"; "rich_text"=@(@{ "type"="text"; "text"=@{ "content"=$Body }}) } }
    )
}

# -------- Fixed ROADMAP for ZenMarket AI (edit if desired) --------
$Roadmap = @(
    @{ "title"="Stage-1 Exec Audit: NYSE Holiday Gate (verify)"; "task"=@"
Verify enhanced audit with NYSE holiday calendar producing holiday_weekdays and true_weekday_gaps; assert true_weekday_gaps=0; emit summary.csv + detail CSVs.
"@ },
    @{ "title"="Stage-3 Forecast Writer v1 (Baseline)"; "task"=@"
Write Python to read FEATURES_DAILY from Snowflake and write baseline forecast (persistence) into FORECAST_DAILY via idempotent MERGE; add audit logging.
"@ },
    @{ "title"="Stage-3 Forecast Audit"; "task"=@"
Create forecast_audit.py scoring latest forecasts vs actuals (hit-rate, MAE) and emit forecast_audit_summary.csv with STATUS rules; console report.
"@ },
    @{ "title"="CI: Forecast Jobs"; "task"=@"
Add GitLab CI job to run forecast writer + forecast audit on schedule; publish artifacts to audit_exports/forecast_*; fail pipeline on Red.
"@ },
    @{ "title"="Notifier: AM Kneeboard"; "task"=@"
Generate AM kneeboard (forecast summary, ATM straddle band) and post to Slack + Notion Daily DB.
"@ },
    @{ "title"="Post-Mortem Scorer"; "task"=@"
Implement post_mortem_scorer.py logging Date|Symbol|Bias|Band Hit|% Change|Notes to Notion Daily DB; produce one-line verdict per day.
"@ },
    @{ "title"="Alert: Forecast Band Break"; "task"=@"
Add intraday watcher posting Slack alert if price exits forecast band by X%; include suggested mitigation.
"@ },
    @{ "title"="Guardrails & Audit Hardening"; "task"=@"
Expand audits: duplicates, gaps, schema checks, thresholds; update summary.csv with explicit counts; failing metrics trigger non-zero exit in CI.
"@ },
    @{ "title"="Weekly Rollup"; "task"=@"
Compile weekly rollup of audits, forecasts, post-mortems to Notion report + Slack digest.
"@ },
    @{ "title"="Readiness & Handoff"; "task"=@"
Produce readiness checklist (Green/Yellow/Red lights), env vars list, CI links, Notion DB links; post to #zen-forecaster-mr.
"@ }
)

# ------------------------- MAIN -------------------------
try {
    if (Test-Path ".env") { Load-DotEnv ".env" }

    $mode     = ($env:ZENFACTORY_MODE   ? $env:ZENFACTORY_MODE   : "ROADMAP")  # ROADMAP | RULE
    $maxSteps = ($env:MAX_STEPS         ? [int]$env:MAX_STEPS     : 10)
    $model    = ($env:ANTHROPIC_MODEL   ? $env:ANTHROPIC_MODEL    : "claude-3-haiku-20240307")  # Haiku-only default

    Info "Mode=$mode  MaxSteps=$maxSteps  Model=$model"

    for ($step = 1; $step -le $maxSteps; $step++) {

        # 1) Run summarizer (prints & posts summary via sonnet-audit.ps1)
        Info "Step $step — Running sonnet-audit.ps1…"
        $summOut = & .\sonnet-audit.ps1 2>&1 | Out-String

        # Extract plain summary text between markers (fallback to raw if not found)
        $summaryText = ($summOut -split "===== CLAUDE",2)[-1]
        if ($summaryText) { $summaryText = $summaryText -replace '^[^=]*AUDIT SUMMARY =====','' -replace '==========================================','' }
        if (-not $summaryText -or $summaryText.Trim().Length -lt 10) { $summaryText = $summOut }

        $parsed = Parse-StatusAndHeadline -SummaryText $summaryText
        $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss 'ET'")
        $ctx = "_zenfactory-10x_ | Step $step/$maxSteps | Status: $($parsed.status ?? 'n/a') | $ts"

        # 2) Decide next task
        $title = ""; $task = ""
        if ($mode -ieq "ROADMAP") {
            if ($step -le $Roadmap.Count) {
                $title = $Roadmap[$step-1].title
                $task  = $Roadmap[$step-1].task
            } else {
                $title = "Roadmap Complete"; $task = "All roadmap steps executed."
            }
        } else {
            switch -Regex ($parsed.status) {
                '^Green$'  { $title="Build: Stage-3 Forecast Writer v1"; $task=$Roadmap[1].task }
                '^Yellow$' { $title="Diagnose & Re-Audit"; $task="Run gap/duplicate detail; regenerate audit; re-summarize until Green." }
                '^Red$'    { $title="Fix Blocker & Recheck"; $task="Perform targeted backfill/dedup; rerun ingest; rerun audit; re-summarize to Green." }
                default    { $title="Re-Summarize"; $task="Ensure a proper STATUS line; rerun summarizer; then follow Green/Yellow/Red branch." }
            }
        }

        # 3) Post Next Step to Slack and Notion
        $nextBody = "$title`n`n$task"
        Post-Slack -Payload (Build-SlackPayload -Title "Next Suggested Step" -Body $nextBody -Context $ctx)

        if ($env:NOTION_TOKEN -and $env:NOTION_PROMPT_DB_ID) {
            $titleProp = ($env:NOTION_PROMPT_TITLE_PROP ? $env:NOTION_PROMPT_TITLE_PROP : "Name")
            $npTitle = "[Next Step $step/$maxSteps] $title"
            $blocks = NotionBlocks -H "Next Suggested Step" -Body $nextBody -Note "Status: $($parsed.status ?? 'n/a') | Headline: $($parsed.headline ?? '(none)')"
            NotionNewPage -DatabaseId $env:NOTION_PROMPT_DB_ID -TitleProp $titleProp -Title $npTitle -Blocks $blocks
        }

        # 4) AUTO_NEXT: Launch Claude to generate the code/patch for this step
        $auto = ($env:AUTO_NEXT ? $env:AUTO_NEXT : "1")
        if ($auto -eq "1" -and $task.Trim().Length -gt 0) {
            $promptText = @"
ROLE: Senior engineer continuing ZenMarket AI build.
CONTEXT:
- Latest audit summary (verbatim):
---
$summaryText
---
STATUS: $($parsed.status ?? 'n/a')
HEADLINE: $($parsed.headline ?? '(none)')

TASK:
$title
$task

CONSTRAINTS:
- Produce complete, copy/paste-ready scripts with headers (File, Title, Commit Notes).
- No placeholders. Include how to run and where to save files in repo.
- Prefer Python + PowerShell for runners; Snowflake via SQLAlchemy; idempotent MERGE; clear logs.
"@
            Info "Launching Claude for Step $step: $title"
            & .\claude.ps1 -Prompt $promptText -Model $model
        } else {
            Info "AUTO_NEXT disabled or no task — pausing after suggestion."
        }

        # 5) Early exit if ROADMAP finished
        if ($mode -ieq "ROADMAP" -and $step -ge $Roadmap.Count) {
            Info "Roadmap complete at step $step."
            break
        }
    }

    Info "zenfactory-10x finished."
}
catch {
    Fail $_.Exception.Message
    exit 1
}
