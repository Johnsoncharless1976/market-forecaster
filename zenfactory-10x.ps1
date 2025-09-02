# ============================================
# File: zenfactory-10x-fixed.ps1
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

# -------- Fixed ROADMAP for ZenMarket AI --------
$Roadmap = @(
    @{
        title = "Stage-1 Exec Audit: NYSE Holiday Gate (verify)"
        task = "Verify enhanced audit with NYSE holiday calendar producing holiday_weekdays and true_weekday_gaps; assert true_weekday_gaps=0; emit summary.csv + detail CSVs."
    },
    @{
        title = "Stage-3 Forecast Writer v1 (Baseline)"
        task = "Write Python to read FEATURES_DAILY from Snowflake and write baseline forecast (persistence) into FORECAST_DAILY via idempotent MERGE; add audit logging."
    },
    @{
        title = "Stage-3 Forecast Audit"
        task = "Create forecast_audit.py scoring latest forecasts vs actuals (hit-rate, MAE) and emit forecast_audit_summary.csv with STATUS rules; console report."
    },
    @{
        title = "CI: Forecast Jobs"
        task = "Add GitLab CI job to run forecast writer + forecast audit on schedule; publish artifacts to audit_exports/forecast_*; fail pipeline on Red."
    },
    @{
        title = "Notifier: AM Kneeboard"
        task = "Generate AM kneeboard (forecast summary, ATM straddle band) and post to Slack + Notion Daily DB."
    },
    @{
        title = "Post-Mortem Scorer"
        task = "Implement post_mortem_scorer.py logging Date|Symbol|Bias|Band Hit|% Change|Notes to Notion Daily DB; produce one-line verdict per day."
    },
    @{
        title = "Alert: Forecast Band Break"
        task = "Add intraday watcher posting Slack alert if price exits forecast band by X%; include suggested mitigation."
    },
    @{
        title = "Guardrails & Audit Hardening"
        task = "Expand audits: duplicates, gaps, schema checks, thresholds; update summary.csv with explicit counts; failing metrics trigger non-zero exit in CI."
    },
    @{
        title = "Weekly Rollup"
        task = "Compile weekly rollup of audits, forecasts, post-mortems to Notion report + Slack digest."
    },
    @{
        title = "Readiness & Handoff"
        task = "Produce readiness checklist (Green/Yellow/Red lights), env vars list, CI links, Notion DB links; post to #zen-forecaster-mr."
    }
)

# ------------------------- MAIN -------------------------
try {
    if (Test-Path ".env") { Load-DotEnv ".env" }

    $mode = if ($env:ZENFACTORY_MODE) { $env:ZENFACTORY_MODE } else { "ROADMAP" }  # ROADMAP | RULE
    $maxSteps = if ($env:MAX_STEPS) { [int]$env:MAX_STEPS } else { 10 }
    $model = if ($env:ANTHROPIC_MODEL) { $env:ANTHROPIC_MODEL } else { "claude-3-haiku-20240307" }

    Info "Mode=$mode  MaxSteps=$maxSteps  Model=$model"

    for ($step = 1; $step -le $maxSteps; $step++) {

        # 1) Run summarizer
        Info "Step $step - Running sonnet-audit.ps1..."
        $summOut = & .\sonnet-audit.ps1 2>&1 | Out-String

        # Extract summary text between markers
        $summaryText = ($summOut -split "===== CLAUDE",2)[-1]
        if ($summaryText) { 
            $summaryText = $summaryText -replace '^[^=]*AUDIT SUMMARY =====','' -replace '==========================================','' 
        }
        if (-not $summaryText -or $summaryText.Trim().Length -lt 10) { 
            $summaryText = $summOut 
        }

        # Simple status parsing
        $status = $null
        foreach ($line in ($summaryText -split "`r?`n")) {
            if ($line -match '^\s*STATUS:\s*(\w+)') {
                $status = $matches[1]
                break
            }
        }

        $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss 'ET'")
        
        # 2) Decide next task
        $title = ""
        $task = ""
        
        if ($mode -ieq "ROADMAP") {
            if ($step -le $Roadmap.Count) {
                $title = $Roadmap[$step-1].title
                $task = $Roadmap[$step-1].task
            } else {
                $title = "Roadmap Complete"
                $task = "All roadmap steps executed."
            }
        } else {
            switch ($status) {
                "Green"  { 
                    $title = "Build: Stage-3 Forecast Writer v1"
                    $task = $Roadmap[1].task 
                }
                "Yellow" { 
                    $title = "Diagnose and Re-Audit"
                    $task = "Run gap/duplicate detail; regenerate audit; re-summarize until Green." 
                }
                "Red"    { 
                    $title = "Fix Blocker and Recheck"
                    $task = "Perform targeted backfill/dedup; rerun ingest; rerun audit; re-summarize to Green." 
                }
                default  { 
                    $title = "Re-Summarize"
                    $task = "Ensure a proper STATUS line; rerun summarizer; then follow Green/Yellow/Red branch." 
                }
            }
        }

        Info "Next Step: $title"
        Write-Host "Task: $task" -ForegroundColor Yellow

        # 4) AUTO_NEXT: Launch Claude to generate the code/patch for this step
        $auto = if ($env:AUTO_NEXT) { $env:AUTO_NEXT } else { "1" }
        if ($auto -eq "1" -and $task.Trim().Length -gt 0) {
            $statusText = if ($status) { $status } else { 'n/a' }
            $promptText = @"
ROLE: Senior engineer continuing ZenMarket AI build.
CONTEXT:
Latest audit summary (verbatim):
$summaryText
STATUS: $statusText

TASK:
$title
$task

CONSTRAINTS:
Produce complete, copy/paste-ready scripts with headers (File, Title, Commit Notes).
No placeholders. Include how to run and where to save files in repo.
Prefer Python + PowerShell for runners; Snowflake via SQLAlchemy; idempotent MERGE; clear logs.
"@
            Info "Launching Claude for Step $step - $title"
            & .\claude-launch.ps1 -Prompt $promptText -Model $model
        } else {
            Info "AUTO_NEXT disabled or no task - pausing after suggestion."
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