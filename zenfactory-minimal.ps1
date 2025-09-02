# Simple test version of zenfactory orchestrator
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" }

$Roadmap = @(
    @{ title="Stage-1 Exec Audit"; task="Verify enhanced audit with NYSE holiday calendar." }
)

try {
    $mode = if ($env:ZENFACTORY_MODE) { $env:ZENFACTORY_MODE } else { "ROADMAP" }
    $maxSteps = if ($env:MAX_STEPS) { [int]$env:MAX_STEPS } else { 1 }
    
    Info "Mode=$mode  MaxSteps=$maxSteps"
    
    for ($step = 1; $step -le $maxSteps; $step++) {
        Info "Step $step - Running sonnet-audit.ps1"
        
        # Run audit
        $summOut = & .\sonnet-audit.ps1 2>&1 | Out-String
        Info "Audit completed"
        
        # Get next task
        $title = ""
        $task = ""
        if ($mode -ieq "ROADMAP") {
            if ($step -le $Roadmap.Count) {
                $title = $Roadmap[$step-1].title
                $task = $Roadmap[$step-1].task
            }
        }
        
        Info "Next task: $title"
        Info "Description: $task"
        
        # Exit after roadmap complete
        if ($mode -ieq "ROADMAP" -and $step -ge $Roadmap.Count) {
            Info "Roadmap complete."
            break
        }
    }
    
    Info "zenfactory-minimal finished."
}
catch {
    Write-Error $_.Exception.Message
    exit 1
}