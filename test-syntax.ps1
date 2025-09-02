# Test PowerShell hashtable syntax
$test = @{
    "title" = "Stage-3 Forecast Writer v1 (Baseline)"
    "task" = @"
Write Python to read FEATURES_DAILY from Snowflake and write baseline forecast.
"@
}

Write-Host "Test syntax works: $($test.title)"