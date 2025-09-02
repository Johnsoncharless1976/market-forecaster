try {
    Write-Host "Testing basic structure"
    $step = 1
    $title = "Test Title"
    Write-Host "Step ${step}: ${title}"
}
catch {
    Write-Error $_.Exception.Message
    exit 1
}