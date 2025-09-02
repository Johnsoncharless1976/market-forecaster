param(
    [string]$prompt = "Hello from runner"
)

$body = @{
    model       = "claude-3-haiku-20240307"
    max_tokens  = 512
    temperature = 0.7
    messages    = @(@{ role = "user"; content = $prompt })
} | ConvertTo-Json -Depth 5 -Compress

$response = Invoke-RestMethod `
    -Uri "https://api.anthropic.com/v1/messages" `
    -Headers @{
        "x-api-key"          = $env:ANTHROPIC_API_KEY
        "anthropic-version"  = "2023-06-01"
        "Content-Type"       = "application/json"
    } `
    -Method Post `
    -Body $body

$response.content[0].text
