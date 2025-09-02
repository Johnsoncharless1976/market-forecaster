# load-dotenv.ps1
param(
  [string]$Path = ".env"
)

if (-not (Test-Path $Path)) {
  Write-Error "'.env' not found at $Path"
  exit 1
}

Get-Content -Path $Path | ForEach-Object {
  $line = $_.Trim()
  if ($line -eq "" -or $line.StartsWith("#")) { return }

  $splitIndex = $line.IndexOf("=")
  if ($splitIndex -lt 1) { return }

  $key = $line.Substring(0, $splitIndex).Trim()
  $val = $line.Substring($splitIndex + 1).Trim()

  # Strip optional surrounding quotes
  if ($val.StartsWith('"') -and $val.EndsWith('"')) { 
    $val = $val.Substring(1, $val.Length-2) 
  }
  if ($val.StartsWith("'") -and $val.EndsWith("'")) { 
    $val = $val.Substring(1, $val.Length-2) 
  }

  # Correct way to set environment variables dynamically
  Set-Item -Path "Env:$key" -Value $val
}

Write-Host "Loaded environment from $Path"
