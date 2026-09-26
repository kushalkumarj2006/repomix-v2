# Repomix backend launcher — loads .env into the process, then runs the server.
if (-not (Test-Path ".env")) {
    Write-Error ".env not found. Copy .env.example to .env and fill it in."
    exit 1
}
Get-Content .env | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]*)=(.*)$') {
        [Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), "Process")
    }
}
Write-Host "Env loaded. Starting server..." -ForegroundColor Green
go run .
