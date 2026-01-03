Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repoRoot ".venv\\Scripts\\python.exe"
$dbPath = Join-Path $repoRoot "data\\analytics.duckdb"

if (Test-Path $dbPath) {
    Remove-Item $dbPath -Force
}

$env:PYTHONPATH = $repoRoot

& $python (Join-Path $repoRoot "backend\\ingest.py") --csv (Join-Path $repoRoot "data\\sample.csv")
& $python (Join-Path $repoRoot "backend\\pipeline.py")
& $python -m pytest
