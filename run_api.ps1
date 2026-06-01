$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
Set-Location $root
if (-not (Test-Path "$root\.venv\Scripts\python.exe")) {
    Write-Error "未找到 .venv。请先执行: py -3.12 -m venv .venv ; .\.venv\Scripts\pip install -r requirements.txt"
}
& "$root\.venv\Scripts\python.exe" -m uvicorn main:app --host 0.0.0.0 --port 8012
