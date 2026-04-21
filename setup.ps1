param(
    [string]$PythonVersion = '3.14',
    [string]$VenvName = '.venv'
)

$ErrorActionPreference = 'Stop'
$projectRoot = $PSScriptRoot
$venvPath = Join-Path $projectRoot $VenvName
$pythonExe = Join-Path $venvPath 'Scripts\python.exe'
$requirementsPath = Join-Path $projectRoot 'requirements.txt'

if (-not (Get-Command py -ErrorAction SilentlyContinue)) {
    throw "Python launcher 'py' was not found. Install Python from python.org and retry."
}

if (-not (Test-Path $pythonExe)) {
    Write-Host "Creating virtual environment at $venvPath"
    & py -$PythonVersion -m venv $venvPath
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create virtual environment with Python $PythonVersion"
    }
}

Write-Host "Using Python: $pythonExe"
& $pythonExe -m pip install --upgrade pip setuptools wheel
if ($LASTEXITCODE -ne 0) {
    throw 'Failed to upgrade pip/setuptools/wheel.'
}

& $pythonExe -m pip install -r $requirementsPath
if ($LASTEXITCODE -ne 0) {
    throw 'Failed to install project requirements.'
}

Write-Host ''
Write-Host 'Setup complete.'
Write-Host "Activate with: $venvPath\Scripts\Activate.ps1"
Write-Host 'Run with: .\run.ps1'
