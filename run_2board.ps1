param(
    [string]$BoardA = 'boards\board_41-45-1920x1080.json',
    [string]$BoardB = 'boards\boardLab55center.json',
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$PassThruArgs
)

$ErrorActionPreference = 'Stop'
$scriptDir = $PSScriptRoot
$runScript = Join-Path $scriptDir 'run.ps1'
$pwshExe = Join-Path $PSHOME 'pwsh.exe'

if (-not (Test-Path $runScript)) {
    throw "run.ps1 was not found at $runScript"
}
if (-not (Test-Path $pwshExe)) {
    throw "pwsh.exe was not found at $pwshExe"
}

$boardJson = "$BoardA,$BoardB"

& $pwshExe -NoProfile -File $runScript -BoardJson $boardJson @PassThruArgs
exit $LASTEXITCODE