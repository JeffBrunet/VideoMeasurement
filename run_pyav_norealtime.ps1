param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$PassThruArgs
)

$ErrorActionPreference = 'Stop'
$scriptDir = $PSScriptRoot
$runScript = Join-Path $scriptDir 'run.ps1'

if (-not (Test-Path $runScript)) {
    throw "run.ps1 was not found at $runScript"
}

& $runScript -VideoDecodeBackend pyav -VideoNoRealtime @PassThruArgs
exit $LASTEXITCODE
