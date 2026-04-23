param(
    [switch]$List,
    [switch]$ListBoards,
    [string]$VideoPath = '',
    [string]$VideoSearchDir = 'Videos',
    [switch]$VideoLoop,
    [switch]$VideoNoRealtime,
    [ValidateSet('auto', 'opencv', 'pyav')]
    [string]$VideoDecodeBackend = 'auto',
    [int]$VideoDecodeThreads = 0,
    [string]$Dicts = 'DICT_APRILTAG_36h11',
    [string]$BoardJson = 'boards\boardLab55center.json',
    [switch]$NoDisplay,
    [switch]$Display,
    [int]$GpuIndex = 0,
    [double]$TelemetryInterval = 1.0,
    [double]$FocalLength = 2180,
    [double]$TagSizeMm = 295.6,
    [string]$TagSizeMapJson = '',
    [double]$DisplayFps = 30.0,
    [double]$DisplayScale = 0.5,
    [ValidateSet('auto', 'off', 'blend')]
    [string]$VideoDeinterlace = 'auto',
    [switch]$InterlacedFastProfile,
    [string]$RawRecordOutputDir = 'recordings',
    [ValidateSet('auto', 'opencv', 'ffmpeg')]
    [string]$RawRecordBackend = 'ffmpeg',
    [string]$RawRecordFfmpegBin = 'ffmpeg',
    [string]$RawRecordFfmpegEncoder = 'h264_nvenc',
    [string]$RawRecordFfmpegPreset = 'p5',
    [double]$RawRecordScale = 0.5,
    [switch]$RawRecordStartDisabled,
    [switch]$OverlayDataDisable,
    [switch]$MqttEnable,
    [switch]$MqttDisable,
    [switch]$ParquetDisable,
    [string]$ParquetOutputDir = 'recordings',
    [string]$MqttHost = '127.0.0.1',
    [int]$MqttPort = 1883,
    [string]$MqttTopicPrefix = 'ndi/telemetry',
    [switch]$BoardPoseStreamEnable,
    [string]$BoardPoseStreamHost = '0.0.0.0',
    [int]$BoardPoseStreamPort = 9102,
    [double]$BoardPoseStreamHz = 50.0,
    [switch]$TelemetryRecordStartDisabled,
    [double]$AprilTagQuadDecimate = 2.0,
    [double]$AprilTagQuadSigma = 0.0,
    [int]$AdaptiveThreshWinSizeMin = 3,
    [int]$AdaptiveThreshWinSizeMax = 23,
    $CornerRefinementMethod = 3,
    [double]$MinMarkerPerimeterRate = 0.03,
    [double]$ErrorCorrectionRate = 0.6,
    [int]$AprilTagMinWhiteBlackDiff = 5,
    [switch]$EnableBoardRefinement
)

$ErrorActionPreference = 'Stop'
$projectRoot = $PSScriptRoot

function Resolve-PythonExe {
    $candidates = [System.Collections.Generic.List[string]]::new()

    if ($env:VIRTUAL_ENV) {
        $activePython = Join-Path $env:VIRTUAL_ENV 'Scripts\python.exe'
        if (Test-Path $activePython) {
            $candidates.Add($activePython)
        }
    }

    foreach ($candidate in @(
        (Join-Path $projectRoot '.venv\Scripts\python.exe'),
        (Join-Path $projectRoot 'venv\Scripts\python.exe')
    )) {
        if ((Test-Path $candidate) -and (-not $candidates.Contains($candidate))) {
            $candidates.Add($candidate)
        }
    }

    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) {
            return $candidate
        }
    }

    foreach ($commandName in @('python', 'py')) {
        try {
            $cmd = Get-Command $commandName -ErrorAction Stop | Select-Object -First 1
            if ($cmd -and $cmd.Source) {
                return [string]$cmd.Source
            }
        }
        catch {
        }
    }

    throw "No usable Python interpreter found. Run .\setup.ps1 first or activate the target venv."
}

function Resolve-FfmpegExecutable {
    param([string]$ConfiguredBin)

    $candidate = if ([string]::IsNullOrWhiteSpace($ConfiguredBin)) { 'ffmpeg' } else { $ConfiguredBin }
    if ([System.IO.Path]::IsPathRooted($candidate) -and (Test-Path $candidate)) {
        return $candidate
    }

    try {
        $cmd = Get-Command $candidate -ErrorAction Stop | Select-Object -First 1
        if ($cmd -and $cmd.Source -and (Test-Path $cmd.Source)) {
            return [string]$cmd.Source
        }
    }
    catch {
    }

    return $candidate
}

function Resolve-BoardJsonPaths {
    param([string]$RawValue)

    $resolved = [System.Collections.Generic.List[string]]::new()
    if ([string]::IsNullOrWhiteSpace($RawValue)) {
        return $resolved.ToArray()
    }

    foreach ($item in @($RawValue -split ',') | ForEach-Object { $_.Trim() } | Where-Object { $_ }) {
        $candidate = if ([System.IO.Path]::IsPathRooted($item)) {
            $item
        }
        else {
            Join-Path $projectRoot $item
        }

        if (Test-Path $candidate) {
            $resolved.Add((Resolve-Path $candidate).Path)
        }
        else {
            Write-Warning "Board JSON not found at '$candidate'; skipping."
        }
    }

    return $resolved.ToArray()
}

function Resolve-OptionalPath {
    param([string]$RawValue)

    if ([string]::IsNullOrWhiteSpace($RawValue)) {
        return ''
    }

    if ([System.IO.Path]::IsPathRooted($RawValue)) {
        return $RawValue
    }

    return (Join-Path $projectRoot $RawValue)
}

if ($ListBoards.IsPresent) {
    $boardsDir = Join-Path $projectRoot 'boards'
    if (-not (Test-Path $boardsDir)) {
        Write-Host 'No boards directory found.'
        exit 0
    }

    Write-Host 'Available board definitions:'
    Get-ChildItem -Path $boardsDir -Filter '*.json' -File | Sort-Object Name | ForEach-Object {
        Write-Host ("  - {0}" -f $_.FullName)
    }
    exit 0
}

$python = Resolve-PythonExe
$scriptPath = Join-Path $projectRoot 'video_gpu_preview_with_apriltag.py'
if (-not (Test-Path $scriptPath)) {
    throw "Script not found at $scriptPath"
}

$resolvedVideoSearchDir = Resolve-OptionalPath -RawValue $VideoSearchDir
$resolvedTagSizeMapJson = Resolve-OptionalPath -RawValue $TagSizeMapJson
$resolvedBoardJsonPaths = Resolve-BoardJsonPaths -RawValue $BoardJson
$resolvedRawRecordOutputDir = Resolve-OptionalPath -RawValue $RawRecordOutputDir
$resolvedParquetOutputDir = Resolve-OptionalPath -RawValue $ParquetOutputDir
$resolvedFfmpegBin = Resolve-FfmpegExecutable -ConfiguredBin $RawRecordFfmpegBin

$pyArgs = @(
    $scriptPath,
    '--video-search-dir', $resolvedVideoSearchDir,
    '--dicts', $Dicts,
    '--gpu-index', "$GpuIndex",
    '--telemetry-interval', "$TelemetryInterval",
    '--focal-length', "$FocalLength",
    '--tag-size-mm', "$TagSizeMm",
    '--display-fps', "$DisplayFps",
    '--display-scale', "$DisplayScale",
    '--video-decode-backend', $VideoDecodeBackend,
    '--video-decode-threads', "$VideoDecodeThreads",
    '--video-deinterlace', $VideoDeinterlace,
    '--raw-record-output-dir', $resolvedRawRecordOutputDir,
    '--raw-record-backend', $RawRecordBackend,
    '--raw-record-ffmpeg-bin', $resolvedFfmpegBin,
    '--raw-record-ffmpeg-encoder', $RawRecordFfmpegEncoder,
    '--raw-record-ffmpeg-preset', $RawRecordFfmpegPreset,
    '--raw-record-scale', "$RawRecordScale",
    '--parquet-output-dir', $resolvedParquetOutputDir,
    '--mqtt-host', $MqttHost,
    '--mqtt-port', "$MqttPort",
    '--mqtt-topic-prefix', $MqttTopicPrefix,
    '--board-pose-stream-host', $BoardPoseStreamHost,
    '--board-pose-stream-port', "$BoardPoseStreamPort",
    '--board-pose-stream-hz', "$BoardPoseStreamHz",
    '--april-tag-quad-decimate', "$AprilTagQuadDecimate",
    '--april-tag-quad-sigma', "$AprilTagQuadSigma",
    '--adaptive-thresh-win-size-min', "$AdaptiveThreshWinSizeMin",
    '--adaptive-thresh-win-size-max', "$AdaptiveThreshWinSizeMax",
    '--min-marker-perimeter-rate', "$MinMarkerPerimeterRate",
    '--error-correction-rate', "$ErrorCorrectionRate",
    '--april-tag-min-white-black-diff', "$AprilTagMinWhiteBlackDiff",
    '--show-timestamp'
)

if ($List.IsPresent) {
    $pyArgs += '--list'
}
if (-not [string]::IsNullOrWhiteSpace($VideoPath)) {
    $pyArgs += @('--video-path', $VideoPath)
}
if ($VideoLoop.IsPresent) {
    $pyArgs += '--video-loop'
}
if ($VideoNoRealtime.IsPresent) {
    $pyArgs += '--video-no-realtime'
}
if ($NoDisplay.IsPresent -or -not $Display.IsPresent) {
    $pyArgs += '--no-display'
}
if ($null -ne $CornerRefinementMethod) {
    $pyArgs += @('--corner-refinement-method', "$CornerRefinementMethod")
}
if ($MqttEnable.IsPresent -and -not $MqttDisable.IsPresent) {
    $pyArgs += '--mqtt-enable'
}
if ($ParquetDisable.IsPresent) {
    $pyArgs += '--parquet-disable'
}
if ($RawRecordStartDisabled.IsPresent) {
    $pyArgs += '--raw-record-start-disabled'
}
if ($OverlayDataDisable.IsPresent) {
    $pyArgs += '--overlay-data-disable'
}
if ($BoardPoseStreamEnable.IsPresent) {
    $pyArgs += '--board-pose-stream-enable'
}
if ($TelemetryRecordStartDisabled.IsPresent) {
    $pyArgs += '--telemetry-record-start-disabled'
}
if ($EnableBoardRefinement.IsPresent) {
    $pyArgs += '--enable-board-refinement'
}
if ($InterlacedFastProfile.IsPresent) {
    $pyArgs += '--interlaced-fast-profile'
}
if (-not [string]::IsNullOrWhiteSpace($resolvedTagSizeMapJson)) {
    $pyArgs += @('--tag-size-map-json', $resolvedTagSizeMapJson)
}
foreach ($boardPath in $resolvedBoardJsonPaths) {
    $pyArgs += @('--board-json', $boardPath)
}

Push-Location $projectRoot
try {
    & $python @pyArgs
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
