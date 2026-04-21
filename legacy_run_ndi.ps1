param(
    [switch]$List,
    # [string]$SourceHint = '192.168.42.148',
    [string]$SourceHint = 'MAGEWELL',
    # [string]$SourceHint = '10.65.113.141',
    # [string]$SourceHint = '192.168.10.205',
    [double]$DiscoverTimeout = 8.0,
    [int]$GpuIndex = 0,
    [double]$TelemetryInterval = 1.0,
    [string]$Dicts = 'DICT_APRILTAG_36h11',
    [string]$BoardName = '',
    [string]$BoardJson = 'boards\boardLab55center.json',
    [switch]$ListBoards,
    [switch]$NoDisplay,
    [switch]$ShowTimestamp,
    [string]$RawRecordOutputDir = 'recordings',
    [ValidateSet('auto', 'opencv', 'ffmpeg')]
    [string]$RawRecordBackend = 'ffmpeg',
    [string]$RawRecordFfmpegBin = 'ffmpeg',
    [string]$RawRecordFfmpegEncoder = 'h264_nvenc',
    [string]$RawRecordFfmpegPreset = 'p5',
    [double]$FocalLength = 2180,
    [double]$TagSizeMm = 295.6, # Fallback tag size (used when per-tag size is not provided via board JSON or -TagSizeMapJson)
    [string]$TagSizeMapJson = '',
    [int]$AnalysisWorkers = 0,
    [double]$DisplayFps = 30.0,
    [double]$DisplayScale = 0.5,
    [double]$DisplayPrepOversample = 1.3333333333,
    [int]$DisplayDelayFrames = 1,
    [double]$SyncTimeoutMs = 0.0,
    [double]$FreedAngleScale = 32768.0,
    [string]$FreedListenIp = '0.0.0.0',
    [int]$FreedPort = 10244,
    [switch]$MqttEnable,
    [switch]$MqttDisable,
    [ValidateSet('local', 'lab-a', 'lab-b', 'lab-c', 'custom')]
    [string]$TelemetryServer = 'local',
    [switch]$ListTelemetryServers,
    [string]$MqttHost = '127.0.0.1',
    [int]$MqttPort = 1883,
    [string]$MqttTopicPrefix = 'ndi/telemetry',
    [switch]$BoardPoseStreamEnable,
    [switch]$BoardPoseStreamDisable,
    [string]$BoardPoseStreamHost = '0.0.0.0',
    [int]$BoardPoseStreamPort = 9102,
    [double]$BoardPoseStreamHz = 50.0,
    [switch]$TelemetryRecordStartEnabled,
    [switch]$TelemetryRecordStartDisabled,
    # Detector tuning for robustness (distance, soft focus, lighting)
    # Lower quad decimation (1.0-1.5) detects smaller/distant tags; higher (3.0+) is faster
    [double]$AprilTagQuadDecimate = 2.0,
    # Gaussian blur sigma before edge detection; higher (0.5-1.5) improves soft-focus tolerance
    [double]$AprilTagQuadSigma = 0.0,
    # Minimum adaptive threshold window size (must be odd); controls sensitivity to local lighting
    [int]$AdaptiveThreshWinSizeMin = 3,
    # Maximum adaptive threshold window size (must be odd); larger tolerates bigger lighting gradients
    [int]$AdaptiveThreshWinSizeMax = 23,
    # Corner refinement method default is APRILTAG (3): 0=NONE, 1=SUBPIX, 2=CONTOUR, 3=APRILTAG
    $CornerRefinementMethod = 3,
    # Minimum marker perimeter as fraction of image diagonal; lower (0.01-0.02) catches smaller tags
    [double]$MinMarkerPerimeterRate = 0.03,
    # Hamming distance acceptance threshold (0.0-1.0); higher tolerates more bit errors
    [double]$ErrorCorrectionRate = 0.6,
    # Minimum pixel value difference to detect edge; higher (10-20) reduces noise sensitivity
    [int]$AprilTagMinWhiteBlackDiff = 5,
    # Enable board-geometry-aware marker refinement (recovers markers missed by initial detection)
    [switch]$EnableBoardRefinement
)

$ErrorActionPreference = 'Stop'
$repoRoot = $PSScriptRoot

function Get-TelemetryServerProfiles {
    $profiles = [ordered]@{}

    # Edit these hosts once and then select targets via -TelemetryServer.
    $profiles['local'] = [ordered]@{
        Host = '127.0.0.1'
        Port = 1883
        TopicPrefix = 'ndi/telemetry'
    }
    $profiles['lab-a'] = [ordered]@{
        Host = '192.168.10.101'
        Port = 1883
        TopicPrefix = 'ndi/telemetry'
    }
    $profiles['lab-b'] = [ordered]@{
        Host = '192.168.10.102'
        Port = 1883
        TopicPrefix = 'ndi/telemetry'
    }
    $profiles['lab-c'] = [ordered]@{
        Host = '192.168.10.103'
        Port = 1883
        TopicPrefix = 'ndi/telemetry'
    }

    return $profiles
}

function Test-TcpEndpoint {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TargetHost,
        [Parameter(Mandatory = $true)]
        [int]$Port,
        [int]$TimeoutMs = 1200
    )

    $client = [System.Net.Sockets.TcpClient]::new()
    try {
        $iar = $client.BeginConnect($TargetHost, $Port, $null, $null)
        if (-not $iar.AsyncWaitHandle.WaitOne($TimeoutMs)) {
            return $false
        }
        $client.EndConnect($iar)
        return $true
    }
    catch {
        return $false
    }
    finally {
        $client.Close()
    }
}

function Get-WslPrimaryIp {
    if (-not (Get-Command wsl -ErrorAction SilentlyContinue)) {
        return $null
    }

    try {
        $raw = (& wsl -e bash -lc "hostname -I" | Out-String).Trim()
        if ([string]::IsNullOrWhiteSpace($raw)) {
            return $null
        }

        $tokens = @($raw -split '\s+') | Where-Object { $_ -match '^\d+\.\d+\.\d+\.\d+$' }
        if ($tokens.Count -gt 0) {
            return [string]$tokens[0]
        }
    }
    catch {
        # Best-effort fallback only.
    }

    return $null
}

function Get-BoardCatalog {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RepoRoot
    )

    $boardsDir = Join-Path $RepoRoot 'boards'
    $catalog = @{}

    if (-not (Test-Path $boardsDir)) {
        return $catalog
    }

    $boardFiles = Get-ChildItem -Path $boardsDir -Filter '*.json' -File | Sort-Object Name
    foreach ($file in $boardFiles) {
        $baseName = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
        $displayName = if ($baseName -match '^(?i)board[_-](.+)$') { $Matches[1] } else { $baseName }
        $aliases = @(
            $displayName,
            $baseName,
            $file.Name
        )

        foreach ($alias in $aliases) {
            if ([string]::IsNullOrWhiteSpace($alias)) {
                continue
            }
            $key = $alias.ToLowerInvariant()
            if (-not $catalog.ContainsKey($key)) {
                $catalog[$key] = [ordered]@{
                    Name = $displayName
                    Path = $file.FullName
                    RelativePath = (Join-Path 'boards' $file.Name)
                }
            }
        }
    }

    return $catalog
}

function Resolve-BoardJsonPaths {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RepoRoot,
        [Parameter(Mandatory = $true)]
        [hashtable]$Catalog,
        [string]$BoardName,
        [string]$BoardJson
    )

    $resolved = [System.Collections.Generic.List[string]]::new()

    function Add-IfValidPath {
        param([string]$CandidatePath)
        if (Test-Path $CandidatePath) {
            if (-not $resolved.Contains($CandidatePath)) {
                $resolved.Add($CandidatePath)
            }
        }
        else {
            Write-Warning "Board JSON not found at '$CandidatePath'; skipping."
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($BoardJson)) {
        $items = @($BoardJson -split ',') | ForEach-Object { $_.Trim() } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
        foreach ($item in $items) {
            $root = [System.IO.Path]::GetPathRoot($item)
            $isDriveOrUncRooted = [System.IO.Path]::IsPathRooted($item) -and $root -and $root.Length -gt 1
            $candidate = if ($isDriveOrUncRooted) {
                $item
            }
            else {
                Join-Path $RepoRoot ($item.TrimStart([char[]]"\\/"))
            }
            Add-IfValidPath -CandidatePath $candidate
        }
        return $resolved.ToArray()
    }

    if ([string]::IsNullOrWhiteSpace($BoardName)) {
        return $resolved.ToArray()
    }

    $names = @($BoardName -split ',') | ForEach-Object { $_.Trim() } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    foreach ($name in $names) {
        $key = $name.ToLowerInvariant()
        if ($Catalog.ContainsKey($key)) {
            Add-IfValidPath -CandidatePath ([string]$Catalog[$key].Path)
        }
        else {
            Write-Warning "Board name '$name' not found; skipping. Use -ListBoards to view available boards."
        }
    }

    return $resolved.ToArray()
}

function Resolve-FfmpegExecutable {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ConfiguredBin
    )

    $candidate = $ConfiguredBin
    if ([string]::IsNullOrWhiteSpace($candidate)) {
        $candidate = 'ffmpeg'
    }

    if ([System.IO.Path]::IsPathRooted($candidate)) {
        if (Test-Path $candidate) {
            return $candidate
        }
        return $ConfiguredBin
    }

    if ($candidate.Contains('\') -or $candidate.Contains('/')) {
        $resolvedRelative = Join-Path $repoRoot $candidate
        if (Test-Path $resolvedRelative) {
            return $resolvedRelative
        }
    }

    try {
        $cmd = Get-Command $candidate -ErrorAction Stop | Select-Object -First 1
        if ($cmd -and $cmd.Source -and (Test-Path $cmd.Source)) {
            return [string]$cmd.Source
        }
    }
    catch {
        # Continue with fallback probing below.
    }

    if ($candidate -in @('ffmpeg', 'ffmpeg.exe')) {
        $wingetPackagesRoot = Join-Path $env:LOCALAPPDATA 'Microsoft\WinGet\Packages'
        if (Test-Path $wingetPackagesRoot) {
            $found = Get-ChildItem -Path $wingetPackagesRoot -Filter 'ffmpeg.exe' -Recurse -ErrorAction SilentlyContinue |
                Where-Object { $_.FullName -match 'Gyan\.FFmpeg_' -and $_.FullName -match '\\bin\\ffmpeg\.exe$' } |
                Sort-Object LastWriteTime -Descending |
                Select-Object -First 1
            if ($found) {
                return [string]$found.FullName
            }
        }

        $windowsAppsAlias = Join-Path $env:LOCALAPPDATA 'Microsoft\WindowsApps\ffmpeg.exe'
        if (Test-Path $windowsAppsAlias) {
            return $windowsAppsAlias
        }
    }

    return $ConfiguredBin
}

function Test-NdiImport {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PythonExe
    )

    if (-not (Test-Path $PythonExe)) {
        return $false
    }

    & $PythonExe -c "import NDIlib" *> $null
    return ($LASTEXITCODE -eq 0)
}

function Get-RecommendAnalysisWorkers {
    $physical = $null
    $logical = $null

    try {
        $cpuInfo = Get-CimInstance Win32_Processor
        if ($cpuInfo) {
            $physical = [int](($cpuInfo | Measure-Object -Property NumberOfCores -Sum).Sum)
            $logical = [int](($cpuInfo | Measure-Object -Property NumberOfLogicalProcessors -Sum).Sum)
        }
    }
    catch {
        # Fall back below.
    }

    if (-not $logical -or $logical -le 0) {
        $logical = [int]([Environment]::ProcessorCount)
    }
    if (-not $physical -or $physical -le 0) {
        $physical = [int][Math]::Max(1, [Math]::Floor($logical / 2))
    }

    # Keep detection workers on physical cores and leave some headroom for decode/render.
    $recommended = [int][Math]::Max(2, [Math]::Min(6, $physical - 2))
    return @{
        Physical = $physical
        Logical = $logical
        Recommended = $recommended
    }
}

$pythonCandidates = [System.Collections.Generic.List[string]]::new()

if ($env:VIRTUAL_ENV) {
    $activePython = Join-Path $env:VIRTUAL_ENV "Scripts\python.exe"
    if (Test-Path $activePython) {
        $pythonCandidates.Add($activePython)
    }
}

$defaultPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
$venvPython = Join-Path $repoRoot "venv\Scripts\python.exe"

foreach ($candidate in @($defaultPython, $venvPython)) {
    if ((Test-Path $candidate) -and (-not $pythonCandidates.Contains($candidate))) {
        $pythonCandidates.Add($candidate)
    }
}

$python = $null
foreach ($candidate in $pythonCandidates) {
    if (Test-NdiImport -PythonExe $candidate) {
        $python = $candidate
        break
    }
}

if (-not $python) {
    $checked = if ($pythonCandidates.Count -gt 0) {
        ($pythonCandidates | ForEach-Object { "  - $_" }) -join [Environment]::NewLine
    }
    else {
        "  - no candidate interpreters found"
    }

    Write-Error (
        "No usable project Python interpreter could import NDIlib." + [Environment]::NewLine +
        "Checked:" + [Environment]::NewLine +
        $checked + [Environment]::NewLine +
        "Activate the correct venv first, or install ndi-python into one of the above interpreters."
    )
}

$scriptPath = Join-Path $repoRoot "ndi_hx3_gpu_preview_with_apriltag.py"
if (-not (Test-Path $scriptPath)) {
    Write-Error "Script not found at $scriptPath"
}

$effectiveRawRecordOutputDir = if ([System.IO.Path]::IsPathRooted($RawRecordOutputDir)) {
    $RawRecordOutputDir
}
else {
    Join-Path $repoRoot $RawRecordOutputDir
}

$effectiveRawRecordFfmpegBin = Resolve-FfmpegExecutable -ConfiguredBin $RawRecordFfmpegBin
if ($RawRecordBackend -in @('ffmpeg', 'auto')) {
    if ($effectiveRawRecordFfmpegBin -ne $RawRecordFfmpegBin) {
        Write-Host "Resolved ffmpeg binary: $effectiveRawRecordFfmpegBin"
    }
    elseif (-not (Test-Path $effectiveRawRecordFfmpegBin) -and -not (Get-Command $effectiveRawRecordFfmpegBin -ErrorAction SilentlyContinue)) {
        Write-Warning "Could not resolve ffmpeg binary '$RawRecordFfmpegBin'. Raw recorder may fail to start with backend '$RawRecordBackend'."
    }
}

$telemetryProfiles = Get-TelemetryServerProfiles
if ($ListTelemetryServers.IsPresent) {
    Write-Host 'Telemetry server profiles:'
    foreach ($name in $telemetryProfiles.Keys) {
        $profile = $telemetryProfiles[$name]
        Write-Host ("  - {0}: {1}:{2} prefix={3}" -f $name, $profile.Host, $profile.Port, $profile.TopicPrefix)
    }
    Write-Host "  - custom: use -MqttHost/-MqttPort/-MqttTopicPrefix directly"
    exit 0
}

$mqttHostProvided = $PSBoundParameters.ContainsKey('MqttHost')
$mqttPortProvided = $PSBoundParameters.ContainsKey('MqttPort')
$mqttTopicProvided = $PSBoundParameters.ContainsKey('MqttTopicPrefix')

$effectiveMqttHost = $MqttHost
$effectiveMqttPort = $MqttPort
$effectiveMqttTopicPrefix = $MqttTopicPrefix

if ($TelemetryServer -ne 'custom') {
    if (-not $telemetryProfiles.Contains($TelemetryServer)) {
        Write-Error "Unknown telemetry server profile '$TelemetryServer'. Use -ListTelemetryServers to view options."
    }

    $selected = $telemetryProfiles[$TelemetryServer]
    if (-not $mqttHostProvided) {
        $effectiveMqttHost = [string]$selected.Host
    }
    if (-not $mqttPortProvided) {
        $effectiveMqttPort = [int]$selected.Port
    }
    if (-not $mqttTopicProvided) {
        $effectiveMqttTopicPrefix = [string]$selected.TopicPrefix
    }

    if (
        $TelemetryServer -eq 'local' -and
        -not $mqttHostProvided -and
        $effectiveMqttHost -eq '127.0.0.1' -and
        -not (Test-TcpEndpoint -TargetHost $effectiveMqttHost -Port $effectiveMqttPort)
    ) {
        $wslIp = Get-WslPrimaryIp
        if ($wslIp -and (Test-TcpEndpoint -TargetHost $wslIp -Port $effectiveMqttPort)) {
            Write-Warning "Local MQTT broker not reachable at 127.0.0.1:$effectiveMqttPort. Using WSL IP fallback: ${wslIp}:$effectiveMqttPort"
            $effectiveMqttHost = $wslIp
        }
        else {
            Write-Warning "Local MQTT broker not reachable at 127.0.0.1:$effectiveMqttPort and no reachable WSL fallback IP was found."
        }
    }

    Write-Host ("TelemetryServer '{0}' selected -> {1}:{2} prefix={3}" -f $TelemetryServer, $effectiveMqttHost, $effectiveMqttPort, $effectiveMqttTopicPrefix)
}

$boardCatalog = Get-BoardCatalog -RepoRoot $repoRoot
if ($ListBoards.IsPresent) {
    $boardsDir = Join-Path $repoRoot 'boards'
    $boardFiles = @()
    if (Test-Path $boardsDir) {
        $boardFiles = Get-ChildItem -Path $boardsDir -Filter '*.json' -File | Sort-Object Name
    }

    if (-not $boardFiles -or $boardFiles.Count -eq 0) {
        Write-Host 'No board definitions found in .\boards'
        exit 0
    }

    Write-Host 'Available board definitions:'
    foreach ($file in $boardFiles) {
        $baseName = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
        $displayName = if ($baseName -match '^(?i)board[_-](.+)$') { $Matches[1] } else { $baseName }
        $rel = Join-Path 'boards' $file.Name
        Write-Host "  - $displayName ($rel)"
    }
    exit 0
}

$cpuTuning = Get-RecommendAnalysisWorkers
$effectiveAnalysisWorkers = $AnalysisWorkers
if ($effectiveAnalysisWorkers -le 0) {
    $effectiveAnalysisWorkers = $cpuTuning.Recommended
    Write-Host "AnalysisWorkers auto-selected: $effectiveAnalysisWorkers (physical=$($cpuTuning.Physical), logical=$($cpuTuning.Logical))"
}
elseif ($effectiveAnalysisWorkers -gt $cpuTuning.Physical) {
    Write-Warning "AnalysisWorkers=$effectiveAnalysisWorkers exceeds physical cores ($($cpuTuning.Physical)); this can reduce stability/FPS on this workload."
}

$pyArgs = @($scriptPath, "--discover-timeout", "$DiscoverTimeout", "--dicts", $Dicts)

if ($List.IsPresent) {
    $pyArgs += "--list"
}
else {
    $pyArgs += @("--source-hint", $SourceHint)
}

if ($NoDisplay.IsPresent) {
    $pyArgs += "--no-display"
}

$boardJsonPaths = Resolve-BoardJsonPaths -RepoRoot $repoRoot -Catalog $boardCatalog -BoardName $BoardName -BoardJson $BoardJson
if ($boardJsonPaths -and $boardJsonPaths.Count -gt 0) {
    foreach ($boardPath in $boardJsonPaths) {
        Write-Host "Using board definition: $boardPath"
        $pyArgs += @("--board-json", $boardPath)
    }
}

if (-not [string]::IsNullOrWhiteSpace($TagSizeMapJson)) {
    $candidate = if ([System.IO.Path]::IsPathRooted($TagSizeMapJson)) {
        $TagSizeMapJson
    }
    else {
        Join-Path $repoRoot $TagSizeMapJson
    }
    if (-not (Test-Path $candidate)) {
        Write-Error "Tag size map JSON not found at $candidate"
    }
    Write-Host "Using tag size map: $candidate"
    $pyArgs += @("--tag-size-map-json", $candidate)
}

# Show timestamp by default
$pyArgs += "--show-timestamp"

$pyArgs += @(
    "--gpu-index", "$GpuIndex",
    "--telemetry-interval", "$TelemetryInterval",
    "--focal-length", "$FocalLength",
    "--tag-size-mm", "$TagSizeMm",
    "--analysis-workers", "$effectiveAnalysisWorkers",
    "--display-fps", "$DisplayFps",
    "--display-scale", "$DisplayScale",
    "--display-prep-oversample", "$DisplayPrepOversample",
    "--display-delay-frames", "$DisplayDelayFrames",
    "--sync-timeout-ms", "$SyncTimeoutMs",
    "--raw-record-output-dir", $effectiveRawRecordOutputDir,
    "--raw-record-backend", $RawRecordBackend,
    "--raw-record-ffmpeg-bin", $effectiveRawRecordFfmpegBin,
    "--raw-record-ffmpeg-encoder", $RawRecordFfmpegEncoder,
    "--raw-record-ffmpeg-preset", $RawRecordFfmpegPreset,
    "--freed-angle-scale", "$FreedAngleScale",
    "--freed-listen-ip", $FreedListenIp,
    "--freed-port", "$FreedPort",
    "--mqtt-host", $effectiveMqttHost,
    "--mqtt-port", "$effectiveMqttPort",
    "--mqtt-topic-prefix", $effectiveMqttTopicPrefix,
    "--board-pose-stream-host", $BoardPoseStreamHost,
    "--board-pose-stream-port", "$BoardPoseStreamPort",
    "--board-pose-stream-hz", "$BoardPoseStreamHz",
    # Detector tuning parameters
    "--april-tag-quad-decimate", "$AprilTagQuadDecimate",
    "--april-tag-quad-sigma", "$AprilTagQuadSigma",
    "--adaptive-thresh-win-size-min", "$AdaptiveThreshWinSizeMin",
    "--adaptive-thresh-win-size-max", "$AdaptiveThreshWinSizeMax",
    "--min-marker-perimeter-rate", "$MinMarkerPerimeterRate",
    "--error-correction-rate", "$ErrorCorrectionRate",
    "--april-tag-min-white-black-diff", "$AprilTagMinWhiteBlackDiff"
)

if ($null -ne $CornerRefinementMethod) {
    $pyArgs += @("--corner-refinement-method", "$CornerRefinementMethod")
}

if (-not $MqttDisable.IsPresent) {
    $pyArgs += "--mqtt-enable"
}

if (-not $BoardPoseStreamDisable.IsPresent) {
    $pyArgs += "--board-pose-stream-enable"
}

if ($TelemetryRecordStartDisabled.IsPresent) {
    $pyArgs += "--telemetry-record-start-disabled"
}

if ($EnableBoardRefinement.IsPresent) {
    $pyArgs += "--enable-board-refinement"
}

& $python @pyArgs
exit $LASTEXITCODE
