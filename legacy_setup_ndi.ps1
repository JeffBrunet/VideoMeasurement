param(
    [string]$PythonVersion = '3.14',
    [string]$VenvName = '.venv'
)

$ErrorActionPreference = 'Stop'
$repoRoot = $PSScriptRoot
if (-not $repoRoot) {
    $repoRoot = (Get-Location).Path
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

function Get-NdiSdkDir {
        $candidates = [System.Collections.Generic.List[string]]::new()

        if ($env:NDI_SDK_DIR) {
                $candidates.Add($env:NDI_SDK_DIR)
        }

        foreach ($candidate in @(
                'D:\Program Files\NDI\NDI 6 Advanced SDK',
                'E:\Program Files\NDI\NDI 6 Advanced SDK',
                'C:\Program Files\NDI\NDI 6 Advanced SDK',
                'E:\Program Files\NDI\NDI 6 SDK',
                'C:\Program Files\NDI\NDI 6 SDK',
                'C:\Program Files\NDI\NDI 5 SDK'
        )) {
                if (-not $candidates.Contains($candidate)) {
                        $candidates.Add($candidate)
                }
        }

        foreach ($candidate in $candidates) {
                if (
                        (Test-Path (Join-Path $candidate 'Include\Processing.NDI.Lib.h')) -and
                        (Test-Path (Join-Path $candidate 'Lib\x64'))
                ) {
                        return $candidate
                }
        }

        throw (
                'NDI SDK not found. Set NDI_SDK_DIR or install the SDK in a standard location.' + [Environment]::NewLine +
                'Checked:' + [Environment]::NewLine +
                ($candidates | ForEach-Object { "  - $_" } | Out-String)
        )
}

function Get-Pybind11CmakeDir {
        param(
                [Parameter(Mandatory = $true)]
                [string]$PythonExe
        )

        $pybind11Dir = & $PythonExe -c "import pathlib, pybind11; print((pathlib.Path(pybind11.__file__).resolve().parent / 'share' / 'cmake' / 'pybind11'))"
        if ($LASTEXITCODE -ne 0 -or -not $pybind11Dir) {
                throw 'Unable to resolve pybind11 CMake directory.'
        }

        $resolved = ($pybind11Dir | Select-Object -Last 1).Trim()
        if (-not (Test-Path $resolved)) {
                throw "pybind11 CMake directory was not found at $resolved"
        }

        return $resolved
}

function Expand-NdiSourceArchive {
        param(
                [Parameter(Mandatory = $true)]
                [string]$ArchivePath,
                [Parameter(Mandatory = $true)]
                [string]$WorkRoot
        )

        if (Test-Path $WorkRoot) {
                Remove-Item -Recurse -Force $WorkRoot
        }
        New-Item -ItemType Directory -Path $WorkRoot | Out-Null

        tar -xf $ArchivePath -C $WorkRoot
        if ($LASTEXITCODE -ne 0) {
                throw "Failed to extract $ArchivePath"
        }

        $sourceRoot = Get-ChildItem -Path $WorkRoot -Directory | Select-Object -First 1
        if (-not $sourceRoot) {
                throw "No source directory was extracted from $ArchivePath"
        }

        return $sourceRoot.FullName
}

function Update-NdiPythonSourceForLocalBuild {
        param(
                [Parameter(Mandatory = $true)]
                [string]$SourceRoot
        )

        $cmakeListsPath = Join-Path $SourceRoot 'CMakeLists.txt'
        $findNdiPath = Join-Path $SourceRoot 'cmake\Modules\FindNDI.cmake'

        $cmakeLists = @'
cmake_minimum_required(VERSION 3.17)

project(NDIlib VERSION 5.1.1)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(pybind11 CONFIG REQUIRED)
find_package(NDI REQUIRED)

file(GLOB INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/src/*.hpp)
file(GLOB SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)

source_group("" FILES ${SOURCES} ${INCLUDES})

pybind11_add_module(NDIlib SHARED ${INCLUDES} ${SOURCES})

target_include_directories(NDIlib PRIVATE ${NDI_INCLUDE_DIR})
target_link_directories(NDIlib PRIVATE ${NDI_LIBRARY_DIR})
target_link_libraries(NDIlib PRIVATE pybind11::module ${NDI_LIBS})

set_target_properties(NDIlib PROPERTIES SKIP_RPATH TRUE)
set_target_properties(NDIlib PROPERTIES BUILD_RPATH_USE_ORIGIN TRUE)
set_target_properties(NDIlib PROPERTIES SKIP_BUILD_RPATH FALSE)
set_target_properties(NDIlib PROPERTIES BUILD_WITH_INSTALL_RPATH TRUE)
set_target_properties(NDIlib PROPERTIES INSTALL_RPATH_USE_LINK_PATH FALSE)
set_target_properties(NDIlib PROPERTIES INSTALL_RPATH "@loader_path")

install(
    TARGETS NDIlib
    RUNTIME DESTINATION ${CMAKE_INSTALL_PREFIX}
    LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX}
)
if(WIN32)
    set(NDI_RUNTIME_FILES "${NDI_RUNTIME_DLL}")
    if(EXISTS "${NDI_LICENSE_FILE}")
        list(APPEND NDI_RUNTIME_FILES "${NDI_LICENSE_FILE}")
    endif()
    install(
        FILES ${NDI_RUNTIME_FILES}
        DESTINATION ${CMAKE_INSTALL_PREFIX}
    )
elseif(APPLE)
install(
    FILES
    "${NDI_LIBRARY_DIR}/libndi.dylib"
    "${NDI_LICENSE_DIR}/libndi_licenses.txt"
    DESTINATION ${CMAKE_INSTALL_PREFIX}
)
elseif(UNIX)
    file(GLOB DLL "${NDI_LIBRARY_DIR}/libndi.so*")
    install(
        FILES
        ${DLL}
        "${NDI_LICENSE_DIR}/libndi_licenses.txt"
        DESTINATION ${CMAKE_INSTALL_PREFIX}
    )
endif()
'@

        $findNdi = @'
if(WIN32)
    if(DEFINED ENV{NDI_SDK_DIR})
        set(NDI_DIR "$ENV{NDI_SDK_DIR}")
        string(REPLACE "\\" "/" NDI_DIR "${NDI_DIR}")
        set(NDI_INCLUDE_DIR "${NDI_DIR}/Include")
        set(NDI_LIBRARY_DIR "${NDI_DIR}/Lib/x64")
        set(NDI_LICENSE_DIR "${NDI_DIR}/Lib/x64")
        set(NDI_LICENSE_FILE "${NDI_DIR}/Bin/x64/Processing.NDI.Lib.Licenses.txt")

        if(EXISTS "${NDI_LIBRARY_DIR}/Processing.NDI.Lib.Advanced.x64.lib")
            set(NDI_LIBS "Processing.NDI.Lib.Advanced.x64")
            set(NDI_RUNTIME_DLL "${NDI_DIR}/Bin/x64/Processing.NDI.Lib.Advanced.x64.dll")
            set(NDI_FOUND TRUE)
        elseif(EXISTS "${NDI_LIBRARY_DIR}/Processing.NDI.Lib.x64.lib")
            set(NDI_LIBS "Processing.NDI.Lib.x64")
            set(NDI_RUNTIME_DLL "${NDI_DIR}/Bin/x64/Processing.NDI.Lib.x64.dll")
            set(NDI_FOUND TRUE)
        else()
            set(NDI_FOUND FALSE)
        endif()

        if(NOT EXISTS "${NDI_INCLUDE_DIR}/Processing.NDI.Lib.h")
            set(NDI_FOUND FALSE)
        endif()
        if(NOT EXISTS "${NDI_RUNTIME_DLL}")
            set(NDI_FOUND FALSE)
        endif()
    else()
        set(NDI_FOUND FALSE)
    endif()
elseif(APPLE)
    if(EXISTS "/Library/NDI SDK for Apple/include/Processing.NDI.Lib.h")
        set(NDI_FOUND TRUE)
        set(NDI_DIR "/Library/NDI SDK for Apple")
        set(NDI_INCLUDE_DIR "${NDI_DIR}/include")
        set(NDI_LIBRARY_DIR "${NDI_DIR}/lib/macOS")
        set(NDI_LICENSE_DIR "${NDI_DIR}/licenses")
        file(GLOB NDI_LIBS "${NDI_LIBRARY_DIR}/*.dylib")
    else()
        set(NDI_FOUND FALSE)
    endif()
elseif(UNIX)
    if(EXISTS "${NDI_SDK_DIR}/include/Processing.NDI.Lib.h")
        set(NDI_FOUND TRUE)
        set(NDI_DIR ${NDI_SDK_DIR})
        set(NDI_INCLUDE_DIR "${NDI_DIR}/include")
        set(NDI_LIBRARY_DIR "${NDI_DIR}/lib/x86_64-linux-gnu")
        set(NDI_LICENSE_DIR "${NDI_DIR}/licenses")
        set(NDI_LIBS "ndi")
    elseif(EXISTS "/usr/include/Processing.NDI.Lib.h")
        set(NDI_FOUND TRUE)
        set(NDI_DIR "/usr")
        set(NDI_INCLUDE_DIR "${NDI_DIR}/include")
        set(NDI_LIBRARY_DIR "${NDI_DIR}/lib")
        set(NDI_LICENSE_DIR "${NDI_DIR}/share/licenses/ndi-sdk")
        set(NDI_LIBS "ndi")
    else()
        set(NDI_FOUND FALSE)
    endif()
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(NDI DEFAULT_MSG NDI_DIR ${NDI_FOUND})
'@

        Set-Content -Path $cmakeListsPath -Value $cmakeLists
        Set-Content -Path $findNdiPath -Value $findNdi
}

Write-Host "Repo root: $repoRoot"

$venvPath = Join-Path $repoRoot $VenvName
$venvPython = Join-Path $venvPath 'Scripts\python.exe'
$requirements = Join-Path $repoRoot 'requirements.txt'
$wheelsDir = Join-Path $repoRoot 'wheels'
$localNdiSource = Join-Path $repoRoot '_build_ndi_python\src\ndi-python-5.1.1.1.tar.gz'
$localNdiWorkRoot = Join-Path $repoRoot '_build_ndi_python\work'

if (-not (Get-Command py -ErrorAction SilentlyContinue)) {
    throw "Python launcher 'py' was not found. Install Python from python.org and retry."
}

if (-not (Test-Path $requirements)) {
    throw "requirements.txt not found at $requirements"
}

if (-not (Test-Path $venvPython)) {
    Write-Host "Creating virtual environment: $venvPath"
    & py "-$PythonVersion" -m venv $venvPath
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create venv with py -$PythonVersion"
    }
}
else {
    Write-Host "Using existing virtual environment: $venvPath"
}

Write-Host 'Upgrading pip/setuptools/wheel...'
& $venvPython -m pip install --upgrade pip setuptools wheel
if ($LASTEXITCODE -ne 0) {
    throw 'Failed to upgrade pip/setuptools/wheel.'
}

Write-Host 'Installing Python requirements...'
& $venvPython -m pip install -r $requirements
if ($LASTEXITCODE -ne 0) {
    throw 'Failed to install requirements.txt'
}

Write-Host 'Ensuring pybind11 is installed for local ndi-python builds...'
& $venvPython -m pip install pybind11
if ($LASTEXITCODE -ne 0) {
    throw 'Failed to install pybind11.'
}

if (Test-Path $wheelsDir) {
    $ndiWheel = Get-ChildItem -Path $wheelsDir -Filter '*ndi*python*.whl' -File | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if ($ndiWheel) {
        Write-Host "Installing NDI wheel: $($ndiWheel.Name)"
        & $venvPython -m pip install --force-reinstall $ndiWheel.FullName
        if ($LASTEXITCODE -ne 0) {
            throw 'Failed to install NDI wheel.'
        }
    }
}

if (-not (Test-NdiImport -PythonExe $venvPython)) {
    if (-not (Test-Path $localNdiSource)) {
        throw (
            "Neither a local NDI wheel nor fallback source archive was found." + [Environment]::NewLine +
            "Checked:" + [Environment]::NewLine +
            "  - $wheelsDir" + [Environment]::NewLine +
            "  - $localNdiSource"
        )
    }

    $env:NDI_SDK_DIR = Get-NdiSdkDir
    $env:pybind11_DIR = Get-Pybind11CmakeDir -PythonExe $venvPython
    $patchedSourceRoot = Expand-NdiSourceArchive -ArchivePath $localNdiSource -WorkRoot $localNdiWorkRoot
    Update-NdiPythonSourceForLocalBuild -SourceRoot $patchedSourceRoot

    Write-Host "Installing NDIlib from patched local source: $patchedSourceRoot"
    Write-Host "Using NDI_SDK_DIR=$($env:NDI_SDK_DIR)"
    & $venvPython -m pip install --force-reinstall --no-build-isolation $patchedSourceRoot
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to install NDIlib from local source archive.'
    }
}

Write-Host 'Verifying NDIlib import...'
& $venvPython -c "import NDIlib; print('NDIlib OK')"
if ($LASTEXITCODE -ne 0) {
    throw 'NDIlib import failed after installation.'
}

Write-Host ''
Write-Host 'Setup complete.'
Write-Host "Run the app with: .\\run.ps1 -List"
