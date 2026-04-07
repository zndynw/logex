[CmdletBinding()]
param(
    [string]$Target = "x86_64-unknown-linux-gnu.2.17",
    [ValidateSet("debug", "release")]
    [string]$Profile = "release",
    [switch]$SkipUpx,
    [string]$UpxPath = "upx.exe"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Section {
    param([string]$Message)

    Write-Host ""
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Write-Detail {
    param([string]$Message)

    Write-Host "[INFO] $Message" -ForegroundColor DarkGray
}

function Resolve-ToolPath {
    param([Parameter(Mandatory = $true)][string]$Name)

    $command = Get-Command -Name $Name -ErrorAction SilentlyContinue
    if (-not $command) {
        throw "Required tool '$Name' was not found in PATH."
    }

    return $command.Source
}

function Resolve-ProjectMetadata {
    $metadataJson = & cargo metadata --no-deps --format-version 1
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to query cargo metadata."
    }

    $metadata = $metadataJson | ConvertFrom-Json
    $package = $metadata.packages | Select-Object -First 1
    if (-not $package) {
        throw "Unable to resolve package metadata from Cargo.toml."
    }

    [pscustomobject]@{
        Name            = $package.name
        TargetDirectory = $metadata.target_directory
    }
}

function Get-ProfileDirectoryName {
    param([Parameter(Mandatory = $true)][string]$SelectedProfile)

    if ($SelectedProfile -eq "release") {
        return "release"
    }

    return "debug"
}

function Get-TargetDirectoryName {
    param([Parameter(Mandatory = $true)][string]$SelectedTarget)

    if ($SelectedTarget.Contains(".")) {
        return ($SelectedTarget -split "\.")[0]
    }

    return $SelectedTarget
}

function Resolve-BinaryPath {
    param(
        [Parameter(Mandatory = $true)][string]$TargetDirectory,
        [Parameter(Mandatory = $true)][string]$TargetName,
        [Parameter(Mandatory = $true)][string]$ProfileDirectory,
        [Parameter(Mandatory = $true)][string]$PackageName
    )

    $targetDirName = Get-TargetDirectoryName -SelectedTarget $TargetName
    $candidate = Join-Path $TargetDirectory $targetDirName
    $candidate = Join-Path $candidate $ProfileDirectory
    $candidate = Join-Path $candidate $PackageName

    if (Test-Path -LiteralPath $candidate) {
        return $candidate
    }

    $candidateExe = "$candidate.exe"
    if (Test-Path -LiteralPath $candidateExe) {
        return $candidateExe
    }

    throw "Built artifact was not found. Expected '$candidate' or '$candidateExe'."
}

function Invoke-Build {
    param(
        [Parameter(Mandatory = $true)][string]$SelectedTarget,
        [Parameter(Mandatory = $true)][string]$SelectedProfile
    )

    $arguments = @("zigbuild", "--target", $SelectedTarget)
    if ($SelectedProfile -eq "release") {
        $arguments += "--release"
    }

    Write-Detail ("cargo " + ($arguments -join " "))
    & cargo @arguments

    if ($LASTEXITCODE -ne 0) {
        throw "cargo zigbuild failed with exit code $LASTEXITCODE."
    }
}

function Invoke-Upx {
    param(
        [Parameter(Mandatory = $true)][string]$ExecutablePath,
        [Parameter(Mandatory = $true)][string]$SelectedUpxPath
    )

    $resolvedUpx = Resolve-ToolPath -Name $SelectedUpxPath
    Write-Detail ("$resolvedUpx $ExecutablePath")
    & $resolvedUpx $ExecutablePath

    if ($LASTEXITCODE -ne 0) {
        throw "UPX compression failed with exit code $LASTEXITCODE."
    }
}

try {
    Write-Section "Checking toolchain"
    $null = Resolve-ToolPath -Name "cargo"
    $null = Resolve-ToolPath -Name "rustup"
    $null = Resolve-ToolPath -Name "zig"
    $null = Resolve-ToolPath -Name "cargo-zigbuild"

    $metadata = Resolve-ProjectMetadata
    $profileDirectory = Get-ProfileDirectoryName -SelectedProfile $Profile

    Write-Section "Building $($metadata.Name)"
    Write-Detail "Target: $Target"
    Write-Detail "Profile: $Profile"
    Invoke-Build -SelectedTarget $Target -SelectedProfile $Profile

    Write-Section "Resolving artifact"
    $binaryPath = Resolve-BinaryPath `
        -TargetDirectory $metadata.TargetDirectory `
        -TargetName $Target `
        -ProfileDirectory $profileDirectory `
        -PackageName $metadata.Name
    Write-Detail "Artifact: $binaryPath"

    if (-not $SkipUpx) {
        Write-Section "Compressing artifact"
        Invoke-Upx -ExecutablePath $binaryPath -SelectedUpxPath $UpxPath
    }
    else {
        Write-Section "Skipping UPX compression"
        Write-Detail "Artifact left uncompressed."
    }

    Write-Section "Build finished"
    Write-Host $binaryPath -ForegroundColor Green
    exit 0
}
catch {
    Write-Host ""
    Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
