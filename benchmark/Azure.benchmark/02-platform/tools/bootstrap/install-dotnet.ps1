#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Cross-platform .NET SDK installer. Single source of truth for the SDK
    channels installed on both Linux and Windows VMSS instances.

.DESCRIPTION
    Uses Microsoft's official dotnet-install script (dotnet-install.sh on
    Linux, dotnet-install.ps1 on Windows) to install the latest SDK for each
    channel listed below, then ensures `dotnet` is resolvable on PATH.
#>

$ErrorActionPreference = 'Stop'

# The one place SDK channels are declared for every OS.
$channels = @('8.0', '9.0', '10.0')

# Detect Windows: $IsWindows is $null under Windows PowerShell 5.1 (Windows-only).
$onWindows = if ($null -ne $IsWindows) { $IsWindows } else { $true }
$tempDir = [System.IO.Path]::GetTempPath()

if ($onWindows) {
    $installDir = 'C:\Program Files\dotnet'
    $installScript = Join-Path $tempDir 'dotnet-install.ps1'
    Remove-Item $installScript -Force -ErrorAction SilentlyContinue
    Invoke-WebRequest -Uri 'https://dot.net/v1/dotnet-install.ps1' -OutFile $installScript

    foreach ($channel in $channels) {
        Write-Host "Installing .NET SDK channel $channel (latest)"
        & $installScript -Channel $channel -InstallDir $installDir
    }

    # Ensure dotnet is on PATH for all users
    $machinePath = [Environment]::GetEnvironmentVariable('Path', 'Machine')
    if ($machinePath -notlike "*$installDir*") {
        [Environment]::SetEnvironmentVariable('Path', "$machinePath;$installDir", 'Machine')
    }
}
else {
    $installDir = '/usr/share/dotnet'
    $installScript = Join-Path $tempDir 'dotnet-install.sh'
    bash -c "rm -f '$installScript'"
    Invoke-WebRequest -Uri 'https://dot.net/v1/dotnet-install.sh' -OutFile $installScript
    bash -c "chmod +x '$installScript'"

    foreach ($channel in $channels) {
        Write-Host "Installing .NET SDK channel $channel (latest)"
        bash -c "'$installScript' --channel $channel --install-dir '$installDir' --no-path"
    }

    # Setup PATH via a stable symlink
    bash -c "ln -sf '$installDir/dotnet' /usr/bin/dotnet"
    bash -c "chmod +x '$installDir/dotnet'"
}
