#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Set up a tmpfs ramdisk sized as a percentage of total VM memory.

.DESCRIPTION
    PowerShell port of setup-ramdisk.sh. Targets Linux VMSS instances (uses
    /proc/meminfo, mount, /etc/fstab). Usage: setup-ramdisk.ps1 [mountPath] [sizePercent]
#>
param(
    [Parameter(Position = 0)][string]$MountPath,
    [Parameter(Position = 1)][int]$SizePercent = 50
)

$ErrorActionPreference = 'Stop'

# --- Load shared config.env ---
$configFile = '/opt/deploy-actions/config.env'
if (Test-Path $configFile) {
    Get-Content $configFile | ForEach-Object {
        if ($_ -match '^\s*([A-Z_]+)="?([^"]*)"?\s*$' -and $_ -notmatch '^\s*#') {
            Set-Variable -Name $Matches[1] -Value $Matches[2] -Scope Script
        }
    }
}

if (-not $MountPath) { $MountPath = $RAMDISK_DIR }

$memLine = Get-Content /proc/meminfo | Where-Object { $_ -match '^MemTotal:\s+(\d+)' } | Select-Object -First 1
$null = $memLine -match '^MemTotal:\s+(\d+)'
$totalMemKb = [int]$Matches[1]
$ramdiskKb = [int]($totalMemKb * $SizePercent / 100)
$ramdiskMb = [int]($ramdiskKb / 1024)

Write-Host "==== Setting up ramdisk ===="
Write-Host "  Total memory: $([int]($totalMemKb / 1024)) MB"
Write-Host "  Ramdisk size: ${ramdiskMb} MB (${SizePercent}%)"
Write-Host "  Mount path:   ${MountPath}"

# Create mount point
& mkdir -p $MountPath

# Unmount if already mounted
& mountpoint -q $MountPath 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "  Ramdisk already mounted, remounting..."
    & umount $MountPath
}

# Mount tmpfs
& mount -t tmpfs -o "size=${ramdiskMb}m" tmpfs $MountPath
if ($LASTEXITCODE -ne 0) { throw "Failed to mount tmpfs at $MountPath" }

# Set ownership
& chown -R "${DEPLOY_USER}:${DEPLOY_USER}" $MountPath

# Add to fstab for persistence across reboots (idempotent)
$uid = (& id -u $DEPLOY_USER).Trim()
$gid = (& id -g $DEPLOY_USER).Trim()
$fstabEntry = "tmpfs ${MountPath} tmpfs size=${ramdiskMb}m,uid=${uid},gid=${gid} 0 0"
$fstab = Get-Content /etc/fstab -Raw -ErrorAction SilentlyContinue
if ($fstab -notmatch [regex]::Escape($MountPath)) {
    Add-Content -Path /etc/fstab -Value $fstabEntry
    Write-Host "  Added fstab entry for persistence"
}

Write-Host "==== Ramdisk ready at ${MountPath} (${ramdiskMb} MB) ===="
