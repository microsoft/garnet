#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Discover, RAID0-combine, format and mount local ephemeral NVMe disks.

.DESCRIPTION
    PowerShell port of setup-nvme.sh. Targets Linux VMSS instances (uses
    lsblk, mdadm, mkfs, mount). Usage: setup-nvme.ps1 [mount_path] [--force] [--fs=xfs|ext4]

    IMPORTANT: Local NVMe disks are EPHEMERAL - contents are LOST on deallocation.
    Scratch/benchmark storage only. Not persisted to /etc/fstab on purpose.
#>
param(
    [Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments
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

$mountPath = $NVME_DIR
$force = $false
$fsType = 'xfs'
foreach ($arg in $Arguments) {
    switch -Regex ($arg) {
        '^--force$' { $force = $true }
        '^--fs=(.+)$' { $fsType = $Matches[1] }
        '^/' { $mountPath = $arg }
        default { Write-Error "Unknown argument: $arg"; exit 1 }
    }
}
if (-not $mountPath) { $mountPath = '/mnt/nvme' }

Write-Host "==== Setting up local NVMe storage ===="
Write-Host "  Mount path : $mountPath"
Write-Host "  Filesystem : $fsType"

# Already mounted?
& mountpoint -q $mountPath 2>$null
if ($LASTEXITCODE -eq 0) {
    if (-not $force) {
        Write-Host "  $mountPath is already mounted; nothing to do (use --force to rebuild)."
        & df -h $mountPath
        exit 0
    }
    Write-Host "  --force specified: unmounting existing $mountPath"
    & umount $mountPath
}

# Discover local ephemeral NVMe disks by model string.
$disks = @(& lsblk -dno NAME, MODEL |
    Where-Object { $_ -match 'Microsoft NVMe Direct Disk' } |
    ForEach-Object { '/dev/' + (($_ -split '\s+') | Where-Object { $_ })[0] })

if ($disks.Count -eq 0) {
    Write-Error "  ERROR: No local NVMe (Microsoft NVMe Direct Disk) devices found."
    Write-Error "  This VM size may not have local NVMe storage. Current block devices:"
    & lsblk -o NAME, SIZE, MODEL, MOUNTPOINT
    exit 1
}

Write-Host "  Found $($disks.Count) local NVMe disk(s): $($disks -join ' ')"

# Safety: refuse any candidate disk that is currently mounted.
foreach ($d in $disks) {
    $mp = (& lsblk -no MOUNTPOINT $d) -join ''
    if ($mp.Trim()) {
        Write-Error "  ERROR: $d appears to be in use (has a mountpoint). Aborting for safety."
        exit 1
    }
}

& mkdir -p $mountPath

if ($disks.Count -eq 1) {
    $target = $disks[0]
    Write-Host "  Single disk -> formatting $target as $fsType"
}
else {
    $target = '/dev/md/nvme0'
    Write-Host "  $($disks.Count) disks -> creating RAID0 array at $target"
    # Tear down any stale array from a previous run before recreating.
    & mdadm --stop $target 2>$null
    $global:LASTEXITCODE = 0
    $diskList = $disks -join ' '
    & bash -c "yes | mdadm --create '$target' --level=0 --raid-devices=$($disks.Count) $diskList --force"
    if ($LASTEXITCODE -ne 0) { throw "mdadm --create failed" }
}

# Format
if ($fsType -eq 'ext4') {
    & mkfs.ext4 -F -m 0 $target
}
else {
    & mkfs.xfs -f $target
}
if ($LASTEXITCODE -ne 0) { throw "mkfs failed on $target" }

& mount $target $mountPath
if ($LASTEXITCODE -ne 0) { throw "mount failed for $target" }
& chown -R "${DEPLOY_USER}:${DEPLOY_USER}" $mountPath

Write-Host "==== Local NVMe ready at $mountPath ===="
& df -h $mountPath
