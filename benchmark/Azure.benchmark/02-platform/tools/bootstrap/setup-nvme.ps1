#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Initialize blank Azure ephemeral NVMe disks or remount an existing volume.

.DESCRIPTION
    Usage: setup-nvme.ps1 [mount_path] [--if-present] [--fs=xfs|ext4] [--force]
    Multiple disks use RAID0. Existing filesystems are preserved unless --force
    explicitly requests reformatting an unmounted volume.

    Local NVMe disks are EPHEMERAL: contents are lost on VM deallocation.
    Scratch/benchmark storage only. Not persisted to /etc/fstab.
#>
param(
    [Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments
)

$ErrorActionPreference = 'Stop'

function Get-NvmeSignatures([string]$Device) {
    $output = & wipefs --json --no-act $Device
    if ($LASTEXITCODE -ne 0) { throw "Cannot inspect signatures on $Device." }
    @((($output -join "`n") | ConvertFrom-Json).signatures | ForEach-Object { $_.type })
}

function Assert-NvmeArray([string]$Device, [string[]]$Disks) {
    $layout = @(& lsblk --inverse --noheadings --raw --paths --output 'NAME,TYPE' $Device)
    if ($LASTEXITCODE -ne 0) { throw "Cannot inspect RAID members of $Device." }
    $members = @($layout | Where-Object { $_ -match '\sdisk$' } | ForEach-Object { ($_ -split '\s+')[0] })
    if ($layout[0] -notmatch '\sraid0$' -or
        $members.Count -ne $Disks.Count -or (Compare-Object $Disks $members)) {
        throw "$Device is not a RAID0 array of the discovered local NVMe disks."
    }
}

function Mount-NvmeStorage {
    param(
        [string]$MountPath,
        [ValidateSet('xfs', 'ext4')][string]$FileSystem = 'xfs',
        [switch]$IfPresent,
        [switch]$Force
    )

    $output = & lsblk --json --nodeps --output 'PATH,MODEL'
    if ($LASTEXITCODE -ne 0) { throw 'Cannot discover local NVMe disks.' }
    $disks = @((($output -join "`n") | ConvertFrom-Json).blockdevices |
        Where-Object { $_.model -match 'Microsoft NVMe Direct Disk' } |
        ForEach-Object { $_.path })
    if ($disks.Count -eq 0) {
        if ($IfPresent) {
            Write-Host 'No Azure local NVMe disks found; skipping optional NVMe setup.'
            return
        }
        throw 'No Azure local NVMe disks found.'
    }

    $target = if ($disks.Count -eq 1) { $disks[0] } else { '/dev/md/nvme0' }
    if ($disks.Count -gt 1 -and (Test-Path $target)) {
        Assert-NvmeArray $target $disks
    }

    $mountedSource = & findmnt --noheadings --raw --output SOURCE --mountpoint $MountPath
    if ($LASTEXITCODE -notin @(0, 1)) { throw "Cannot inspect mountpoint $MountPath." }
    if ($mountedSource) {
        $sourceDevice = & readlink -f $mountedSource
        if ($LASTEXITCODE -ne 0) { throw "Cannot resolve mounted device $mountedSource." }
        $targetDevice = & readlink -f $target
        if ($LASTEXITCODE -ne 0) { throw "Cannot resolve NVMe device $target." }
        if ($sourceDevice -ne $targetDevice) { throw "$MountPath is mounted from an unrelated device." }
        if ($Force) { throw '--force requires an unmounted NVMe volume. Stop applications and unmount it first.' }
        Write-Host "$MountPath is already mounted from $target; leaving it unchanged."
        return
    }

    $signatures = @{}
    foreach ($disk in $disks) {
        $mounts = & lsblk --noheadings --raw --output MOUNTPOINTS $disk
        if ($LASTEXITCODE -ne 0) { throw "Cannot inspect mounts on $disk." }
        if ($mounts -match '\S') { throw "$disk is in use by another mount." }
        $signatures[$disk] = @(Get-NvmeSignatures $disk)
    }

    if ($disks.Count -gt 1) {
        $signedDisks = @($disks | Where-Object { $signatures[$_].Count -gt 0 })
        if ($signedDisks.Count -gt 0) {
            $raidMembers = @($disks | Where-Object {
                $signatures[$_] -contains 'linux_raid_member' -and
                @($signatures[$_] | Where-Object { $_ -ne 'linux_raid_member' }).Count -eq 0
            })
            if ($raidMembers.Count -ne $disks.Count) {
                throw 'NVMe disks contain existing data or mixed signatures; refusing to create a RAID array.'
            }
            if (-not (Test-Path $target)) {
                & mdadm --assemble --run $target @disks
                if ($LASTEXITCODE -ne 0) { throw 'Cannot assemble the existing NVMe array.' }
            }
        } else {
            if (Test-Path $target) { throw "$target already exists; refusing to replace it." }
            & mdadm --create $target --level=0 "--raid-devices=$($disks.Count)" --run @disks
            if ($LASTEXITCODE -ne 0) { throw 'Cannot create the NVMe RAID0 array.' }
        }
        Assert-NvmeArray $target $disks
    }

    $types = @(Get-NvmeSignatures $target)
    if (@($types | Where-Object { $_ -notin @('xfs', 'ext4') }).Count -gt 0 -or $types.Count -gt 1) {
        throw "$target has unsupported signatures; refusing to format or mount it."
    }
    if ($types.Count -eq 0 -or $Force) {
        if ($FileSystem -eq 'ext4') {
            & mkfs.ext4 -F -m 0 $target
        } else {
            & mkfs.xfs -f $target
        }
        if ($LASTEXITCODE -ne 0) { throw "Cannot format $target as $FileSystem." }
    } else {
        Write-Host "Preserving the existing $($types[0]) filesystem on $target."
    }

    New-Item -ItemType Directory -Path $MountPath -Force | Out-Null
    & mount $target $MountPath
    if ($LASTEXITCODE -ne 0) { throw "Cannot mount $target at $MountPath." }
    & chown "${DEPLOY_USER}:${DEPLOY_USER}" $MountPath
    if ($LASTEXITCODE -ne 0) { throw "Cannot set ownership on $MountPath." }
    & df -h $MountPath
    if ($LASTEXITCODE -ne 0) { throw "Cannot inspect filesystem capacity at $MountPath." }
}

$configFile = '/opt/deploy-actions/config.env'
if (Test-Path $configFile) {
    Get-Content $configFile | ForEach-Object {
        if ($_ -match '^\s*([A-Z_]+)="?([^"]*)"?\s*$' -and $_ -notmatch '^\s*#') {
            Set-Variable -Name $Matches[1] -Value $Matches[2] -Scope Script
        }
    }
}
$mountPath = if ($NVME_DIR) { $NVME_DIR } else { '/mnt/nvme' }
if (-not $DEPLOY_USER) { $DEPLOY_USER = 'guser' }
$force = $false
$ifPresent = $false
$fsType = 'xfs'
foreach ($arg in $Arguments) {
    switch -Regex ($arg) {
        '^--force$' { $force = $true }
        '^--if-present$' { $ifPresent = $true }
        '^--fs=(xfs|ext4)$' { $fsType = $Matches[1] }
        '^/.+' { $mountPath = $arg }
        default { throw "Unknown argument: $arg" }
    }
}
Mount-NvmeStorage -MountPath $mountPath -FileSystem $fsType -IfPresent:$ifPresent -Force:$force
