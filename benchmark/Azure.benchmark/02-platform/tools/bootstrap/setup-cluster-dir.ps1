#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Create per-port cluster data directories for redis/valkey/garnet.

.DESCRIPTION
    PowerShell port of setup-cluster-dir.sh. Targets Linux VMSS instances.
    Usage: setup-cluster-dir.ps1 <system> [nodes] [--ramdisk]
#>
param(
    [Parameter(Position = 0, Mandatory = $true)][string]$System,
    [Parameter(ValueFromRemainingArguments = $true)][string[]]$Rest
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

# Parse remaining args: --ramdisk flag and optional node count
$useRamdisk = $false
$numNodes = $null
foreach ($arg in $Rest) {
    if ($arg -eq '--ramdisk') { $useRamdisk = $true }
    else { $numNodes = $arg }
}
if (-not $numNodes) { $numNodes = (& nproc).Trim() }
$numNodes = [int]$numNodes
$basePort = [int]$BASE_PORT

switch -Regex ($System) {
    '^(redis|valkey)$' {
        $dir = "$env:HOME/valkey-cluster"
        & mkdir -p $dir
        for ($i = 0; $i -lt $numNodes; $i++) {
            $port = $basePort + $i
            & mkdir -p "$dir/$port"
            if ($useRamdisk) { & mkdir -p "$RAMDISK_DIR/valkey-cluster/$port" }
        }
        & chown -R "${DEPLOY_USER}:${DEPLOY_USER}" $dir
        $last = $basePort + $numNodes - 1
        if ($useRamdisk) {
            & chown -R "${DEPLOY_USER}:${DEPLOY_USER}" "$RAMDISK_DIR/valkey-cluster"
            Write-Host "Created $dir + $RAMDISK_DIR/valkey-cluster with $numNodes port folders (${basePort}-${last})"
        }
        else {
            Write-Host "Created $dir with $numNodes port folders (${basePort}-${last})"
        }
    }
    '^garnet$' {
        $dir = "$env:HOME/garnet-cluster"
        & mkdir -p $dir
        if ($useRamdisk) {
            & mkdir -p "$RAMDISK_DIR/garnet-cluster"
            & chown -R "${DEPLOY_USER}:${DEPLOY_USER}" "$RAMDISK_DIR/garnet-cluster"
        }
        & chown -R "${DEPLOY_USER}:${DEPLOY_USER}" $dir
        Write-Host "Created $dir (single instance, multi-threaded)"
    }
    default {
        Write-Host "Unknown system: $System (use redis, valkey, or garnet)"
        exit 1
    }
}
