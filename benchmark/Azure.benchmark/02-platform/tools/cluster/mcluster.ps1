#!/usr/bin/env pwsh
[CmdletBinding()]
<#
.SYNOPSIS
    Manage valkey/garnet cluster instances: start, stop, clean, or update configs.

.EXAMPLE
    mcluster.ps1 -Action start -System valkey -Conf config/valkey/valkey-cache.conf -Nodes 16
    mcluster.ps1 -Action start -System valkey -Conf config/valkey/valkey-cache.conf -Nodes 16 -Clean
    mcluster.ps1 -Action start -System garnet -Conf config/garnet/garnet-cache.conf -Nodes 1 -NoCluster
    mcluster.ps1 -Action stop -System valkey -Nodes 16
    mcluster.ps1 -Action stop
    mcluster.ps1 -Action clean -System valkey
    mcluster.ps1 -Action clean
    mcluster.ps1 -Action update -System garnet -Conf config/garnet/garnet-cache.conf
    mcluster.ps1 -Action update -System valkey -Conf config/valkey/valkey-cache.conf -Nodes 16 -NoCluster
    mcluster.ps1 -Action stage -System garnet -Conf config/garnet/garnet-cache.conf -Nodes 1 -Clean
#>
param(
    [ValidateSet("start","stop","update","clean","stage")][string]$Action,
    [string]$System,
    [Alias('Config')][string]$Conf,
    [string]$ConfContent,
    [string]$ConfName,
    [int]$Nodes = 0,
    [switch]$NoCluster,
    [switch]$Clean,
    [switch]$Help
)

if ($Help -or -not $Action) {
    Write-Host "Usage: mcluster.ps1 -Action <start|stop|update|clean|stage> [-System <valkey|garnet>] [-Conf <path>] [-Nodes <n>] [-NoCluster] [-Clean]"
    Write-Host ""
    Write-Host "Manage valkey/garnet cluster instances: start, stop, clean, or update configs."
    Write-Host ""
    Write-Host "Actions:"
    Write-Host "  start    Start cluster instances (requires -System, -Conf, -Nodes)"
    Write-Host "  stop     Stop running instances (optionally filter by -System, -Nodes)"
    Write-Host "  clean    Remove cluster directories (optionally filter by -System)"
    Write-Host "  update   Pull configs and regenerate instance configs (requires -System and -Conf)"
    Write-Host "  stage    Resolve configs and print the commands to start instances manually"
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  -System      Target system: valkey or garnet"
    Write-Host "  -Conf        Explicit config file path on this node (repo-relative or absolute)"
    Write-Host "  -ConfContent Base64 config content shipped from the caller (decoded to a temp file); highest precedence"
    Write-Host "  -ConfName    Leaf filename to use when writing -ConfContent (default: shipped.conf)"
    Write-Host "  -Nodes       Number of instances to manage"
    Write-Host "  -NoCluster   Disable cluster mode in generated configs"
    Write-Host "  -Clean       Remove cluster directory before starting"
    Write-Host "  -Help        Show this help message"
    return
}

$ErrorActionPreference = "Stop"

# Load config
$configFile = "/opt/deploy-actions/config.env"
if (Test-Path $configFile) {
    Get-Content $configFile | ForEach-Object {
        if ($_ -match '^\s*([A-Z_]+)="?([^"]*)"?\s*$' -and $_ -notmatch '^\s*#') {
            Set-Variable -Name $Matches[1] -Value $Matches[2] -Scope Script
        }
    }
}

# Defaults if config.env not loaded
if (-not $IFACE) { $IFACE = "eth1" }
if (-not $BASE_PORT) { $BASE_PORT = 7000 } else { $BASE_PORT = [int]$BASE_PORT }
if (-not $DEPLOY_USER) { $DEPLOY_USER = "guser" }
if (-not $RAMDISK_DIR) { $RAMDISK_DIR = "/mnt/ramdisk" }
if (-not $NVME_DIR) { $NVME_DIR = "/mnt/nvme" }

$RepoDir = "$HOME/tools"
$ClusterMode = if ($NoCluster) { "false" } else { "true" }

function Pull-Configs {
    if (Test-Path "$RepoDir/.git") {
        Write-Host "Pulling latest configs..."
        git -C $RepoDir pull --ff-only -q 2>$null
        if ($LASTEXITCODE -ne 0) { Write-Host "  WARNING: git pull failed, using cached configs" -ForegroundColor Yellow }
    }
}

function Get-Eth1Ip {
    $output = ip -4 addr show $IFACE 2>$null
    $inetLine = $output | Where-Object { $_ -match 'inet\s+([\d.]+)' } | Select-Object -First 1
    if ($inetLine -match 'inet\s+([\d.]+)') { return $Matches[1] }
    throw "ERROR: Could not determine $IFACE IP"
}

function Resolve-Config {
    param([string]$Sys, [int]$Count, [string]$ConfPath, [string]$ConfB64, [string]$ConfLeaf)

    # Precedence for the source configuration:
    #   1. -ConfContent : base64 file content shipped from the workstation
    #                     (decoded to a temp file; no git pull required).
    #   2. -Conf        : explicit path on this node (repo-relative or absolute).
    if ($ConfB64) {
        $tmpDir = "$HOME/.mcluster-conf"
        New-Item -ItemType Directory -Path $tmpDir -Force | Out-Null
        $leaf = if ($ConfLeaf) { $ConfLeaf } else { "shipped.conf" }
        $sourceFile = "$tmpDir/$leaf"
        $bytes = [Convert]::FromBase64String($ConfB64)
        [System.IO.File]::WriteAllBytes($sourceFile, $bytes)
        Write-Host "  Using shipped conf: $leaf ($($bytes.Length) bytes)" -ForegroundColor DarkGray
    } else {
        $sourceFile = if ([System.IO.Path]::IsPathRooted($ConfPath)) { $ConfPath } else { "$RepoDir/$ConfPath" }
    }
    if (-not (Test-Path $sourceFile -PathType Leaf)) {
        throw "ERROR: Configuration file not found: $sourceFile"
    }

    $eth1Ip = Get-Eth1Ip
    $clusterDir = if ($Sys -eq "garnet") { "$HOME/garnet-cluster" } else { "$HOME/valkey-cluster" }
    New-Item -ItemType Directory -Path $clusterDir -Force | Out-Null

    # Check if the configuration uses ramdisk and ensure directories exist
    $sourceContent = Get-Content $sourceFile -Raw
    $usesRamdisk = $sourceContent -match '/mnt/ramdisk|RAMDISK'
    $ramdiskDir = "$RAMDISK_DIR/$($Sys)-cluster"

    if ($usesRamdisk) {
        sudo mkdir -p $ramdiskDir
        sudo chown "$($DEPLOY_USER):$($DEPLOY_USER)" $ramdiskDir
    }

    # Check if the configuration uses local NVMe and ensure directories exist.
    # Requires setup-nvme.ps1 to have discovered + mounted the disk beforehand.
    $usesNvme = $sourceContent -match '/mnt/nvme|\$nvme|NVME'
    $nvmeDir = "$NVME_DIR/$($Sys)-cluster"

    if ($usesNvme) {
        if (-not (bash -c "mountpoint -q '$NVME_DIR' && echo ok") ) {
            throw "ERROR: Configuration uses local NVMe ($NVME_DIR) but nothing is mounted there. Run 'sudo /opt/deploy-actions/setup-nvme.ps1' first."
        }
        sudo mkdir -p $nvmeDir
        sudo chown "$($DEPLOY_USER):$($DEPLOY_USER)" $nvmeDir
    }

    for ($i = 0; $i -lt $Count; $i++) {
        $port = $BASE_PORT + $i
        $portDir = "$clusterDir/$port"
        New-Item -ItemType Directory -Path $portDir -Force | Out-Null

        # Create the ramdisk port directory when the configuration points there
        if ($usesRamdisk) {
            New-Item -ItemType Directory -Path "$ramdiskDir/$port" -Force | Out-Null
        }

        # Create the NVMe port directory when the configuration points there
        if ($usesNvme) {
            New-Item -ItemType Directory -Path "$nvmeDir/$port" -Force | Out-Null
        }

        $content = $sourceContent
        $content = $content -replace '\$ramdisk', $RAMDISK_DIR
        $content = $content -replace '\$nvme', $NVME_DIR
        $content = $content -replace '\$eth1', $eth1Ip
        $content = $content -replace '\$port', $port

        if ($Sys -eq "garnet") {
            $content = $content -replace '"EnableCluster":\s*true', "`"EnableCluster`": $ClusterMode"
            Set-Content -Path "$portDir/garnet.conf" -Value $content -NoNewline
        } else {
            # Handle cluster-enabled for valkey
            if ($content -match '(?m)^cluster-enabled') {
                $clusterVal = if ($ClusterMode -eq "true") { "cluster-enabled yes" } else { "cluster-enabled no" }
                $content = $content -replace '(?m)^cluster-enabled.*', $clusterVal
            } else {
                $clusterVal = if ($ClusterMode -eq "true") { "cluster-enabled yes" } else { "cluster-enabled no" }
                $content += "`n$clusterVal`n"
            }
            Set-Content -Path "$portDir/valkey.conf" -Value $content -NoNewline
        }
    }
    Write-Host "Resolved $Count config(s) from $(Split-Path $sourceFile -Leaf) (cluster=$ClusterMode) -> $clusterDir/" -ForegroundColor Green
}

function Start-Valkey {
    param([int]$Count)
    $clusterDir = "$HOME/valkey-cluster"

    Write-Host "Applying valkey network profile ($Count instances)..."
    sudo /opt/deploy-actions/setup-network.ps1 valkey $Count

    Write-Host "Starting $Count valkey-server instances (ports ${BASE_PORT}-$($BASE_PORT + $Count - 1))..."
    for ($i = 0; $i -lt $Count; $i++) {
        $port = $BASE_PORT + $i
        $dir = "$clusterDir/$port"
        $conf = "$dir/valkey.conf"
        if (-not (Test-Path $conf)) { throw "ERROR: $conf not found" }

        $running = bash -c "pgrep -f 'valkey-server.*:${port}'" 2>$null
        if ($running) { Write-Host "  Port ${port}: already running (skipped)" -ForegroundColor DarkGray; continue }

        Set-Location $dir
        bash -c "valkey-server '$conf' --daemonize yes --logfile '$dir/valkey.log' --pidfile '$dir/valkey.pid'"
        Write-Host "  Port ${port}: started" -ForegroundColor Cyan
    }
}

function Start-Garnet {
    param([int]$Count)
    $clusterDir = "$HOME/garnet-cluster"

    Write-Host "Applying garnet network profile..."
    sudo /opt/deploy-actions/setup-network.ps1 garnet $Count

    Write-Host "Starting $Count GarnetServer instance(s)..."
    for ($i = 0; $i -lt $Count; $i++) {
        $port = $BASE_PORT + $i
        $dir = "$clusterDir/$port"
        $conf = "$dir/garnet.conf"
        if (-not (Test-Path $conf)) { throw "ERROR: $conf not found" }

        $running = bash -c "pgrep -f 'GarnetServer.*/${port}/garnet.conf'" 2>$null
        if ($running) { Write-Host "  Port ${port}: already running (skipped)" -ForegroundColor DarkGray; continue }

        Set-Location $dir
        bash -c "nohup GarnetServer --config-import-path=$conf > $dir/garnet.log 2> $dir/garnet.err &"
        Write-Host "  Port ${port}: started" -ForegroundColor Cyan
    }
}

function Clean-System {
    param([string]$Sys)
    if ($Sys -eq "garnet" -or [string]::IsNullOrEmpty($Sys)) {
        $garnetDir = "$HOME/garnet-cluster"
        if (Test-Path $garnetDir) {
            Remove-Item -Recurse -Force $garnetDir
            Write-Host "  Removed $garnetDir"
        } else {
            Write-Host "  $garnetDir does not exist (skipped)" -ForegroundColor DarkGray
        }
        $ramdiskDir = "$RAMDISK_DIR/garnet-cluster"
        if (Test-Path $ramdiskDir) {
            sudo rm -rf $ramdiskDir
            Write-Host "  Removed $ramdiskDir"
        }
        $nvmeDir = "$NVME_DIR/garnet-cluster"
        if (Test-Path $nvmeDir) {
            sudo rm -rf $nvmeDir
            Write-Host "  Removed $nvmeDir"
        }
    }
    if ($Sys -eq "valkey" -or [string]::IsNullOrEmpty($Sys)) {
        $valkeyDir = "$HOME/valkey-cluster"
        if (Test-Path $valkeyDir) {
            Remove-Item -Recurse -Force $valkeyDir
            Write-Host "  Removed $valkeyDir"
        } else {
            Write-Host "  $valkeyDir does not exist (skipped)" -ForegroundColor DarkGray
        }
        $ramdiskDir = "$RAMDISK_DIR/valkey-cluster"
        if (Test-Path $ramdiskDir) {
            sudo rm -rf $ramdiskDir
            Write-Host "  Removed $ramdiskDir"
        }
        $nvmeDir = "$NVME_DIR/valkey-cluster"
        if (Test-Path $nvmeDir) {
            sudo rm -rf $nvmeDir
            Write-Host "  Removed $nvmeDir"
        }
    }
}

function Stop-System {
    param([string]$Sys, [int]$Count)
    if ($Sys -eq "garnet") {
        if ($Count -gt 0) {
            for ($i = 0; $i -lt $Count; $i++) {
                $port = $BASE_PORT + $i
                $raw = bash -c "pgrep -f 'GarnetServer.*/${port}/garnet.conf'" 2>$null
                $procId = if ($raw) { $raw.Trim() } else { "" }
                if ($procId) { bash -c "kill $procId"; Write-Host "  Garnet port ${port}: stopped (pid $procId)" }
                else { Write-Host "  Garnet port ${port}: not running" -ForegroundColor DarkGray }
            }
        } else {
            $raw = bash -c "pgrep -f GarnetServer" 2>$null
            $procIds = if ($raw) { $raw.Trim() -split "`n" | Where-Object { $_ } } else { @() }
            if ($procIds) {
                foreach ($p in $procIds) {
                    $portMatch = bash -c "ps -p $p -o args= 2>/dev/null" | Select-String -Pattern '/(\d+)/garnet\.conf'
                    $port = if ($portMatch) { $portMatch.Matches[0].Groups[1].Value } else { "?" }
                    bash -c "kill $p"
                    Write-Host "  GarnetServer port ${port}: stopped (pid $p)"
                }
            } else {
                Write-Host "  No GarnetServer running." -ForegroundColor DarkGray
            }
        }
    } else {
        if ($Count -gt 0) {
            for ($i = 0; $i -lt $Count; $i++) {
                $port = $BASE_PORT + $i
                $raw = bash -c "pgrep -f 'valkey-server.*:${port}'" 2>$null
                $procId = if ($raw) { $raw.Trim() } else { "" }
                if ($procId) { bash -c "kill $procId"; Write-Host "  Valkey port ${port}: stopped (pid $procId)" }
                else { Write-Host "  Valkey port ${port}: not running" -ForegroundColor DarkGray }
            }
        } else {
            $raw = bash -c "pgrep -f valkey-server" 2>$null
            $procIds = if ($raw) { $raw.Trim() -split "`n" | Where-Object { $_ } } else { @() }
            if ($procIds) {
                foreach ($p in $procIds) {
                    $portMatch = bash -c "ps -p $p -o args= 2>/dev/null" | Select-String -Pattern ':(\d+)'
                    $port = if ($portMatch) { $portMatch.Matches[0].Groups[1].Value } else { "?" }
                    bash -c "kill $p"
                    Write-Host "  valkey-server port ${port}: stopped (pid $p)"
                }
            } else {
                Write-Host "  No valkey-server running." -ForegroundColor DarkGray
            }
        }
    }
}

# Main logic
switch ($Action) {
    "start" {
        if (-not $System) { throw "Usage: mcluster.ps1 -Action start -System <system> (-Conf <path> | -ConfContent <b64>) -Nodes <n>" }
        if (-not $Conf -and -not $ConfContent) { throw "Usage: mcluster.ps1 -Action start -System <system> (-Conf <path> | -ConfContent <b64>) -Nodes <n>" }
        if ($Conf -and $ConfContent) { throw "ERROR: -Conf and -ConfContent are mutually exclusive; specify only one." }
        if ($Nodes -le 0) { throw "Usage: mcluster.ps1 -Action start -System <system> (-Conf <path> | -ConfContent <b64>) -Nodes <n>" }

        Write-Host "==== mcluster (start) ====" -ForegroundColor Cyan
        Write-Host "  System:   $System"
        if ($ConfContent) { Write-Host "  Conf:     $ConfName (shipped)" } else { Write-Host "  Conf:     $Conf" }
        Write-Host "  Nodes:    $Nodes"
        Write-Host "  Cluster:  $ClusterMode"
        Write-Host ""

        if ($Clean) {
            Write-Host "Cleaning $System cluster directory..."
            Clean-System -Sys $System
        }

        Pull-Configs
        Resolve-Config -Sys $System -Count $Nodes -ConfPath $Conf -ConfB64 $ConfContent -ConfLeaf $ConfName

        if ($System -eq "garnet") { Start-Garnet -Count $Nodes }
        else { Start-Valkey -Count $Nodes }
        Write-Host "Done." -ForegroundColor Green
    }

    "stop" {
        if (-not $System) {
            Write-Host "Stopping all instances..."
            Stop-System -Sys "valkey" -Count 0
            Stop-System -Sys "garnet" -Count 0
            Write-Host "Done." -ForegroundColor Green
        } else {
            Stop-System -Sys $System -Count $Nodes
        }
    }

    "clean" {
        Write-Host "Cleaning cluster directories..."
        Clean-System -Sys $System
        Write-Host "Done." -ForegroundColor Green
    }

    "update" {
        if (-not $System) { throw "Usage: mcluster.ps1 -Action update -System <system> (-Conf <path> | -ConfContent <b64>) [-Nodes <n>]" }
        if (-not $Conf -and -not $ConfContent) { throw "Usage: mcluster.ps1 -Action update -System <system> (-Conf <path> | -ConfContent <b64>) [-Nodes <n>]" }
        if ($Conf -and $ConfContent) { throw "ERROR: -Conf and -ConfContent are mutually exclusive; specify only one." }

        Pull-Configs

        # Auto-detect node count if not specified
        if ($Nodes -le 0) {
            $clusterDir = if ($System -eq "garnet") { "$HOME/garnet-cluster" } else { "$HOME/valkey-cluster" }
            $dirs = Get-ChildItem -Path $clusterDir -Directory -ErrorAction SilentlyContinue | Where-Object { $_.Name -match '^\d+$' }
            $Nodes = ($dirs | Measure-Object).Count
            if ($Nodes -eq 0) { throw "ERROR: No existing cluster dir found and no -Nodes specified" }
        }

        Resolve-Config -Sys $System -Count $Nodes -ConfPath $Conf -ConfB64 $ConfContent -ConfLeaf $ConfName
        Write-Host "Updated configs in place. Restart instances to apply." -ForegroundColor Yellow
    }

    "stage" {
        if (-not $System) { throw "Usage: mcluster.ps1 -Action stage -System <system> (-Conf <path> | -ConfContent <b64>) -Nodes <n>" }
        if (-not $Conf -and -not $ConfContent) { throw "Usage: mcluster.ps1 -Action stage -System <system> (-Conf <path> | -ConfContent <b64>) -Nodes <n>" }
        if ($Conf -and $ConfContent) { throw "ERROR: -Conf and -ConfContent are mutually exclusive; specify only one." }
        if ($Nodes -le 0) { throw "Usage: mcluster.ps1 -Action stage -System <system> (-Conf <path> | -ConfContent <b64>) -Nodes <n>" }

        if ($Clean) {
            Write-Host "Cleaning $System cluster directory..."
            Clean-System -Sys $System
        }

        Pull-Configs
        Resolve-Config -Sys $System -Count $Nodes -ConfPath $Conf -ConfB64 $ConfContent -ConfLeaf $ConfName

        Write-Host ""
        Write-Host "==== Staged commands (copy & paste to run manually) ====" -ForegroundColor Cyan
        $clusterDir = if ($System -eq "garnet") { "$HOME/garnet-cluster" } else { "$HOME/valkey-cluster" }
        for ($i = 0; $i -lt $Nodes; $i++) {
            $port = $BASE_PORT + $i
            $dir = "$clusterDir/$port"
            if ($System -eq "garnet") {
                $conf = "$dir/garnet.conf"
                Write-Host "  cd $dir && GarnetServer --config-import-path=$conf" -ForegroundColor Yellow
            } else {
                $conf = "$dir/valkey.conf"
                Write-Host "  cd $dir && valkey-server $conf" -ForegroundColor Yellow
            }
        }
        Write-Host ""
        Write-Host "Configs resolved. Run the commands above to start instances in the foreground." -ForegroundColor Green
    }
}
