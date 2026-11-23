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
    [ValidateRange(0, 65535)][int]$BasePort = 0,
    [switch]$InstanceOnly,
    [switch]$NoCluster,
    [switch]$Tls,
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
    Write-Host "  -BasePort    Override the configured starting port"
    Write-Host "  -InstanceOnly Limit stopping and cleanup to -Nodes instances from the base port"
    Write-Host "  -NoCluster   Disable cluster mode in generated configs"
    Write-Host "  -Tls         Enable Garnet TLS using role-specific Key Vault material"
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
if ($BasePort -gt 0) { $BASE_PORT = $BasePort }
if ($InstanceOnly -and (-not $System -or $Nodes -le 0 -or $BASE_PORT + $Nodes - 1 -gt 65535)) {
    throw '-InstanceOnly requires -System, a positive -Nodes count, and valid ports.'
}
if (-not $DEPLOY_USER) { $DEPLOY_USER = "guser" }
if (-not $RAMDISK_DIR) { $RAMDISK_DIR = "/mnt/ramdisk" }
if (-not $NVME_DIR) { $NVME_DIR = "/mnt/nvme" }

$RepoDir = "$HOME/tools"
$ClusterMode = if ($NoCluster) { "false" } else { "true" }
if ($Tls -and $System -ne 'garnet') { throw '-Tls is currently supported only for Garnet.' }

function Get-GarnetTlsSettings {
    $deploymentFile = '/opt/deploy-actions/deployment.env'
    $role = if (Test-Path $deploymentFile) {
        (Select-String -Path $deploymentFile -Pattern '^DEPLOYMENT_ROLE=' | Select-Object -First 1).Line -replace '^DEPLOYMENT_ROLE=', ''
    }
    if ($role -ne 'server') {
        throw "Garnet TLS servers require DEPLOYMENT_ROLE=server; this node is '$role'."
    }

    sudo /opt/deploy-actions/setup-tls.ps1
    if ($LASTEXITCODE -ne 0) { throw 'TLS material setup failed.' }

    $tlsDir = '/opt/azurebench/tls'
    $metadata = Get-Content "$tlsDir/metadata.json" -Raw | ConvertFrom-Json
    $password = (Get-Content "$tlsDir/server.password" -Raw).Trim()
    foreach ($path in @("$tlsDir/server.pfx", "$tlsDir/ca.crt")) {
        if (-not (Test-Path $path -PathType Leaf)) { throw "Required TLS file is missing: $path" }
    }
    if (-not $password -or -not $metadata.targetHost) { throw 'TLS password or target host is missing.' }

    return @{
        CertFileName          = "$tlsDir/server.pfx"
        CertPassword          = $password
        IssuerCertificatePath = "$tlsDir/ca.crt"
        TargetHost            = [string]$metadata.targetHost
    }
}

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

function Get-GarnetLogMemorySize {
    param([string]$AofMemorySize)

    $memInfo = Get-Content -LiteralPath '/proc/meminfo' -Raw
    if ($memInfo -notmatch '(?m)^MemTotal:\s+(\d+)\s+kB\s*$') {
        throw 'Cannot determine total VM memory from /proc/meminfo.'
    }
    $totalMemoryBytes = [decimal]$Matches[1] * 1KB
    if ($AofMemorySize -notmatch '^(\d+)([kmgt]?)$') {
        throw "Unsupported AofMemorySize for VM-sized log memory: $AofMemorySize"
    }
    $units = @{ '' = 1; k = 1KB; m = 1MB; g = 1GB; t = 1TB }
    $aofMemoryBytes = [decimal]$Matches[1] * $units[$Matches[2].ToLowerInvariant()]
    $logMemoryBytes = [long][decimal]::Floor($totalMemoryBytes * [decimal]0.8 - $aofMemoryBytes)
    if ($logMemoryBytes -lt 64MB) {
        throw 'VM memory remaining after the 20% reserve and AOF buffer is below the 64 MiB log minimum.'
    }
    return [string]$logMemoryBytes
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
    if ($Sys -eq 'garnet' -and $sourceContent.Contains('$logMemorySize')) {
        if ($Count -ne 1) {
            throw 'VM-sized log memory requires exactly one Garnet instance per VM.'
        }
        $memorySettings = ($sourceContent -replace '\$port', $BASE_PORT) | ConvertFrom-Json
        $logMemorySize = Get-GarnetLogMemorySize -AofMemorySize $memorySettings.AofMemorySize
        $sourceContent = $sourceContent.Replace('$logMemorySize', $logMemorySize)
    }
    $configEnablesTls = $sourceContent -match '"EnableTLS"\s*:\s*true'
    if ($Sys -eq 'garnet' -and $configEnablesTls -and -not $Tls) {
        throw "Configuration enables TLS; pass -Tls so certificate material is provisioned."
    }
    $tlsSettings = if ($Sys -eq 'garnet' -and $Tls) { Get-GarnetTlsSettings } else { $null }
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
            if ($Tls -or $BasePort -gt 0) {
                try {
                    $garnetConfig = $content | ConvertFrom-Json
                } catch {
                    throw "Garnet configuration is not valid JSON: $($_.Exception.Message)"
                }
                if ($BasePort -gt 0) {
                    $garnetConfig | Add-Member -NotePropertyName Port -NotePropertyValue $port -Force
                    $garnetConfig | Add-Member -NotePropertyName EnableCluster -NotePropertyValue ($ClusterMode -eq 'true') -Force
                }
                if ($Tls) {
                    $tlsProperties = [ordered]@{
                        EnableTLS                     = $true
                        CertFileName                  = $tlsSettings.CertFileName
                        CertPassword                  = $tlsSettings.CertPassword
                        CertificateRefreshFrequency   = 0
                        ClientCertificateRequired     = $false
                        ServerCertificateRequired     = $true
                        IssuerCertificatePath         = $tlsSettings.IssuerCertificatePath
                        ClusterTlsClientTargetHost    = $tlsSettings.TargetHost
                    }
                    foreach ($entry in $tlsProperties.GetEnumerator()) {
                        $garnetConfig | Add-Member -NotePropertyName $entry.Key -NotePropertyValue $entry.Value -Force
                    }
                }
                $content = $garnetConfig | ConvertTo-Json -Depth 20
            }
            Set-Content -Path "$portDir/garnet.conf" -Value $content -NoNewline
            if ($Tls) { chmod 0600 "$portDir/garnet.conf" }
        } else {
            if ($BasePort -gt 0) {
                if ($content -match '(?m)^port\s+') { $content = $content -replace '(?m)^port\s+.*', "port $port" }
                else { $content += "`nport $port`n" }
            }
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
    Write-Host "Resolved $Count config(s) from $(Split-Path $sourceFile -Leaf) (cluster=$ClusterMode, tls=$Tls) -> $clusterDir/" -ForegroundColor Green
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

function Get-GarnetRuntimeDirectory {
    param([string]$ConfigPath, [string]$DefaultDirectory)
    $settings = Get-Content -LiteralPath $ConfigPath -Raw | ConvertFrom-Json
    if ($settings.CheckpointDir -and ([string]$settings.CheckpointDir).StartsWith('/dev/shm/', [StringComparison]::Ordinal)) {
        $fileSystem = & findmnt -n -o FSTYPE --target /dev/shm
        if ($LASTEXITCODE -ne 0 -or "$fileSystem".Trim() -ne 'tmpfs') {
            throw '/dev/shm is not mounted as tmpfs; refusing to write diskless runtime output to disk.'
        }
        return [string]$settings.CheckpointDir
    }
    if ($settings.LogDir -and ([string]$settings.LogDir).StartsWith("$($NVME_DIR.TrimEnd('/'))/", [StringComparison]::Ordinal)) {
        & mountpoint -q $NVME_DIR
        if ($LASTEXITCODE -ne 0) { throw "NVMe mount $NVME_DIR is unavailable; refusing to write to the OS disk." }
        return [string]$settings.LogDir
    }
    return $DefaultDirectory
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

        $runtimeDir = Get-GarnetRuntimeDirectory -ConfigPath $conf -DefaultDirectory $dir
        New-Item -ItemType Directory -Path $runtimeDir -Force | Out-Null
        Set-Location $runtimeDir
        bash -c "nohup GarnetServer --config-import-path='$conf' > '$runtimeDir/garnet.log' 2> '$runtimeDir/garnet.err' &"
        Write-Host "  Port ${port}: started" -ForegroundColor Cyan
    }
}

function Clean-System {
    param([string]$Sys, [int]$Count = 0)
    if ($Count -gt 0) {
        for ($i = 0; $i -lt $Count; $i++) {
            $port = $BASE_PORT + $i
            foreach ($root in @($HOME, $RAMDISK_DIR, $NVME_DIR, '/dev/shm')) {
                $directory = "$root/$Sys-cluster/$port"
                if (Test-Path -LiteralPath $directory -PathType Container) {
                    Remove-Item -LiteralPath $directory -Recurse -Force
                    Write-Host "  Removed $directory"
                }
            }
        }
        return
    }
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

function Get-SystemProcessIds {
    param([string]$Sys, [int]$Count)
    $patterns = @()
    if ($Count -le 0) {
        $patterns = @($(if ($Sys -eq 'garnet') { '[G]arnetServer' } else { '[v]alkey-server' }))
    } else {
        for ($i = 0; $i -lt $Count; $i++) {
            $port = $BASE_PORT + $i
            $patterns += if ($Sys -eq 'garnet') {
                '[G]arnetServer.*' + [regex]::Escape("$HOME/garnet-cluster/$port/garnet.conf") + '([[:space:]]|$)'
            } else { "[v]alkey-server.*:${port}([[:space:]]|$)" }
        }
    }
    foreach ($pattern in $patterns) {
        $output = @(& pgrep -f -- $pattern)
        if ($LASTEXITCODE -eq 1) { continue }
        if ($LASTEXITCODE -ne 0) { throw "Process lookup failed for $Sys (exit $LASTEXITCODE)." }
        foreach ($line in $output) {
            if ($line -notmatch '^\d+$') { throw "Invalid process ID from pgrep: $line" }
            [int]$line
        }
    }
}

function Stop-System {
    param([string]$Sys, [int]$Count)
    $processIds = @(Get-SystemProcessIds -Sys $Sys -Count $Count)
    foreach ($processId in $processIds) {
        Stop-Process -Id $processId -Force -ErrorAction Stop
        Write-Host "  $Sys stopped (pid $processId)"
    }
    if (-not $processIds.Count) { Write-Host "  No matching $Sys instances running." -ForegroundColor DarkGray }
}

function Confirm-Stopped {
    param([string]$Sys, [int]$TimeoutSec = 15, [int]$Count = 0)
    $patterns = @()
    if ($Sys -eq "garnet" -or [string]::IsNullOrEmpty($Sys)) { $patterns += "GarnetServer" }
    if ($Sys -eq "valkey" -or [string]::IsNullOrEmpty($Sys)) { $patterns += "valkey-server" }
    foreach ($pat in $patterns) {
        $deadline = (Get-Date).AddSeconds($TimeoutSec)
        $procIds = @()
        do {
            $targetSystem = if ($pat -eq 'GarnetServer') { 'garnet' } else { 'valkey' }
            $procIds = @(Get-SystemProcessIds -Sys $targetSystem -Count $Count)
            if ($procIds.Count -eq 0) { break }
            Start-Sleep -Milliseconds 500
        } while ((Get-Date) -lt $deadline)

        if ($procIds.Count -gt 0) {
            Write-Host "  $pat still running after ${TimeoutSec}s; sending SIGKILL..." -ForegroundColor Yellow
            foreach ($p in $procIds) { Stop-Process -Id $p -Force -ErrorAction Stop }
            Start-Sleep -Milliseconds 500
            $procIds = @(Get-SystemProcessIds -Sys $targetSystem -Count $Count)
            if ($procIds.Count -gt 0) {
                throw "Failed to stop all $pat processes; aborting clean to avoid deleting data under a live process."
            }
        }
        Write-Host "  ${pat}: confirmed stopped" -ForegroundColor DarkGray
    }
}

# Stop all instances of the given system (or both when unspecified) and confirm
# termination before any data is removed, so a live process can never keep
# running against deleted files.
function Stop-And-Confirm {
    param([string]$Sys, [int]$Count = 0)
    if ([string]::IsNullOrEmpty($Sys)) {
        Stop-System -Sys "valkey" -Count 0
        Stop-System -Sys "garnet" -Count 0
    } else {
        Stop-System -Sys $Sys -Count $Count
    }
    Confirm-Stopped -Sys $Sys -Count $Count
}

# Main logic
$cleanupCount = if ($InstanceOnly) { $Nodes } else { 0 }
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
            Write-Host "Stopping $System instances before clean..."
            Stop-And-Confirm -Sys $System -Count $cleanupCount
            Write-Host "Cleaning $System cluster directory..."
            Clean-System -Sys $System -Count $cleanupCount
        }

        Pull-Configs
        Resolve-Config -Sys $System -Count $Nodes -ConfPath $Conf -ConfB64 $ConfContent -ConfLeaf $ConfName

        if ($System -eq "garnet") { Start-Garnet -Count $Nodes }
        else { Start-Valkey -Count $Nodes }
        Write-Host "Done." -ForegroundColor Green
    }

    "stop" {
        if ($InstanceOnly) {
            Stop-And-Confirm -Sys $System -Count $Nodes
            return
        }
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
        Write-Host "Stopping instances before clean..."
        Stop-And-Confirm -Sys $System -Count $cleanupCount
        Write-Host "Cleaning cluster directories..."
        Clean-System -Sys $System -Count $cleanupCount
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
            Write-Host "Stopping $System instances before clean..."
            Stop-And-Confirm -Sys $System -Count $cleanupCount
            Write-Host "Cleaning $System cluster directory..."
            Clean-System -Sys $System -Count $cleanupCount
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
                $runtimeDir = Get-GarnetRuntimeDirectory -ConfigPath $conf -DefaultDirectory $dir
                Write-Host "  cd $runtimeDir && GarnetServer --config-import-path=$conf" -ForegroundColor Yellow
            } else {
                $conf = "$dir/valkey.conf"
                Write-Host "  cd $dir && valkey-server $conf" -ForegroundColor Yellow
            }
        }
        Write-Host ""
        Write-Host "Configs resolved. Run the commands above to start instances in the foreground." -ForegroundColor Green
    }
}
