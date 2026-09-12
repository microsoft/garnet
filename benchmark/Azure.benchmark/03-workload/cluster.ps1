#!/usr/bin/env pwsh
[CmdletBinding()]
<#
.SYNOPSIS
    Controls cluster lifecycle (start/stop) on remote VMSS server instances via SSH.

.DESCRIPTION
    SSHs into the first server VM and invokes cluster-deploy.ps1 to manage
    the cluster across all discovered peers. The 'start' action automatically
    starts instances and forms the cluster (setup) in one step.

.EXAMPLE
    pwsh .\03-workload\cluster.ps1 -Action start -System valkey -Conf .\02-platform\tools\config\valkey\valkey-cache.conf -InstancePerVm 2
    pwsh .\03-workload\cluster.ps1 -Action start -System valkey -Conf .\02-platform\tools\config\valkey\valkey-cache.conf -InstancePerVm 2 -Clean
    pwsh .\03-workload\cluster.ps1 -Action start -System garnet -Conf .\02-platform\tools\config\garnet\garnet-cache-replication.conf -InstancePerVm 1 -NoCluster
    pwsh .\03-workload\cluster.ps1 -Action stop -System valkey -InstancePerVm 2
    pwsh .\03-workload\cluster.ps1 -Action restart -System garnet -Conf .\02-platform\tools\config\garnet\garnet-aofx8.conf -InstancePerVm 1 -Replicas 1 -CreateManual
#>
param(
    [ValidateSet("start","stop","restart")]
    [string]$Action,

    [ValidateSet("valkey","garnet")]
    [string]$System,

    [Alias('Config')][string]$Conf,
    [Alias('ICount', 'InstancesPerVm')][int]$InstancePerVm = 1,
    [Alias('NodeCount')][int]$VmCount = 0,
    [int]$MaxScan = 0,
    [int]$Replicas = 0,
    [switch]$Clean,
    [switch]$NoCluster,
    [switch]$CreateManual,
    [string]$ConfigFile = "$PSScriptRoot\bench\bench.conf",
    [string]$ServerHost,
    [string]$ResourceGroup,
    [string]$VmssName,
    [string]$SshUser,
    [string]$SshKey,
    [switch]$ForcePeerRefresh,
    [int]$PeerCacheTtlMinutes = 30,
    [switch]$Help
)

if ($Help -or -not $Action) {
    Write-Host "Usage: cluster.ps1 -Action <start|stop|restart> -System <valkey|garnet> [options]"
    Write-Host ""
    Write-Host "Controls cluster lifecycle on remote VMSS server instances via SSH."
    Write-Host "SSHs into the server VM and runs cluster-deploy.ps1 to orchestrate."
    Write-Host ""
    Write-Host "Actions:"
    Write-Host "  start    Start instances + form cluster (start then setup)"
    Write-Host "  stop     Stop cluster instances on all server VMs"
    Write-Host "  restart  Stop instances, then start + form cluster (stop then start)"
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  -System      Target system: valkey or garnet (required)"
    Write-Host "  -Conf        Local config file path on THIS workstation (required for start/restart); its content is shipped to the nodes"
    Write-Host "  -InstancePerVm Number of server processes per VM (default: 1; aliases: -InstancesPerVm, -ICount)"
    Write-Host "  -VmCount       Expected/selected VMSS machine count (default: 0 = all; alias: -NodeCount)"
    Write-Host "  -MaxScan     Cap subnet-scan probes when Azure API discovery is unavailable (0 = unlimited)"
    Write-Host "  -Replicas    Number of replicas per primary (default: 0)"
    Write-Host "  -Clean       Remove cluster directory before starting"
    Write-Host "  -NoCluster   Disable cluster mode (skip setup step)"
    Write-Host "  -CreateManual Form the cluster manually (MEET + ADDSLOTSRANGE + REPLICATE) instead of '--cluster create'"
    Write-Host "  -ConfigFile  Path to bench.conf for SSH host/key resolution (default: bench.conf)"
    Write-Host "  -ServerHost  Override server SSH host (default: from bench.conf Host field)"
    Write-Host "  -ResourceGroup Azure resource group containing the server VMSS (inferred when possible)"
    Write-Host "  -VmssName    Server VMSS name (inferred from vmN.<vmss>.* ServerHost when possible)"
    Write-Host "  -SshUser     Override SSH user (default: from bench.conf or guser)"
    Write-Host "  -SshKey      Override SSH key path (default: from security/manifest.json)"
    Write-Host "  -ForcePeerRefresh Ignore local peer cache and regenerate it through Azure CLI"
    Write-Host "  -PeerCacheTtlMinutes Regenerate caches older than this value through Azure CLI (default: 30; 0 disables)"
    return
}

$ErrorActionPreference = "Stop"

# --- Validate params ---
if (-not $System) { Write-Error "-System is required"; exit 1 }
if (($Action -eq "start" -or $Action -eq "restart") -and -not $Conf) {
    Write-Error "-Conf is required for '$Action'"; exit 1
}

# --- Resolve -Conf as a LOCAL workstation file and base64-encode it for shipping ---
$confContent = ""
$confName = ""
if ($Conf) {
    $confPath = $Conf
    if (-not [System.IO.Path]::IsPathRooted($confPath)) {
        if (Test-Path $confPath) {
            $confPath = (Resolve-Path $confPath).Path
        } elseif (Test-Path (Join-Path (Split-Path $PSScriptRoot -Parent) $Conf)) {
            $confPath = (Join-Path (Split-Path $PSScriptRoot -Parent) $Conf)
        }
    }
    if (-not (Test-Path $confPath)) {
        Write-Error "-Conf file not found on this workstation: $Conf"; exit 1
    }
    $confName = Split-Path $confPath -Leaf
    $confBytes = [System.IO.File]::ReadAllBytes($confPath)
    $confContent = [Convert]::ToBase64String($confBytes)
}

# --- Load config file for defaults ---
$config = @{}
if (Test-Path $ConfigFile) {
    Get-Content $ConfigFile | ForEach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith("#")) {
            $parts = $line -split "=", 2
            if ($parts.Count -eq 2) {
                $config[$parts[0].Trim()] = $parts[1].Trim()
            }
        }
    }
}

# --- Resolve SSH key ---
if (-not $SshKey) {
    $manifestPath = Join-Path (Split-Path $PSScriptRoot -Parent) "01-resources\security\manifest.json"
    if (Test-Path $manifestPath) {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) "01-resources\security\ssh-key-utils.ps1")
        $manifest = Get-SshKeyManifest -ManifestPath $manifestPath
        $SshKey = Resolve-SshUserPrivateKey -Manifest $manifest -AllowMissing
    }
    if (-not $SshKey) {
        $SshKey = "$env:USERPROFILE\.ssh\id_ed25519"
    }
}
if (-not (Test-Path $SshKey)) {
    Write-Error "SSH key not found: $SshKey"
    exit 1
}

# --- Resolve SSH user and server host ---
if (-not $SshUser) { $SshUser = $config["SshUser"] ?? "guser" }
if (-not $ServerHost) { $ServerHost = $config["Host"] ?? "" }
if (-not $ServerHost) {
    Write-Error "No server host specified. Use -ServerHost or set Host= in bench.conf"
    exit 1
}

# --- Helper to run a remote command ---
$sshOpts = @('-i', $SshKey, '-o', 'StrictHostKeyChecking=no', '-o', 'ConnectTimeout=10', '-t')

function Get-InferredVmssName {
    param([string]$HostName)
    if ($HostName -match '^vm\d+\.([^.]+)\.') { return $Matches[1] }
    return $null
}

function Get-PeerCacheRoot {
    return Join-Path (Split-Path $PSScriptRoot -Parent) '.peer-cache'
}

function Get-LocalPeerCache {
    param([string]$CacheRoot, [string]$TargetVmss, [string]$TargetResourceGroup, [int]$ExpectedVmCount)

    if (-not (Test-Path -LiteralPath $CacheRoot -PathType Container)) { return $null }
    $matches = @()
    foreach ($file in Get-ChildItem -LiteralPath $CacheRoot -Filter '*.json' -File -ErrorAction SilentlyContinue) {
        try {
            $manifest = Get-Content -LiteralPath $file.FullName -Raw | ConvertFrom-Json
        } catch {
            continue
        }
        if ($manifest.schemaVersion -ne 1 -or $manifest.vmssName -ne $TargetVmss) { continue }
        if ($TargetResourceGroup -and $manifest.resourceGroup -ne $TargetResourceGroup) { continue }
        if ($ExpectedVmCount -gt 0 -and @($manifest.peers).Count -ne $ExpectedVmCount) { continue }
        $matches += [pscustomobject]@{ Path = $file.FullName; Manifest = $manifest }
    }
    if ($matches.Count -gt 1 -and -not $TargetResourceGroup) {
        throw "Multiple local peer caches match VMSS '$TargetVmss'. Specify -ResourceGroup."
    }
    return $matches | Select-Object -First 1
}

function Save-LocalPeerCache {
    param([string]$CacheRoot, [object]$Manifest)

    New-Item -ItemType Directory -Path $CacheRoot -Force | Out-Null
    $safeSubscription = ([string]$Manifest.subscriptionId) -replace '[^A-Za-z0-9_.-]', '_'
    $safeGroup = ([string]$Manifest.resourceGroup) -replace '[^A-Za-z0-9_.-]', '_'
    $safeVmss = ([string]$Manifest.vmssName) -replace '[^A-Za-z0-9_.-]', '_'
    $path = Join-Path $CacheRoot "$safeSubscription-$safeGroup-$safeVmss.json"
    $tmp = "$path.$([guid]::NewGuid().ToString('N')).tmp"
    $Manifest | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $tmp -Encoding utf8
    Move-Item -LiteralPath $tmp -Destination $path -Force
    return [pscustomobject]@{ Path = $path; Manifest = $Manifest }
}

function Resolve-AzureVmssTarget {
    param([string]$TargetVmss, [string]$TargetResourceGroup)

    if (-not $TargetResourceGroup) {
        $groupsJson = az vmss list --query "[?name=='$TargetVmss'].resourceGroup" -o json 2>$null
        if ($LASTEXITCODE -ne 0 -or -not $groupsJson) {
            throw "Unable to find VMSS '$TargetVmss'. Run 'az login' and select the intended subscription."
        }
        $groups = @($groupsJson | ConvertFrom-Json | Where-Object { $_ } | Select-Object -Unique)
        if ($groups.Count -eq 0) { throw "VMSS '$TargetVmss' was not found in the active subscription." }
        if ($groups.Count -gt 1) { throw "VMSS '$TargetVmss' exists in multiple resource groups. Specify -ResourceGroup." }
        $TargetResourceGroup = $groups[0]
    }

    $vmssJson = az vmss show -g $TargetResourceGroup -n $TargetVmss `
        --query '{name:name,resourceGroup:resourceGroup}' -o json 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $vmssJson) {
        throw "Unable to read VMSS '$TargetVmss' in resource group '$TargetResourceGroup'."
    }
    return $vmssJson | ConvertFrom-Json
}

function Get-AzurePeerManifest {
    param([string]$TargetVmss, [string]$TargetResourceGroup, [int]$ExpectedVmCount)

    $accountJson = az account show --query '{subscriptionId:id}' -o json 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $accountJson) {
        throw "Unable to read the active Azure subscription. Run 'az login'."
    }
    $account = $accountJson | ConvertFrom-Json
    $target = Resolve-AzureVmssTarget -TargetVmss $TargetVmss -TargetResourceGroup $TargetResourceGroup
    Write-Host "Building peer cache through Azure CLI for VMSS '$($target.name)' in resource group '$($target.resourceGroup)'..." -ForegroundColor Yellow

    $instancesJson = az vmss list-instances -g $target.resourceGroup -n $target.name `
        --query "[].{instanceId:instanceId}" -o json 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $instancesJson) { throw "Failed to enumerate VMSS instances." }
    $instances = @($instancesJson | ConvertFrom-Json)

    $nicsJson = az vmss nic list -g $target.resourceGroup --vmss-name $target.name -o json 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $nicsJson) { throw "Failed to enumerate VMSS accelerated-networking NICs." }
    $nics = @($nicsJson | ConvertFrom-Json | Where-Object { -not $_.primary } | ForEach-Object {
        [pscustomobject]@{
            vmId = $_.virtualMachine.id
            eth1Ip = $_.ipConfigurations[0].privateIPAddress
        }
    })

    $nicsByInstance = @{}
    foreach ($nic in $nics) {
        if ($nic.vmId -match '/virtualMachines/(\d+)$') { $nicsByInstance[$Matches[1]] = $nic.eth1Ip }
    }

    $peers = @()
    foreach ($instance in $instances | Sort-Object { [int]$_.instanceId }) {
        $eth1Ip = $nicsByInstance[[string]$instance.instanceId]
        if (-not $eth1Ip) {
            Write-Warning "Instance $($instance.instanceId) has no non-primary eth1 address; skipping."
            continue
        }
        $peers += [ordered]@{
            instanceId = [string]$instance.instanceId
            eth1Ip     = [string]$eth1Ip
        }
    }
    if ($peers.Count -eq 0) { throw "Azure returned no VMSS peers with eth1 addresses." }
    if ($ExpectedVmCount -gt 0 -and $peers.Count -ne $ExpectedVmCount) {
        throw "Expected $ExpectedVmCount VM(s), but Azure returned $($peers.Count) with eth1 addresses."
    }

    return [ordered]@{
        schemaVersion  = 1
        source         = 'azure-cli'
        subscriptionId = [string]$account.subscriptionId
        resourceGroup  = [string]$target.resourceGroup
        vmssName       = [string]$target.name
        generatedAt    = (Get-Date).ToUniversalTime().ToString('o')
        peers          = $peers
    }
}

function Send-PeerManifest {
    param([object]$Manifest)

    $json = $Manifest | ConvertTo-Json -Depth 8 -Compress
    $content = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($json))
    $cmd = "cluster-deploy.ps1 -Action import-peers -PeerManifestContent $content"
    if ($VmCount -gt 0) { $cmd += " -VmCount $VmCount" }

    Write-Host "Pushing peer inventory to coordinator for validation..." -ForegroundColor Yellow
    $output = @(ssh @sshOpts "${SshUser}@${ServerHost}" "pwsh -c '$cmd'" 2>&1)
    $code = $LASTEXITCODE
    $output | ForEach-Object { Write-Host $_ }
    return $code -eq 0 -and (($output -join "`n") -match 'PEER_IMPORT_OK')
}

function Invoke-Remote {
    param([string]$Cmd, [string]$Label)
    Write-Host "[$Label] $Cmd" -ForegroundColor Yellow
    Write-Host ""
    ssh @sshOpts "${SshUser}@${ServerHost}" "pwsh -c '$Cmd'"
    $code = $LASTEXITCODE
    Write-Host ""
    if ($code -ne 0) {
        Write-Host "[$Label] FAILED (exit code $code)" -ForegroundColor Red
        exit $code
    }
    Write-Host "[$Label] Done." -ForegroundColor Green
    Write-Host ""
}

$cacheRoot = Get-PeerCacheRoot
if (-not $VmssName) { $VmssName = Get-InferredVmssName -HostName $ServerHost }
if (-not $VmssName) {
    Write-Error "Unable to infer the VMSS name from '$ServerHost'. Specify -VmssName."
    exit 1
}

$peerCache = $null
if (-not $ForcePeerRefresh) {
    try {
        $peerCache = Get-LocalPeerCache -CacheRoot $cacheRoot -TargetVmss $VmssName `
            -TargetResourceGroup $ResourceGroup -ExpectedVmCount $VmCount
    } catch {
        Write-Error $_.Exception.Message
        exit 1
    }
}

if ($peerCache) {
    Write-Host "Using local peer cache: $($peerCache.Path)" -ForegroundColor DarkGray
    if ($PeerCacheTtlMinutes -gt 0) {
        $generatedAt = [datetimeoffset]::MinValue
        if (-not [datetimeoffset]::TryParse([string]$peerCache.Manifest.generatedAt, [ref]$generatedAt)) {
            Write-Warning "Local peer cache has an invalid generatedAt timestamp; regenerating it through Azure CLI."
            $peerCache = $null
        } else {
            $age = [datetimeoffset]::UtcNow - $generatedAt.ToUniversalTime()
            if ($age.TotalMinutes -gt $PeerCacheTtlMinutes) {
                Write-Host "Local peer cache expired after $([math]::Round($age.TotalMinutes)) minute(s); regenerating it through Azure CLI." -ForegroundColor Yellow
                $peerCache = $null
            }
        }
    }
    if ($peerCache -and -not (Send-PeerManifest -Manifest $peerCache.Manifest)) {
        Write-Warning "Coordinator rejected the local peer cache; regenerating it through Azure CLI."
        $peerCache = $null
    }
}

if (-not $peerCache) {
    try {
        $manifest = Get-AzurePeerManifest -TargetVmss $VmssName `
            -TargetResourceGroup $ResourceGroup -ExpectedVmCount $VmCount
        $ResourceGroup = $manifest.resourceGroup
        $peerCache = Save-LocalPeerCache -CacheRoot $cacheRoot -Manifest $manifest
    } catch {
        Write-Error $_.Exception.Message
        exit 1
    }
    Write-Host "Saved local peer cache: $($peerCache.Path)" -ForegroundColor DarkGray
    if (-not (Send-PeerManifest -Manifest $peerCache.Manifest)) {
        Write-Error "Coordinator rejected the Azure-generated peer inventory."
        exit 1
    }
}

# --- Summary ---
Write-Host "==== cluster ($Action) ====" -ForegroundColor Cyan
Write-Host "  Server:    $ServerHost"
Write-Host "  System:    $System"
if ($Conf)      { Write-Host "  Conf:      $Conf ($($confBytes.Length) bytes, shipped as $confName)" }
Write-Host "  InstancePerVm:  $InstancePerVm"
if ($VmCount -gt 0) { Write-Host "  VmCount:        $VmCount" }
Write-Host "  VMSS:           $($peerCache.Manifest.vmssName)"
Write-Host "  Resource group: $($peerCache.Manifest.resourceGroup)"
if ($Replicas -gt 0) { Write-Host "  Replicas:  $Replicas" }
if ($Clean)     { Write-Host "  Clean:     True" }
if ($NoCluster) { Write-Host "  NoCluster: True" }
if ($CreateManual) { Write-Host "  CreateManual: True" }
Write-Host ""

# --- Execute ---
$doStart = {
    # Step 1: Start instances
    $startCmd = "cluster-deploy.ps1 -Action start -System $System -InstancePerVm $InstancePerVm"
    if ($VmCount -gt 0) { $startCmd += " -VmCount $VmCount" }
    if ($MaxScan -gt 0) { $startCmd += " -MaxScan $MaxScan" }
    $startCmd += " -ConfContent $confContent -ConfName $confName"
    if ($Clean) { $startCmd += " -Clean" }
    if ($NoCluster) { $startCmd += " -NoCluster" }
    Invoke-Remote -Cmd $startCmd -Label "start"

    # Step 2: Form cluster (skip if NoCluster)
    if (-not $NoCluster) {
        $setupCmd = "cluster-deploy.ps1 -Action setup -System $System -InstancePerVm $InstancePerVm"
        if ($VmCount -gt 0) { $setupCmd += " -VmCount $VmCount" }
        if ($MaxScan -gt 0) { $setupCmd += " -MaxScan $MaxScan" }
        if ($Replicas -gt 0) { $setupCmd += " -Replicas $Replicas" }
        if ($CreateManual) { $setupCmd += " -CreateManual" }
        Invoke-Remote -Cmd $setupCmd -Label "setup"
    }
}

$doStop = {
    $stopCmd = "cluster-deploy.ps1 -Action stop -System $System -InstancePerVm $InstancePerVm"
    if ($VmCount -gt 0) { $stopCmd += " -VmCount $VmCount" }
    if ($MaxScan -gt 0) { $stopCmd += " -MaxScan $MaxScan" }
    Invoke-Remote -Cmd $stopCmd -Label "stop"
}

switch ($Action) {
    "start"   { & $doStart }
    "stop"    { & $doStop }
    "restart" { & $doStop; & $doStart }
}

Write-Host "==== cluster ($Action) complete ====" -ForegroundColor Green
