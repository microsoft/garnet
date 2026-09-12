#!/usr/bin/env pwsh
[CmdletBinding()]
<#
.SYNOPSIS
    Orchestrates cluster lifecycle across VMSS instances with automatic peer discovery.

.DESCRIPTION
    Discovers peer VMs on the accelerated subnet, then performs actions:
    discover, start, setup, or stop.

.EXAMPLE
    cluster-deploy.ps1 -Action discover -InstancePerVm 4
    cluster-deploy.ps1 -Action start -System valkey -Conf config/valkey/valkey-cache.conf -InstancePerVm 4
    cluster-deploy.ps1 -Action start -System valkey -Conf config/valkey/valkey-cache.conf -InstancePerVm 4 -Clean
    cluster-deploy.ps1 -Action setup -System valkey -InstancePerVm 4 -Replicas 1
    cluster-deploy.ps1 -Action stop -System valkey -InstancePerVm 4
    cluster-deploy.ps1 -Action start -System garnet -Conf config/garnet/garnet-cache.conf -InstancePerVm 1 -NoCluster
    cluster-deploy.ps1 -Action start -VmCount 6 -System valkey -Conf config/valkey/valkey-cache.conf -InstancePerVm 4
#>
param(
    [ValidateSet("discover","import-peers","start","setup","stop")][string]$Action,
    [ValidateSet("valkey","garnet")][string]$System,
    [Alias('Config')][string]$Conf,
    [string]$ConfContent,
    [string]$ConfName,
    [Alias('ICount', 'InstancesPerVm')][int]$InstancePerVm = 1,
    [Alias('NodeCount')][int]$VmCount,
    [switch]$Clean,
    [int]$Replicas = 0,
    [switch]$NoCluster,
    [switch]$CreateManual,
    [string]$User = "guser",
    [int]$Port = 7000,
    [int]$MaxScan = 0,
    [int]$SshTimeout = 10,
    [int]$TcpTimeout = 60,
    [string]$PeerManifestContent,
    [switch]$Help
)

if ($Help -or -not $Action) {
    Write-Host "Usage: cluster-deploy.ps1 -Action <discover|import-peers|start|setup|stop> [options]"
    Write-Host ""
    Write-Host "Orchestrates cluster lifecycle across VMSS instances with automatic peer discovery."
    Write-Host ""
    Write-Host "Actions:"
    Write-Host "  discover   Scan subnet for peers and probe ports for running instances"
    Write-Host "  import-peers Validate and cache a Base64 JSON peer inventory from cluster.ps1"
    Write-Host "  start      Discover peers, SSH mcluster start on each, wait for endpoints"
    Write-Host "  setup      Discover peers, verify endpoints, form cluster (assign slots)"
    Write-Host "  stop       Discover peers, SSH mcluster stop on each"
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  -System         Target system: valkey or garnet (required for start/setup/stop)"
    Write-Host "  -Conf           Explicit config file path on server (repo-relative or absolute)"
    Write-Host "  -ConfContent    Base64 config content shipped from the workstation; forwarded to nodes"
    Write-Host "  -ConfName       Leaf filename for -ConfContent"
    Write-Host "  -InstancePerVm  Number of server processes per VM (aliases: -InstancesPerVm, -ICount)"
    Write-Host "  -VmCount        Expected/selected VM count (alias: -NodeCount)"
    Write-Host "  -Clean          Clean cluster directories before starting"
    Write-Host "  -Replicas       Number of replicas per primary (default: 0)"
    Write-Host "  -NoCluster      Disable cluster mode in configs"
    Write-Host "  -CreateManual   Form the cluster manually (MEET + ADDSLOTSRANGE + REPLICATE) instead of '--cluster create'"
    Write-Host "  -User           SSH user (default: guser)"
    Write-Host "  -Port           Base port (default: 7000)"
    Write-Host "  -MaxScan        Cap subnet-scan probe attempts (0 = unlimited)"
    Write-Host "  -SshTimeout     SSH connection timeout in seconds (default: 10)"
    Write-Host "  -TcpTimeout     TCP endpoint wait timeout in seconds (default: 60)"
    Write-Host "  -PeerManifestContent Base64 JSON peer inventory for -Action import-peers"
    Write-Host "  -Help           Show this help message"
    return
}

$ErrorActionPreference = "Stop"

# --- Helper Functions ---

function InvokeSsh {
    param([string]$Ip, [string]$SshUser, [string]$Command, [int]$Timeout = 10, [switch]$Background)
    $sshArgs = @("-o", "ConnectTimeout=$Timeout", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes")
    if ($Background) {
        return ssh @sshArgs "$SshUser@$Ip" "nohup $Command > /tmp/mcluster-deploy.log 2>&1 &" 2>&1
    } else {
        return ssh @sshArgs "$SshUser@$Ip" $Command 2>&1
    }
}

function Get-OwnEth1Info {
    $output = ip -4 addr show eth1 2>$null
    $inetLine = $output | Where-Object { $_ -match 'inet\s+([\d.]+)/([\d]+)' } | Select-Object -First 1
    if ($inetLine -match 'inet\s+([\d.]+)/([\d]+)') {
        return @{ Ip = $Matches[1]; Prefix = [int]$Matches[2] }
    }
    throw "ERROR: Could not detect eth1 IP/subnet."
}

function Get-SubnetIps {
    param([string]$Ip, [int]$Prefix)
    $parts = $Ip -split '\.'
    $ipInt = ([int]$parts[0] -shl 24) + ([int]$parts[1] -shl 16) + ([int]$parts[2] -shl 8) + [int]$parts[3]
    $mask = -bnot ((1 -shl (32 - $Prefix)) - 1)
    $network = $ipInt -band $mask
    $hostCount = (1 -shl (32 - $Prefix)) - 2  # exclude network and broadcast

    $ips = @()
    # Skip first 4 (Azure reserved: network, gateway, DNS x2) and last (broadcast)
    $start = $network + 4
    $end = $network + (1 -shl (32 - $Prefix)) - 2
    for ($i = $start; $i -le $end; $i++) {
        $o1 = ($i -shr 24) -band 0xFF
        $o2 = ($i -shr 16) -band 0xFF
        $o3 = ($i -shr 8) -band 0xFF
        $o4 = $i -band 0xFF
        $ips += "$o1.$o2.$o3.$o4"
    }
    return $ips
}

# Enumerate this VMSS's eth1 (accelerated / non-primary) NIC IPs directly from the
# Azure API using the VM's managed identity. This scales to thousands of instances
# and replaces the brute-force subnet scan (which would probe ~16k IPs on a /18).
# Returns $null on any failure so callers can fall back to the subnet scan.
function Get-PeersFromAzure {
    param([string]$OwnIp)
    try {
        $metaUri = "http://169.254.169.254/metadata/instance?api-version=2021-02-01"
        $meta = Invoke-RestMethod -Uri $metaUri -Headers @{ Metadata = "true" } -TimeoutSec 5
        $subscriptionId = $meta.compute.subscriptionId
        $resourceGroup  = $meta.compute.resourceGroupName
        $vmssName       = $meta.compute.vmScaleSetName
        if (-not $vmssName) { return $null }

        $tokenUri = "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://management.azure.com/"
        $tokenResp = Invoke-RestMethod -Uri $tokenUri -Headers @{ Metadata = "true" } -TimeoutSec 5
        $headers = @{ Authorization = "Bearer $($tokenResp.access_token)"; "Content-Type" = "application/json" }

        # VMSS-level NIC list, following nextLink paging.
        $nicsUri = "https://management.azure.com/subscriptions/$subscriptionId/resourceGroups/$resourceGroup/providers/Microsoft.Compute/virtualMachineScaleSets/$vmssName/networkInterfaces?api-version=2022-11-01"
        $nics = @()
        $next = $nicsUri
        while ($next) {
            $page = Invoke-RestMethod -Uri $next -Headers $headers -TimeoutSec 30
            if ($page.value) { $nics += $page.value }
            $next = $page.nextLink
        }

        $ips = @()
        foreach ($nic in $nics) {
            if (($nic.properties.primary -eq $false) -or ($nic.name -like "*acc*")) {
                $ip = $nic.properties.ipConfigurations[0].properties.privateIPAddress
                if ($ip) { $ips += $ip }
            }
        }
        if (-not $ips) { return $null }

        # Self first, then the remaining peers in stable IP order.
        $others = $ips | Where-Object { $_ -ne $OwnIp } |
            Sort-Object { [version]($_ -replace '(\d+)\.(\d+)\.(\d+)\.(\d+)', '$1.$2.$3.$4') }
        return @($OwnIp) + $others
    } catch {
        return $null
    }
}

# Build a hashtable mapping each VMSS eth1 private IP -> public DNS FQDN
# (e.g. 10.5.64.34 -> vm0.ds16v2server.southcentralus.cloudapp.azure.com) using the
# VM's managed identity. Correlates NICs (privateIP -> instanceId) with public IPs
# (instanceId -> dnsSettings.fqdn). Returns an empty hashtable on any failure so
# callers can fall back to reverse DNS.
function Get-IpFqdnMap {
    $map = @{}
    try {
        $metaUri = "http://169.254.169.254/metadata/instance?api-version=2021-02-01"
        $meta = Invoke-RestMethod -Uri $metaUri -Headers @{ Metadata = "true" } -TimeoutSec 5
        $subscriptionId = $meta.compute.subscriptionId
        $resourceGroup  = $meta.compute.resourceGroupName
        $vmssName       = $meta.compute.vmScaleSetName
        if (-not $vmssName) { return $map }

        $tokenUri = "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://management.azure.com/"
        $tokenResp = Invoke-RestMethod -Uri $tokenUri -Headers @{ Metadata = "true" } -TimeoutSec 5
        $headers = @{ Authorization = "******"; "Content-Type" = "application/json" }

        $base = "https://management.azure.com/subscriptions/$subscriptionId/resourceGroups/$resourceGroup/providers/Microsoft.Compute/virtualMachineScaleSets/$vmssName"

        # privateIP -> instanceId (from NICs)
        $ipToInstance = @{}
        $next = "$base/networkInterfaces?api-version=2022-11-01"
        while ($next) {
            $page = Invoke-RestMethod -Uri $next -Headers $headers -TimeoutSec 30
            foreach ($nic in $page.value) {
                $instId = if ($nic.properties.virtualMachine.id -match '/virtualMachines/(\d+)') { $Matches[1] } else { $null }
                foreach ($cfg in $nic.properties.ipConfigurations) {
                    $pip = $cfg.properties.privateIPAddress
                    if ($pip -and $instId) { $ipToInstance[$pip] = $instId }
                }
            }
            $next = $page.nextLink
        }

        # instanceId -> fqdn (from public IPs)
        $instanceToFqdn = @{}
        $next = "$base/publicIPAddresses?api-version=2022-11-01"
        while ($next) {
            $page = Invoke-RestMethod -Uri $next -Headers $headers -TimeoutSec 30
            foreach ($pub in $page.value) {
                $fqdn = $pub.properties.dnsSettings.fqdn
                $instId = if ($pub.properties.ipConfiguration.id -match '/virtualMachines/(\d+)') { $Matches[1] } else { $null }
                if ($fqdn -and $instId) { $instanceToFqdn[$instId] = $fqdn }
            }
            $next = $page.nextLink
        }

        foreach ($pip in $ipToInstance.Keys) {
            $fqdn = $instanceToFqdn[$ipToInstance[$pip]]
            if ($fqdn) { $map[$pip] = $fqdn }
        }
    } catch {
        return $map
    }
    return $map
}

function Find-Peers {
    param(
        [string]$OwnIp,
        [int]$Prefix,
        [string]$SshUser,
        [int]$Timeout,
        [int]$MaxScan = 0,
        [int]$ExpectedVmCount = 0
    )

    # Preferred path: enumerate real VMSS instance IPs via the Azure API (scales to
    # thousands; all returned NICs belong to this VMSS so no family filtering needed).
    Write-Host "Discovering peers via Azure API (managed identity)..." -ForegroundColor Yellow
    $apiPeers = Get-PeersFromAzure -OwnIp $OwnIp
    if ($apiPeers) {
        Write-Host "  Found $($apiPeers.Count) peer(s) via Azure API (including self)." -ForegroundColor Green
        return $apiPeers
    }

    Write-Host "  Azure API unavailable; falling back to subnet scan." -ForegroundColor DarkYellow
    Write-Host "Discovering peers on eth1 subnet ($OwnIp/$Prefix)..." -ForegroundColor Yellow

    # Get local VMSS prefix to filter out VMs from other scale sets
    $localHostname = hostname
    $localVmssPrefix = if ($localHostname -match '^(.+?)[0-9A-Z]{6}$') { $Matches[1] } else { "" }
    if ($localVmssPrefix) {
        Write-Host "  Local VMSS prefix: $localVmssPrefix" -ForegroundColor DarkGray
    }

    $candidateIps = Get-SubnetIps -Ip $OwnIp -Prefix $Prefix
    $scanNote = if ($MaxScan -gt 0) { " (scan cap: $MaxScan)" } else { "" }
    Write-Host "  Scanning $($candidateIps.Count) candidate IPs (100ms timeout)$scanNote..." -ForegroundColor DarkGray

    # Always include self
    $peers = @($OwnIp)
    Write-Host "  $OwnIp : self ✓" -ForegroundColor DarkGray

    $scanned = 0
    foreach ($ip in $candidateIps) {
        if ($ExpectedVmCount -gt 0 -and $peers.Count -ge $ExpectedVmCount) {
            Write-Host "  Found expected $ExpectedVmCount VM(s); stopping scan." -ForegroundColor Green
            break
        }
        if ($ip -eq $OwnIp) { continue }  # already included

        if ($MaxScan -gt 0 -and $scanned -ge $MaxScan) {
            Write-Host "  Reached scan cap ($MaxScan probes); stopping scan." -ForegroundColor DarkYellow
            break
        }
        $scanned++
        try {
            $tcp = [System.Net.Sockets.TcpClient]::new()
            $task = $tcp.ConnectAsync($ip, 22)
            if ($task.Wait([TimeSpan]::FromMilliseconds(100))) {
                $tcp.Close()

                # Filter by VMSS family if we know our prefix
                if ($localVmssPrefix) {
                    $raw = InvokeSsh -Ip $ip -SshUser $SshUser -Command "hostname" -Timeout $Timeout
                    $peerHostname = ($raw | Where-Object { $_ -is [string] } | Select-Object -Last 1)
                    if ($peerHostname -and $peerHostname -match "^${localVmssPrefix}[0-9A-Z]{6}$") {
                        $peers += $ip
                        Write-Host "  $ip : $peerHostname ✓" -ForegroundColor DarkGray
                    } else {
                        Write-Host "  $ip : $peerHostname (different VMSS, skipped)" -ForegroundColor DarkGray
                    }
                } else {
                    $peers += $ip
                    Write-Host "  $ip : alive" -ForegroundColor DarkGray
                }
            } else {
                $tcp.Dispose()
            }
        } catch {
            # not reachable
        }
    }

    if ($peers.Count -eq 1) {
        Write-Host "  No other peers found (only self)." -ForegroundColor Yellow
    } else {
        Write-Host "  Found $($peers.Count) peer(s) (including self)." -ForegroundColor Green
    }
    return $peers
}

function Test-Ports {
    param([string[]]$Ips, [int]$BasePort, [int]$Count)
    $results = @()
    foreach ($ip in $Ips) {
        $portStatus = @()
        for ($i = 0; $i -lt $Count; $i++) {
            $p = $BasePort + $i
            $listening = $false
            try {
                $tcp = [System.Net.Sockets.TcpClient]::new()
                $task = $tcp.ConnectAsync($ip, $p)
                if ($task.Wait([TimeSpan]::FromSeconds(2))) {
                    $listening = $true
                }
                $tcp.Close()
            } catch { }
            $portStatus += @{ Port = $p; Listening = $listening }
        }
        $results += @{ Ip = $ip; Ports = $portStatus }
    }
    return $results
}

function Show-Discovery {
    param($ProbeResults, [int]$BasePort, [int]$Count)
    # Header
    $header = "  {0,-16}" -f "IP"
    for ($i = 0; $i -lt $Count; $i++) {
        $header += " {0,-10}" -f "Port $($BasePort + $i)"
    }
    Write-Host $header -ForegroundColor Cyan

    $totalListening = 0
    $totalPorts = 0
    foreach ($r in $ProbeResults) {
        $line = "  {0,-16}" -f $r.Ip
        foreach ($ps in $r.Ports) {
            $totalPorts++
            if ($ps.Listening) {
                $totalListening++
                $line += " {0,-10}" -f "listening"
            } else {
                $line += " {0,-10}" -f "---"
            }
        }
        $color = if ($r.Ports | Where-Object { $_.Listening }) { "Green" } else { "White" }
        Write-Host $line -ForegroundColor $color
    }
    Write-Host ""
    Write-Host "  Peers: $($ProbeResults.Count) | Listening: $totalListening/$totalPorts" -ForegroundColor Yellow
}

# Resolve a hostname/FQDN for an IP via reverse DNS. Returns the IP itself if
# no PTR record exists so callers always get a printable label.
function Resolve-HostName {
    param([string]$Ip)
    try {
        $entry = [System.Net.Dns]::GetHostEntry($Ip)
        if ($entry -and $entry.HostName -and $entry.HostName -ne $Ip) {
            return $entry.HostName
        }
    } catch {
        # No PTR record / resolution failed — fall through to returning the IP.
    }
    return $Ip
}

function Test-SshConnectivity {
    param([string[]]$Ips, [string]$SshUser, [int]$Timeout)
    Write-Host "Validating SSH connectivity to $($Ips.Count) VMs..." -ForegroundColor Yellow
    $failed = @()
    foreach ($ip in $Ips) {
        $raw = InvokeSsh -Ip $ip -SshUser $SshUser -Command "echo ok" -Timeout $Timeout
        $result = ($raw | Where-Object { $_ -is [string] } | Select-Object -Last 1)
        if ($result -ne "ok") {
            $failed += $ip
        } else {
            Write-Host "  $ip : reachable" -ForegroundColor DarkGray
        }
    }
    if ($failed.Count -gt 0) {
        Write-Host "ERROR: SSH failed for the following VMs:" -ForegroundColor Red
        $failed | ForEach-Object {
            $name = Resolve-HostName -Ip $_
            if ($name -ne $_) {
                Write-Host "  $_ ($name)" -ForegroundColor Red
            } else {
                Write-Host "  $_ (hostname not resolvable)" -ForegroundColor Red
            }
        }
        throw "Aborting: $($failed.Count) VM(s) unreachable"
    }
    Write-Host "  All $($Ips.Count) VMs reachable." -ForegroundColor Green
}

function Test-VmssFamily {
    param([string[]]$Ips, [string]$SshUser, [int]$Timeout)
    Write-Host "Validating VMSS family membership..." -ForegroundColor Yellow
    $hostnames = @()
    foreach ($ip in $Ips) {
        $raw = InvokeSsh -Ip $ip -SshUser $SshUser -Command "hostname" -Timeout $Timeout
        # Filter out stderr (ErrorRecords) and take the last stdout line
        $hostname = ($raw | Where-Object { $_ -is [string] } | Select-Object -Last 1)
        if (-not $hostname) {
            throw "ERROR: Could not get hostname from $ip"
        }
        $hostnames += @{ Ip = $ip; Hostname = $hostname.Trim() }
    }

    $prefixes = $hostnames | ForEach-Object {
        if ($_.Hostname -match '^(.+?)[0-9A-Z]{6}$') { $Matches[1] } else { $_.Hostname }
    } | Sort-Object -Unique

    if ($prefixes.Count -ne 1) {
        Write-Host "ERROR: VMs belong to different VMSS families:" -ForegroundColor Red
        $hostnames | ForEach-Object { Write-Host "  $($_.Ip) -> $($_.Hostname)" -ForegroundColor Red }
        throw "Aborting: Mixed VMSS families detected ($($prefixes -join ', '))"
    }

    Write-Host "  All VMs belong to VMSS: $($prefixes[0]) ✓" -ForegroundColor Green
    return $prefixes[0]
}

function Confirm-Endpoints {
    param([string[]]$Ips, [int]$BasePort, [int]$InstancesPerVm, [string]$SshUser, [string]$RemoteLog, [int]$Timeout = 5)
    Write-Host "  Probing ports (timeout: ${Timeout}s)..." -ForegroundColor Yellow

    $deadline = (Get-Date).AddSeconds($Timeout)
    $failures = @($Ips)

    while ($failures.Count -gt 0 -and (Get-Date) -lt $deadline) {
        $stillFailing = @()
        foreach ($ip in $failures) {
            $allUp = $true
            for ($i = 0; $i -lt $InstancesPerVm; $i++) {
                $p = $BasePort + $i
                try {
                    $tcp = [System.Net.Sockets.TcpClient]::new()
                    $task = $tcp.ConnectAsync($ip, $p)
                    if (-not $task.Wait([TimeSpan]::FromMilliseconds(200))) {
                        $allUp = $false
                    }
                    $tcp.Close()
                } catch {
                    $allUp = $false
                }
                if (-not $allUp) { break }
            }
            if (-not $allUp) { $stillFailing += $ip }
        }
        $failures = $stillFailing
        if ($failures.Count -gt 0) { Start-Sleep -Milliseconds 500 }
    }

    # Print final status
    foreach ($ip in $Ips) {
        if ($ip -in $failures) {
            Write-Host "  [$ip] FAILED" -ForegroundColor Red
        } else {
            $ports = @()
            for ($i = 0; $i -lt $InstancesPerVm; $i++) { $ports += ($BasePort + $i) }
            $portStr = $ports -join ","
            Write-Host "  [$ip]:$portStr ok" -ForegroundColor Green
        }
    }

    # Fetch logs from failed nodes
    if ($failures.Count -gt 0 -and $RemoteLog) {
        Write-Host ""
        Write-Host "Fetching logs from failed VMs..." -ForegroundColor Yellow
        foreach ($ip in $failures) {
            Write-Host "  --- [$ip] $RemoteLog ---" -ForegroundColor Red
            $log = InvokeSsh -Ip $ip -SshUser $SshUser -Command "cat $RemoteLog 2>/dev/null" -Timeout 5
            $log | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }
        }
        Write-Host ""
        Write-Host "WARNING: $($failures.Count) VM(s) failed." -ForegroundColor Red
    } elseif ($failures.Count -eq 0) {
        Write-Host "  All $($Ips.Count) VMs running successfully." -ForegroundColor Green
    }

    return $failures
}

function Invoke-ParallelMcluster {
    param([string[]]$Ips, [string]$SshUser, [string]$MclusterArgs, [string]$OwnIp, [int]$BasePort, [int]$InstancesPerVm)
    Write-Host ""
    Write-Host "Running mcluster.ps1 on $($Ips.Count) VMs..." -ForegroundColor Yellow
    Write-Host "  Command: mcluster.ps1 $MclusterArgs" -ForegroundColor DarkGray

    $remoteLog = "/tmp/mcluster-deploy.log"

    foreach ($ip in $Ips) {
        if ($ip -eq $OwnIp) {
            Write-Host "  [$ip] (local) ..." -NoNewline
            Invoke-Expression "mcluster.ps1 $MclusterArgs" 2>&1 | Out-Null
            Write-Host " dispatched" -ForegroundColor Green
        } else {
            Write-Host "  [$ip] (ssh) ..." -NoNewline
            InvokeSsh -Ip $ip -SshUser $SshUser -Command "mcluster.ps1 $MclusterArgs" -Background | Out-Null
            Write-Host " dispatched" -ForegroundColor Green
        }
    }

    Write-Host ""
    Write-Host "  Summary: dispatched 'mcluster.ps1 $MclusterArgs' to $($Ips.Count) VM(s)" -ForegroundColor Cyan

    # Skip port probing if InstancesPerVm is 0 (e.g., stop action)
    if ($InstancesPerVm -le 0) { return @() }

    # Wait a moment for processes to start
    Write-Host ""
    Write-Host "  Waiting for instances to come up..." -ForegroundColor DarkGray
    Start-Sleep -Seconds 3

    $failures = Confirm-Endpoints -Ips $Ips -BasePort $BasePort -InstancesPerVm $InstancesPerVm -SshUser $SshUser -RemoteLog $remoteLog
    return $failures
}

function Wait-ForEndpoints {
    param([string[]]$Endpoints, [int]$Timeout)
    Write-Host ""
    Write-Host "Waiting for $($Endpoints.Count) endpoints to be reachable (timeout: ${Timeout}s)..." -ForegroundColor Yellow

    $deadline = (Get-Date).AddSeconds($Timeout)
    $pending = [System.Collections.Generic.List[string]]::new($Endpoints)

    while ($pending.Count -gt 0 -and (Get-Date) -lt $deadline) {
        $stillPending = [System.Collections.Generic.List[string]]::new()
        foreach ($ep in $pending) {
            $parts = $ep -split ':'
            $ip = $parts[0]; $port = [int]$parts[1]
            try {
                $tcp = [System.Net.Sockets.TcpClient]::new()
                $tcp.Connect($ip, $port)
                $tcp.Close()
            } catch {
                $stillPending.Add($ep)
            }
        }
        if ($stillPending.Count -gt 0) {
            Start-Sleep -Seconds 2
        }
        $pending = $stillPending
    }

    if ($pending.Count -gt 0) {
        Write-Host "ERROR: Timed out waiting for endpoints:" -ForegroundColor Red
        $pending | ForEach-Object { Write-Host "  $_" -ForegroundColor Red }
        throw "Aborting: $($pending.Count) endpoint(s) not reachable"
    }

    Write-Host "  All $($Endpoints.Count) endpoints responding." -ForegroundColor Green
}

function New-ClusterManual {
    param([string[]]$Endpoints, [int]$ReplicaCount, [string]$Cli)

    $totalNodes = $Endpoints.Count
    $mastersCount = [math]::Floor($totalNodes / ($ReplicaCount + 1))
    if ($mastersCount -lt 1) { $mastersCount = 1 }
    $masters  = @($Endpoints[0..($mastersCount - 1)])
    $replicas = @(if ($totalNodes -gt $mastersCount) { $Endpoints[$mastersCount..($totalNodes - 1)] })

    Write-Host "  Primaries: $($masters -join ', ')" -ForegroundColor DarkGray
    if ($replicas.Count -gt 0) { Write-Host "  Replicas:  $($replicas -join ', ')" -ForegroundColor DarkGray }

    # Follow the redis-cli --cluster create sequence exactly, operating on nodes
    # while they are still ISOLATED (before MEET):
    #   1) ADDSLOTS on primaries        ("Nodes configuration updated")
    #   2) SET-CONFIG-EPOCH on each node ("Assign a different config epoch to each node")
    #   3) MEET to join                  ("Sending CLUSTER MEET messages to join the cluster")
    #   4) REPLICATE on replicas
    # Step 2 is critical: SET-CONFIG-EPOCH only works while currentEpoch is 0, and
    # distinct epochs are required for the cluster to converge and accept replicas.

    # 1) Assign slots across primaries (contiguous ranges) while isolated
    Write-Host "  Assigning 16384 slots across $($masters.Count) primary(ies)..." -ForegroundColor Yellow
    for ($i = 0; $i -lt $masters.Count; $i++) {
        $m = $masters[$i] -split ':'
        $slotStart = [math]::Floor(16384 * $i / $masters.Count)
        $slotEnd   = [math]::Floor(16384 * ($i + 1) / $masters.Count) - 1
        bash -c "$Cli -h $($m[0]) -p $($m[1]) CLUSTER ADDSLOTSRANGE $slotStart $slotEnd" 2>&1 | Out-Null
    }

    # 2) Assign a distinct config epoch to each node (1-based) while isolated
    Write-Host "  Assigning distinct config epochs to $totalNodes node(s)..." -ForegroundColor Yellow
    for ($i = 0; $i -lt $totalNodes; $i++) {
        $n = $Endpoints[$i] -split ':'
        $epoch = $i + 1
        $res = bash -c "$Cli -h $($n[0]) -p $($n[1]) CLUSTER SET-CONFIG-EPOCH $epoch" 2>&1
        $resStr = ($res | Out-String).Trim()
        if ($resStr -notmatch 'OK') {
            Write-Host "    WARNING: SET-CONFIG-EPOCH $epoch on $($Endpoints[$i]) returned: $resStr" -ForegroundColor Yellow
        }
    }

    # 3) Gossip: MEET every other node from the first node
    $first = $Endpoints[0] -split ':'
    Write-Host "  Sending CLUSTER MEET from $($Endpoints[0]) to $($totalNodes - 1) peer(s)..." -ForegroundColor Yellow
    for ($i = 1; $i -lt $totalNodes; $i++) {
        $n = $Endpoints[$i] -split ':'
        bash -c "$Cli -h $($first[0]) -p $($first[1]) CLUSTER MEET $($n[0]) $($n[1])" 2>&1 | Out-Null
    }
    Start-Sleep -Seconds 2

    # 4) Attach replicas to primaries (round-robin)
    if ($replicas.Count -gt 0) {
        $masterIds = @()
        foreach ($m in $masters) {
            $hp = $m -split ':'
            $id = (bash -c "$Cli -h $($hp[0]) -p $($hp[1]) CLUSTER MYID" 2>&1 | Select-Object -First 1).Trim()
            $masterIds += $id
        }
        Write-Host "  Attaching $($replicas.Count) replica(s) to primaries..." -ForegroundColor Yellow
        for ($i = 0; $i -lt $replicas.Count; $i++) {
            $r = $replicas[$i] -split ':'
            $mid = $masterIds[$i % $masterIds.Count]

            # Wait until this replica has learned the master node via gossip;
            # CLUSTER REPLICATE fails with "Unknown node" until it does.
            $known = $false
            for ($try = 0; $try -lt 15; $try++) {
                $nodes = bash -c "$Cli -h $($r[0]) -p $($r[1]) CLUSTER NODES" 2>&1
                if ($nodes -match [regex]::Escape($mid)) { $known = $true; break }
                Start-Sleep -Seconds 1
            }
            if (-not $known) {
                Write-Host "    WARNING: $($replicas[$i]) has not learned master $mid via gossip; trying REPLICATE anyway" -ForegroundColor Yellow
            }

            # Issue REPLICATE with retries and surface the result.
            $ok = $false; $resStr = ""
            for ($try = 0; $try -lt 10; $try++) {
                $res = bash -c "$Cli -h $($r[0]) -p $($r[1]) CLUSTER REPLICATE $mid" 2>&1
                $resStr = ($res | Out-String).Trim()
                if ($resStr -match 'OK') { $ok = $true; break }
                Start-Sleep -Seconds 1
            }
            if ($ok) {
                Write-Host "    $($replicas[$i]) -> replicates $mid ✓" -ForegroundColor DarkGray
            } else {
                Write-Host "    ERROR: $($replicas[$i]) failed to replicate ${mid}: $resStr" -ForegroundColor Red
            }
        }
    }
    Start-Sleep -Seconds 2
}

function New-Cluster {
    param([string[]]$Endpoints, [int]$ReplicaCount, [string]$Sys, [switch]$Manual)
    Write-Host ""
    $mode = if ($Manual) { "MANUAL" } else { "auto" }
    Write-Host "Forming cluster [$mode] ($($Endpoints.Count) nodes, $ReplicaCount replica(s) per primary)..." -ForegroundColor Yellow

    $cli = if ($Sys -eq "valkey") { "valkey-cli" } else { "redis-cli" }

    if ($Manual) {
        New-ClusterManual -Endpoints $Endpoints -ReplicaCount $ReplicaCount -Cli $cli
    } else {
        $endpointStr = $Endpoints -join " "
        $cmd = "$cli --cluster create $endpointStr --cluster-replicas $ReplicaCount --cluster-yes"
        Write-Host "  -> $cmd" -ForegroundColor DarkGray
        $output = bash -c $cmd 2>&1
        $output | ForEach-Object { Write-Host "  $_" }
    }

    # Verify cluster state
    $firstEp = $Endpoints[0] -split ':'
    Write-Host ""
    Write-Host "Verifying cluster state..." -ForegroundColor Yellow
    $verifyCmd = if ($Sys -eq "valkey") { "valkey-cli" } else { "redis-cli" }
    $clusterInfo = bash -c "$verifyCmd -h $($firstEp[0]) -p $($firstEp[1]) CLUSTER INFO" 2>&1
    $stateLine = $clusterInfo | Where-Object { $_ -match "cluster_state" }
    $slotsLine = $clusterInfo | Where-Object { $_ -match "cluster_slots_ok" }

    Write-Host "  $stateLine"
    Write-Host "  $slotsLine"

    # Check if slots are properly assigned
    $slotsOk = if ($slotsLine -match "cluster_slots_ok:(\d+)") { [int]$Matches[1] } else { 0 }

    if ($slotsOk -lt 16384) {
        Write-Host "  Slots not fully assigned ($slotsOk/16384), running ADDSLOTSRANGE..." -ForegroundColor Yellow
        # Assign slot ranges across endpoints (round-robin for multi-node, all to single node)
        $totalNodes = $Endpoints.Count
        for ($i = 0; $i -lt $totalNodes; $i++) {
            $ep = $Endpoints[$i] -split ':'
            $slotStart = [math]::Floor(16384 * $i / $totalNodes)
            $slotEnd = [math]::Floor(16384 * ($i + 1) / $totalNodes) - 1
            bash -c "$verifyCmd -h $($ep[0]) -p $($ep[1]) CLUSTER ADDSLOTSRANGE $slotStart $slotEnd" 2>&1 | Out-Null
        }
        # Re-verify
        Start-Sleep -Seconds 1
        $clusterInfo = bash -c "$verifyCmd -h $($firstEp[0]) -p $($firstEp[1]) CLUSTER INFO" 2>&1
        $stateLine = $clusterInfo | Where-Object { $_ -match "cluster_state" }
        $slotsLine = $clusterInfo | Where-Object { $_ -match "cluster_slots_ok" }
        $slotsOk = if ($slotsLine -match "cluster_slots_ok:(\d+)") { [int]$Matches[1] } else { 0 }
        Write-Host "  $stateLine"
        Write-Host "  $slotsLine"
    }

    if ($stateLine -match "cluster_state:ok" -and $slotsOk -eq 16384) {
        Write-Host "  Cluster formed successfully ✓" -ForegroundColor Green
    } elseif ($stateLine -match "cluster_state:ok") {
        Write-Host "  WARNING: Cluster state ok but only $slotsOk/16384 slots assigned" -ForegroundColor Red
    } else {
        Write-Host "  WARNING: Cluster state is not 'ok'" -ForegroundColor Red
    }
}

# --- Peer Cache ---

$PeerCacheFile = "$HOME/.cluster-deploy-peers.json"

function Save-PeerCache {
    param([string[]]$Ips, [string]$OwnIp)
    $cache = [ordered]@{
        schemaVersion = 1
        source = 'guest-discovery'
        generatedAt = (Get-Date).ToUniversalTime().ToString('o')
        coordinatorEth1Ip = $OwnIp
        peers = @($Ips | ForEach-Object { [ordered]@{ eth1Ip = $_ } })
    }
    $tmp = "$PeerCacheFile.$([guid]::NewGuid().ToString('N')).tmp"
    $cache | ConvertTo-Json -Depth 6 | Set-Content $tmp
    Move-Item -LiteralPath $tmp -Destination $PeerCacheFile -Force
    Write-Host "  Peer cache saved to $PeerCacheFile" -ForegroundColor DarkGray
}

function Get-PeerCache {
    if (-not (Test-Path $PeerCacheFile)) { return $null }
    try {
        $cache = Get-Content $PeerCacheFile -Raw | ConvertFrom-Json
    } catch {
        Write-Host "  Ignoring unreadable peer cache: $($_.Exception.Message)" -ForegroundColor DarkYellow
        return $null
    }
    if ($cache.schemaVersion -ne 1 -or -not $cache.peers) {
        Write-Host "  Ignoring unsupported peer cache format." -ForegroundColor DarkYellow
        return $null
    }
    $ips = @($cache.peers | ForEach-Object { $_.eth1Ip } | Where-Object { $_ })
    if ($ips.Count -eq 0) { return $null }
    Write-Host "  Using cached peers from $($cache.generatedAt) (source: $($cache.source))" -ForegroundColor DarkGray
    Write-Host "  (run '-Action discover' to refresh)" -ForegroundColor DarkGray
    Write-Host "  Peers: $($ips -join ', ')" -ForegroundColor Cyan
    return @{ Ips = $ips; OwnIp = $cache.coordinatorEth1Ip; Manifest = $cache }
}

function Convert-Ipv4ToUInt32 {
    param([string]$Ip)
    $parts = $Ip -split '\.'
    if ($parts.Count -ne 4) { throw "Invalid IPv4 address: $Ip" }
    return [uint32](
        ([uint32]$parts[0] -shl 24) -bor
        ([uint32]$parts[1] -shl 16) -bor
        ([uint32]$parts[2] -shl 8) -bor
        [uint32]$parts[3]
    )
}

function Test-IpInSubnet {
    param([string]$Ip, [string]$NetworkIp, [int]$Prefix)
    $mask = if ($Prefix -eq 0) { [uint32]0 } else { [uint32]::MaxValue -shl (32 - $Prefix) }
    return ((Convert-Ipv4ToUInt32 $Ip) -band $mask) -eq
        ((Convert-Ipv4ToUInt32 $NetworkIp) -band $mask)
}

function Get-LocalVmssIdentity {
    try {
        return Invoke-RestMethod `
            -Uri 'http://169.254.169.254/metadata/instance?api-version=2021-02-01' `
            -Headers @{ Metadata = 'true' } -TimeoutSec 5
    } catch {
        return $null
    }
}

function Import-PeerManifest {
    param([string]$Content, [int]$ExpectedVmCount, [string]$SshUser, [int]$Timeout)

    if (-not $Content) { throw "ERROR: -PeerManifestContent is required for import-peers." }
    try {
        $json = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($Content))
        $manifest = $json | ConvertFrom-Json
    } catch {
        throw "ERROR: Invalid peer manifest: $($_.Exception.Message)"
    }
    if ($manifest.schemaVersion -ne 1 -or $manifest.source -ne 'azure-cli' -or -not $manifest.peers) {
        throw "ERROR: Unsupported or incomplete peer manifest."
    }

    $eth1 = Get-OwnEth1Info
    $identity = Get-LocalVmssIdentity
    if ($identity) {
        if ($manifest.subscriptionId -ne $identity.compute.subscriptionId -or
            $manifest.resourceGroup -ne $identity.compute.resourceGroupName -or
            $manifest.vmssName -ne $identity.compute.vmScaleSetName) {
            throw "ERROR: Peer manifest targets a different subscription, resource group, or VMSS."
        }
    }

    $peers = @($manifest.peers)
    if ($ExpectedVmCount -gt 0 -and $peers.Count -ne $ExpectedVmCount) {
        throw "ERROR: Expected $ExpectedVmCount VM(s), but the peer manifest contains $($peers.Count)."
    }
    $ips = @($peers | ForEach-Object { [string]$_.eth1Ip })
    if ($ips -contains $null -or $ips -contains '') { throw "ERROR: Peer manifest contains an empty eth1 address." }
    if (($ips | Sort-Object -Unique).Count -ne $ips.Count) { throw "ERROR: Peer manifest contains duplicate eth1 addresses." }
    $instanceIds = @($peers | ForEach-Object { [string]$_.instanceId })
    if (($instanceIds | Sort-Object -Unique).Count -ne $instanceIds.Count) { throw "ERROR: Peer manifest contains duplicate instance IDs." }
    if ($eth1.Ip -notin $ips) { throw "ERROR: Coordinator eth1 address $($eth1.Ip) is absent from the peer manifest." }
    foreach ($ip in $ips) {
        if (-not (Test-IpInSubnet -Ip $ip -NetworkIp $eth1.Ip -Prefix $eth1.Prefix)) {
            throw "ERROR: Peer address '$ip' is outside coordinator subnet $($eth1.Ip)/$($eth1.Prefix)."
        }
    }

    Test-SshConnectivity -Ips $ips -SshUser $SshUser -Timeout $Timeout
    Test-VmssFamily -Ips $ips -SshUser $SshUser -Timeout $Timeout | Out-Null

    $manifest | Add-Member -NotePropertyName coordinatorEth1Ip -NotePropertyValue $eth1.Ip -Force
    $manifest | Add-Member -NotePropertyName validatedAt -NotePropertyValue ((Get-Date).ToUniversalTime().ToString('o')) -Force
    $tmp = "$PeerCacheFile.$([guid]::NewGuid().ToString('N')).tmp"
    $manifest | ConvertTo-Json -Depth 8 | Set-Content $tmp
    Move-Item -LiteralPath $tmp -Destination $PeerCacheFile -Force

    Write-Host "Peer inventory validated and cached: $PeerCacheFile" -ForegroundColor Green
    Write-Host "  VMSS:  $($manifest.vmssName)"
    Write-Host "  Peers: $($ips -join ', ')"
    Write-Host "PEER_IMPORT_OK"
}

# --- Resolve Peers ---

function Resolve-Peers {
    param([int]$VmCount, [string]$User, [int]$SshTimeout, [switch]$ForceDiscover, [int]$MaxScan = 0)

    # Try cache first (unless forced)
    $peers = $null
    if (-not $ForceDiscover) {
        $cached = Get-PeerCache
        if ($cached) {
            $peers = $cached.Ips
        }
    }

    # Discovery mode if no cache
    if (-not $peers) {
        $eth1 = Get-OwnEth1Info
        $peers = Find-Peers -OwnIp $eth1.Ip -Prefix $eth1.Prefix -SshUser $User `
            -Timeout $SshTimeout -MaxScan $MaxScan -ExpectedVmCount $VmCount
        Save-PeerCache -Ips $peers -OwnIp $eth1.Ip
        Test-VmssFamily -Ips $peers -SshUser $User -Timeout $SshTimeout
    }

    if ($VmCount -gt 0 -and $VmCount -ne $peers.Count) {
        throw "ERROR: Expected $VmCount VM(s), but peer discovery returned $($peers.Count)."
    }

    Test-SshConnectivity -Ips $peers -SshUser $User -Timeout $SshTimeout
    $ownIp = if ($cached) { $cached.OwnIp } else { $eth1.Ip }
    return @{ Ips = $peers; OwnIp = $ownIp }
}

# --- Main ---

Write-Host "==== cluster-deploy ($Action) ====" -ForegroundColor Cyan

switch ($Action) {
    "import-peers" {
        Import-PeerManifest -Content $PeerManifestContent -ExpectedVmCount $VmCount `
            -SshUser $User -Timeout $SshTimeout
    }

    "discover" {
        $peerInfo = Resolve-Peers -VmCount $VmCount -User $User -SshTimeout $SshTimeout -ForceDiscover -MaxScan $MaxScan
        $ips = $peerInfo.Ips

        if ($InstancePerVm -gt 0) {
            Write-Host ""
            Write-Host "Probing ports on discovered peers..." -ForegroundColor Yellow
            $probeResults = Test-Ports -Ips $ips -BasePort $Port -Count $InstancePerVm

            Write-Host ""
            Show-Discovery -ProbeResults $probeResults -BasePort $Port -Count $InstancePerVm
        } else {
            Write-Host ""
            Write-Host "Discovered peers:" -ForegroundColor Yellow
            $ips | ForEach-Object { Write-Host "  $_" }
            Write-Host ""
            Write-Host "  Total: $($ips.Count) peer(s)" -ForegroundColor Green
            Write-Host "  (use -InstancePerVm to probe ports)" -ForegroundColor DarkGray
        }
    }

    "start" {
        if (-not $System) { throw "ERROR: -System is required for start." }
        if (-not $Conf -and -not $ConfContent) { throw "ERROR: -Conf or -ConfContent is required for start." }
        if ($Conf -and $ConfContent) { throw "ERROR: -Conf and -ConfContent are mutually exclusive; specify only one." }
        if (-not $InstancePerVm) { throw "ERROR: -InstancePerVm is required for start." }

        $peerInfo = Resolve-Peers -VmCount $VmCount -User $User -SshTimeout $SshTimeout -MaxScan $MaxScan
        $ips = $peerInfo.Ips
        $ownIp = $peerInfo.OwnIp

        Write-Host ""
        Write-Host "  Peers:         $($ips -join ', ')"
        Write-Host "  System:        $System"
        if ($ConfContent) { Write-Host "  Conf:          $ConfName (shipped)" } else { Write-Host "  Conf:          $Conf" }
        Write-Host "  InstancePerVm: $InstancePerVm"
        Write-Host "  Port:          $Port"
        Write-Host "  Clean:         $Clean"
        Write-Host "  NoCluster:     $NoCluster"
        Write-Host ""

        # Build mcluster arguments
        $mclusterArgs = "-Action start -System $System -Nodes $InstancePerVm"
        if ($ConfContent) { $mclusterArgs += " -ConfContent $ConfContent -ConfName $ConfName" }
        elseif ($Conf) { $mclusterArgs += " -Conf $Conf" }
        if ($Clean) { $mclusterArgs += " -Clean" }
        if ($NoCluster) { $mclusterArgs += " -NoCluster" }

        # Run on all peers
        $failures = Invoke-ParallelMcluster -Ips $ips -SshUser $User -MclusterArgs $mclusterArgs -OwnIp $ownIp -BasePort $Port -InstancesPerVm $InstancePerVm

        if ($failures.Count -gt 0) {
            Write-Host ""
            Write-Host "WARNING: Some VMs failed. Cluster may be incomplete." -ForegroundColor Red
        }
    }

    "setup" {
        if (-not $System) { throw "ERROR: -System is required for setup." }
        if (-not $InstancePerVm) { throw "ERROR: -InstancePerVm is required for setup." }

        $peerInfo = Resolve-Peers -VmCount $VmCount -User $User -SshTimeout $SshTimeout -MaxScan $MaxScan
        $ips = $peerInfo.Ips

        # Build endpoint list
        $endpoints = @()
        foreach ($ip in $ips) {
            for ($p = 0; $p -lt $InstancePerVm; $p++) {
                $endpoints += "${ip}:$($Port + $p)"
            }
        }

        # Verify all endpoints are listening
        Write-Host ""
        Write-Host "Verifying all endpoints are listening..." -ForegroundColor Yellow
        $probeResults = Test-Ports -Ips $ips -BasePort $Port -Count $InstancePerVm
        $notListening = @()
        foreach ($r in $probeResults) {
            foreach ($ps in $r.Ports) {
                if (-not $ps.Listening) {
                    $notListening += "$($r.Ip):$($ps.Port)"
                }
            }
        }

        if ($notListening.Count -gt 0) {
            Write-Host "ERROR: The following endpoints are not listening:" -ForegroundColor Red
            $notListening | ForEach-Object { Write-Host "  $_" -ForegroundColor Red }
            throw "Aborting setup: $($notListening.Count) endpoint(s) not ready. Run 'start' first."
        }

        Write-Host "  All $($endpoints.Count) endpoints listening ✓" -ForegroundColor Green

        # Form cluster
        if ($NoCluster) {
            Write-Host ""
            Write-Host "NOTE: -NoCluster specified, skipping cluster formation." -ForegroundColor Yellow
        } else {
            New-Cluster -Endpoints $endpoints -ReplicaCount $Replicas -Sys $System -Manual:$CreateManual
        }
    }

    "stop" {
        if (-not $System) { throw "ERROR: -System is required for stop." }

        $peerInfo = Resolve-Peers -VmCount $VmCount -User $User -SshTimeout $SshTimeout -MaxScan $MaxScan
        $ips = $peerInfo.Ips
        $ownIp = $peerInfo.OwnIp

        Write-Host ""
        Write-Host "  Peers:  $($ips -join ', ')"
        Write-Host "  System: $System"
        Write-Host ""

        $mclusterArgs = "-Action stop -System $System"
        $failures = Invoke-ParallelMcluster -Ips $ips -SshUser $User -MclusterArgs $mclusterArgs -OwnIp $ownIp -BasePort $Port -InstancesPerVm 0
    }
}

Write-Host ""
Write-Host "==== Done ====" -ForegroundColor Cyan
