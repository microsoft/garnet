#!/usr/bin/env pwsh
[CmdletBinding()]
<#
.SYNOPSIS
    Spawns background SSH sessions to run Resp.benchmark on remote VMs.

.DESCRIPTION
    Reads benchmark parameters from a config file and launches SSH sessions
    via Start-Process with Tee-Object for local capture. Supports multiple
    client VMs by specifying a base hostname and count. Automatically aggregates
    results when all instances complete.

    Use -Verbose to spawn Windows Terminal panes for visual inspection.
    Without -Verbose, runs inline and aggregates results automatically.

.EXAMPLE
    .\resp-bench.ps1
    .\resp-bench.ps1 -ConfigFile .\my-bench.conf
    .\resp-bench.ps1 -ConfigFile .\my-bench.conf -Verbose
#>
param(
    [string]$ConfigFile = "$PSScriptRoot\bench-cluster.conf",
    [string[]]$ClientHosts,
    [string]$ClientMachineHostnames,
    [string]$ClientMachineCount = '1',
    [ValidateRange(1, 2147483647)][int]$Multiplier = 1,
    [string]$SshUser,
    [string]$SshKey,
    [string]$Server,
    [ValidateRange(1, 65535)][int]$Port,
    [switch]$Background,
    [switch]$Detail,
    [switch]$Monitor,
    [switch]$PrintOutput,
    [switch]$Tls,
    [switch]$Help
)

if ($Help) {
    Write-Host "Usage: resp-bench.ps1 [-ConfigFile <path>] [-Detail] [-Monitor] [-Background] [-Tls] [-Verbose]"
    Write-Host ""
    Write-Host "Forwards a native JSON .conf file unchanged and runs"
    Write-Host "Resp.benchmark on remote VMs via SSH."
    Write-Host ""
    Write-Host "Without -Verbose: runs inline in parallel, aggregates results."
    Write-Host "With -Verbose: spawns Windows Terminal panes for visual inspection."
    Write-Host "-ClientHosts supplies exact clients (one workload each), overriding host counts/multiplier."
    Write-Host "-SshUser/-SshKey/-Server/-Port override connection settings for declarative execution."
    Write-Host "-PrintOutput prints captured benchmark-client output inline; failed output is always printed."
    Write-Host ""
    Write-Host "  -Detail   During the run, show a live completed/pending status view"
    Write-Host "            listing which instances are still running. After the run,"
    Write-Host "            print per-instance throughput and completion duration, plus"
    Write-Host "            a list of the slowest instances. A fastest/avg/slowest"
    Write-Host "            duration summary is always shown in inline mode."
    Write-Host ""
    Write-Host "  -Monitor  Show only the live completed/pending status view while the"
    Write-Host "            benchmark runs (a subset of -Detail). Does not print the"
    Write-Host "            full per-instance result breakdown after the run."
    Write-Host ""
    Write-Host "Use Resp.benchmark --help for native option names; JSON keys use Options property names."
    Write-Host "SSH placement uses -ClientHosts or -ClientMachineHostnames/-ClientMachineCount/-Multiplier."
    return
}

$ErrorActionPreference = 'Stop'
# --- Load shared utilities ---
. "$PSScriptRoot\utils.ps1"

$configJson = Get-Content -LiteralPath $ConfigFile -Raw
$document = [System.Text.Json.JsonDocument]::Parse($configJson)
$document.Dispose()
$config = $configJson | ConvertFrom-Json -AsHashtable
if ($config -isnot [System.Collections.IDictionary]) { throw 'Benchmark config must be a JSON object.' }

# --- Resolve parameters ---
# --- Resolve SSH keys from security/manifest.json ---
$manifestPath = Join-Path $PSScriptRoot "..\..\01-resources\security\manifest.json"
if ($SshKey) {
    if (-not (Test-Path -LiteralPath $SshKey -PathType Leaf)) { throw "SSH key not found: $SshKey" }
} elseif (Test-Path $manifestPath) {
    . (Join-Path $PSScriptRoot "..\..\01-resources\security\ssh-key-utils.ps1")
    try {
        $manifest = Get-SshKeyManifest -ManifestPath $manifestPath
        $sshKey = Resolve-SshUserPrivateKey -Manifest $manifest
    } catch {
        Write-Error $_.Exception.Message
        exit 1
    }
} else {
    $sshKey = "$env:USERPROFILE\.ssh\id_ed25519"
    if (-not (Test-Path -LiteralPath $sshKey -PathType Leaf)) {
        throw 'SSH key configuration is missing. Supply -SshKey or configure the security manifest.'
    }
}
$sshUser = if ($SshUser) { $SshUser } else { 'guser' }
$multiplier = if ($ClientHosts) { 1 } else { $Multiplier }
$benchHost = if ($Server) { $Server } else { $config["Address"] ?? '127.0.0.1' }
$benchPort = if ($Port) { [string]$Port } else { [string]($config["Port"] ?? 6379) }
$runtime = $config["RunTime"] ?? 15
$clusterBench = if ($config["ClusterBench"]) { 'true' } else { 'false' }
$tlsEnabled = $Tls -or $config["EnableTLS"]
$tlsHost = $config["TlsHost"] ?? $(if ($Tls) { 'azurebench-server' } else { 'GarnetTest' })
if ([Uri]::CheckHostName($benchHost) -eq [UriHostNameType]::Unknown) { throw "Invalid benchmark target: $benchHost" }

if ($tlsEnabled -and $tlsHost -notmatch '^[a-zA-Z0-9](?:[a-zA-Z0-9.-]*[a-zA-Z0-9])?$') {
    Write-Error "TlsHost is not a valid DNS name: '$tlsHost'."
    exit 1
}

# --- Derive host list from base + count ---
if (-not $ClientHosts -and -not $ClientMachineHostnames) {
    throw 'Supply -ClientHosts or -ClientMachineHostnames, or use benchmark.ps1 with a declarative definition.'
}
$sshHosts = @(Get-BenchmarkClientHosts -ClientHosts $ClientHosts -HostBase $ClientMachineHostnames -HostCount $ClientMachineCount -Multiplier ([string]$multiplier))

# --- Build benchmark command ---
$nativeOverrides = @()
if ($Server) { $nativeOverrides += @('--host', $Server) }
if ($Port) { $nativeOverrides += @('--port', [string]$Port) }
if ($Tls) {
    $nativeOverrides += @('--tls', '--tlshost', $tlsHost,
        '--cert-file-name', '/opt/azurebench/tls/client.pfx',
        '--cert-password-file', '/opt/azurebench/tls/client.password',
        '--issuer-certificate-path', '/opt/azurebench/tls/ca.crt')
}
$benchCmd = "Resp.benchmark --config <uploaded $([IO.Path]::GetFileName($ConfigFile))> $($nativeOverrides -join ' ')"
$remoteBenchCommand = New-RespBenchmarkRemoteCommand -ConfigFile $ConfigFile -Arguments $nativeOverrides
$validateCommand = New-RespBenchmarkRemoteCommand -ConfigFile $ConfigFile -Arguments ($nativeOverrides + '--validate-config')

# --- Probe server info ---
$probeOpts = @('-n', '-o', 'ConnectTimeout=10', '-o', 'StrictHostKeyChecking=no', '-o', 'BatchMode=yes')
$probeHost = $sshHosts | Select-Object -First 1

foreach ($clientHost in ($sshHosts | Select-Object -Unique)) {
    $validationOutput = ssh -i $sshKey @probeOpts "${sshUser}@${clientHost}" $validateCommand 2>&1
    if ($LASTEXITCODE -ne 0) { throw "Native benchmark configuration validation failed on '${clientHost}': $($validationOutput -join "`n")" }
}

if ($tlsEnabled) {
    $tlsPreflightScript = @"
`$ErrorActionPreference = 'Stop'
`$deploymentFile = '/opt/deploy-actions/deployment.env'
if (-not (Test-Path `$deploymentFile)) { throw "Deployment role file not found: `$deploymentFile" }
`$roleLine = Select-String -Path `$deploymentFile -Pattern '^DEPLOYMENT_ROLE=' | Select-Object -First 1
`$role = if (`$roleLine) { (`$roleLine.Line -replace '^DEPLOYMENT_ROLE=', '').Trim('"') } else { '' }
if (`$role -ne 'client') { throw "Resp.benchmark TLS requires DEPLOYMENT_ROLE=client; this node is '`$role'." }
foreach (`$path in @('/opt/azurebench/tls/client.pfx', '/opt/azurebench/tls/client.password', '/opt/azurebench/tls/ca.crt', '/opt/azurebench/tls/metadata.json')) {
    if (-not (Test-Path `$path -PathType Leaf)) { throw "Required TLS file not found: `$path" }
}
`$metadata = Get-Content /opt/azurebench/tls/metadata.json -Raw | ConvertFrom-Json
if (`$metadata.status -ne 'complete') { throw 'TLS metadata is incomplete.' }
if (`$metadata.targetHost -ne '$tlsHost') { throw "TLS metadata target '`$(`$metadata.targetHost)' does not match '$tlsHost'." }
if (-not (Get-Command Resp.benchmark -ErrorAction SilentlyContinue)) { throw 'Resp.benchmark is not installed.' }
if (-not (Get-Command redis-cli-tls -ErrorAction SilentlyContinue)) { throw 'redis-cli-tls is not installed.' }
`$respHelp = Resp.benchmark --help 2>&1 | Out-String
if (`$respHelp -notmatch '--cert-password-file' -or `$respHelp -notmatch '--issuer-certificate-path') {
    throw 'Resp.benchmark does not support AzureBench TLS options; redeploy the client tools.'
}
"@
    $tlsPreflightEncoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($tlsPreflightScript))
    Write-Host "Validating TLS clients..." -ForegroundColor DarkGray
    foreach ($clientHost in ($sshHosts | Select-Object -Unique)) {
        $preflightOutput = ssh -i $sshKey @probeOpts "${sshUser}@${clientHost}" "pwsh -NoProfile -NonInteractive -EncodedCommand $tlsPreflightEncoded" 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Error "TLS preflight failed on '$clientHost': $($preflightOutput -join [Environment]::NewLine)"
            exit 1
        }
    }
}

$redisProbeCommand = if ($tlsEnabled) {
    "redis-cli-tls --tls --cacert /opt/azurebench/tls/ca.crt --sni $tlsHost"
} else {
    "redis-cli"
}

if ($tlsEnabled) {
    $pingOutput = ssh -i $sshKey @probeOpts "${sshUser}@${probeHost}" "$redisProbeCommand -h $benchHost -p $benchPort ping" 2>&1
    if ($LASTEXITCODE -ne 0 -or ($pingOutput | ForEach-Object { "$_".Trim() }) -notcontains 'PONG') {
        Write-Error "TLS connection validation failed for ${benchHost}:${benchPort}: $($pingOutput -join [Environment]::NewLine)"
        exit 1
    }
}

Write-Host "Probing server info (${benchHost}:${benchPort})..." -ForegroundColor DarkGray
$serverInfoRaw = ssh -i $sshKey @probeOpts "${sshUser}@${probeHost}" "$redisProbeCommand -h $benchHost -p $benchPort info server 2>/dev/null" 2>&1
$serverReplRaw = ssh -i $sshKey @probeOpts "${sshUser}@${probeHost}" "$redisProbeCommand -h $benchHost -p $benchPort info replication 2>/dev/null" 2>&1
$serverCpuRaw = ssh -i $sshKey @probeOpts "${sshUser}@${probeHost}" "ssh -o StrictHostKeyChecking=no ${sshUser}@${benchHost} nproc 2>/dev/null" 2>&1
$serverCpuCount = ($serverCpuRaw | ForEach-Object { "$_".Trim() } | Where-Object { $_ -match '^\d+$' } | Select-Object -Last 1)

# Parse replication info to count sublogs (comma-separated values in master_repl_offset)
$serverSublogs = 0
foreach ($line in $serverReplRaw) {
    $lineStr = "$line"
    if ($lineStr -match '^master_repl_offset:(.+)$') {
        $serverSublogs = ($Matches[1].Trim() -split ',').Count
        break
    }
}

$serverInfo = @{}
foreach ($line in $serverInfoRaw) {
    $lineStr = "$line"
    if ($lineStr -match '^(\w+):(.+)$') {
        $serverInfo[$Matches[1]] = $Matches[2].Trim()
    }
}
if ($serverInfo.Count -gt 0) {
    $serverName = $serverInfo["server_name"] ?? "unknown"
    $serverVersion = if ($serverInfo["valkey_version"]) { $serverInfo["valkey_version"] }
                     elseif ($serverInfo["redis_version"]) { $serverInfo["redis_version"] }
                     else { "?" }
    Write-Host ""
    Write-Host "=== Server Info ===" -ForegroundColor Cyan
    Write-Host "  Server  : $serverName $serverVersion"
    if ($serverCpuCount) { Write-Host "  CPUs    : $serverCpuCount" }
    if ($serverSublogs -gt 0) { Write-Host "  Sublogs : $serverSublogs" }
    if ($serverInfo["os"]) { Write-Host "  OS      : $($serverInfo["os"])" }
    if ($serverInfo["tcp_port"]) { Write-Host "  Port    : $($serverInfo["tcp_port"])" }
    if ($serverInfo["uptime_in_seconds"]) { Write-Host "  Uptime  : $($serverInfo["uptime_in_seconds"])s" }
    Write-Host "===================" -ForegroundColor Cyan
    Write-Host ""
} else {
    Write-Host "  WARNING: Could not retrieve server info from ${benchHost}:${benchPort}" -ForegroundColor Yellow
    Write-Host ""
}

# --- Probe benchmark config by running a 1s test on one host ---
$instances = $sshHosts.Count
$uniqueHosts = ($sshHosts | Select-Object -Unique).Count

Write-Host ""
Write-Host "=== Benchmark Command ===" -ForegroundColor Yellow
Write-Host "  $benchCmd" -ForegroundColor Yellow
Write-Host "=========================" -ForegroundColor Yellow
Write-Host ""

$workersPerInstance = $null
Write-Host 'Configuration validated natively without launching a probe workload.'

# --- Print client machine info ---
Show-ClientMachineInfo -SshHosts $sshHosts -SshKey $sshKey -SshUser $sshUser

# --- Print instance configuration ---
Write-Host "=== Instance Configuration ===" -ForegroundColor Cyan
Write-Host "  SSH Key    : $sshKey"
Write-Host "  SSH User   : $sshUser"
Write-Host "  TLS        : $(if ($tlsEnabled) { "enabled ($tlsHost)" } else { "disabled" })"
Write-Host "  Instances  : $instances ($multiplier x $uniqueHosts hosts)"
if ($null -ne $workersPerInstance) {
    $totalWorkers = $workersPerInstance * $instances
    Write-Host "  Workers    : $totalWorkers total ($workersPerInstance per instance x $instances instances)"
} else {
    Write-Host '  Workers    : reported in per-instance benchmark output'
}
Write-Host "===============================" -ForegroundColor Cyan
Write-Host ""

# --- Results directory ---
$resultsDir = "$PSScriptRoot\..\results"

# --- Launch benchmark panes ---

# Create timestamped results folder
$runTimestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runDir = "$resultsDir\$runTimestamp"
New-Item -ItemType Directory -Path $runDir -Force | Out-Null
Write-Host "Results will be saved to: $runDir" -ForegroundColor DarkGray
Write-Host ""

# --- Launch benchmark ---

# SSH options for inline mode
# UserKnownHostsFile=NUL prevents parallel ssh sessions from racing on a shared
# known_hosts file (rename to .old fails under contention, causing false failures).
$sshOpts = @('-o', 'StrictHostKeyChecking=no', '-o', 'BatchMode=yes', '-o', 'UserKnownHostsFile=NUL')

if ($Background) {
    # Background mode: spawn Windows Terminal panes for visual inspection
    Write-Host "Launching $($sshHosts.Count) pane(s) in Windows Terminal..." -ForegroundColor Yellow

    $maxPerTab = 2
    $wtArgs = @()
    $logFiles = @()
    for ($i = 0; $i -lt $sshHosts.Count; $i++) {
        $host_ = $sshHosts[$i]
        $logFile = "$runDir\$($host_ -replace '\.', '-')-$i.log"
        $logFiles += $logFile
        $paneCmd = "powershell -NoExit -Command `"ssh -i '$sshKey' -o StrictHostKeyChecking=no $sshUser@$host_ '$remoteBenchCommand' 2>&1 | Tee-Object -FilePath '$logFile'`""

        if ($i % $maxPerTab -eq 0) {
            if ($i -eq 0) {
                $wtArgs += "new-tab --title `"$host_`" $paneCmd"
            } else {
                $wtArgs += "; new-tab --title `"$host_`" $paneCmd"
            }
        } else {
            $wtArgs += "; split-pane -H --title `"$host_`" $paneCmd"
        }
    }

    $tabs = [math]::Ceiling($sshHosts.Count / $maxPerTab)
    $wtArgString = $wtArgs -join " "
    Start-Process -FilePath "wt" -ArgumentList $wtArgString -Wait:$false

    Write-Host "$($sshHosts.Count) benchmark pane(s) launched across $tabs tab(s)." -ForegroundColor Green
    Write-Host ""

    # Wait for all benchmarks to complete
    Write-Host "Waiting for benchmarks to complete (runtime: ${runtime}s)..." -ForegroundColor Yellow

    $timeout = [int]$runtime + 120
    $elapsed = 0
    $pollInterval = 5

    while ($elapsed -lt $timeout) {
        Start-Sleep -Seconds $pollInterval
        $elapsed += $pollInterval

        $completed = 0
        foreach ($lf in $logFiles) {
            if (Test-Path $lf) {
                $content = Get-Content $lf -Raw -ErrorAction SilentlyContinue
                if ($content -and $content -match 'Total throughput:') {
                    $completed++
                }
            }
        }

        Write-Host "`r  Progress: $completed/$($logFiles.Count) complete | ${elapsed}s elapsed" -NoNewline
        if ($completed -eq $logFiles.Count) { break }
    }

    Write-Host ""
    Write-Host ""
} else {
    # Non-verbose: run inline in parallel, collect results
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $totalCount = $sshHosts.Count
    $benchProgress = [hashtable]::Synchronized(@{ done = 0; failed = 0 })

    # Per-instance status for the -Detail live view. Keyed by instance index so
    # multiple instances on the same host (Multiplier > 1) stay distinct.
    $hostStatus = [hashtable]::Synchronized(@{})
    $hostNames = @()
    for ($i = 0; $i -lt $totalCount; $i++) {
        $hostStatus[$i] = 'pending'
        $parts = $sshHosts[$i] -split '\.'
        $vm = $parts[0]
        # Use "<pool>:<vm>" where <pool> is the 2nd hostname label with 'client'
        # stripped (e.g. vm0.fs4v2client100... -> fs4v2100:vm0), so pools stay
        # distinguishable in the live monitor. Fall back to the vm label alone.
        $label = if ($parts.Count -ge 2 -and $parts[1]) { "$($parts[1] -replace 'client', ''):$vm" } else { $vm }
        $hostNames += if ($multiplier -gt 1) { "$label#$i" } else { $label }
    }

    $benchJob = 0..($totalCount - 1) | ForEach-Object -Parallel {
        $idx = $_
        $host_ = ($using:sshHosts)[$idx]
        $logFile = "$using:runDir\$($host_ -replace '\.', '-')-$idx.log"
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        $success = $false
        $outputText = ""

        try {
            $benchCommand = $using:remoteBenchCommand
            $output = ssh -n -i $using:sshKey $using:sshOpts "${using:sshUser}@${host_}" $benchCommand 2>&1
            $exitCode = $LASTEXITCODE
            $outputText = $output -join "`n"
            $success = ($exitCode -eq 0)
            $outputText | Out-File -FilePath $logFile -Encoding utf8
        } catch {
            $outputText = $_.Exception.Message
        }
        $sw.Stop()
        $duration = $sw.Elapsed.ToString('mm\:ss\.ff')

        $p = $using:benchProgress
        $p.done++
        if (-not $success) { $p.failed++ }
        ($using:hostStatus)[$idx] = if ($success) { 'done' } else { 'failed' }

        [PSCustomObject]@{
            Index   = $idx
            Host    = $host_
            Success = $success
            Output  = $outputText
            LogFile = $logFile
            Duration = $duration
            DurationSec = $sw.Elapsed.TotalSeconds
        }
    } -ThrottleLimit $totalCount -AsJob

    Wait-ParallelJob -Job $benchJob -Progress $benchProgress -Total $totalCount -Stopwatch $stopwatch -Label "Running" -HostStatus $hostStatus -HostNames $hostNames -Detail:($Detail -or $Monitor)

    $benchResults = $benchJob | Receive-Job -Wait
    Remove-Job $benchJob

    foreach ($result in $benchResults) {
        if ($result.Success) {
            try {
                Assert-RespBenchmarkResult -Path $result.LogFile -RequireThroughput:($clusterBench -eq 'true')
            } catch {
                $result.Success = $false
                $hostStatus[$result.Index] = 'failed'
                Write-Host "  $($result.Host): $($_.Exception.Message)" -ForegroundColor Red
            }
        }
    }

    Show-BenchmarkClientOutput -Results $benchResults -All:$PrintOutput
    $stopwatch.Stop()
    $benchSuccess = ($benchResults | Where-Object { $_.Success }).Count
    $benchFailed = $benchResults.Count - $benchSuccess
    Write-Host "  ✓ Benchmark complete: $benchSuccess/$totalCount succeeded | Elapsed: $($stopwatch.Elapsed.ToString('mm\:ss\.ff'))" -ForegroundColor $(if ($benchFailed -gt 0) { 'Yellow' } else { 'Green' })

    # Per-instance completion durations. The timed benchmark window is fixed
    # (--runtime), so any spread here comes from untimed phases: SSH setup,
    # offline buffer preparation (scales with batch size) and the DB load phase
    # (more round-trips at small batch). The whole run is a barrier: total
    # elapsed is set by the slowest instance.
    $durByLog = @{}
    foreach ($r in $benchResults) { $durByLog[$r.LogFile] = $r.Duration }

    $secs = @($benchResults | Where-Object { $null -ne $_.DurationSec } | ForEach-Object { $_.DurationSec })
    if ($secs.Count -gt 0) {
        $fastest = ($secs | Measure-Object -Minimum).Minimum
        $slowest = ($secs | Measure-Object -Maximum).Maximum
        $avg = ($secs | Measure-Object -Average).Average
        $fmt = { param($s) ([TimeSpan]::FromSeconds($s)).ToString('mm\:ss\.ff') }
        Write-Host ("  Instance durations: fastest {0} | avg {1} | slowest {2} | spread {3}" -f `
            (& $fmt $fastest), (& $fmt $avg), (& $fmt $slowest), (& $fmt ($slowest - $fastest))) -ForegroundColor DarkCyan

        if ($Detail) {
            Write-Host "  Slowest instances:" -ForegroundColor DarkCyan
            $benchResults | Sort-Object DurationSec -Descending | Select-Object -First 5 | ForEach-Object {
                Write-Host ("    {0}  {1}" -f $_.Duration, $_.Host) -ForegroundColor DarkGray
            }
        }
    }

    if ($benchFailed -gt 0) {
        Write-Host "  Failed hosts:" -ForegroundColor Red
        $benchResults | Where-Object { -not $_.Success } | ForEach-Object {
            Write-Host "    $($_.Host) [$($_.Duration)]" -ForegroundColor Red
        }
        if ($ClientHosts) { throw "$benchFailed declarative benchmark instance(s) failed. Logs: $runDir" }
    }
    Write-Host ""

    $logFiles = $benchResults | ForEach-Object { $_.LogFile }
}

# --- Aggregate results ---
foreach ($lf in $logFiles) {
    Assert-RespBenchmarkResult -Path $lf -RequireThroughput:($clusterBench -eq 'true')
}
Write-Host "=== Aggregate Results ($runTimestamp) ===" -ForegroundColor Cyan
$totalKops = 0.0
$totalData = 0.0
$totalWire = 0.0
if (-not (Get-Variable -Name durByLog -Scope Local -ErrorAction SilentlyContinue)) { $durByLog = @{} }

$maxHostLen = ($logFiles | ForEach-Object { (Split-Path $_ -Leaf).Replace('.log','').Length } | Measure-Object -Maximum).Maximum

foreach ($lf in $logFiles) {
    $name = (Split-Path $lf -Leaf).Replace('.log','')
    if (Test-Path $lf) {
        $content = Get-Content $lf
        $kopsLine = $content | Where-Object { $_ -match 'Total throughput:.*?([\d,.]+)\s*Kops/sec' } | Select-Object -Last 1
        $dataLine = $content | Where-Object { $_ -match 'Data throughput:.*?([\d.]+)\s*GB/sec' } | Select-Object -Last 1
        $wireLine = $content | Where-Object { $_ -match 'Wire throughput:.*?([\d.]+)\s*GB/sec' } | Select-Object -Last 1

        $kops = 0.0; $data = 0.0; $wire = 0.0
        if ($kopsLine -match '([\d,.]+)\s*Kops/sec') { $kops = [double]($Matches[1] -replace ',', '') }
        if ($clusterBench -ne 'true') {
            $standaloneLine = $content | Where-Object { $_ -match '^\[Throughput\]:\s*([\d,.]+)\s*ops/sec' } | Select-Object -Last 1
            if ($standaloneLine -match '^\[Throughput\]:\s*([\d,.]+)\s*ops/sec') {
                $kops = [double]($Matches[1] -replace ',', '') / 1000
            } else {
                throw "No standalone throughput results found. Log: $lf"
            }
        }
        if ($dataLine -match '([\d.]+)\s*GB/sec') { $data = [double]$Matches[1] }
        if ($wireLine -match '([\d.]+)\s*GB/sec') { $wire = [double]$Matches[1] }

        $totalKops += $kops
        $totalData += $data
        $totalWire += $wire
        if ($Detail) {
            $durSuffix = if ($durByLog -and $durByLog.ContainsKey($lf)) { " | $($durByLog[$lf])" } else { "" }
            if ($clusterBench -eq 'true') {
                Write-Host ("  {0}  {1,12:N2} Kops/sec | {2,6:N3} GB/s data | {3,6:N3} GB/s wire{4}" -f $name.PadRight($maxHostLen), $kops, $data, $wire, $durSuffix)
            } else {
                Write-Host ("  {0}  {1,12:N2} Kops/sec{2}" -f $name.PadRight($maxHostLen), $kops, $durSuffix)
            }
        }
    } else {
        if ($Detail) {
            Write-Host "  $($name.PadRight($maxHostLen))  (no results)" -ForegroundColor DarkGray
        }
    }
}
if ($Detail) { Write-Host ("  " + ("-" * 70)) }
if ($clusterBench -eq 'true') {
    Write-Host ("  {0}  {1,12:N2} Kops/sec | {2,6:N3} GB/s data | {3,6:N3} GB/s wire" -f "TOTAL".PadRight($maxHostLen), $totalKops, $totalData, $totalWire) -ForegroundColor Green
} else {
    Write-Host ("  {0}  {1,12:N2} Kops/sec" -f "TOTAL".PadRight($maxHostLen), $totalKops) -ForegroundColor Green
}

# --- Probe DBSIZE per shard ---
Write-Host ""
Write-Host "Probing database size..." -ForegroundColor DarkGray
$dbSizeTotal = 0
$shardResults = @()

if ($clusterBench -eq "true") {
    $clusterNodesRaw = ssh -i $sshKey @probeOpts "${sshUser}@${probeHost}" "$redisProbeCommand -h $benchHost -p $benchPort cluster nodes 2>/dev/null" 2>&1
    $shardEndpoints = @()
    foreach ($line in $clusterNodesRaw) {
        $lineStr = "$line".Trim()
        if ($lineStr -match 'master' -and $lineStr -notmatch 'fail') {
            if ($lineStr -match '(\d+\.\d+\.\d+\.\d+):(\d+)') {
                $shardEndpoints += @{ Host = $Matches[1]; Port = $Matches[2] }
            }
        }
    }
} else {
    $shardEndpoints = @(@{ Host = $benchHost; Port = $benchPort })
}

if ($shardEndpoints.Count -gt 0) {
    $dbSizeCmds = ($shardEndpoints | ForEach-Object {
        "echo `"SHARD $($_.Host):$($_.Port)`" && $redisProbeCommand -h $($_.Host) -p $($_.Port) dbsize 2>/dev/null"
    }) -join " && "
    $dbSizeRaw = ssh -i $sshKey @probeOpts "${sshUser}@${probeHost}" "$dbSizeCmds" 2>&1

    $currentShard = ""
    foreach ($line in $dbSizeRaw) {
        $lineStr = "$line".Trim()
        if ($lineStr -match '^SHARD\s+(.+)$') {
            $currentShard = $Matches[1]
        } elseif ($lineStr -match '(\d+)' -and $currentShard) {
            $keys = [long]$Matches[1]
            $shardResults += [PSCustomObject]@{ Shard = $currentShard; Keys = $keys }
            $dbSizeTotal += $keys
            $currentShard = ""
        }
    }

    if ($Detail) {
        Write-Host ""
        Write-Host "=== Database Size ===" -ForegroundColor Cyan
        foreach ($s in $shardResults) {
            Write-Host ("  {0}  {1,12:N0} keys" -f $s.Shard.PadRight(22), $s.Keys)
        }
        if ($shardResults.Count -gt 1) {
            Write-Host ("  " + ("-" * 40))
        }
        Write-Host ("  {0}  {1,12:N0} keys" -f "TOTAL".PadRight(22), $dbSizeTotal) -ForegroundColor Green
        Write-Host "=====================" -ForegroundColor Cyan
    } else {
        Write-Host "  Database: $($dbSizeTotal.ToString('N0')) keys across $($shardResults.Count) shard(s)" -ForegroundColor Cyan
    }
} else {
    Write-Host "  WARNING: No shard endpoints discovered" -ForegroundColor Yellow
}
