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
    [string]$ConfigFile = "$PSScriptRoot\bench.conf",
    [switch]$Background,
    [switch]$Detail,
    [switch]$Monitor,
    [switch]$Help
)

if ($Help) {
    Write-Host "Usage: resp-bench.ps1 [-ConfigFile <path>] [-Detail] [-Monitor] [-Background] [-Verbose]"
    Write-Host ""
    Write-Host "Reads parameters from a key=value config file and runs"
    Write-Host "Resp.benchmark on remote VMs via SSH."
    Write-Host ""
    Write-Host "Without -Verbose: runs inline in parallel, aggregates results."
    Write-Host "With -Verbose: spawns Windows Terminal panes for visual inspection."
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
    Write-Host "Config file keys:"
    Write-Host "  SshKey           - Path to SSH private key"
    Write-Host "  SshUser          - SSH username"
    Write-Host "  ClientMachineHostnames - Base remote hostname (vm prefix before index)"
    Write-Host "  ClientMachineCount     - Number of physical hosts/VMs"
    Write-Host "  Multiplier       - Benchmark instances per VM (default: 1)"
    Write-Host "  Server           - Benchmark target host (--host)"
    Write-Host "  Port             - Benchmark target port (--port)"
    Write-Host "  Threads          - Number of threads (--threads)"
    Write-Host "  Runtime          - Runtime in seconds (--runtime)"
    Write-Host "  DbSize           - Database size (--dbsize)"
    Write-Host "  KeyLength        - Key length in bytes (--keylength)"
    Write-Host "  ValueLength      - Value length in bytes (--valuelength)"
    Write-Host "  BatchSize        - Batch size (--batchsize)"
    Write-Host "  Op               - Operation type (--op, e.g., GET, SET, MGET)"
    Write-Host "  Broadcast        - Enable broadcast mode (true/false)"
    Write-Host "  ClusterBench     - Enable cluster bench mode (true/false)"
    Write-Host "  Pool             - Enable connection pooling (true/false, default: true)"
    Write-Host "  ExtraArgs        - Additional arguments to pass"
    return
}

# --- Load shared utilities ---
. "$PSScriptRoot\utils.ps1"

# --- Parse config file ---
if (-not (Test-Path $ConfigFile)) {
    Write-Error "Config file not found: $ConfigFile"
    exit 1
}

$config = @{}
Get-Content $ConfigFile | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith("#")) {
        $parts = $line -split "=", 2
        if ($parts.Count -eq 2) {
            $config[$parts[0].Trim()] = $parts[1].Trim()
        }
    }
}

# --- Resolve parameters ---
# --- Resolve SSH keys from security/manifest.json ---
$manifestPath = Join-Path $PSScriptRoot "..\..\01-resources\security\manifest.json"
if (Test-Path $manifestPath) {
    . (Join-Path $PSScriptRoot "..\..\01-resources\security\ssh-key-utils.ps1")
    try {
        $manifest = Get-SshKeyManifest -ManifestPath $manifestPath
        $sshKey = Resolve-SshUserPrivateKey -Manifest $manifest
    } catch {
        Write-Error $_.Exception.Message
        exit 1
    }
} elseif ($config["SshKey"]) {
    $sshKeyRaw = $config["SshKey"]
    if ($sshKeyRaw -match '^\[(.+)\]$') {
        $candidates = $Matches[1] -split ',\s*'
        $sshKey = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
        if (-not $sshKey) {
            Write-Error "None of the SSH keys exist: $($candidates -join ', ')"
            exit 1
        }
    } else {
        $sshKey = $sshKeyRaw
    }
} else {
    $sshKey = "$env:USERPROFILE\.ssh\id_ed25519"
}
$sshUser         = $config["SshUser"]         ?? "guser"
$sshHostBase     = $config["ClientMachineHostnames"] ?? "vm0.dps8v6vmss.southcentralus.cloudapp.azure.com"
$hostCountRaw    = $config["ClientMachineCount"] ?? "1"
$multiplier      = [int]($config["Multiplier"] ?? "1")

# Parse ClientMachineCount — supports scalar or array [n1, n2, ...]
$hostCounts = @()
if ($hostCountRaw -match '^\[(.+)\]$') {
    $hostCounts = $Matches[1] -split ',\s*' | ForEach-Object { [int]$_ }
} else {
    $hostCounts = @([int]$hostCountRaw)
}
$benchHost    = $config["Server"]       ?? "10.5.1.4"
$benchPort    = $config["Port"]         ?? "7000"
$threads      = $config["Threads"]      ?? "4"
$runtime      = $config["Runtime"]      ?? "60"
$dbSize       = $config["DbSize"]       ?? ""
$keyLength    = $config["KeyLength"]    ?? ""
$valueLength  = $config["ValueLength"]  ?? ""
$batchSize    = $config["BatchSize"]    ?? ""
$op           = $config["Op"]           ?? ""
$broadcast    = $config["Broadcast"]    ?? ""
$clusterBench = $config["ClusterBench"] ?? "true"
$pool         = $config["Pool"]         ?? "true"
$extraArgs    = $config["ExtraArgs"]    ?? ""

# --- Derive host list from base + count ---
$sshHosts = @()

# Support array syntax: [host1, host2, ...]
if ($sshHostBase -match '^\[(.+)\]$') {
    $baseHosts = $Matches[1] -split ',\s*'
    if ($hostCounts.Count -eq 1) {
        # Single count applies to all hostnames
        $hostCounts = @($hostCounts[0]) * $baseHosts.Count
    } elseif ($hostCounts.Count -ne $baseHosts.Count) {
        Write-Error "ClientMachineCount has $($hostCounts.Count) entries but ClientMachineHostnames has $($baseHosts.Count). They must match."
        exit 1
    }
    for ($h = 0; $h -lt $baseHosts.Count; $h++) {
        $base = $baseHosts[$h]
        $count = $hostCounts[$h]
        if ($base -match '^([a-zA-Z]+)(\d+)\.(.+)$') {
            $prefix = $Matches[1]
            $startIndex = [int]$Matches[2]
            $domain = $Matches[3]
            for ($i = $startIndex; $i -lt ($startIndex + $count); $i++) {
                for ($m = 0; $m -lt $multiplier; $m++) {
                    $sshHosts += "$prefix$i.$domain"
                }
            }
        } else {
            Write-Error "Invalid host in array: $base (expected <prefix><index>.<domain>)"
            exit 1
        }
    }
} else {
    # Single host pattern
    $hostCount = $hostCounts[0]
    if ($sshHostBase -match '^([a-zA-Z]+)(\d+)\.(.+)$') {
        $prefix = $Matches[1]
        $startIndex = [int]$Matches[2]
        $domain = $Matches[3]
    } else {
        Write-Error "ClientMachineHostnames must follow pattern: <prefix><index>.<domain> (e.g., vm0.example.com)"
        exit 1
    }
    for ($i = $startIndex; $i -lt ($startIndex + $hostCount); $i++) {
        for ($m = 0; $m -lt $multiplier; $m++) {
            $sshHosts += "$prefix$i.$domain"
        }
    }
}

# --- Build benchmark command ---
$benchCmd = "Resp.benchmark --host $benchHost --port $benchPort --threads $threads --runtime $runtime"
if ($dbSize)       { $benchCmd += " --dbsize $dbSize" }
if ($keyLength)    { $benchCmd += " --keylength $keyLength" }
if ($valueLength)  { $benchCmd += " --valuelength $valueLength" }
if ($batchSize)    { $benchCmd += " --batchsize $batchSize" }
if ($op)           { $benchCmd += " --op $op" }
if ($broadcast -eq "true") {
    $benchCmd += " --broadcast"
}
if ($clusterBench -eq "true") {
    $benchCmd += " --cluster-bench"
}
if ($pool -eq "true") {
    $benchCmd += " --pool"
}
if ($extraArgs) {
    $benchCmd += " $extraArgs"
}

# --- Probe server info ---
$probeOpts = @('-n', '-o', 'ConnectTimeout=10', '-o', 'StrictHostKeyChecking=no', '-o', 'BatchMode=yes')
$probeHost = $sshHosts | Select-Object -First 1
Write-Host "Probing server info (${benchHost}:${benchPort})..." -ForegroundColor DarkGray
$serverInfoRaw = ssh -i $sshKey @probeOpts "${sshUser}@${probeHost}" "redis-cli -h $benchHost -p $benchPort info server 2>/dev/null" 2>&1
$serverReplRaw = ssh -i $sshKey @probeOpts "${sshUser}@${probeHost}" "redis-cli -h $benchHost -p $benchPort info replication 2>/dev/null" 2>&1
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

$workersPerInstance = Show-BenchmarkConfig -BenchCmd $benchCmd -SshKey $sshKey -SshUser $sshUser -ProbeHost $probeHost -Runtime $runtime -DbSize ($config["DbSize"] ?? "") -Threads ([int]$threads)

# --- Print client machine info ---
Show-ClientMachineInfo -SshHosts $sshHosts -SshKey $sshKey -SshUser $sshUser

# --- Print instance configuration ---
$totalWorkers = $workersPerInstance * $instances
Write-Host "=== Instance Configuration ===" -ForegroundColor Cyan
Write-Host "  SSH Key    : $sshKey"
Write-Host "  SSH User   : $sshUser"
Write-Host "  Instances  : $instances ($multiplier x $uniqueHosts hosts)"
Write-Host "  Workers    : $totalWorkers total ($workersPerInstance per instance x $instances instances)"
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
        $paneCmd = "powershell -NoExit -Command `"ssh -i '$sshKey' -o StrictHostKeyChecking=no $sshUser@$host_ '$benchCmd' 2>&1 | Tee-Object -FilePath '$logFile'`""

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
            $benchCommand = $using:benchCmd
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
    }
    Write-Host ""

    $logFiles = $benchResults | ForEach-Object { $_.LogFile }
}

# --- Aggregate results ---
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
        if ($dataLine -match '([\d.]+)\s*GB/sec') { $data = [double]$Matches[1] }
        if ($wireLine -match '([\d.]+)\s*GB/sec') { $wire = [double]$Matches[1] }

        $totalKops += $kops
        $totalData += $data
        $totalWire += $wire
        if ($Detail) {
            $durSuffix = if ($durByLog -and $durByLog.ContainsKey($lf)) { " | $($durByLog[$lf])" } else { "" }
            Write-Host ("  {0}  {1,12:N2} Kops/sec | {2,6:N3} GB/s data | {3,6:N3} GB/s wire{4}" -f $name.PadRight($maxHostLen), $kops, $data, $wire, $durSuffix)
        }
    } else {
        if ($Detail) {
            Write-Host "  $($name.PadRight($maxHostLen))  (no results)" -ForegroundColor DarkGray
        }
    }
}
if ($Detail) { Write-Host ("  " + ("-" * 70)) }
Write-Host ("  {0}  {1,12:N2} Kops/sec | {2,6:N3} GB/s data | {3,6:N3} GB/s wire" -f "TOTAL".PadRight($maxHostLen), $totalKops, $totalData, $totalWire) -ForegroundColor Green

# --- Probe DBSIZE per shard ---
Write-Host ""
Write-Host "Probing database size..." -ForegroundColor DarkGray
$dbSizeTotal = 0
$shardResults = @()

if ($clusterBench -eq "true") {
    $clusterNodesRaw = ssh -i $sshKey @probeOpts "${sshUser}@${probeHost}" "redis-cli -h $benchHost -p $benchPort cluster nodes 2>/dev/null" 2>&1
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
        "echo `"SHARD $($_.Host):$($_.Port)`" && redis-cli -h $($_.Host) -p $($_.Port) dbsize 2>/dev/null"
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
