#!/usr/bin/env pwsh
[CmdletBinding()]
<#
.SYNOPSIS
    Drives memtier_benchmark on remote VMs via SSH from user PC.

.DESCRIPTION
    Reads benchmark parameters from a config file and runs memtier_benchmark
    on remote client VMs. Supports a load phase (parallel in-terminal) and
    a benchmark phase (Windows Terminal panes or inline parallel).

    Use -Verbose to spawn Windows Terminal panes for visual inspection.
    Without -Verbose, runs inline and aggregates results automatically.

.EXAMPLE
    .\memtier-bench.ps1
    .\memtier-bench.ps1 -ConfigFile .\memtier.conf
    .\memtier-bench.ps1 -ConfigFile .\memtier.conf -SkipLoad
    .\memtier-bench.ps1 -ConfigFile .\memtier.conf -Verbose
#>
param(
    [string]$ConfigFile = "$PSScriptRoot\memtier.conf",
    [switch]$SkipLoad,
    [switch]$Help
)

if ($Help) {
    Write-Host "Usage: memtier-bench.ps1 [-ConfigFile <path>] [-SkipLoad] [-Verbose]" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Drives memtier_benchmark on remote VMs via SSH."
    Write-Host ""
    Write-Host "Config file keys:"
    Write-Host "  SshUser          - SSH username (default: guser)"
    Write-Host "  ClientMachineHostnames - Base remote hostname or [host1, host2] array"
    Write-Host "  ClientMachineCount     - Number of physical hosts/VMs"
    Write-Host "  Multiplier       - Benchmark instances per VM (default: 1)"
    Write-Host "  Server           - Benchmark target host (-s)"
    Write-Host "  Port             - Benchmark target port (--port)"
    Write-Host "  Threads          - Number of threads (default: 128)"
    Write-Host "  Clients          - Clients per thread (default: 64)"
    Write-Host "  Pipeline         - Pipeline depth (default: 1024)"
    Write-Host "  DbSize           - Number of keys (default: 268435456)"
    Write-Host "  DataSize         - Value size in bytes (default: 8)"
    Write-Host "  TestTime         - Benchmark duration in seconds (default: 15)"
    Write-Host "  Ratio            - SET:GET ratio (default: 1:9)"
    Write-Host "  KeyPattern       - Key pattern (default: R:R)"
    Write-Host "  Cluster          - Enable cluster mode (true/false)"
    Write-Host "  SkipLoad         - Skip load phase (true/false)"
    Write-Host ""
    Write-Host "Flags:"
    Write-Host "  -SkipLoad        Skip the key loading phase"
    Write-Host "  -Verbose         Spawn Windows Terminal panes for visual inspection"
    Write-Host "  -Help            Show this help message"
    Write-Host ""
    return
}

$ErrorActionPreference = 'Stop'

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

# --- Resolve SSH key from security/manifest.json ---
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$manifestPath = Join-Path $scriptDir "..\..\01-resources\security\manifest.json"
if (Test-Path $manifestPath) {
    . (Join-Path $scriptDir "..\..\01-resources\security\ssh-key-utils.ps1")
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

# --- Resolve parameters ---
$sshUser         = $config["SshUser"]         ?? "guser"
$sshHostBase     = $config["ClientMachineHostnames"] ?? "vm0.example.com"
$hostCount       = [int]($config["ClientMachineCount"] ?? "1")
$multiplier      = [int]($config["Multiplier"] ?? "1")
$benchHost       = $config["Server"]       ?? "10.5.1.4"
$benchPort       = $config["Port"]            ?? "6379"
$threads         = $config["Threads"]         ?? "128"
$clients         = $config["Clients"]         ?? "64"
$pipeline        = $config["Pipeline"]        ?? "1024"
$dbSize          = $config["DbSize"]          ?? "268435456"
$dataSize        = $config["DataSize"]        ?? "8"
$testTime        = $config["TestTime"]        ?? "15"
$ratio           = $config["Ratio"]           ?? "1:9"
$keyPattern      = $config["KeyPattern"]      ?? "R:R"
$clusterMode     = $config["Cluster"]         ?? "false"

if ($config["SkipLoad"] -eq "true") { $SkipLoad = $true }

$clusterFlag = if ($clusterMode -eq "true") { "--cluster-mode" } else { "" }

# --- Derive host list from base + count ---
$sshHosts = @()

if ($sshHostBase -match '^\[(.+)\]$') {
    $baseHosts = $Matches[1] -split ',\s*'
    foreach ($base in $baseHosts) {
        if ($base -match '^([a-zA-Z]+)(\d+)\.(.+)$') {
            $prefix = $Matches[1]
            $startIndex = [int]$Matches[2]
            $domain = $Matches[3]
            for ($i = $startIndex; $i -lt ($startIndex + $hostCount); $i++) {
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

# --- Build memtier commands ---
$loadCmd = "memtier_benchmark -s $benchHost --port=$benchPort --ratio=1:0 --pipeline=$pipeline --data-size=$dataSize --clients=$clients --threads=$threads --key-minimum=1 --key-maximum=$dbSize --key-pattern=P:P --run-count=1 --hide-histogram --requests=allkeys"
if ($clusterFlag) { $loadCmd += " $clusterFlag" }

$benchCmd = "memtier_benchmark -s $benchHost --port=$benchPort --ratio=$ratio --pipeline=$pipeline --data-size=$dataSize --clients=$clients --threads=$threads --test-time=$testTime --run-count=1 --hide-histogram --key-minimum=1 --key-maximum=$dbSize --key-pattern=$keyPattern"
if ($clusterFlag) { $benchCmd += " $clusterFlag" }

# --- Print summary ---
$instances = $sshHosts.Count
$uniqueHosts = ($sshHosts | Select-Object -Unique).Count
Show-ClientMachineInfo -SshHosts $sshHosts -SshKey $sshKey -SshUser $sshUser
Write-Host "=== Instance Configuration ===" -ForegroundColor Cyan
Write-Host "  SSH Key      : $sshKey"
Write-Host "  SSH User     : $sshUser"
Write-Host "  Instances    : $instances ($multiplier x $uniqueHosts hosts)"
Write-Host "  SkipLoad     : $SkipLoad"
Write-Host "  Verbose      : $($VerbosePreference -ne 'SilentlyContinue')"
Write-Host "===============================" -ForegroundColor Cyan
Write-Host ""
Write-Host "=== Benchmark Configuration ===" -ForegroundColor Cyan
Write-Host "  Target       : $benchHost`:$benchPort"
Write-Host "  Threads      : $threads"
Write-Host "  Clients      : $clients"
Write-Host "  Pipeline     : $pipeline"
Write-Host "  DbSize       : $dbSize"
Write-Host "  DataSize     : $dataSize"
Write-Host "  TestTime     : $testTime"
Write-Host "  Ratio        : $ratio"
Write-Host "  KeyPattern   : $keyPattern"
Write-Host "  Cluster      : $clusterMode"
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""

# --- SSH options ---
# UserKnownHostsFile=NUL prevents parallel ssh sessions from racing on a shared
# known_hosts file (rename to .old fails under contention, causing false failures).
$sshOpts = @('-o', 'StrictHostKeyChecking=no', '-o', 'BatchMode=yes', '-o', 'UserKnownHostsFile=NUL')

# --- Results directory ---
$resultsDir = "$PSScriptRoot\..\results"
$runTimestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runDir = "$resultsDir\memtier-$runTimestamp"
New-Item -ItemType Directory -Path $runDir -Force | Out-Null
Write-Host "Results will be saved to: $runDir" -ForegroundColor DarkGray
Write-Host ""

# --- Phase 1: Load keys (parallel in same window) ---
if (-not $SkipLoad) {
    Write-Host "=== Phase 1: Loading keys ($instances instances in parallel) ===" -ForegroundColor Cyan
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $loadProgress = [hashtable]::Synchronized(@{ done = 0; failed = 0 })

    $loadJob = $sshHosts | ForEach-Object -Parallel {
        $host_ = $_
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        $success = $false
        $outputText = ""

        try {
            $output = ssh -n -i $using:sshKey $using:sshOpts "${using:sshUser}@${host_}" $using:loadCmd 2>&1
            $exitCode = $LASTEXITCODE
            $outputText = $output -join "`n"
            $success = ($exitCode -eq 0)
        } catch {
            $outputText = $_.Exception.Message
        }
        $sw.Stop()
        $duration = $sw.Elapsed.ToString('mm\:ss\.ff')

        $p = $using:loadProgress
        $p.done++
        if (-not $success) { $p.failed++ }

        [PSCustomObject]@{
            Host     = $host_
            Success  = $success
            Output   = $outputText
            Duration = $duration
        }
    } -ThrottleLimit $instances -AsJob

    Wait-ParallelJob -Job $loadJob -Progress $loadProgress -Total $instances -Stopwatch $stopwatch -Label "Loading"

    $loadResults = $loadJob | Receive-Job -Wait
    Remove-Job $loadJob

    $stopwatch.Stop()
    $loadSuccess = ($loadResults | Where-Object { $_.Success }).Count
    $loadFailed = $loadResults.Count - $loadSuccess
    Write-Host "  ✓ Load complete: $loadSuccess/$($loadResults.Count) succeeded | Elapsed: $($stopwatch.Elapsed.ToString('mm\:ss\.ff'))" -ForegroundColor $(if ($loadFailed -gt 0) { 'Yellow' } else { 'Green' })

    if ($loadFailed -gt 0) {
        Write-Host "  Failed hosts:" -ForegroundColor Red
        $loadResults | Where-Object { -not $_.Success } | ForEach-Object {
            Write-Host "    $($_.Host) [$($_.Duration)]" -ForegroundColor Red
        }
    }
    Write-Host ""
} else {
    Write-Host "=== Phase 1: Skipping load phase ===" -ForegroundColor DarkGray
    Write-Host ""
}

# --- Phase 2: Benchmark ---
Write-Host "=== Phase 2: Running benchmark ===" -ForegroundColor Cyan

$isVerbose = $VerbosePreference -ne 'SilentlyContinue'

if ($isVerbose) {
    # Verbose mode: spawn Windows Terminal panes for visual inspection
    Write-Host "Launching $instances pane(s) in Windows Terminal..." -ForegroundColor Yellow

    $maxPerTab = 2
    $wtArgs = @()
    $logFiles = @()
    $hostCount = @{}
    for ($i = 0; $i -lt $sshHosts.Count; $i++) {
        $host_ = $sshHosts[$i]
        $baseName = $host_ -replace '\.', '-'
        if ($hostCount.ContainsKey($baseName)) {
            $hostCount[$baseName]++
            $logFile = "$runDir\$baseName-$($hostCount[$baseName]).log"
        } else {
            $hostCount[$baseName] = 0
            $logFile = "$runDir\$baseName.log"
        }
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

    Write-Host "$instances benchmark pane(s) launched across $tabs tab(s)." -ForegroundColor Green
    Write-Host ""

    # Wait for all benchmarks to complete by polling log files
    Write-Host "Waiting for benchmarks to complete (test-time: ${testTime}s)..." -ForegroundColor Yellow

    $timeout = [int]$testTime + 120
    $elapsed = 0
    $pollInterval = 5

    while ($elapsed -lt $timeout) {
        Start-Sleep -Seconds $pollInterval
        $elapsed += $pollInterval

        $completed = 0
        foreach ($lf in $logFiles) {
            if (Test-Path $lf) {
                $content = Get-Content $lf -Raw -ErrorAction SilentlyContinue
                if ($content -and $content -match 'Totals') {
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
    $benchProgress = [hashtable]::Synchronized(@{ done = 0; failed = 0 })

    $benchJob = $sshHosts | ForEach-Object -Parallel {
        $host_ = $_
        $logFile = "$using:runDir\$($host_ -replace '\.', '-').log"
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        $success = $false
        $outputText = ""

        try {
            $output = ssh -n -i $using:sshKey $using:sshOpts "${using:sshUser}@${host_}" $using:benchCmd 2>&1
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

        [PSCustomObject]@{
            Host     = $host_
            Success  = $success
            Output   = $outputText
            Duration = $duration
            LogFile  = $logFile
        }
    } -ThrottleLimit $instances -AsJob

    Wait-ParallelJob -Job $benchJob -Progress $benchProgress -Total $instances -Stopwatch $stopwatch -Label "Running"

    $benchResults = $benchJob | Receive-Job -Wait
    Remove-Job $benchJob

    $stopwatch.Stop()
    $benchSuccess = ($benchResults | Where-Object { $_.Success }).Count
    $benchFailed = $benchResults.Count - $benchSuccess
    Write-Host "  ✓ Benchmark complete: $benchSuccess/$($benchResults.Count) succeeded | Elapsed: $($stopwatch.Elapsed.ToString('mm\:ss\.ff'))" -ForegroundColor $(if ($benchFailed -gt 0) { 'Yellow' } else { 'Green' })

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
Write-Host "=== Aggregate Results (memtier-$runTimestamp) ===" -ForegroundColor Cyan

$totalOps = 0.0
$totalKbSec = 0.0
$totalLatency = 0.0
$resultCount = 0

$maxHostLen = ($logFiles | ForEach-Object { (Split-Path $_ -Leaf).Replace('.log','').Length } | Measure-Object -Maximum).Maximum

foreach ($lf in $logFiles) {
    $name = (Split-Path $lf -Leaf).Replace('.log','')
    if (Test-Path $lf) {
        $content = Get-Content $lf

        # memtier Totals line format: Totals <ops/sec> <hits/sec> <misses/sec> <avg latency> ...
        $totalsLine = $content | Where-Object { $_ -match '^\s*Totals' } | Select-Object -Last 1

        if ($totalsLine) {
            $fields = ($totalsLine.Trim() -split '\s+')
            # Memtier format: Totals ops/sec hits/sec misses/sec MOVED/sec ASK/sec Avg.Latency p50 p99 p99.9 KB/sec
            $ops = 0.0
            $kbSec = 0.0
            $avgLatency = 0.0

            if ($fields.Count -ge 2) {
                try { $ops = [double]($fields[1] -replace ',', '') } catch { }
            }
            if ($fields.Count -ge 7 -and $fields[6] -ne '-nan' -and $fields[6] -ne '---') {
                try { $avgLatency = [double]($fields[6] -replace ',', '') } catch { }
            }
            # KB/sec is always the last field
            $lastField = $fields[-1]
            if ($lastField -ne '---') {
                try { $kbSec = [double]($lastField -replace ',', '') } catch { }
            }

            $totalOps += $ops
            $totalKbSec += $kbSec
            $totalLatency += $avgLatency
            $resultCount++

            Write-Host ("  {0}  {1,16:N2} ops/sec | {2,10:N3} avg-lat | {3,12:N2} KB/sec" -f $name.PadRight($maxHostLen), $ops, $avgLatency, $kbSec)
        } else {
            Write-Host "  $($name.PadRight($maxHostLen))  (no Totals line found)" -ForegroundColor DarkGray
        }
    } else {
        Write-Host "  $($name.PadRight($maxHostLen))  (no results)" -ForegroundColor DarkGray
    }
}

$avgLatency = if ($resultCount -gt 0) { $totalLatency / $resultCount } else { 0 }
Write-Host ("  " + ("-" * ($maxHostLen + 60)))
Write-Host ("  {0}  {1,16:N2} ops/sec | {2,10:N3} avg-lat | {3,12:N2} KB/sec" -f "TOTAL".PadRight($maxHostLen), $totalOps, $avgLatency, $totalKbSec) -ForegroundColor Green
Write-Host ""

# Save summary
$summaryFile = "$runDir\summary.txt"
@"
memtier-benchmark summary ($runTimestamp)
pipeline: $pipeline, threads: $threads, clients: $clients, dataSize: $dataSize, dbSize: $dbSize, testTime: $testTime, ratio: $ratio
Total ops/sec: $($totalOps.ToString('N2'))
Total KB/sec: $($totalKbSec.ToString('N2'))
Avg latency: $($avgLatency.ToString('N3'))
Instances: $instances
"@ | Out-File -FilePath $summaryFile -Encoding utf8
Write-Host "Summary saved to: $summaryFile" -ForegroundColor DarkGray
