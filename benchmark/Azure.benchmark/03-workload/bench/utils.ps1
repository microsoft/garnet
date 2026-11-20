# Shared utility functions for benchmark scripts

function Read-BenchmarkDefinition {
    param([string]$Path)

    $path = (Resolve-Path -LiteralPath $Path -ErrorAction Stop).Path
    $json = Get-Content -LiteralPath $path -Raw
    $document = [System.Text.Json.JsonDocument]::Parse($json)
    $document.Dispose()
    $definition = $json | ConvertFrom-Json
    foreach ($property in $definition.PSObject.Properties.Name) {
        if ($property -notin @('SchemaVersion', 'Kind', 'System', 'InstanceConfig', 'Target', 'Instances')) {
            throw "Unknown benchmark definition property: $property"
        }
    }
    if (($definition.SchemaVersion -isnot [int] -and $definition.SchemaVersion -isnot [long]) -or
        $definition.SchemaVersion -ne 1 -or $definition.Kind -ne 'Benchmark') {
        throw 'Expected a SchemaVersion 1 Benchmark definition.'
    }
    if ($definition.System -notin @('Resp.benchmark', 'memtier_benchmark')) {
        throw 'System must be Resp.benchmark or memtier_benchmark.'
    }
    if ($definition.Target -isnot [pscustomobject]) { throw 'Target must be an object.' }
    foreach ($property in $definition.Target.PSObject.Properties.Name) {
        if ($property -notin @('Vm', 'Port')) { throw "Unknown benchmark target property: $property" }
    }
    if (($definition.Target.Port -isnot [int] -and $definition.Target.Port -isnot [long]) -or
        $definition.Target.Port -lt 1 -or $definition.Target.Port -gt 65535) { throw 'Target.Port must be an integer between 1 and 65535.' }
    if ($definition.Instances -isnot [array] -or -not $definition.Instances.Count) { throw 'Instances must be a nonempty array.' }
    $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($instance in $definition.Instances) {
        if ($instance -isnot [pscustomobject] -or @($instance.PSObject.Properties.Name | Where-Object { $_ -ne 'Vm' }).Count) {
            throw 'Each benchmark instance must be an object containing only Vm.'
        }
        if (-not $seen.Add([string]$instance.Vm)) { throw "Duplicate benchmark VM: $($instance.Vm)" }
    }
    foreach ($entry in @($definition.Target) + @($definition.Instances)) {
        if ($entry.Vm -isnot [string] -or $entry.Vm -notmatch '^[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$') {
            throw 'Each benchmark VM must be a valid short DNS label.'
        }
    }
    if ($definition.InstanceConfig -isnot [string] -or -not $definition.InstanceConfig) { throw 'InstanceConfig is required.' }
    $configPath = $definition.InstanceConfig
    if (-not [IO.Path]::IsPathRooted($configPath)) { $configPath = Join-Path (Split-Path -Parent $path) $configPath }
    if (-not (Test-Path -LiteralPath $configPath -PathType Leaf)) { throw "Benchmark instance configuration not found: $configPath" }
    $definition.InstanceConfig = [IO.Path]::GetFullPath($configPath)
    return $definition
}

function Get-BenchmarkClientHosts {
    param([string[]]$ClientHosts, [string]$HostBase, [string]$HostCount = '1', [string]$Multiplier = '1')

    if ($ClientHosts.Count) {
        $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
        foreach ($hostName in $ClientHosts) {
            if ([Uri]::CheckHostName($hostName) -eq [UriHostNameType]::Unknown -or -not $seen.Add($hostName)) {
                throw "Invalid or duplicate benchmark client hostname: $hostName"
            }
        }
        return $ClientHosts
    }
    $copies = [int]$Multiplier
    $bases = @(if ($HostBase -match '^\[(.+)\]$') { $Matches[1] -split ',\s*' } else { $HostBase })
    $counts = @(if ($HostCount -match '^\[(.+)\]$') { $Matches[1] -split ',\s*' | ForEach-Object { [int]$_ } } else { [int]$HostCount })
    if ($counts.Count -eq 1) { $counts = @($counts[0]) * $bases.Count }
    if ($counts.Count -ne $bases.Count) { throw 'ClientMachineCount entries must match ClientMachineHostnames.' }
    if ($copies -lt 1 -or @($counts | Where-Object { $_ -lt 1 }).Count) { throw 'Client counts and Multiplier must be positive.' }
    for ($h = 0; $h -lt $bases.Count; $h++) {
        if ($bases[$h] -notmatch '^([a-zA-Z]+)(\d+)\.(.+)$') {
            throw "ClientMachineHostnames must follow <prefix><index>.<domain>: $($bases[$h])"
        }
        $prefix = $Matches[1]; $start = [int]$Matches[2]; $domain = $Matches[3]
        for ($i = $start; $i -lt $start + $counts[$h]; $i++) {
            for ($m = 0; $m -lt $copies; $m++) { "$prefix$i.$domain" }
        }
    }
}

function Invoke-WorkloadRemoteScript {
    param([string]$HostName, [string]$SshUser, [string]$SshKey, [string]$Script)

    $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes("`$ErrorActionPreference = 'Stop'`n$Script"))
    $output = @(ssh -n -i $SshKey -T -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=no `
        "${SshUser}@${HostName}" "pwsh -NoProfile -NonInteractive -OutputFormat Text -EncodedCommand $encoded")
    if ($LASTEXITCODE -ne 0) { throw "Benchmark preflight failed on '$HostName' (exit $LASTEXITCODE): $($output -join "`n")" }
    return $output
}

function New-RespBenchmarkRemoteCommand {
    param([string]$ConfigFile, [string[]]$Arguments = @())

    $config = [Convert]::ToBase64String([IO.File]::ReadAllBytes((Resolve-Path -LiteralPath $ConfigFile).Path))
    $argumentsJson = ConvertTo-Json -InputObject @($Arguments) -Compress
    $encodedArguments = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($argumentsJson))
    $script = @'
$ErrorActionPreference = 'Stop'
$path = Join-Path ([IO.Path]::GetTempPath()) ('garnet-benchmark-' + [guid]::NewGuid().ToString('N') + '.conf')
try {
    [IO.File]::WriteAllBytes($path, [Convert]::FromBase64String('__CONFIG__'))
    if (-not $IsWindows) {
        [IO.File]::SetUnixFileMode($path, [IO.UnixFileMode]::UserRead -bor [IO.UnixFileMode]::UserWrite)
    }
    $benchmarkArgs = @('--config', $path) + @([Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('__ARGUMENTS__')) | ConvertFrom-Json)
    & Resp.benchmark @benchmarkArgs
    $code = $LASTEXITCODE
} finally {
    if (Test-Path -LiteralPath $path) { Remove-Item -LiteralPath $path }
}
exit $code
'@
    $script = $script.Replace('__CONFIG__', $config).Replace('__ARGUMENTS__', $encodedArguments)
    $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($script))
    return "pwsh -NoProfile -NonInteractive -OutputFormat Text -EncodedCommand $encoded"
}

function Assert-RespBenchmarkResult {
    param([string]$Path, [switch]$RequireThroughput)

    $content = Get-Content -LiteralPath $Path -Raw -ErrorAction Stop
    if ([string]::IsNullOrWhiteSpace($content)) { throw "Resp.benchmark produced no output. Log: $Path" }
    if ($content -match '(?m)^\s*ERROR\(S\):\s*([^\r\n]+)') {
        throw "Resp.benchmark rejected its arguments: $($Matches[1].Trim()) Verify that the client binary matches the workload options. Log: $Path"
    }
    if ($RequireThroughput -and $content -notmatch 'Total throughput:[^\r\n]*?([\d,.]+)\s*Kops/sec') {
        throw "Resp.benchmark did not report completed throughput results. Log: $Path"
    }
}

function Show-BenchmarkClientOutput {
    param([object[]]$Results, [switch]$All)

    foreach ($result in $Results) {
        if ($All -or -not $result.Success) {
            Write-Host "=== $($result.Host) benchmark-client output ===" -ForegroundColor Cyan
            Write-Host $result.Output
        }
    }
}

function Wait-ParallelJob {
    <#
    .SYNOPSIS
        Displays a progress bar while waiting for a parallel job to complete.
    .PARAMETER Job
        The job object returned by ForEach-Object -Parallel -AsJob.
    .PARAMETER Progress
        A synchronized hashtable with a 'done' key tracking completed items.
    .PARAMETER Total
        Total number of items being processed.
    .PARAMETER Stopwatch
        A running Stopwatch for elapsed time display.
    .PARAMETER Label
        Text label shown before the progress bar (e.g., "Loading", "Running").
    .PARAMETER HostStatus
        Optional synchronized hashtable keyed by instance index whose values are
        'pending', 'done', or 'failed'. Required for the -Detail live view.
    .PARAMETER HostNames
        Optional array of display names indexed by instance index, used to list
        the instances still pending in the -Detail live view.
    .PARAMETER Detail
        When set (with HostStatus/HostNames), renders a live multi-line view
        showing per-instance completed/pending status instead of a single bar.
    .PARAMETER DetailMaxHosts
        Maximum number of pending instance names to list in the -Detail view.
    #>
    param(
        [Parameter(Mandatory)]$Job,
        [Parameter(Mandatory)][hashtable]$Progress,
        [Parameter(Mandatory)][int]$Total,
        [Parameter(Mandatory)][System.Diagnostics.Stopwatch]$Stopwatch,
        [string]$Label = "Running",
        [hashtable]$HostStatus,
        [string[]]$HostNames,
        [switch]$Detail,
        [int]$DetailMaxHosts = 15
    )
    $barWidth = 30
    $fillChar = ([char]0x2588).ToString()
    $emptyChar = ([char]0x2591).ToString()
    $esc = [char]27

    $useDetail = $Detail -and $HostStatus -and $HostNames -and $HostNames.Count -gt 0

    if ($useDetail) {
        $renderDetail = {
            param([int]$Done, [bool]$IsFinal)
            $filled = if ($IsFinal) { $barWidth } else { [math]::Floor(($Done / $Total) * $barWidth) }
            $empty = $barWidth - $filled
            $bar = $fillChar * $filled + $emptyChar * $empty
            $elapsed = $Stopwatch.Elapsed.ToString('mm\:ss')

            $completed = 0; $failed = 0; $pendingNames = @()
            for ($k = 0; $k -lt $HostNames.Count; $k++) {
                switch ($HostStatus[$k]) {
                    'done'   { $completed++ }
                    'failed' { $failed++ }
                    default  { $pendingNames += $HostNames[$k] }
                }
            }
            $lines = @()
            $lines += "  $Label... [$bar] $Done/$Total ($elapsed)"
            $failSuffix = if ($failed -gt 0) { " | failed: $failed" } else { "" }
            $lines += "    completed: $completed | pending: $($pendingNames.Count)$failSuffix"
            if ($pendingNames.Count -gt 0) {
                $shown = @($pendingNames | Select-Object -First $DetailMaxHosts)
                $listStr = ($shown -join ', ')
                $more = $pendingNames.Count - $shown.Count
                if ($more -gt 0) { $listStr += " (+$more more)" }
                $lines += "    pending: $listStr"
            }
            return ,$lines
        }

        $prevLines = 0
        while ($Job.State -eq 'Running') {
            $lines = & $renderDetail $Progress.done $false
            if ($prevLines -gt 0) { [Console]::Out.Write("$esc[${prevLines}A$esc[0J") }
            foreach ($l in $lines) { [Console]::Out.WriteLine($l) }
            $prevLines = $lines.Count
            Start-Sleep -Milliseconds 300
        }
        $lines = & $renderDetail $Total $true
        if ($prevLines -gt 0) { [Console]::Out.Write("$esc[${prevLines}A$esc[0J") }
        foreach ($l in $lines) { [Console]::Out.WriteLine($l) }
        return
    }

    while ($Job.State -eq 'Running') {
        $done = $Progress.done
        $filled = [math]::Floor(($done / $Total) * $barWidth)
        $empty = $barWidth - $filled
        $bar = $fillChar * $filled + $emptyChar * $empty
        $elapsed = $Stopwatch.Elapsed.ToString('mm\:ss')
        Write-Host "`r  $Label... [$bar] $done/$Total ($elapsed)" -NoNewline
        Start-Sleep -Milliseconds 250
    }
    # Final bar at 100%
    $bar = $fillChar * $barWidth
    $elapsed = $Stopwatch.Elapsed.ToString('mm\:ss')
    Write-Host "`r  $Label... [$bar] $Total/$Total ($elapsed)" -NoNewline
    Write-Host ""
}

function Get-BenchmarkConfig {
    <#
    .SYNOPSIS
        Probes benchmark configuration by running a short test on one host and parsing the config block.
    .PARAMETER BenchCmd
        The full benchmark command string.
    .PARAMETER SshKey
        Path to SSH private key.
    .PARAMETER SshUser
        SSH username.
    .PARAMETER ProbeHost
        The SSH host to run the probe on.
    .PARAMETER Runtime
        Actual runtime value to display (probe overrides to 1s).
    .PARAMETER DbSize
        Actual DbSize value to display (probe overrides to 1).
    .RETURNS
        Hashtable with ConfigBlock (string[]) and WorkersPerInstance (int), or $null if probe fails.
    #>
    param(
        [Parameter(Mandatory)][string]$BenchCmd,
        [Parameter(Mandatory)][string]$SshKey,
        [Parameter(Mandatory)][string]$SshUser,
        [Parameter(Mandatory)][string]$ProbeHost,
        [string]$Runtime = "60",
        [string]$DbSize = ""
    )

    $probeOpts = @('-n', '-o', 'ConnectTimeout=10', '-o', 'StrictHostKeyChecking=no', '-o', 'BatchMode=yes')

    # Build probe command with short runtime + small dbsize
    $probeCmd = $BenchCmd -replace '--runtime\s+\d+', '--runtime 1'
    $probeCmd = $probeCmd -replace '--dbsize\s+\d+', '--dbsize 1'
    if ($probeCmd -notmatch '--dbsize') { $probeCmd += ' --dbsize 1' }

    Write-Host "Probing benchmark configuration..." -ForegroundColor DarkGray
    $probeOutput = ssh -i $SshKey @probeOpts "${SshUser}@${ProbeHost}" $probeCmd 2>&1

    # Parse config block between === lines
    $configBlock = @()
    $inConfig = $false
    foreach ($line in $probeOutput) {
        $lineStr = "$line"
        if ($lineStr -match '={3,}.*[Cc]onfiguration') {
            $inConfig = $true
            $configBlock += $lineStr
        } elseif ($inConfig -and $lineStr -match '^={3,}\s*$') {
            $configBlock += $lineStr
            break
        } elseif ($inConfig) {
            $configBlock += $lineStr
        }
    }

    if ($configBlock.Count -eq 0) { return $null }

    # Fix values back to actual (probe uses --runtime 1, --dbsize 1)
    $fixedBlock = $configBlock | ForEach-Object {
        $line = $_ -replace 'Runtime:\s*1s', "Runtime: ${Runtime}s"
        $line -replace 'DB Size:\s*\d+', "DB Size: $(if ($DbSize) { $DbSize } else { '0' })"
    }

    # Extract workers per instance
    $workersPerInstance = 0
    $configBlock | ForEach-Object {
        if ($_ -match 'Workers:\s*(\d+)') {
            $workersPerInstance = [int]$Matches[1]
        }
    }

    return @{
        ConfigBlock       = $fixedBlock
        WorkersPerInstance = $workersPerInstance
    }
}

function Show-ClientMachineInfo {
    <#
    .SYNOPSIS
        Prints per-VMSS client machine info: VM count, cores per VM (probed via
        nproc on one host per VMSS), and total cores.
    .PARAMETER SshHosts
        Full list of client SSH hosts (may include Multiplier duplicates).
    .PARAMETER SshKey
        Path to SSH private key.
    .PARAMETER SshUser
        SSH username.
    #>
    param(
        [Parameter(Mandatory)][string[]]$SshHosts,
        [Parameter(Mandatory)][string]$SshKey,
        [Parameter(Mandatory)][string]$SshUser
    )

    # Count physical VMs only (exclude the per-VM instance duplication from Multiplier).
    $uniqueHosts = $SshHosts | Select-Object -Unique

    # Group by VMSS name = first domain label after the short hostname:
    #   vm99.fs4v2client100.southcentralus.cloudapp.azure.com -> fs4v2client100
    $groups = $uniqueHosts | Group-Object { ($_ -split '\.')[1] }

    $probeOpts = @('-n', '-o', 'ConnectTimeout=10', '-o', 'StrictHostKeyChecking=accept-new', '-o', 'BatchMode=yes')

    Write-Host "Probing client machine info..." -ForegroundColor DarkGray

    $rows = foreach ($g in $groups) {
        # Probe several hosts (not just the first) so one unreachable instance or a
        # first-time host key doesn't zero out the whole VMSS. Stop at the first that
        # returns a valid core count; leave $cores null if none respond.
        $cores = $null
        foreach ($probeHost in ($g.Group | Select-Object -First 5)) {
            $nprocRaw = ssh -i $SshKey @probeOpts "${SshUser}@${probeHost}" "nproc" 2>&1
            foreach ($line in $nprocRaw) {
                if ("$line" -match '^\s*(\d+)\s*$') { $cores = [int]$Matches[1]; break }
            }
            if ($null -ne $cores) { break }
        }
        [PSCustomObject]@{
            Vmss       = $g.Name
            VMs        = $g.Count
            CoresPerVM = $cores
            TotalCores = if ($null -ne $cores) { $g.Count * $cores } else { $null }
        }
    }

    $totalVMs   = ($rows | Measure-Object -Property VMs -Sum).Sum
    $totalCores = ($rows | Where-Object { $null -ne $_.TotalCores } | Measure-Object -Property TotalCores -Sum).Sum

    $vmssW = ($rows | ForEach-Object { $_.Vmss.Length } | Measure-Object -Maximum).Maximum
    $vmssW = [Math]::Max(4, $vmssW)

    $rowFmt    = "  {0}  {1,4}  {2,8}  {3,11}"
    $lineWidth = $vmssW + 31
    $title     = ("=== Client Machines ").PadRight($lineWidth, '=')
    $footer    = '=' * $lineWidth
    $sep       = "  {0}  {1}  {2}  {3}" -f ('-' * $vmssW), ('-' * 4), ('-' * 8), ('-' * 11)

    Write-Host ""
    Write-Host $title -ForegroundColor Cyan
    Write-Host ($rowFmt -f "VMSS".PadRight($vmssW), "VMs", "Cores/VM", "Total Cores") -ForegroundColor Cyan
    Write-Host $sep -ForegroundColor DarkGray
    foreach ($r in $rows) {
        $coresDisp = if ($null -ne $r.CoresPerVM) { $r.CoresPerVM } else { '?' }
        $totalDisp = if ($null -ne $r.TotalCores) { $r.TotalCores } else { '?' }
        $note = if ($null -eq $r.CoresPerVM) { '  (unreachable)' } else { '' }
        Write-Host (($rowFmt -f $r.Vmss.PadRight($vmssW), $r.VMs, $coresDisp, $totalDisp) + $note)
    }
    Write-Host $sep -ForegroundColor DarkGray
    Write-Host ($rowFmt -f "TOTAL".PadRight($vmssW), $totalVMs, "", $totalCores) -ForegroundColor Green
    Write-Host $footer -ForegroundColor Cyan
    Write-Host ""
}

function Show-BenchmarkConfig {
    param(
        [string]$BenchCmd,
        [string]$SshKey,
        [string]$SshUser,
        [string]$ProbeHost,
        [string]$Runtime,
        [string]$DbSize,
        [int]$Threads
    )

    $probeResult = Get-BenchmarkConfig -BenchCmd $BenchCmd -SshKey $SshKey -SshUser $SshUser -ProbeHost $ProbeHost -Runtime $Runtime -DbSize $DbSize

    if ($probeResult) {
        Write-Host ""
        $probeResult.ConfigBlock | ForEach-Object { Write-Host $_ -ForegroundColor Cyan }
        Write-Host ""
        $workersPerInstance = $probeResult.WorkersPerInstance
    } else {
        Write-Host "=== Benchmark Configuration ===" -ForegroundColor Cyan
        Write-Host "  Command    : $BenchCmd"
        Write-Host "===============================" -ForegroundColor Cyan
        Write-Host ""
        $workersPerInstance = [int]$Threads
    }

    return $workersPerInstance
}
