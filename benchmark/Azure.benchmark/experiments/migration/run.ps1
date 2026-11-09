#!/usr/bin/env pwsh
[CmdletBinding()]
param(
    [Parameter(Mandatory)][Alias('rg')][string]$ResourceGroup,
    [Parameter(Mandatory)][string]$VmssName,
    [ValidateRange(1, 100)][int]$Repetitions = 3,
    [ValidateRange(0, 16383)][int]$StartSlot = 0,
    [ValidateRange(0, 16383)][int]$EndSlot = 8191,
    [ValidateRange(1, 86400)][int]$TimeoutSec = 600,
    [ValidateRange(1, 16384)][int]$ParallelMigrateTaskCount = 1,
    [bool]$FastMigrate = $false,
    [ValidateRange(1, 262143)][int]$MigrateBatchSize = 262143,
    [ValidateRange(1, 128)][int]$LoadThreads = 16,
    [ValidateSet(8, 256, 1024)][int[]]$ValueLengths = @(8, 256, 1024),
    [string]$SshKey
)

$ErrorActionPreference = 'Stop'
function New-MigrationInstanceConfig([string]$Text, [int]$TaskCount, [bool]$Fast, [int]$BatchSize = 262143) {
    if ($Text -match '"(?:ParallelMigrateTaskCount|FastMigrate|MigrateBatchSize)"\s*:') {
        throw 'Base migration configuration already declares migration overrides.'
    }
    if ($Text -notmatch '^\s*\{') { throw 'Expected a JSON object for the migration configuration.' }
    $settings = "`n  `"ParallelMigrateTaskCount`": $TaskCount,`n  `"FastMigrate`": $($Fast.ToString().ToLowerInvariant()),`n  `"MigrateBatchSize`": $BatchSize,"
    return [regex]::Replace($Text, '^(\s*\{)', "`$1$settings")
}
function Get-MigrationLoadMetrics([string]$Text, [long]$ExpectedKeys) {
    $timings = [regex]::Matches($Text,
        'Operation type: SET\s+Num threads: (\d+)\s+\[Total time\]: ([0-9,.]+)ms for ([0-9,.]+) ops')
    if ($timings.Count -ne 1) { throw 'Expected exactly one initial SET preload timing.' }
    $culture = [Globalization.CultureInfo]::InvariantCulture
    $seconds = [double]::Parse($timings[0].Groups[2].Value, $culture) / 1000
    $keys = [long]::Parse($timings[0].Groups[3].Value, [Globalization.NumberStyles]::Number, $culture)
    if ($seconds -le 0 -or $keys -ne $ExpectedKeys) { throw 'Invalid SET preload duration or operation count.' }
    return [pscustomobject]@{ Seconds = $seconds; OpsPerSecond = $keys / $seconds; Threads = [int]$timings[0].Groups[1].Value }
}
function New-MigrationLoadConfig([string]$Text, [ValidateSet(8, 256, 1024)][int]$ValueLength, [int]$Threads) {
    $profile = $Text | ConvertFrom-Json
    $profile.ValueLength = $ValueLength
    $profile.DbSize = switch ($ValueLength) {
        8 { 67108864 }
        256 { 8388608 }
        1024 { 2097152 }
    }
    $profile.LoadThreads = $Threads
    $profile.NumThreads = @($Threads)
    return $profile
}
if ($StartSlot -gt $EndSlot) { throw 'StartSlot must not exceed EndSlot.' }
$benchmarkDir = [IO.Path]::GetFullPath("$PSScriptRoot\..\..")
. "$benchmarkDir\03-workload\bench\utils.ps1"
. "$benchmarkDir\02-platform\vmss-utils.ps1"
. "$benchmarkDir\01-resources\security\ssh-key-utils.ps1"
if (-not $SshKey) {
    $manifest = Get-SshKeyManifest -ManifestPath "$benchmarkDir\01-resources\security\manifest.json"
    $SshKey = Resolve-SshUserPrivateKey -Manifest $manifest
}
$context = Resolve-VmssExecutionContext -ResourceGroup $ResourceGroup -VmssName $VmssName `
    -VmNames vm0,vm1,vm2 -NonInteractive
$runId = "migrate-slots-$([datetime]::Now.ToString('yyyyMMdd-HHmmss'))-$([guid]::NewGuid().ToString('N').Substring(0, 8))"
$results = Join-Path "$benchmarkDir\03-workload\results" $runId
$null = New-Item -ItemType Directory -Path $results
$clusterPath = Join-Path $results 'cluster.json'
$instancePath = Join-Path $results 'garnet.conf'
$cluster = Get-Content "$PSScriptRoot\cluster.json" -Raw | ConvertFrom-Json
$baseInstancePath = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot $cluster.InstanceConfig))
$instanceText = Get-Content -LiteralPath $baseInstancePath -Raw
New-MigrationInstanceConfig $instanceText $ParallelMigrateTaskCount $FastMigrate $MigrateBatchSize |
    Set-Content -LiteralPath $instancePath
$cluster.InstanceConfig = $instancePath
$cluster | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $clusterPath
Write-Host "Migration settings: ParallelMigrateTaskCount=$ParallelMigrateTaskCount; FastMigrate=$FastMigrate; MigrateBatchSize=$MigrateBatchSize; LoadThreads=$LoadThreads"
$remoteRoot = "/tmp/$runId"
$rows = [Collections.Generic.List[object]]::new()
$prepared = [Collections.Generic.List[object]]::new()
$awake = $false
try {
    if ($IsWindows) {
        if (-not ('MigrationWakeGuard' -as [type])) {
            Add-Type 'using System.Runtime.InteropServices; public static class MigrationWakeGuard { [DllImport("kernel32.dll")] public static extern uint SetThreadExecutionState(uint flags); }'
        }
        if ([MigrationWakeGuard]::SetThreadExecutionState([uint32]2147483649) -eq 0) { throw 'Could not prevent Windows sleep.' }
        $awake = $true
    }
    $repo = [IO.Path]::GetFullPath("$benchmarkDir\..\..")
    $publish = Join-Path $results 'migratebench'
    & dotnet publish "$repo\playground\MigrateBench\MigrateBench.csproj" -c Release -f net10.0 `
        -r linux-x64 --self-contained false -o $publish --nologo
    if ($LASTEXITCODE -ne 0) { throw 'MigrateBench publish failed.' }
    $addresses = @{}
    foreach ($node in $context.Nodes) {
        Invoke-WorkloadRemoteScript -HostName $node.HostName -SshUser $context.SshUser -SshKey $SshKey `
            -Script "New-Item -ItemType Directory -Path '$remoteRoot' | Out-Null" | Write-Host
        $prepared.Add($node)
        & scp -i $SshKey -o BatchMode=yes -o ConnectTimeout=10 `
            "$benchmarkDir\02-platform\tools\cluster\cluster-utils.ps1" `
            "$($context.SshUser)@$($node.HostName):$remoteRoot/cluster-utils.ps1"
        if ($LASTEXITCODE -ne 0) { throw "Cluster helper upload failed on $($node.Vm)." }
        $prepare = @"
`$deploy = Get-Command cluster-deploy.ps1 -ErrorAction Stop
`$helper = Join-Path (Split-Path -Parent `$deploy.Source) 'cluster-utils.ps1'
& sudo -n install -m 755 '$remoteRoot/cluster-utils.ps1' `$helper
if (`$LASTEXITCODE -ne 0) { throw 'Cluster helper installation failed.' }
. `$helper
Write-Output "MIGRATION_ADDRESS=`$(Get-ClusterDataPlaneIp)"
"@
        $output = @(Invoke-WorkloadRemoteScript -HostName $node.HostName -SshUser $context.SshUser -SshKey $SshKey -Script $prepare)
        $markers = @($output | Where-Object { $_ -match '^MIGRATION_ADDRESS=' })
        if ($markers.Count -ne 1) { throw "Cannot resolve data-plane address for $($node.Vm)." }
        $addresses[$node.Vm] = $markers[0].Substring('MIGRATION_ADDRESS='.Length)
    }
    $client = $context.Nodes | Where-Object Vm -eq 'vm2'
    & scp -r -i $SshKey -o BatchMode=yes -o ConnectTimeout=10 $publish `
        "$($context.SshUser)@$($client.HostName):$remoteRoot/migratebench"
    if ($LASTEXITCODE -ne 0) { throw 'MigrateBench upload failed.' }

    foreach ($valueLength in $ValueLengths) {
        Write-Host "=== Fresh diskless cluster and $valueLength-byte preload ==="
        & "$benchmarkDir\03-workload\cluster.ps1" -Action create -Definition $clusterPath `
            -ResourceGroup $ResourceGroup -VmssName $VmssName -SshKey $SshKey -NonInteractive
        $profile = New-MigrationLoadConfig (Get-Content "$PSScriptRoot\preload.conf" -Raw) $valueLength $LoadThreads
        $profilePath = Join-Path $results "$valueLength-load.conf"
        $profile | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $profilePath
        $benchmark = Get-Content "$PSScriptRoot\benchmark.json" -Raw | ConvertFrom-Json
        $benchmark.InstanceConfig = $profilePath
        $benchmarkPath = Join-Path $results "$valueLength-benchmark.json"
        $benchmark | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $benchmarkPath
        $preloadPath = Join-Path $results "$valueLength-preload.log"
        & "$benchmarkDir\03-workload\benchmark.ps1" -Definition $benchmarkPath `
            -ResourceGroup $ResourceGroup -VmssName $VmssName -SshKey $SshKey -NonInteractive -Verbose 6>&1 |
            Tee-Object -FilePath $preloadPath
        $load = Get-MigrationLoadMetrics (Get-Content -LiteralPath $preloadPath -Raw) $profile.DbSize
        if ($load.Threads -ne $LoadThreads) { throw 'Initial load did not use the declared worker count.' }
        $measure = @'
$source = '__SOURCE__'
$target = '__TARGET__'
$expectedTotal = __TOTAL__
$startSlot = __START__
$endSlot = __END__
$valueLength = __VALUE__
$binary = '__ROOT__/migratebench/MigrateBench'
& chmod +x $binary
if ($LASTEXITCODE -ne 0) { throw 'Cannot make MigrateBench executable.' }
function Redis([string]$Address, [string[]]$Command) {
    $reply = & redis-cli --raw -h $Address -p 7000 @Command 2>&1
    if ($LASTEXITCODE -ne 0 -or "$reply" -match '^(?:ERR|NOAUTH|MOVED|ASK|CLUSTERDOWN)') {
        throw "Redis command failed on ${Address}: $Command : $reply"
    }
    return ($reply -join "`n").Trim()
}
function Count([string]$Address) {
    $text = Redis $Address @('DBSIZE')
    [long]$count = 0
    if (-not [long]::TryParse($text, [ref]$count)) { throw "Invalid DBSIZE: $text" }
    return $count
}
function AssertSlots([string]$Address, [int[]]$Expected) {
    $members = Redis $Address @('CLUSTER', 'NODES')
    $self = @($members -split '\r?\n' | Where-Object { ($_ -split '\s+')[2] -match 'myself' })
    if ($self.Count -ne 1) { throw "Cannot identify $Address in cluster topology." }
    $fields = $self[0] -split '\s+'
    if ($fields[2] -notmatch 'master') { throw "$Address is not a primary." }
    $actual = [Collections.Generic.HashSet[int]]::new()
    foreach ($token in $fields | Select-Object -Skip 8) {
        if ($token -notmatch '^(\d+)(?:-(\d+))?$') { throw "Unstable slot ownership: $token" }
        $first = [int]$Matches[1]
        $last = if ($Matches[2]) { [int]$Matches[2] } else { $first }
        foreach ($slot in $first..$last) { $null = $actual.Add($slot) }
    }
    if (-not $actual.SetEquals($Expected)) { throw "Unexpected slot ownership on $Address." }
}
function Migrate([string]$From, [string]$To) {
    $output = @(& timeout --signal=TERM --kill-after=10s __TIMEOUT__s $binary `
        --source-endpoint "${From}:7000" --target-endpoint "${To}:7000" `
        --slots "$startSlot,$endSlot" --mtype SLOTS --timeout __TIMEOUT__ --dbsize 2>&1)
    $code = $LASTEXITCODE
    $output | Write-Output
    if ($code -ne 0 -or ($output -join "`n") -match 'Error at|ERR |Unhandled exception') {
        throw "MigrateBench failed (exit $code)."
    }
    $timing = @($output | Where-Object { $_ -match 'SLOTS Elapsed Time: ([0-9.]+) seconds' })
    if ($timing.Count -ne 1) { throw 'MigrateBench did not report exactly one SLOTS duration.' }
}
AssertSlots $source (0..16383)
AssertSlots $target @()
if ((Count $source) -ne $expectedTotal -or (Count $target) -ne 0) { throw 'Preload key counts do not match the declaration.' }
$store = Redis $source @('INFO', 'store')
Write-Output $store
if ($store -notmatch '(?m)^Log.TailAddress:(\d+)') { throw 'Cannot measure loaded store size.' }
$loadedLogBytes = [long]$Matches[1] - 64
if ($loadedLogBytes -lt 2GB) { throw "Loaded DB is smaller than 2 GiB: $loadedLogBytes bytes." }
foreach ($keyNumber in @(0, [int]($expectedTotal / 2), ($expectedTotal - 1))) {
    $key = "$keyNumber".PadLeft(8, 'X')
    if ((Redis $source @('STRLEN', $key)) -ne "$valueLength") { throw "Incorrect preloaded value size for $key." }
}
$range = [int[]]($startSlot..$endSlot)
$remaining = [int[]]@(0..16383 | Where-Object { $_ -lt $startSlot -or $_ -gt $endSlot })
for ($iteration = 1; $iteration -le __REPETITIONS__; $iteration++) {
    $beforeSource = Count $source
    $beforeTarget = Count $target
    $output = @(Migrate $source $target)
    $output | Write-Output
    $line = $output | Where-Object { $_ -match 'SLOTS Elapsed Time: ([0-9.]+) seconds' }
    $null = "$line" -match 'SLOTS Elapsed Time: ([0-9.]+) seconds'
    $seconds = [double]::Parse($Matches[1], [Globalization.CultureInfo]::InvariantCulture)
    if ($seconds -le 0) { throw 'Invalid migration duration.' }
    AssertSlots $source $remaining
    AssertSlots $target $range
    $afterSource = Count $source
    $afterTarget = Count $target
    $moved = $afterTarget - $beforeTarget
    if ($startSlot -eq 0 -and $endSlot -eq 16383 -and $moved -ne $expectedTotal) {
        throw 'Full-slot migration did not transfer every loaded key.'
    }
    if ($moved -le 0 -or ($beforeSource - $afterSource) -ne $moved -or
        ($afterSource + $afterTarget) -ne $expectedTotal) { throw 'Migration key conservation failed.' }
    foreach ($keyNumber in @(0, [int]($expectedTotal / 2), ($expectedTotal - 1))) {
        $key = "$keyNumber".PadLeft(8, 'X')
        $slot = [int](Redis $source @('CLUSTER', 'KEYSLOT', $key))
        $owner = if ($slot -ge $startSlot -and $slot -le $endSlot) { $target } else { $source }
        if ((Redis $owner @('STRLEN', $key)) -ne "$valueLength") { throw "Incorrect migrated value size for $key." }
    }
    $result = [pscustomobject]@{
        ParallelMigrateTaskCount = __TASKS__; FastMigrate = [bool]__FAST__
        MigrateBatchSize = __BATCH__
        ValueBytes = $valueLength; KeyBytes = 8; Iteration = $iteration
        StartSlot = $startSlot; EndSlot = $endSlot
        LoadedKeys = $expectedTotal; LoadedLogBytes = $loadedLogBytes
        MovedKeys = $moved; Seconds = $seconds
        ValueGBps = ($moved * [double]$valueLength) / $seconds / 1e9
        KeyValueGBps = ($moved * [double]($valueLength + 8)) / $seconds / 1e9
    }
    Write-Output "MIGRATION_RESULT=$($result | ConvertTo-Json -Compress)"
    if ($iteration -lt __REPETITIONS__) {
        Migrate $target $source
        AssertSlots $source (0..16383)
        AssertSlots $target @()
        if ((Count $source) -ne $expectedTotal -or (Count $target) -ne 0) { throw 'Return migration key conservation failed.' }
    }
}
'@
        $replacements = @{
            '__SOURCE__' = $addresses.vm0; '__TARGET__' = $addresses.vm1; '__TOTAL__' = $profile.DbSize
            '__START__' = $StartSlot; '__END__' = $EndSlot; '__VALUE__' = $valueLength
            '__ROOT__' = $remoteRoot; '__TIMEOUT__' = $TimeoutSec; '__REPETITIONS__' = $Repetitions
            '__TASKS__' = $ParallelMigrateTaskCount; '__FAST__' = [int]$FastMigrate
            '__BATCH__' = $MigrateBatchSize
        }
        foreach ($entry in $replacements.GetEnumerator()) { $measure = $measure.Replace($entry.Key, [string]$entry.Value) }
        $output = @(Invoke-WorkloadRemoteScript -HostName $client.HostName -SshUser $context.SshUser -SshKey $SshKey -Script $measure)
        $output | Set-Content -LiteralPath (Join-Path $results "$valueLength-migration.log")
        $output | Write-Host
        $sourceNode = $context.Nodes | Where-Object Vm -eq 'vm0'
        $inspect = @'
$config = Get-Content /home/guser/garnet-cluster/7000/garnet.conf -Raw | ConvertFrom-Json
$counts = @(Get-Content /dev/shm/garnet-cluster/7000/garnet.log | ForEach-Object {
    if ($_ -match 'discovered (\d+) keys') { [int]$Matches[1] }
    if ($_ -match '(?:fail:|crit:)') { throw "Server migration failure: $_" }
})
if ($counts.Count -eq 0) { throw 'No migration collection batch evidence in the server log.' }
Write-Output ('MIGRATION_BATCH=' + ([pscustomobject]@{
    ParallelMigrateTaskCount = $config.ParallelMigrateTaskCount
    MigrateBatchSize = $config.MigrateBatchSize
    PageSize = $config.PageSize
    Batches = $counts.Count
    MaxKeys = ($counts | Measure-Object -Maximum).Maximum
    MinKeys = ($counts | Measure-Object -Minimum).Minimum
} | ConvertTo-Json -Compress))
'@
        $batchOutput = @(Invoke-WorkloadRemoteScript -HostName $sourceNode.HostName -SshUser $context.SshUser -SshKey $SshKey -Script $inspect)
        $batchOutput | Set-Content -LiteralPath (Join-Path $results "$valueLength-batches.log")
        $batchLine = @($batchOutput | Where-Object { $_ -match '^MIGRATION_BATCH=' })
        if ($batchLine.Count -ne 1) { throw 'Missing migration batch evidence.' }
        $batch = $batchLine[0].Substring('MIGRATION_BATCH='.Length) | ConvertFrom-Json
        if ($batch.ParallelMigrateTaskCount -ne $ParallelMigrateTaskCount -or
            $batch.MigrateBatchSize -ne $MigrateBatchSize -or $batch.MaxKeys -gt $MigrateBatchSize) {
            throw 'Actual migration worker or batch settings do not match the experiment.'
        }
        foreach ($line in $output | Where-Object { $_ -match '^MIGRATION_RESULT=' }) {
            $row = $line.Substring('MIGRATION_RESULT='.Length) | ConvertFrom-Json
            $migrationOps = $row.MovedKeys / $row.Seconds
            $row | Add-Member -NotePropertyName LoadSeconds -NotePropertyValue $load.Seconds
            $row | Add-Member -NotePropertyName LoadThreads -NotePropertyValue $load.Threads
            $row | Add-Member -NotePropertyName LoadBatchSize -NotePropertyValue $profile.BatchSize[0]
            $row | Add-Member -NotePropertyName ObservedMaxMigrationBatchKeys -NotePropertyValue $batch.MaxKeys
            $row | Add-Member -NotePropertyName MigrationSendBufferSize -NotePropertyValue $batch.PageSize
            $row | Add-Member -NotePropertyName LoadOpsPerSecond -NotePropertyValue $load.OpsPerSecond
            $row | Add-Member -NotePropertyName LoadValueGBps -NotePropertyValue ($load.OpsPerSecond * $valueLength / 1e9)
            $row | Add-Member -NotePropertyName MigrationOpsPerSecond -NotePropertyValue $migrationOps
            $row | Add-Member -NotePropertyName MigrationToLoadRatio -NotePropertyValue ($migrationOps / $load.OpsPerSecond)
            $rows.Add($row)
        }
        $rows | Export-Csv -LiteralPath (Join-Path $results 'measurements.csv') -NoTypeInformation
        if (@($rows | Where-Object ValueBytes -eq $valueLength).Count -ne $Repetitions) { throw 'Missing migration measurements.' }
    }
    $rows | Format-Table ValueBytes,Iteration,LoadedKeys,MovedKeys,Seconds,ValueGBps,KeyValueGBps -AutoSize
    Write-Host "Measurements and logs: $results"
} finally {
    try {
        foreach ($node in $prepared) {
            Invoke-WorkloadRemoteScript -HostName $node.HostName -SshUser $context.SshUser -SshKey $SshKey `
                -Script "Remove-Item -LiteralPath '$remoteRoot' -Recurse" | Write-Host
        }
    } finally {
        if ($awake) { [void][MigrationWakeGuard]::SetThreadExecutionState([uint32]2147483648) }
    }
}
