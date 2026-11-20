[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$SourceAddress,
    [Parameter(Mandatory)][string]$TargetAddress,
    [Parameter(Mandatory)][string]$ServerDirectory,
    [Parameter(Mandatory)][string]$MigrateBenchPath,
    [Parameter(Mandatory)][string]$ServerRevision,
    [string]$SshUser = 'guser',
    [string]$OutputDirectory = "/home/guser/migration-throughput-$([datetime]::UtcNow.ToString('yyyyMMdd-HHmmss'))",
    [ValidateSet(8, 256, 1024, 2048, 4096, 16384)][int[]]$ValueLengths = @(8, 256, 1024),
    [ValidateSet(131072, 262144, 524288, 1048576)][int[]]$SendBuffers = @(131072, 262144, 524288),
    [ValidateSet(1, 8, 16, 32, 64)][int[]]$WorkerCounts = @(1, 8, 64),
    [ValidateSet(1, 2, 8)][int[]]$PipelineDepths = @(1, 8),
    [Nullable[bool]]$ScanAhead,
    [ValidateRange(0, 2147483647)][int]$DeleteBatchSize = 0,
    [ValidateSet(0, 1, 8, 16, 32, 64)][int]$LoadThreads = 0,
    [ValidateRange(262144, 67108864)][int]$KeyCount = 2097152,
    [ValidateSet(16, 32, 128)][int]$LogMemoryGiB = 16,
    [ValidateSet(1, 4)][int]$IndexMemoryGiB = 1,
    [ValidateSet(524288, 16777216)][int]$LogPageBytes = 524288,
    [ValidateRange(1, 262143)][int]$CollectionBatchLimit = 262143,
    [switch]$MeasureProtocolBytes,
    [ValidateRange(1, 60)][int]$MinimumSeconds = 5,
    [ValidateRange(2, 100)][int]$MinimumPasses = 2,
    [ValidateRange(2, 100)][int]$MaximumPasses = 20,
    [ValidateRange(1, 60)][int]$NetworkTestSeconds = 10
)
$ErrorActionPreference = 'Stop'

function New-ThroughputServerConfig([string]$BaseText, [string]$Address, [string]$StateDirectory,
    [int]$Workers, [int]$BufferBytes, [int]$PipelineDepth, [int]$LogMemoryGiB = 16, [int]$IndexMemoryGiB = 1,
    [int]$LogPageBytes = 524288, [int]$DeleteBatchSize = 0, [bool]$UseScanAheadMigration = $false) {
    $config = $BaseText.Replace('$port', '7000').Replace('$eth1', $Address).
        Replace('$logMemorySize', "${LogMemoryGiB}g") | ConvertFrom-Json
    $overrides = @{
        Address = $Address; LogMemorySize = "${LogMemoryGiB}g"; IndexMemorySize = "${IndexMemoryGiB}g"; PageSize = "$LogPageBytes"
        CheckpointDir = $StateDirectory; ParallelMigrateTaskCount = $Workers
        MigrationSendBufferSize = "$BufferBytes"; MigrationPipelineDepth = $PipelineDepth
        FastMigrate = $false; EnableVectorSetPreview = $false; LogLevel = 'Information'
        FileLogger = "$StateDirectory-server.log"; DisableConsoleLogger = $true
    }
    foreach ($entry in $overrides.GetEnumerator()) {
        $config | Add-Member -NotePropertyName $entry.Key -NotePropertyValue $entry.Value -Force
    }
    if ($DeleteBatchSize -gt 0) {
        $config.PSObject.Properties.Remove('FastMigrate')
        $config | Add-Member -NotePropertyName UseScanAheadMigration -NotePropertyValue $UseScanAheadMigration -Force
        $config | Add-Member -NotePropertyName MigrationDeleteBatchSize -NotePropertyValue $DeleteBatchSize -Force
    }
    return $config
}

function Get-ThroughputWireStats([string]$Text, [int]$Workers, [long]$KeyCount) {
    $entries = [regex]::Matches($Text, 'MIGRATION_WIRE_STATS records=(\d+) bytes=(\d+) messages=(\d+)')
    if ($entries.Count -ne $Workers) { throw 'Missing unique protocol statistics for every migration worker.' }
    $records = 0L
    $bytes = 0L
    $messages = 0L
    foreach ($entry in $entries) {
        $count = [long]$entry.Groups[1].Value
        $size = [long]$entry.Groups[2].Value
        $commands = [long]$entry.Groups[3].Value
        if ($count -le 0 -or $size -le 0 -or $commands -le 0 -or $commands -gt $count) {
            throw 'Invalid migration protocol statistics.'
        }
        $records += $count
        $bytes += $size
        $messages += $commands
    }
    if ($records -ne $KeyCount) { throw 'Protocol record count does not match the migrated key count.' }
    [pscustomobject]@{ ProtocolBytes = $bytes; ProtocolMessages = $messages; ProtocolRecords = $records }
}

function Get-ThroughputTiming([string]$Text) {
    $durations = [regex]::Matches($Text, 'SLOTS Elapsed Time: ([0-9.]+) seconds')
    $bounds = [regex]::Matches($Text, 'MIGRATION_UTC (\S+) (\S+)')
    if ($durations.Count -ne 1 -or $bounds.Count -ne 1) { throw 'Missing unique migration timing/UTC evidence.' }
    $seconds = [double]::Parse($durations[0].Groups[1].Value, [cultureinfo]::InvariantCulture)
    $start = [datetime]::Parse($bounds[0].Groups[1].Value).ToUniversalTime()
    $end = [datetime]::Parse($bounds[0].Groups[2].Value).ToUniversalTime()
    if ($seconds -le 0 -or $end -le $start -or [math]::Abs(($end - $start).TotalSeconds - $seconds) -gt 0.1) {
        throw 'Migration stopwatch and UTC bounds disagree.'
    }
    [pscustomobject]@{ Seconds = $seconds; Start = $start; End = $end }
}

function Assert-ThroughputSettings([string]$Text, [int]$Workers, [int]$BufferBytes, [int]$PipelineDepth,
    [int]$CollectionBatchLimit = 262143, [Nullable[bool]]$ScanAhead = $null, [int]$DeleteBatchSize = 0) {
    foreach ($pattern in @("NetworkBufferSettings\.sendBufferSize:$BufferBytes\b",
        "ParallelMigrateTasks:$Workers\b", "MigrationPipelineDepth:$PipelineDepth\b")) {
        if ([regex]::Matches($Text, $pattern).Count -ne 1) { throw "Missing unique effective migration setting: $pattern" }
    }
    if ($DeleteBatchSize -gt 0) {
        if ($null -eq $ScanAhead) { throw 'Modern migration options require an explicit scan-ahead mode.' }
        foreach ($pattern in @("UseScanAheadMigration:$ScanAhead\b", "MigrationDeleteBatchSize:$DeleteBatchSize\b")) {
            if ([regex]::Matches($Text, $pattern).Count -ne 1) { throw "Missing unique effective migration setting: $pattern" }
        }
    } elseif ([regex]::Matches($Text, 'FastMigrate:Disabled\b').Count -ne 1) {
        throw 'Missing unique effective migration setting: FastMigrate:Disabled'
    }
    if ($null -ne $ScanAhead -and $DeleteBatchSize -eq 0) {
        $mode = if ($ScanAhead) { 'Enabled' } else { 'Disabled' }
        if ([regex]::Matches($Text, "MigrationScanAhead:$mode\b").Count -ne 1 -or
            [regex]::Matches($Text, 'MigrationScanAhead:(Enabled|Disabled)\b').Count -ne 1) {
            throw 'Missing unique effective scan-ahead setting.'
        }
    }
    $limits = [regex]::Matches($Text, 'MigrationCollectionBatchLimit:(\d+)\b')
    if ($limits.Count -eq 0) {
        if ($CollectionBatchLimit -ne 262143) { throw 'Missing unique effective collection batch limit.' }
    } elseif ($limits.Count -ne 1 -or [int]$limits[0].Groups[1].Value -ne $CollectionBatchLimit) {
        throw 'Incorrect effective collection batch limit.'
    }
}

function Test-ThroughputSlotOwner([string]$Text, [string]$NodeId) {
    $nodes = @($Text -split '\r?\n' | ForEach-Object { ,($_ -split '\s+') } | Where-Object { $_[0] -eq $NodeId })
    return $nodes.Count -eq 1 -and $nodes[0].Count -eq 9 -and
        $nodes[0][7] -eq 'connected' -and $nodes[0][8] -eq '0-16383'
}

function Get-ThroughputLogSize([string]$Text) {
    $begin = [regex]::Matches($Text, '(?m)^Log\.BeginAddress:(\d+)\r?$')
    $tail = [regex]::Matches($Text, '(?m)^Log\.TailAddress:(\d+)\r?$')
    if ($begin.Count -ne 1 -or $tail.Count -ne 1) { throw 'Missing unique log address bounds.' }
    $bytes = [long]$tail[0].Groups[1].Value - [long]$begin[0].Groups[1].Value
    if ($bytes -lt 0) { throw 'Log tail precedes its begin address.' }
    return $bytes
}

function Assert-ThroughputResidentStore([string]$Text, [long]$LogMemoryBytes, [long]$IndexMemoryBytes, [int]$LogPageBytes = 0) {
    $bytes = Get-ThroughputLogSize $Text
    $values = @{}
    foreach ($name in @('Log.BeginAddress', 'Log.HeadAddress', 'Log.MaxMemorySizeBytes', 'IndexMemorySizeBytes')) {
        $matches = [regex]::Matches($Text, "(?m)^$([regex]::Escape($name)):(\d+)\r?$")
        if ($matches.Count -ne 1) { throw "Missing unique resident-store setting: $name" }
        $values[$name] = [long]$matches[0].Groups[1].Value
    }
    if ($values['Log.HeadAddress'] -ne $values['Log.BeginAddress']) {
        throw 'Log records have been evicted; null-device migration requires a fully resident dataset.'
    }
    if ($values['Log.MaxMemorySizeBytes'] -ne $LogMemoryBytes -or $values['IndexMemorySizeBytes'] -ne $IndexMemoryBytes) {
        throw 'Effective log/index memory does not match the experiment configuration.'
    }
    if ($LogPageBytes -gt 0) {
        $pages = [regex]::Matches($Text, '(?m)^Log\.PageSizeBytes:(\d+)\r?$')
        if ($pages.Count -ne 1 -or [long]$pages[0].Groups[1].Value -ne $LogPageBytes) {
            throw 'Effective log page size does not match the experiment configuration.'
        }
    }
    return $bytes
}

function Measure-ThroughputNic([object[]]$Samples, [object[]]$Windows) {
    $bytesRx = 0L
    $bytesTx = 0L
    $duration = 0.0
    $count = 0
    foreach ($window in $Windows) {
        foreach ($sample in $Samples) {
            if ($sample.Start -ge $window.Start -and $sample.End -le $window.End) {
                $bytesRx += $sample.RxBytes
                $bytesTx += $sample.TxBytes
                $duration += ($sample.End - $sample.Start).TotalSeconds
                $count++
            }
        }
    }
    if ($duration -le 0 -or $count -lt 2) { throw 'Insufficient interior NIC samples; increase the dataset or measurement duration.' }
    [pscustomobject]@{ RxGBps = $bytesRx / $duration / 1e9; TxGBps = $bytesTx / $duration / 1e9; Samples = $count; Seconds = $duration }
}

function Get-ThroughputComparison([object[]]$Rows, [object[]]$Passes, [object[]]$Network,
    [string]$OutputDirectory, [string]$SourceAddress, [string]$TargetAddress) {
    $nic = @{}
    foreach ($address in @($SourceAddress, $TargetAddress)) {
        $lines = @(Get-Content (Join-Path (Join-Path $OutputDirectory $address) 'nic.csv'))
        $nic[$address] = @($lines | Where-Object { $_ -notmatch '^INTERFACE=' } | ConvertFrom-Csv | ForEach-Object {
            [pscustomobject]@{
                Start = [datetime]::Parse($_.StartUtc).ToUniversalTime(); End = [datetime]::Parse($_.EndUtc).ToUniversalTime()
                RxBytes = [long]$_.RxBytes; TxBytes = [long]$_.TxBytes
            }
        })
    }
    foreach ($row in $Rows) {
        $windows = @($Passes | Where-Object { $_.Tag.StartsWith("$($row.Cell)-pass", [StringComparison]::Ordinal) } | ForEach-Object {
            [pscustomobject]@{
                Start = [datetime]::Parse($_.StartUtc).ToUniversalTime()
                End = [datetime]::Parse($_.EndUtc).ToUniversalTime()
            }
        })
        $sourceNic = Measure-ThroughputNic $nic[$SourceAddress] $windows
        $targetNic = Measure-ThroughputNic $nic[$TargetAddress] $windows
        $baseline = @($Network | Where-Object Streams -eq $row.Workers)[0].ReceiverGBps
        if ($baseline -le 0) { throw 'Network baseline must be positive.' }
        foreach ($entry in @{
            SourceTxGBps = $sourceNic.TxGBps; SourceRxGBps = $sourceNic.RxGBps
            TargetRxGBps = $targetNic.RxGBps; TargetTxGBps = $targetNic.TxGBps
            SourceNicSamples = $sourceNic.Samples; TargetNicSamples = $targetNic.Samples
            SourceTxPercentOf675GBps = 100 * $sourceNic.TxGBps / 6.75
            TargetRxPercentOf675GBps = 100 * $targetNic.RxGBps / 6.75
            SourceNicWindowSeconds = $sourceNic.Seconds; TargetNicWindowSeconds = $targetNic.Seconds
            IperfReceiverGBps = $baseline
            TargetRxPercentOfIperf = 100 * $targetNic.RxGBps / $baseline
        }.GetEnumerator()) { $row | Add-Member -NotePropertyName $entry.Key -NotePropertyValue $entry.Value }
        $row
    }
}

function Invoke-Node([string]$Address, [string]$Script) {
    $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes("`$ErrorActionPreference='Stop'; $Script"))
    $output = @(& ssh -T -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=10 -o ServerAliveCountMax=3 `
        "$SshUser@$Address" pwsh -NoProfile -OutputFormat Text -EncodedCommand $encoded 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "Remote failure on ${Address}: $($output -join "`n")" }
    $output | ForEach-Object { "$_" }
}
function Stop-ThroughputProcess([string]$Address, [int]$ProcessId) {
    $null = Invoke-Node $Address @"
`$process = Get-Process -Id $ProcessId -ErrorAction Stop
Stop-Process -InputObject `$process -ErrorAction Stop
Wait-Process -InputObject `$process -Timeout 120 -ErrorAction Stop
"@
}
function Redis([string]$Address, [string[]]$Command) {
    $output = @(& timeout --kill-after=5s 180s redis-cli --raw -h $Address -p 7000 @Command 2>&1)
    if ($LASTEXITCODE -ne 0 -or ($output -join "`n") -match '^(?:ERR|NOAUTH|MOVED|ASK|CLUSTERDOWN)') {
        throw "Redis failure on ${Address}: $Command : $output"
    }
    return ($output -join "`n").Trim()
}
function Assert-KeyCount([string]$Address, [long]$Expected, [string]$Tag) {
    $text = Redis $Address @('DBSIZE')
    $actual = 0L
    if (-not [long]::TryParse($text, [ref]$actual) -or $actual -ne $Expected) {
        throw "Key conservation failed on ${Address}: expected $Expected, received '$text'."
    }
    [pscustomobject]@{ Utc = [datetime]::UtcNow.ToString('o'); Tag = $Tag; Address = $Address; Expected = $Expected; Actual = $actual } |
        Export-Csv (Join-Path $OutputDirectory 'key-counts.csv') -Append -NoTypeInformation
}
function Assert-Transfer([string]$From, [string]$To, [int]$ValueBytes, [string]$Tag) {
    Assert-KeyCount $From 0 "$Tag-source"
    Assert-KeyCount $To $KeyCount "$Tag-target"
    foreach ($number in @(0, [int]($KeyCount / 2), ($KeyCount - 1))) {
        $key = "$number".PadLeft(8, 'X')
        if ((Redis $To @('STRLEN', $key)) -ne "$ValueBytes") { throw "Incorrect migrated value length for $key." }
    }
    $null = Redis $From @('FLUSHDB', 'UNSAFETRUNCATELOG')
}
function Invoke-Transfer([string]$From, [string]$To, [string]$Tag, [int]$ValueBytes) {
    $wireOffset = 0L
    if ($MeasureProtocolBytes) {
        $result = Invoke-Node $From "Write-Output ('WIRE_OFFSET=' + (Get-Item '$OutputDirectory/$group-state-server.log').Length)"
        $marker = @($result | Where-Object { $_ -match '^WIRE_OFFSET=\d+$' })
        if ($marker.Count -ne 1) { throw 'Missing unique source protocol log offset.' }
        $wireOffset = [long]$marker[0].Substring(12)
    }
    $output = @(& timeout --signal=TERM --kill-after=10s 300s $MigrateBenchPath `
        --source-endpoint "${From}:7000" --target-endpoint "${To}:7000" `
        --slots 0,16383 --mtype SLOTS --timeout 300 --poll-interval-ms 5 2>&1)
    $code = $LASTEXITCODE
    $text = $output -join "`n"
    $text | Set-Content (Join-Path $OutputDirectory "$Tag.log")
    if ($code -ne 0 -or $text -match 'Error at|ERR |Unhandled exception') { throw "Migration failed: $Tag (exit $code)." }
    $timing = Get-ThroughputTiming $text
    $store = Redis $To @('INFO', 'store')
    $store | Set-Content (Join-Path $OutputDirectory "$Tag-store.log")
    $null = Assert-ThroughputResidentStore $store ($LogMemoryGiB * 1GB) ($IndexMemoryGiB * 1GB) $LogPageBytes
    Assert-Transfer $From $To $ValueBytes $Tag
    $wire = $null
    if ($MeasureProtocolBytes) {
        $result = Invoke-Node $From @"
`$stream = [IO.File]::Open('$OutputDirectory/$group-state-server.log', [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::ReadWrite)
try {
    [void]`$stream.Seek($wireOffset, [IO.SeekOrigin]::Begin)
    `$reader = [IO.StreamReader]::new(`$stream)
    try { `$text = `$reader.ReadToEnd() } finally { `$reader.Dispose() }
} finally { `$stream.Dispose() }
Write-Output ('WIRE_LOG_B64=' + [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(`$text)))
"@
        $marker = @($result | Where-Object { $_ -match '^WIRE_LOG_B64=' })
        if ($marker.Count -ne 1) { throw 'Missing unique source protocol log evidence.' }
        $wireText = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($marker[0].Substring(13)))
        $wireText | Set-Content (Join-Path $OutputDirectory "$Tag-wire.log")
        $wire = Get-ThroughputWireStats $wireText $workers $KeyCount
        if ($wire.ProtocolBytes -le [long]$KeyCount * ($ValueBytes + 8)) { throw 'Protocol bytes must include framing overhead.' }
    }
    [pscustomobject]@{
        Tag = $Tag; Source = $From; Target = $To; Seconds = $timing.Seconds
        Start = $timing.Start; End = $timing.End; VerifiedKeys = $KeyCount
        StartUtc = $timing.Start.ToString('o'); EndUtc = $timing.End.ToString('o')
        ProtocolBytes = $wire.ProtocolBytes; ProtocolMessages = $wire.ProtocolMessages; ProtocolRecords = $wire.ProtocolRecords
    }
}

if (-not $IsLinux) { throw 'Run this coordinator on the Linux client VM, with shared SSH access to both primaries.' }
if ($SourceAddress -eq $TargetAddress) { throw 'Source and target must differ.' }
if ($KeyCount % 4096 -ne 0) { throw 'KeyCount must be divisible by the SET pipeline size (4096).' }
if ($MaximumPasses -lt $MinimumPasses) { throw 'MaximumPasses must be at least MinimumPasses.' }
if ($DeleteBatchSize -gt 0 -and $null -eq $ScanAhead) { throw 'Modern migration options require an explicit scan-ahead mode.' }
foreach ($valueBytes in $ValueLengths) {
    if ([long]$KeyCount * ($valueBytes + 64) -gt 0.75 * $LogMemoryGiB * 1GB) {
        throw 'Dataset exceeds the conservative resident-log budget; increase LogMemoryGiB.'
    }
}
if (Test-Path $OutputDirectory) { throw "Output directory already exists: $OutputDirectory" }
$null = New-Item -ItemType Directory $OutputDirectory
$basePath = Join-Path $PSScriptRoot 'garnet-cluster-diskless.conf'
$baseText = Get-Content $basePath -Raw
$processes = [Collections.Generic.List[object]]::new()
$rows = [Collections.Generic.List[object]]::new()
$passes = [Collections.Generic.List[object]]::new()
try {
    foreach ($address in @($SourceAddress, $TargetAddress)) {
        $null = Invoke-Node $address @"
if (@(& ss -H -ltn 'sport = :7000').Count -ne 0) { throw 'Port 7000 is occupied; stop the intended experiment instance explicitly before running.' }
New-Item -ItemType Directory -Force '$OutputDirectory' | Out-Null
"@
        & scp -q (Join-Path $PSScriptRoot 'sample-nic.ps1') "${SshUser}@${address}:$OutputDirectory/sample-nic.ps1"
        if ($LASTEXITCODE) { throw "NIC sampler upload failed on $address." }
        $peer = if ($address -eq $SourceAddress) { $TargetAddress } else { $SourceAddress }
        $result = Invoke-Node $address @"
`$process = Start-Process pwsh -ArgumentList '-NoProfile','-File','$OutputDirectory/sample-nic.ps1','-Peer','$peer','-OutputPath','$OutputDirectory/nic.csv' -PassThru -RedirectStandardOutput '$OutputDirectory/nic-console.log' -RedirectStandardError '$OutputDirectory/nic.err'
Write-Output "OWNED_PID=`$(`$process.Id)"
"@
        $pidLine = @($result | Where-Object { $_ -match '^OWNED_PID=(\d+)$' })
        if ($pidLine.Count -ne 1) { throw "Missing sampler PID on $address." }
        $processes.Add([pscustomobject]@{ Address = $address; Id = [int]$pidLine[0].Split('=')[1]; Kind = 'sampler' })
    }
    $result = Invoke-Node $TargetAddress @"
`$process = Start-Process iperf3 -ArgumentList '-s','-B','$TargetAddress','-p','7000' -PassThru -RedirectStandardOutput '$OutputDirectory/iperf-server.log' -RedirectStandardError '$OutputDirectory/iperf-server.err'
Write-Output "OWNED_PID=`$(`$process.Id)"
"@
    $pidLine = @($result | Where-Object { $_ -match '^OWNED_PID=(\d+)$' })
    if ($pidLine.Count -ne 1) { throw 'Missing network-test server PID.' }
    $processes.Add([pscustomobject]@{ Address = $TargetAddress; Id = [int]$pidLine[0].Split('=')[1]; Kind = 'iperf' })
    Start-Sleep -Seconds 1
    $network = foreach ($streams in $WorkerCounts) {
        $result = Invoke-Node $SourceAddress @"
& timeout --kill-after=5s $($NetworkTestSeconds + 15)s iperf3 -c '$TargetAddress' -B '$SourceAddress' -p 7000 -P $streams -t $NetworkTestSeconds -O 2 --connect-timeout 5000 -J | Set-Content '$OutputDirectory/iperf-$streams.json'
if (`$LASTEXITCODE) { throw 'Network baseline failed.' }
Write-Output ('NETWORK_JSON=' + [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes([IO.File]::ReadAllText('$OutputDirectory/iperf-$streams.json'))))
"@
        $jsonLine = @($result | Where-Object { $_ -match '^NETWORK_JSON=' })
        if ($jsonLine.Count -ne 1) { throw 'Missing unique network baseline result.' }
        $json = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($jsonLine[0].Substring(13))) | ConvertFrom-Json
        if ($json.error) { throw "Network baseline failed: $($json.error)" }
        [pscustomobject]@{
            Streams = $streams; SenderGBps = $json.end.sum_sent.bits_per_second / 8e9
            ReceiverGBps = $json.end.sum_received.bits_per_second / 8e9
        }
    }
    $network | Export-Csv (Join-Path $OutputDirectory 'network-baseline.csv') -NoTypeInformation
    Write-Output ($network | Format-Table | Out-String)
    Stop-ThroughputProcess $TargetAddress $processes[-1].Id
    $processes.RemoveAt($processes.Count - 1)

    foreach ($buffer in $SendBuffers) {
        foreach ($workers in $WorkerCounts) {
            foreach ($depth in $PipelineDepths) {
                $group = "b$buffer-w$workers-p$depth"
                if ($null -ne $ScanAhead) { $group += "-sa$([int]$ScanAhead)" }
                $preloadWorkers = if ($LoadThreads) { $LoadThreads } else { $workers }
                Write-Output "START $([datetime]::UtcNow.ToString('o')) $group"
                foreach ($address in @($SourceAddress, $TargetAddress)) {
                    $config = New-ThroughputServerConfig $baseText $address "$OutputDirectory/$group-state" $workers $buffer $depth $LogMemoryGiB $IndexMemoryGiB $LogPageBytes $DeleteBatchSize ([bool]$ScanAhead)
                    $configText = $config | ConvertTo-Json -Depth 10
                    $configText | Set-Content (Join-Path $OutputDirectory "$group-$address.conf")
                    $encodedConfig = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($configText))
                    $scanAheadEnvironment = if ($null -ne $ScanAhead -and $DeleteBatchSize -eq 0) { "`$env:GARNET_BENCHMARK_SCAN_AHEAD = '$ScanAhead'" } else { '' }
                    $result = Invoke-Node $address @"
[IO.File]::WriteAllText('$OutputDirectory/$group.conf', [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('$encodedConfig')))
if (@(& ss -H -ltn 'sport = :7000').Count -ne 0) { throw 'Previous server still holds port 7000.' }
$scanAheadEnvironment
`$process = Start-Process '$ServerDirectory/GarnetServer' -ArgumentList '--config-import-path=$OutputDirectory/$group.conf' -PassThru -RedirectStandardOutput '$OutputDirectory/$group-console.log' -RedirectStandardError '$OutputDirectory/$group-server.err'
Write-Output "OWNED_PID=`$(`$process.Id)"
"@
                    $pidLine = @($result | Where-Object { $_ -match '^OWNED_PID=(\d+)$' })
                    if ($pidLine.Count -ne 1) { throw "Missing server PID on $address." }
                    $processes.Add([pscustomobject]@{ Address = $address; Id = [int]$pidLine[0].Split('=')[1]; Kind = 'server' })
                    $ready = $false
                    for ($attempt = 0; $attempt -lt 100; $attempt++) {
                        $reply = & redis-cli --raw -h $address -p 7000 PING 2>$null
                        if ($LASTEXITCODE -eq 0 -and $reply -eq 'PONG') { $ready = $true; break }
                        Start-Sleep -Milliseconds 100
                    }
                    if (-not $ready) { throw "Server failed to start on $address; inspect $group-server.log/err." }
                    $startup = (Invoke-Node $address "Get-Content '$OutputDirectory/$group-state-server.log' -Raw") -join "`n"
                    Assert-ThroughputSettings $startup $workers $buffer $depth $CollectionBatchLimit $ScanAhead $DeleteBatchSize
                }
                $null = Redis $SourceAddress @('CLUSTER', 'SET-CONFIG-EPOCH', '1')
                $null = Redis $TargetAddress @('CLUSTER', 'SET-CONFIG-EPOCH', '2')
                $null = Redis $SourceAddress @('CLUSTER', 'MEET', $TargetAddress, '7000')
                $null = Redis $SourceAddress @('CLUSTER', 'ADDSLOTSRANGE', '0', '16383')
                $sourceId = Redis $SourceAddress @('CLUSTER', 'MYID')
                $clusterReady = $false
                for ($attempt = 0; $attempt -lt 100; $attempt++) {
                    if ((Redis $SourceAddress @('CLUSTER', 'INFO')) -match 'cluster_state:ok' -and
                        (Redis $TargetAddress @('CLUSTER', 'INFO')) -match 'cluster_state:ok' -and
                        (Test-ThroughputSlotOwner (Redis $SourceAddress @('CLUSTER', 'NODES')) $sourceId) -and
                        (Test-ThroughputSlotOwner (Redis $TargetAddress @('CLUSTER', 'NODES')) $sourceId)) {
                        $clusterReady = $true
                        break
                    }
                    Start-Sleep -Milliseconds 100
                }
                if (-not $clusterReady) { throw 'Cluster did not converge.' }
                foreach ($valueBytes in $ValueLengths) {
                    $cell = "$group-v$valueBytes"
                    $null = Redis $SourceAddress @('FLUSHDB', 'UNSAFETRUNCATELOG')
                    $null = Redis $TargetAddress @('FLUSHDB', 'UNSAFETRUNCATELOG')
                    $load = @(& Resp.benchmark --host $SourceAddress --port 7000 --dbsize $KeyCount `
                        --keylength 8 --valuelength $valueBytes --lset --load-threads $preloadWorkers `
                        --threads $preloadWorkers --batchsize 4096 --runtime 0 2>&1)
                    $code = $LASTEXITCODE
                    $loadText = $load -join "`n"
                    $loadText | Set-Content (Join-Path $OutputDirectory "$cell-load.log")
                    if ($code -ne 0 -or $loadText -notmatch 'Operation type: SET') { throw "SET preload failed: $cell." }
                    $store = Redis $SourceAddress @('INFO', 'store')
                    $store | Set-Content (Join-Path $OutputDirectory "$cell-store.log")
                    $logBytes = Assert-ThroughputResidentStore $store ($LogMemoryGiB * 1GB) ($IndexMemoryGiB * 1GB) $LogPageBytes
                    Assert-KeyCount $SourceAddress $KeyCount "$cell-preload-source"
                    Assert-KeyCount $TargetAddress 0 "$cell-preload-target"
                    if ($logBytes -lt 2 * ($WorkerCounts | Measure-Object -Maximum).Maximum * $LogPageBytes) {
                        throw 'Dataset is too small to supply two log pages per maximum worker.'
                    }
                    $null = Invoke-Transfer $SourceAddress $TargetAddress "$cell-warmup-forward" $valueBytes
                    $null = Invoke-Transfer $TargetAddress $SourceAddress "$cell-warmup-return" $valueBytes
                    $measured = [Collections.Generic.List[object]]::new()
                    $seconds = 0.0
                    for ($pass = 1; $pass -le $MaximumPasses; $pass++) {
                        $transfer = Invoke-Transfer $SourceAddress $TargetAddress "$cell-pass$pass" $valueBytes
                        $measured.Add($transfer)
                        $passes.Add($transfer)
                        $passes | Export-Csv (Join-Path $OutputDirectory 'passes.csv') -NoTypeInformation
                        $seconds += $transfer.Seconds
                        $null = Invoke-Transfer $TargetAddress $SourceAddress "$cell-return$pass" $valueBytes
                        if ($pass -ge $MinimumPasses -and $seconds -ge $MinimumSeconds) { break }
                    }
                    if ($seconds -lt $MinimumSeconds -or $measured.Count -lt $MinimumPasses) { throw "Measurement too short after $MaximumPasses passes: $cell." }
                    $ops = $KeyCount * [double]$measured.Count / $seconds
                    $row = [pscustomobject]@{
                        Cell = $cell; Workers = $workers; ValueBytes = $valueBytes; SendBufferBytes = $buffer
                        PipelineDepth = $depth; FastMigrate = $false; KeyCount = $KeyCount; Passes = $measured.Count
                        ScanAhead = $ScanAhead
                        LoadThreads = $preloadWorkers
                        DeleteBatchSize = $DeleteBatchSize
                        CollectionBatchLimit = $CollectionBatchLimit
                        MigrationSeconds = $seconds; MigrationKeysPerSecond = $ops; PayloadGBps = $ops * $valueBytes / 1e9
                        KeyValueGBps = $ops * ($valueBytes + 8) / 1e9; PayloadPercentOf675GBps = 100 * $ops * $valueBytes / 1e9 / 6.75
                        UsefulGBps = $ops * ($valueBytes + 8) / 1e9; UsefulMiBps = $ops * ($valueBytes + 8) / 1MB
                        ProtocolBytes = $(if ($MeasureProtocolBytes) { ($measured | Measure-Object ProtocolBytes -Sum).Sum } else { $null })
                        ProtocolMessages = $(if ($MeasureProtocolBytes) { ($measured | Measure-Object ProtocolMessages -Sum).Sum } else { $null })
                        ProtocolGBps = $(if ($MeasureProtocolBytes) { ($measured | Measure-Object ProtocolBytes -Sum).Sum / $seconds / 1e9 } else { $null })
                        LoadedLogBytes = $logBytes; PageBytes = $LogPageBytes; ServerRevision = $ServerRevision
                        LogMemoryBytes = $LogMemoryGiB * 1GB; IndexMemoryBytes = $IndexMemoryGiB * 1GB
                        EstimatedBatchesPerWorker = $KeyCount / [double]$workers / $CollectionBatchLimit
                    }
                    $rows.Add($row)
                    $rows | Export-Csv (Join-Path $OutputDirectory 'measurements.csv') -NoTypeInformation
                    Write-Output ("RESULT {0} keys/s={1:F0} payloadGB/s={2:F5} seconds={3:F3} passes={4}; VERIFIED {5}" -f $cell, $ops, $row.PayloadGBps, $seconds, $measured.Count, $rows.Count)
                }
                foreach ($process in @($processes | Where-Object Kind -eq 'server')) {
                    Stop-ThroughputProcess $process.Address $process.Id
                    $null = $processes.Remove($process)
                }
            }
        }
    }
} finally {
    foreach ($process in $processes) {
        try { Stop-ThroughputProcess $process.Address $process.Id }
        catch { Write-Warning "Cleanup failed for $($process.Kind) PID $($process.Id) on $($process.Address): $_" }
    }
    foreach ($address in @($SourceAddress, $TargetAddress)) {
        & scp -q -r "${SshUser}@${address}:$OutputDirectory" (Join-Path $OutputDirectory $address)
        if ($LASTEXITCODE) { Write-Warning "Could not retrieve remote evidence from $address." }
    }
}

$comparison = Get-ThroughputComparison $rows.ToArray() $passes.ToArray() @($network) $OutputDirectory $SourceAddress $TargetAddress
$comparison | Export-Csv (Join-Path $OutputDirectory 'comparison.csv') -NoTypeInformation
$expected = $ValueLengths.Count * $SendBuffers.Count * $WorkerCounts.Count * $PipelineDepths.Count
if ($rows.Count -ne $expected) { throw "Expected $expected verified cells, found $($rows.Count)." }
Write-Output "COMPLETE $expected verified cells: $OutputDirectory"
