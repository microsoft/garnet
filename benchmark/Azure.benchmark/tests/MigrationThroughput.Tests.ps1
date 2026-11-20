$benchmarkDir = Split-Path -Parent $PSScriptRoot
. (Join-Path $benchmarkDir 'experiments\migration\validation.ps1')
. (Join-Path $benchmarkDir 'experiments\migration\foreground.ps1')
$tokens = $null
$errors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile(
    (Join-Path $benchmarkDir 'experiments\migration\throughput.ps1'), [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw ($errors | Out-String) }
foreach ($name in @('New-ThroughputServerConfig', 'Get-ThroughputTiming', 'Assert-ThroughputSettings', 'Test-ThroughputSlotOwner', 'Get-ThroughputLogSize', 'Assert-ThroughputResidentStore', 'Measure-ThroughputNic', 'Get-ThroughputWireStats')) {
    $function = $ast.Find({
        param($node)
        $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $name
    }, $true)
    Invoke-Expression $function.Extent.Text
}
Describe 'Migration validation policy' {
    It 'fully verifies up to one GiB and uses DBSIZE above it' -TestCases @(
        @{ Keys = 262144; Mode = 'Full' }
        @{ Keys = 524288; Mode = 'Full' }
        @{ Keys = 524289; Mode = 'DbSizeOnly' }
        @{ Keys = 4194304; Mode = 'DbSizeOnly' }
        @{ Keys = 52232192; Mode = 'DbSizeOnly' }
    ) {
        param($Keys, $Mode)
        (Get-MigrationValidationPolicy $Keys 2048).Mode | Should Be $Mode
    }

    It 'requires exact source and target counts in both directions' {
        $dir = Join-Path $TestDrive 'counts'
        $null = New-Item -ItemType Directory -Force $dir
        Get-MigrationValidationPolicy 4194304 2048 | ConvertTo-Json |
            Set-Content (Join-Path $dir 'validation-policy.json')
        $counts = foreach ($direction in 'pass', 'return') {
            [pscustomobject]@{ Tag = "test-$($direction)1-source"; Expected = 0; Actual = 0 }
            [pscustomobject]@{ Tag = "test-$($direction)1-target"; Expected = 4194304; Actual = 4194304 }
        }
        $counts | Export-Csv (Join-Path $dir 'key-counts.csv') -NoTypeInformation
        Assert-MigrationValidationResult $dir 4194304 2048 1 0 | Should Be 'DbSizeOnly'
        { Assert-MigrationValidationResult $dir 4194304 2048 1 1 } | Should Throw 'must not claim'
        $counts[-1].Actual--
        $counts | Export-Csv (Join-Path $dir 'key-counts.csv') -NoTypeInformation
        { Assert-MigrationValidationResult $dir 4194304 2048 1 0 } | Should Throw 'DBSIZE evidence'
        $counts | Select-Object -Skip 1 | Export-Csv (Join-Path $dir 'key-counts.csv') -NoTypeInformation
        { Assert-MigrationValidationResult $dir 4194304 2048 1 0 } | Should Throw 'DBSIZE evidence'
    }

    It 'still requires acknowledged-write verification for a one-GiB dataset' {
        $dir = Join-Path $TestDrive 'full'
        $null = New-Item -ItemType Directory -Force $dir
        Get-MigrationValidationPolicy 524288 2048 | ConvertTo-Json |
            Set-Content (Join-Path $dir 'validation-policy.json')
        $counts = foreach ($direction in 'pass', 'return') {
            [pscustomobject]@{ Tag = "test-$($direction)1-source"; Expected = 0; Actual = 0 }
            [pscustomobject]@{ Tag = "test-$($direction)1-target"; Expected = 524288; Actual = 524288 }
        }
        $counts | Export-Csv (Join-Path $dir 'key-counts.csv') -NoTypeInformation
        { Assert-MigrationValidationResult $dir 524288 2048 1 0 } | Should Throw 'acknowledged-write verification'
        Assert-MigrationValidationResult $dir 524288 2048 1 1 | Should Be 'Full'
        { Assert-MigrationValidationResult $dir 4194304 2048 1 1 } | Should Throw 'Incorrect recorded'
    }
}

Describe 'Streamlined migration experiment' {
    It 'configures matching insertion-complete pipeline modes without modifying the shared profile' -TestCases @(
        @{ Depth = 1; Workers = 1; Buffer = 131072 }
        @{ Depth = 8; Workers = 64; Buffer = 524288 }
        @{ Depth = 2; Workers = 32; Buffer = 1048576 }
    ) {
        param($Depth, $Workers, $Buffer)
        $text = Get-Content (Join-Path $benchmarkDir '02-platform\tools\config\garnet\garnet-cluster-diskless.conf') -Raw
        $config = New-ThroughputServerConfig $text '10.0.0.1' '/tmp/test' $Workers $Buffer $Depth
        $config.MigrationPipelineDepth | Should Be $Depth
        $config.ParallelMigrateTaskCount | Should Be $Workers
        $config.MigrationSendBufferSize | Should Be "$Buffer"
        $config.FastMigrate | Should Be $false
        $config.PageSize | Should Be '524288'
        $config.EnableStorageTier | Should Be $false
        $config.UseAofNullDevice | Should Be $true
        $text.Contains('$eth1') | Should Be $true
    }

    Context 'Once-per-case foreground windows' {
        function Send-WorkloadControl([string]$Name, [bool]$Running = $false, [bool]$Validate = $false,
            [Nullable[datetime]]$WindowStart = $null, [Nullable[datetime]]$WindowEnd = $null, [bool]$Stop = $false) {
            throw 'Control must be mocked.'
        }
        function Wait-WorkloadDuration([int]$Seconds) { throw 'Wait must be mocked.' }
        function Redis([string]$Address, [string[]]$Command) { throw 'Redis must be mocked.' }

        It 'runs one baseline and preserves minimum pass and duration requirements' {
            $OutputDirectory = Join-Path $TestDrive 'orchestration'
            $null = New-Item -ItemType Directory $OutputDirectory
            $SourceAddress = 'source'; $TargetAddress = 'target'
            $BaselineSeconds = 30; $MinimumPasses = 5; $MaximumPasses = 40; $MinimumSeconds = 30
            $passes = [Collections.Generic.List[object]]::new()
            Mock Send-WorkloadControl { [pscustomobject]@{ Phase = $Name } }
            Mock Wait-WorkloadDuration {}
            function Invoke-Transfer([string]$From, [string]$To, [string]$Tag, [int]$ValueBytes,
                [int]$PassNumber, [double]$PriorMigrationSeconds) {
                if ($Tag -match '-pass(\d+)$') {
                    if ($PassNumber -ne [int]$Matches[1] -or $PriorMigrationSeconds -ne ($PassNumber - 1) * 2.5) {
                        throw 'Incorrect final-pass inputs.'
                    }
                }
                [pscustomobject]@{ Tag = $Tag; Seconds = 2.5 }
            }
            $result = Invoke-MigrationMeasuredPasses 'test' 2048
            $result.Measured.Count | Should Be 12
            $result.Seconds | Should Be 30
            $passes.Count | Should Be 12
            Assert-MockCalled Send-WorkloadControl -Scope It -Times 1 -Exactly -ParameterFilter { $Name -eq 'test-baseline' }
            Assert-MockCalled Send-WorkloadControl -Scope It -Times 1 -Exactly -ParameterFilter { $Name -eq 'test-baseline-paused' }
            Assert-MockCalled Wait-WorkloadDuration -Scope It -Times 1 -Exactly
        }

        It 'runs recovery only when both final-pass requirements are satisfied' {
            $cell = 'test'
            $MinimumPasses = 3; $MinimumSeconds = 7.5; $RecoverySeconds = 30
            Mock Send-WorkloadControl {}
            Mock Wait-WorkloadDuration {}
            Invoke-MigrationRecovery 1 10 | Should Be $false
            Invoke-MigrationRecovery 3 2.5 | Should Be $false
            Invoke-MigrationRecovery 3 7.5 | Should Be $true
            Assert-MockCalled Send-WorkloadControl -Scope It -Times 1 -Exactly -ParameterFilter { $Name -eq 'test-recovery' -and $Running }
            Assert-MockCalled Wait-WorkloadDuration -Scope It -Times 1 -Exactly -ParameterFilter { $Seconds -eq 30 }
            Assert-MockCalled Send-WorkloadControl -Scope It -Times 0 -Exactly -ParameterFilter { $Name -match '-baseline$' }
        }

        It 'aggregates singleton windows once and preserves legacy per-pass evidence' -TestCases @(
            @{ Once = $true; ExpectedSeconds = 30; ExpectedMode = 'OncePerCase' }
            @{ Once = $false; ExpectedSeconds = 60; ExpectedMode = 'PerPass' }
        ) {
            param($Once, $ExpectedSeconds, $ExpectedMode)
            $dir = Join-Path $TestDrive "summary-$ExpectedMode"
            $null = New-Item -ItemType Directory $dir
            $distribution = @{ Count = 10; Errors = 0; Bins = @(@{ Value = 100; Count = 10 }) }
            $measurements = @{
                Seconds = 30; GET = $distribution; SET = $distribution
                ProfiledOperations = 0; SampledAsk = 0; SampledMoved = 0
            }
            foreach ($pass in 1, 2) {
                $phases = @(@{ Name = "test-pass$pass-migration"; Measurements = $measurements })
                if (-not $Once) {
                    $phases += @{ Name = "test-pass$pass-baseline"; Measurements = $measurements }
                    $phases += @{ Name = "test-pass$pass-recovery"; Measurements = $measurements }
                }
                @{
                    Phase = "test-pass$pass-paused"; VerifiedWrites = 0; Phases = $phases
                    Window = @{ Seconds = 2; Measurements = $measurements }
                } | ConvertTo-Json -Depth 20 | Set-Content "$dir/test-pass$pass-foreground.json"
            }
            if ($Once) {
                @{ Mode = 'OncePerCase'; Cell = 'test'; BaselineSeconds = 30; RecoverySeconds = 30 } |
                    ConvertTo-Json | Set-Content "$dir/measurement-windows.json"
                foreach ($phase in 'baseline', 'recovery') {
                    @{ Phases = @(@{ Name = "test-$phase"; Measurements = $measurements }) } |
                        ConvertTo-Json -Depth 20 | Set-Content "$dir/foreground-$phase.json"
                }
            }
            $summary = Get-ForegroundSummary $dir
            $summary.MeasurementWindowMode | Should Be $ExpectedMode
            $summary.Passes | Should Be 2
            ($summary.Phases | Where-Object Phase -eq 'baseline').Seconds | Should Be $ExpectedSeconds
            ($summary.Phases | Where-Object Phase -eq 'recovery').Seconds | Should Be $ExpectedSeconds
            ($summary.Phases | Where-Object Phase -eq 'migration').GET.Count | Should Be 20
            ($summary.Phases | Where-Object Phase -eq 'migration').Seconds | Should Be 4
            if ($Once) {
                Remove-Item "$dir/foreground-recovery.json"
                { Get-ForegroundSummary $dir } | Should Throw 'Missing once-per-case recovery evidence'
                @{ Phases = @(@{ Name = 'test-recovery'; Measurements = @{ Seconds = 1 } }) } |
                    ConvertTo-Json -Depth 20 | Set-Content "$dir/foreground-recovery.json"
                { Get-ForegroundSummary $dir } | Should Throw 'incomplete once-per-case recovery'
            }
        }
    }

    It 'supports larger resident datasets without changing batch or page sizes' {
        $text = Get-Content (Join-Path $benchmarkDir '02-platform\tools\config\garnet\garnet-cluster-diskless.conf') -Raw
        $config = New-ThroughputServerConfig $text '10.0.0.1' '/tmp/test' 64 262144 8 128 4
        $config.LogMemorySize | Should Be '128g'
        $config.IndexMemorySize | Should Be '4g'
        $config.PageSize | Should Be '524288'
        $config.DeviceType | Should Be 'Null'
        $config.ParallelMigrateTaskCount | Should Be 64
    }

    It 'supports sixteen workers with default-sized log pages and a one-MiB send buffer' {
        $text = Get-Content (Join-Path $benchmarkDir '02-platform\tools\config\garnet\garnet-cluster-diskless.conf') -Raw
        $config = New-ThroughputServerConfig $text '10.0.0.1' '/tmp/test' 16 1048576 8 128 4 16777216
        $config.PageSize | Should Be '16777216'
        $config.ParallelMigrateTaskCount | Should Be 16
        $config.MigrationSendBufferSize | Should Be '1048576'
    }

    It 'requires complete worker protocol counters and exact record conservation' {
        $text = "MIGRATION_WIRE_STATS records=7 bytes=1000 messages=2`nMIGRATION_WIRE_STATS records=5 bytes=800 messages=1"
        $wire = Get-ThroughputWireStats $text 2 12
        $wire.ProtocolBytes | Should Be 1800
        $wire.ProtocolMessages | Should Be 3
        { Get-ThroughputWireStats $text 3 12 } | Should Throw 'every migration worker'
        { Get-ThroughputWireStats $text 2 13 } | Should Throw 'key count'
        { Get-ThroughputWireStats 'MIGRATION_WIRE_STATS records=1 bytes=10 messages=2' 1 1 } |
            Should Throw 'Invalid'
    }

    It 'requires actual startup evidence for every migration setting' {
        $text = "NetworkBufferSettings.sendBufferSize:131072`nParallelMigrateTasks:64`nMigrationPipelineDepth:8`nFastMigrate:Disabled"
        Assert-ThroughputSettings $text 64 131072 8
        { Assert-ThroughputSettings $text 64 131072 1 } | Should Throw 'effective migration setting'
        { Assert-ThroughputSettings "$text`n$text" 64 131072 8 } | Should Throw 'effective migration setting'
    }

    It 'requires exact startup evidence when scan-ahead is selected' -TestCases @(
        @{ Enabled = $false; Mode = 'Disabled' }
        @{ Enabled = $true; Mode = 'Enabled' }
    ) {
        param($Enabled, $Mode)
        $text = "NetworkBufferSettings.sendBufferSize:1048576`nParallelMigrateTasks:32`nMigrationPipelineDepth:2`nFastMigrate:Disabled`nMigrationCollectionBatchLimit:4096"
        { Assert-ThroughputSettings $text 32 1048576 2 4096 $Enabled } | Should Throw 'scan-ahead setting'
        Assert-ThroughputSettings "$text`nMigrationScanAhead:$Mode" 32 1048576 2 4096 $Enabled
        { Assert-ThroughputSettings "$text`nMigrationScanAhead:Enabled`nMigrationScanAhead:Disabled" 32 1048576 2 4096 $Enabled } |
            Should Throw 'scan-ahead setting'
    }

    It 'supports native scan-ahead and delete batching without removed FastMigrate configuration' {
        $text = Get-Content (Join-Path $benchmarkDir '02-platform\tools\config\garnet\garnet-cluster-diskless.conf') -Raw
        $config = New-ThroughputServerConfig $text '10.0.0.1' '/tmp/test' 16 1048576 2 128 4 16777216 256 $false
        @($config.PSObject.Properties.Name) -contains 'FastMigrate' | Should Be $false
        $config.UseScanAheadMigration | Should Be $false
        $config.MigrationDeleteBatchSize | Should Be 256
        $startup = "NetworkBufferSettings.sendBufferSize:1048576`nParallelMigrateTasks:16`nMigrationPipelineDepth:2`nMigrationCollectionBatchLimit:4096`nUseScanAheadMigration:False`nMigrationDeleteBatchSize:256"
        Assert-ThroughputSettings $startup 16 1048576 2 4096 $false 256
        { Assert-ThroughputSettings $startup 16 1048576 2 4096 $true 256 } | Should Throw 'effective migration setting'
        { Assert-ThroughputSettings $startup 16 1048576 2 4096 $false 1 } | Should Throw 'effective migration setting'
        { Assert-ThroughputSettings $startup 16 1048576 2 4096 $null 256 } | Should Throw 'explicit scan-ahead'
    }

    It 'requires effective server evidence for a non-default collection limit' {
        $text = "NetworkBufferSettings.sendBufferSize:262144`nParallelMigrateTasks:64`nMigrationPipelineDepth:8`nFastMigrate:Disabled"
        { Assert-ThroughputSettings $text 64 262144 8 4096 } | Should Throw 'collection batch limit'
        Assert-ThroughputSettings "$text`nMigrationCollectionBatchLimit:4096" 64 262144 8 4096
        { Assert-ThroughputSettings "$text`nMigrationCollectionBatchLimit:4095" 64 262144 8 4096 } |
            Should Throw 'collection batch limit'
        { Assert-ThroughputSettings "$text`nMigrationCollectionBatchLimit:4096" 64 262144 8 } |
            Should Throw 'collection batch limit'
        { Assert-ThroughputSettings "$text`nMigrationCollectionBatchLimit:4096`nMigrationCollectionBatchLimit:4096" 64 262144 8 4096 } |
            Should Throw 'collection batch limit'
    }

    It 'waits for the remote slot map rather than accepting a healthy but unconverged cluster' {
        Test-ThroughputSlotOwner "source 10.0.0.1:7000 master - 0 0 1 connected 0-16383`ntarget 10.0.0.2:7000 myself,master - 0 0 2 connected" source |
            Should Be $true
        Test-ThroughputSlotOwner "source 10.0.0.1:7000 master - 0 0 1 connected" source | Should Be $false
        Test-ThroughputSlotOwner "target 10.0.0.2:7000 master - 0 0 2 connected 0-16383" source | Should Be $false
        Test-ThroughputSlotOwner "source 10.0.0.1:7000 master - 0 0 1 connected 0-16383 [1->-target]" source |
            Should Be $false
    }

    It 'uses precise UTC boundaries and rejects contradictory timing' {
        $text = "SLOTS Elapsed Time: 2.5 seconds`nMIGRATION_UTC 2026-11-09T12:00:00.0000000Z 2026-11-09T12:00:02.5000000Z"
        $timing = Get-ThroughputTiming $text
        $timing.Seconds | Should Be 2.5
        $timing.Start.Kind | Should Be 'Utc'
        { Get-ThroughputTiming ($text.Replace('2.5 seconds', '4.5 seconds')) } | Should Throw 'disagree'
        { Get-ThroughputTiming 'SLOTS Elapsed Time: 1 seconds' } | Should Throw 'timing/UTC'
    }

    It 'measures the active log after truncation instead of counting retired addresses' {
        Get-ThroughputLogSize "Log.BeginAddress:1000000`r`nLog.TailAddress:1200000`r`n" | Should Be 200000
        { Get-ThroughputLogSize 'Log.TailAddress:1200000' } | Should Throw 'address bounds'
        { Get-ThroughputLogSize "Log.BeginAddress:1200000`nLog.TailAddress:1000000" } | Should Throw 'precedes'
    }

    It 'weights contained NIC samples by duration and excludes gaps between transfers' {
        $start = [datetime]'2026-11-09T12:00:00Z'
        $samples = @(
            [pscustomobject]@{ Start = $start; End = $start.AddSeconds(1); RxBytes = 1e9; TxBytes = 2e9 }
            [pscustomobject]@{ Start = $start.AddSeconds(1); End = $start.AddSeconds(3); RxBytes = 4e9; TxBytes = 4e9 }
            [pscustomobject]@{ Start = $start.AddSeconds(3); End = $start.AddSeconds(4); RxBytes = 9e9; TxBytes = 9e9 }
            [pscustomobject]@{ Start = $start.AddSeconds(5); End = $start.AddSeconds(6); RxBytes = 3e9; TxBytes = 2e9 }
        )
        $windows = @(
            [pscustomobject]@{ Start = $start; End = $start.AddSeconds(3) }
            [pscustomobject]@{ Start = $start.AddSeconds(5); End = $start.AddSeconds(6) }
        )
        $result = Measure-ThroughputNic $samples $windows
        $result.Samples | Should Be 3
        $result.Seconds | Should Be 4
        $result.RxGBps | Should Be 2
        $result.TxGBps | Should Be 2
        { Measure-ThroughputNic $samples @([pscustomobject]@{ Start = $start.AddSeconds(20); End = $start.AddSeconds(21) }) } |
            Should Throw 'Insufficient'
    }

    It 'rejects evicted data and incorrect memory budgets before null-device verification' {
        $text = "Log.BeginAddress:1000000`nLog.HeadAddress:1000000`nLog.TailAddress:1200000`nLog.MaxMemorySizeBytes:137438953472`nIndexMemorySizeBytes:4294967296"
        Assert-ThroughputResidentStore $text 128GB 4GB | Should Be 200000
        { Assert-ThroughputResidentStore ($text.Replace('Log.HeadAddress:1000000', 'Log.HeadAddress:1100000')) 128GB 4GB } |
            Should Throw 'evicted'
        { Assert-ThroughputResidentStore $text 16GB 4GB } | Should Throw 'memory does not match'
        { Assert-ThroughputResidentStore $text 128GB 1GB } | Should Throw 'memory does not match'
        { Assert-ThroughputResidentStore ($text.Replace('Log.HeadAddress:1000000', '')) 128GB 4GB } |
            Should Throw 'resident-store setting'
    }

    It 'verifies effective log page size rather than relying on generated configuration' {
        $text = "Log.BeginAddress:64`nLog.HeadAddress:64`nLog.TailAddress:1024`nLog.MaxMemorySizeBytes:137438953472`nIndexMemorySizeBytes:4294967296`nLog.PageSizeBytes:16777216"
        Assert-ThroughputResidentStore $text 128GB 4GB 16777216 | Should Be 960
        { Assert-ThroughputResidentStore $text 128GB 4GB 524288 } | Should Throw 'page size'
        { Assert-ThroughputResidentStore ($text.Replace('Log.PageSizeBytes:', 'WrongPageSize:')) 128GB 4GB 16777216 } |
            Should Throw 'page size'
    }
}
