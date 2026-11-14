$benchmarkDir = Split-Path -Parent $PSScriptRoot
. (Join-Path $benchmarkDir '03-workload\bench\utils.ps1')
function az { throw 'Azure CLI must be mocked' }
function ssh { throw 'SSH must be mocked' }
function Redis { throw 'Redis must be mocked' }

$tokens = $null
$errors = $null
$workflowAst = [Management.Automation.Language.Parser]::ParseFile(
    (Join-Path $benchmarkDir 'experiments\migration\run.ps1'), [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw ($errors | Out-String) }
$configFunction = $workflowAst.Find({
    param($node)
    $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'New-MigrationInstanceConfig'
}, $true)
Invoke-Expression $configFunction.Extent.Text
$loadFunction = $workflowAst.Find({
    param($node)
    $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Get-MigrationLoadMetrics'
}, $true)
Invoke-Expression $loadFunction.Extent.Text
$profileFunction = $workflowAst.Find({
    param($node)
    $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'New-MigrationLoadConfig'
}, $true)
Invoke-Expression $profileFunction.Extent.Text
Describe 'Migration experiment resources' {
    It 'resolves the consolidated benchmark and cluster declarations' {
        . (Join-Path $benchmarkDir '02-platform\tools\cluster\cluster-utils.ps1')
        $experimentDir = Join-Path $benchmarkDir 'experiments\migration'
        $benchmark = Read-BenchmarkDefinition -Path (Join-Path $experimentDir 'benchmark.json')
        $benchmark.InstanceConfig | Should Be (Join-Path $experimentDir 'preload.conf')
        $cluster = Read-ClusterDefinition -Path (Join-Path $experimentDir 'cluster.json') -RequireInstanceConfig
        $cluster.InstanceConfig | Should Be (Join-Path $benchmarkDir '02-platform\tools\config\garnet\garnet-cluster-diskless.conf')
        $cluster.Nodes[0].Slots[0][1] | Should Be 16383
        $cluster.Nodes[1].Slots.Count | Should Be 0
    }

    It 'generates the <ValueLength>-byte preload with <Threads> workers from one profile' -TestCases @(
        @{ ValueLength = 8; Threads = 1; Keys = 67108864 }
        @{ ValueLength = 256; Threads = 8; Keys = 8388608 }
        @{ ValueLength = 1024; Threads = 64; Keys = 2097152 }
    ) {
        param($ValueLength, $Threads, $Keys)
        $text = Get-Content (Join-Path $benchmarkDir 'experiments\migration\preload.conf') -Raw
        $profile = New-MigrationLoadConfig $text $ValueLength $Threads
        $profile.ValueLength | Should Be $ValueLength
        $profile.DbSize | Should Be $Keys
        $profile.LoadThreads | Should Be $Threads
        $profile.NumThreads.Count | Should Be 1
        $profile.NumThreads[0] | Should Be $Threads
        $profile.LSet | Should Be $true
        $profile.BatchSize[0] | Should Be 4096
    }
}
Describe 'Initial migration preload timing' {
    It 'uses SET execution time rather than buffer generation or subsequent GET time' {
        $text = @'
Loading time: 20 secs
Operation type: SET
Num threads: 16
[Total time]: 2,000.00ms for 1,000.00 ops
[Throughput]: 500.00 ops/sec
Operation type: GET
Num threads: 16
[Total time]: 1,000.00ms for 30,000.00 ops
'@
        $result = Get-MigrationLoadMetrics $text 1000
        $result.Seconds | Should Be 2
        $result.OpsPerSecond | Should Be 500
        $result.Threads | Should Be 16
    }

    It 'rejects missing SET timing' {
        { Get-MigrationLoadMetrics 'Loading time: 2 secs' 1000 } | Should Throw 'exactly one'
    }

    It 'rejects an incomplete preload operation count' {
        { Get-MigrationLoadMetrics "Operation type: SET`nNum threads: 16`n[Total time]: 100.00ms for 99.00 ops" 100 } |
            Should Throw 'operation count'
    }
}
Describe 'Migration configuration overrides' {
    It 'preserves server placeholders and applies each migration mode' -TestCases @(
        @{ Tasks = 1; Fast = $false }, @{ Tasks = 8; Fast = $false }, @{ Tasks = 64; Fast = $false }
        @{ Tasks = 1; Fast = $true }, @{ Tasks = 8; Fast = $true }, @{ Tasks = 64; Fast = $true }
    ) {
        param($Tasks, $Fast)
        $text = '{"Port":$port,"LogMemorySize":"$logMemorySize"}'
        $result = New-MigrationInstanceConfig $text $Tasks $Fast
        $result.Contains('$port') | Should Be $true
        $result.Contains('$logMemorySize') | Should Be $true
        $parsed = $result.Replace('$port', '7000') | ConvertFrom-Json
        $parsed.ParallelMigrateTaskCount | Should Be $Tasks
        $parsed.FastMigrate | Should Be $Fast
        $parsed.MigrateBatchSize | Should Be 262143
        $parsed.MigrationSendBufferSize | Should Be '262144'
        $parsed.LogLevel | Should Be 'Information'
    }

    It 'applies a 4096-key migration collection batch' {
        $parsed = New-MigrationInstanceConfig '{"Port":7000}' 64 $true 4096 | ConvertFrom-Json
        $parsed.MigrateBatchSize | Should Be 4096
    }

    It 'sets a <Bytes>-byte send buffer independently of the page and collection size' -TestCases @(
        @{ Bytes = 131072 }, @{ Bytes = 262144 }, @{ Bytes = 524288 }
    ) {
        param($Bytes)
        $parsed = New-MigrationInstanceConfig '{"PageSize":"16m"}' 8 $false 4096 $Bytes | ConvertFrom-Json
        $parsed.MigrationSendBufferSize | Should Be "$Bytes"
        $parsed.PageSize | Should Be '16m'
        $parsed.MigrateBatchSize | Should Be 4096
    }

    It 'rejects duplicate send-buffer overrides' {
        { New-MigrationInstanceConfig '{"MigrationSendBufferSize":"128k"}' 8 $false } | Should Throw 'already declares'
    }

    It 'rejects duplicate settings instead of creating ambiguous JSON' {
        { New-MigrationInstanceConfig '{"FastMigrate":false}' 8 $true } | Should Throw 'already declares'
    }
}
$inspectionText = $workflowAst.Find({
    param($node)
    $node -is [Management.Automation.Language.StringConstantExpressionAst] -and $node.Value.Contains('MIGRATION_BATCH=')
}, $true).Value
Describe 'Effective migration send-buffer evidence' {
    It 'reads the migration manager startup buffer size' {
        Mock Get-Content {
            param($Path)
            if ($Path -like '*garnet.conf') {
                return '{"ParallelMigrateTaskCount":8,"MigrateBatchSize":4096,"MigrationSendBufferSize":"131072","PageSize":"16m","FastMigrate":false}'
            }
            return @(
                '06::50::46 info: MigrationManager[0] NetworkBufferSettings.sendBufferSize:131072'
                '06::50::47 warn: MigrateSession[0] Scan from 64 to 131136 and discovered 4096 keys'
            )
        }
        $output = & ([scriptblock]::Create($inspectionText))
        $parsed = $output.Substring('MIGRATION_BATCH='.Length) | ConvertFrom-Json
        $parsed.EffectiveSendBufferBytes | Should Be 131072
        $parsed.MaxKeys | Should Be 4096
    }

    It 'rejects missing startup evidence' {
        Mock Get-Content {
            param($Path)
            if ($Path -like '*garnet.conf') { return '{}' }
            return '06::50::47 warn: MigrateSession[0] discovered 4096 keys'
        }
        { & ([scriptblock]::Create($inspectionText)) } | Should Throw 'effective migration send-buffer size'
    }
}
$measureText = $workflowAst.Find({
    param($node)
    $node -is [Management.Automation.Language.StringConstantExpressionAst] -and $node.Value.Contains('function Count(')
}, $true).Value
$measureAst = [Management.Automation.Language.Parser]::ParseInput(
    [regex]::Replace($measureText, '__[A-Z]+__', '1'), [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw ($errors | Out-String) }
$countFunction = $measureAst.Find({
    param($node)
    $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Count'
}, $true)
Set-Item Function:script:Count ([scriptblock]::Create($countFunction.Body.Extent.Text.Trim('{', '}')))

Describe 'Migration measurement key-count validation' {
    It 'reads large database counts without narrowing to Int32' {
        Mock Redis { '3000000000' }
        Count '10.0.0.4' | Should Be 3000000000
    }

    It 'rejects nonnumeric database replies' {
        Mock Redis { 'not a count' }
        { Count '10.0.0.4' } | Should Throw 'Invalid DBSIZE'
    }
}

Describe 'Declarative benchmark validation' {
    BeforeEach {
        $script:definition = Get-Content (Join-Path $benchmarkDir 'templates\benchmark-cluster.json') -Raw | ConvertFrom-Json
        $script:definition.InstanceConfig = 'workload.conf'
        'Runtime=1' | Set-Content (Join-Path $TestDrive 'workload.conf')
    }

    It 'resolves the renamed template and its workload configuration' {
        $definition = Read-BenchmarkDefinition -Path (Join-Path $benchmarkDir 'templates\benchmark-cluster.json')
        $definition.InstanceConfig | Should Be (Join-Path $benchmarkDir '03-workload\bench\bench-cluster.conf')
        $definition.Instances[0].Vm | Should Be 'vm2'
        $definition.Target.Vm | Should Be 'vm0'
    }

    It 'rejects invalid benchmark definitions (<Case>)' -TestCases @(
        @{ Case = 'kind'; Expected = 'Benchmark definition' }
        @{ Case = 'system'; Expected = 'System must be' }
        @{ Case = 'unknown'; Expected = 'Unknown benchmark definition' }
        @{ Case = 'target'; Expected = 'Target must be an object' }
        @{ Case = 'target-property'; Expected = 'Unknown benchmark target' }
        @{ Case = 'port'; Expected = 'Target.Port' }
        @{ Case = 'empty'; Expected = 'nonempty array' }
        @{ Case = 'duplicate'; Expected = 'Duplicate benchmark VM' }
        @{ Case = 'instance-property'; Expected = 'containing only Vm' }
        @{ Case = 'vm'; Expected = 'DNS label' }
        @{ Case = 'config'; Expected = 'configuration not found' }
    ) {
        param($Case, $Expected)
        switch ($Case) {
            'kind' { $script:definition.Kind = 'Cluster' }
            'system' { $script:definition.System = 'unknown' }
            'unknown' { $script:definition | Add-Member Multiplier 4 }
            'target' { $script:definition.Target = 'vm0' }
            'target-property' { $script:definition.Target | Add-Member VmDomain 'example.com' }
            'port' { $script:definition.Target.Port = 0 }
            'empty' { $script:definition.Instances = @() }
            'duplicate' { $script:definition.Instances += [pscustomobject]@{ Vm = 'VM2' } }
            'instance-property' { $script:definition.Instances[0] | Add-Member Count 4 }
            'vm' { $script:definition.Instances[0].Vm = 'bad;vm' }
            'config' { $script:definition.InstanceConfig = 'missing.conf' }
        }
        $path = Join-Path $TestDrive 'benchmark.json'
        $script:definition | ConvertTo-Json -Depth 10 | Set-Content $path
        { Read-BenchmarkDefinition -Path $path } | Should Throw $Expected
    }
}

Describe 'Shared benchmark host selection' {
    It 'uses exact irregular clients without applying legacy counts or multiplier' {
        $hosts = @(Get-BenchmarkClientHosts -ClientHosts client-a.example.com,client-c.example.com `
            -HostBase invalid -HostCount invalid -Multiplier invalid)
        ($hosts -join ',') | Should Be 'client-a.example.com,client-c.example.com'
    }

    It 'preserves legacy scalar and per-base count expansion with multipliers' {
        (@(Get-BenchmarkClientHosts -HostBase vm2.example.com -HostCount 2 -Multiplier 2) -join ',') |
            Should Be 'vm2.example.com,vm2.example.com,vm3.example.com,vm3.example.com'
        (@(Get-BenchmarkClientHosts -HostBase '[vm2.a.example.com,vm7.b.example.com]' -HostCount '[1,2]') -join ',') |
            Should Be 'vm2.a.example.com,vm7.b.example.com,vm8.b.example.com'
    }

    It 'rejects duplicate explicit clients' {
        { Get-BenchmarkClientHosts -ClientHosts vm2.example.com,VM2.example.com } | Should Throw 'duplicate'
    }
}

Describe 'Existing benchmark driver overrides' {
    It 'selects the correct RESP implementation from the <Mode> declaration' -TestCases @(
        @{ Mode = 'cluster' }
        @{ Mode = 'standalone' }
    ) {
        param($Mode)
        $definition = Read-BenchmarkDefinition -Path (Join-Path $benchmarkDir "templates\benchmark-$Mode.json")
        $definition.System | Should Be 'Resp.benchmark'
        $source = Get-Content (Join-Path $benchmarkDir '03-workload\bench\resp-bench.ps1') -Raw
        $end = $source.IndexOf('# --- Probe server info ---')
        if ($end -lt 0) { throw 'Driver setup boundary not found' }
        $path = Join-Path $TestDrive 'profile-driver.ps1'
        Set-Content -LiteralPath $path -Value ($source.Substring(0, $end) + "`n`$benchCmd")
        Copy-Item (Join-Path $benchmarkDir '03-workload\bench\utils.ps1') $TestDrive
        $key = Join-Path $TestDrive 'key'
        'test fixture, not a private key' | Set-Content $key
        $command = & $path -ConfigFile $definition.InstanceConfig -ClientHosts client.example.com `
            -SshKey $key -SshUser operator -Server 10.1.0.4 -Port 7000
        $command | Should Match "--config <uploaded bench-$Mode.conf>"
        $command | Should Not Match '--cluster-bench|--use-fixed-worker-pool|--use-parallel-fan-out|--replica-ops-percent|--offline-buffers'
    }

    It 'honors authoritative connection settings in <Driver>' -TestCases @(
        @{ Driver = 'resp-bench.ps1'; Marker = '# --- Probe server info ---' }
        @{ Driver = 'memtier-bench.ps1'; Marker = '# --- SSH options ---' }
    ) {
        param($Driver, $Marker)
        $source = Get-Content (Join-Path $benchmarkDir "03-workload\bench\$Driver") -Raw
        $end = $source.IndexOf($Marker)
        if ($end -lt 0) { throw 'Driver setup boundary not found' }
        $prefix = $source.Substring(0, $end)
        $prefix += "`n[pscustomobject]@{ Hosts = `$sshHosts; Command = `$benchCmd; User = `$sshUser; Multiplier = `$multiplier }"
        $path = Join-Path $TestDrive $Driver
        Set-Content -LiteralPath $path -Value $prefix
        Copy-Item (Join-Path $benchmarkDir '03-workload\bench\utils.ps1') $TestDrive
        $config = Join-Path $TestDrive 'workload.conf'
        if ($Driver -eq 'resp-bench.ps1') {
            '{"Address":"wrong","Port":1,"UseFixedWorkerPool":false}' | Set-Content $config
        } else {
            @('ClientMachineHostnames=wrong', 'ClientMachineCount=invalid', 'Multiplier=invalid',
                'Server=wrong', 'Port=1', 'SshUser=wrong') | Set-Content $config
        }
        $key = Join-Path $TestDrive 'key'
        'test fixture, not a private key' | Set-Content $key
        $resolved = & $path -ConfigFile $config -ClientHosts client-a.example.com,client-b.example.com `
            -Server 10.1.0.4 -Port 7000 -SshUser operator -SshKey $key -Tls:($Driver -eq 'resp-bench.ps1')
        ($resolved.Hosts -join ',') | Should Be 'client-a.example.com,client-b.example.com'
        $resolved.User | Should Be 'operator'
        $resolved.Multiplier | Should Be 1
        $resolved.Command | Should Match '10.1.0.4'
        $resolved.Command | Should Match '7000'
        if ($Driver -eq 'resp-bench.ps1') {
            $resolved.Command | Should Not Match '--(?:no-)?fixed-worker-pool|--use-fixed-worker-pool'
            $resolved.Command | Should Match '--config <uploaded workload.conf>'
            $resolved.Command | Should Match '--tls --tlshost azurebench-server'
        }
    }

    It 'surfaces failed declared instances from <Driver>' -TestCases @(
        @{ Driver = 'resp-bench.ps1' }
        @{ Driver = 'memtier-bench.ps1' }
    ) {
        param($Driver)
        $tokens = $null; $errors = $null
        $ast = [Management.Automation.Language.Parser]::ParseFile((Join-Path $benchmarkDir "03-workload\bench\$Driver"),
            [ref]$tokens, [ref]$errors)
        if ($errors.Count) { throw ($errors | Out-String) }
        $failure = $ast.Find({
            param($node)
            $node -is [Management.Automation.Language.IfStatementAst] -and
                $node.Clauses[0].Item1.Extent.Text -eq '$benchFailed -gt 0' -and
                $node.Extent.Text -match 'declarative benchmark instance'
        }, $true)
        $benchFailed = 1
        $ClientHosts = @('vm2.example.com')
        $benchResults = @([pscustomobject]@{ Success = $false; Host = $ClientHosts[0]; Duration = '00:01' })
        $runDir = $TestDrive
        { & ([scriptblock]::Create($failure.Extent.Text)) } | Should Throw 'declarative benchmark instance'
    }
}

Describe 'RESP benchmark result validation' {
    It 'aggregates <Mode> throughput using its native output format' -TestCases @(
        @{ Mode = 'cluster'; Output = 'Total throughput: 1,234,500 ops/sec (1,234.5 Kops/sec)'; Cluster = 'true' }
        @{ Mode = 'standalone'; Output = '[Throughput]: 1,234,500.00 ops/sec'; Cluster = 'false' }
    ) {
        param($Mode, $Output, $Cluster)
        $path = Join-Path $TestDrive 'benchmark.log'
        Set-Content -LiteralPath $path -Value $Output
        $tokens = $null; $errors = $null
        $ast = [Management.Automation.Language.Parser]::ParseFile(
            (Join-Path $benchmarkDir '03-workload\bench\resp-bench.ps1'), [ref]$tokens, [ref]$errors)
        if ($errors.Count) { throw ($errors | Out-String) }
        $aggregation = $ast.Find({
            param($node)
            $node -is [Management.Automation.Language.ForEachStatementAst] -and
                $node.Variable.VariablePath.UserPath -eq 'lf' -and
                $node.Extent.Text -match '\$totalKops \+='
        }, $true)
        $logFiles = @($path)
        $clusterBench = $Cluster
        $totalKops = 0.0; $totalData = 0.0; $totalWire = 0.0
        $Detail = $false
        . ([scriptblock]::Create($aggregation.Extent.Text))
        $totalKops | Should Be 1234.5
    }

    It 'rejects incomplete or rejected cluster output (<Case>)' -TestCases @(
        @{ Case = 'arguments'; Output = "ERROR(S):`n  Option 'use-fixed-worker-pool' is unknown."; Expected = 'rejected its arguments' }
        @{ Case = 'incomplete'; Output = 'Preparing offline buffers...'; Expected = 'did not report completed throughput' }
        @{ Case = 'empty'; Output = ''; Expected = 'produced no output' }
    ) {
        param($Case, $Output, $Expected)
        $path = Join-Path $TestDrive 'benchmark.log'
        Set-Content -LiteralPath $path -Value $Output
        { Assert-RespBenchmarkResult -Path $path -RequireThroughput } | Should Throw $Expected
    }

    It 'accepts completed output including legitimately zero throughput' {
        $path = Join-Path $TestDrive 'benchmark.log'
        foreach ($throughput in @('1,234.5', '0.0')) {
            Set-Content -LiteralPath $path -Value "Total throughput: 0 ops/sec ($throughput Kops/sec)"
            { Assert-RespBenchmarkResult -Path $path -RequireThroughput } | Should Not Throw
        }
    }

    It 'marks an exit-zero argument rejection as failed before reporting success' {
        $path = Join-Path $TestDrive 'benchmark.log'
        Set-Content -LiteralPath $path -Value "ERROR(S):`n  Option 'use-fixed-worker-pool' is unknown."
        $tokens = $null; $errors = $null
        $ast = [Management.Automation.Language.Parser]::ParseFile(
            (Join-Path $benchmarkDir '03-workload\bench\resp-bench.ps1'), [ref]$tokens, [ref]$errors)
        if ($errors.Count) { throw ($errors | Out-String) }
        $validation = $ast.Find({
            param($node)
            $node -is [Management.Automation.Language.ForEachStatementAst] -and
                $node.Variable.VariablePath.UserPath -eq 'result' -and
                $node.Extent.Text -match 'Assert-RespBenchmarkResult'
        }, $true)
        $benchResults = @([pscustomobject]@{ Success = $true; LogFile = $path; Host = 'vm2.example.com'; Index = 0 })
        $hostStatus = @{ 0 = 'done' }
        $clusterBench = 'true'
        & ([scriptblock]::Create($validation.Extent.Text))
        $benchResults[0].Success | Should Be $false
        $hostStatus[0] | Should Be 'failed'
    }
}

Describe 'Benchmark client output reporting' {
    BeforeEach {
        $script:clientMessages = [Collections.Generic.List[string]]::new()
        Mock Write-Host { param($Object) $script:clientMessages.Add([string]$Object) }
        $script:clientResults = @(
            [pscustomobject]@{ Host = 'vm2.example.com'; Success = $true; Output = "successful stdout`nsuccessful stderr" }
            [pscustomobject]@{ Host = 'vm4.example.com'; Success = $false; Output = 'Unhandled exception: DbSize 13514 must be divisible by number of loading threads 8' }
        )
    }

    It 'prints captured output for all clients when requested' {
        Show-BenchmarkClientOutput -Results $script:clientResults -All
        ($script:clientMessages -join "`n") | Should Match "successful stdout`nsuccessful stderr"
        ($script:clientMessages -join "`n") | Should Match 'vm2.example.com'
        ($script:clientMessages -join "`n") | Should Match 'vm4.example.com'
        ($script:clientMessages -join "`n") | Should Match 'DbSize 13514'
    }

    It 'always prints failure details while keeping successful output quiet by default' {
        Show-BenchmarkClientOutput -Results $script:clientResults
        ($script:clientMessages -join "`n") | Should Not Match 'successful stdout|vm2.example.com'
        ($script:clientMessages -join "`n") | Should Match 'vm4.example.com'
        ($script:clientMessages -join "`n") | Should Match 'DbSize 13514'
    }
}

Describe 'Declarative benchmark runner' {
    BeforeEach {
        $global:BenchmarkTestInvocation = $null
        $global:BenchmarkTestRemote = [Collections.Generic.List[object]]::new()
        $global:BenchmarkTestFailClient = $false
        $global:BenchmarkTestFailDriver = $false
        foreach ($directory in @('03-workload\bench', '02-platform', '01-resources\security')) {
            New-Item -ItemType Directory -Path (Join-Path $TestDrive $directory) -Force | Out-Null
        }
        Copy-Item (Join-Path $benchmarkDir '03-workload\benchmark.ps1') (Join-Path $TestDrive '03-workload')
        Copy-Item (Join-Path $benchmarkDir '03-workload\bench\utils.ps1') (Join-Path $TestDrive '03-workload\bench')
        Copy-Item (Join-Path $benchmarkDir '02-platform\vmss-utils.ps1') (Join-Path $TestDrive '02-platform')
        Copy-Item (Join-Path $benchmarkDir '01-resources\security\ssh-key-utils.ps1') (Join-Path $TestDrive '01-resources\security')
        $stub = @'
param($ConfigFile, $ClientHosts, $SshUser, $SshKey, $Server, $Port, [switch]$Tls, [switch]$Detail,
    [switch]$Monitor, [switch]$SkipLoad, [switch]$PrintOutput, [switch]$Verbose)
$global:BenchmarkTestInvocation = $PSBoundParameters
$global:BenchmarkTestInvocation.Driver = $MyInvocation.MyCommand.Name
if ($global:BenchmarkTestFailDriver) { throw 'Benchmark driver failure' }
'@
        foreach ($driver in @('resp-bench.ps1', 'memtier-bench.ps1')) {
            Set-Content -LiteralPath (Join-Path $TestDrive "03-workload\bench\$driver") -Value $stub
        }
        $script:definition = Get-Content (Join-Path $benchmarkDir 'templates\benchmark-cluster.json') -Raw | ConvertFrom-Json
        $script:definition.InstanceConfig = Join-Path $TestDrive 'workload.conf'
        $script:definition.Instances = @([pscustomobject]@{ Vm = 'vm2' }, [pscustomobject]@{ Vm = 'vm4' })
        'Multiplier=100' | Set-Content $script:definition.InstanceConfig
        $script:key = Join-Path $TestDrive 'key'
        'test fixture, not a private key' | Set-Content $script:key
        Mock az {
            $global:LASTEXITCODE = 0
            switch ($args[1]) {
                'list' { '[{"name":"clients","state":"Succeeded"},{"name":"servers","state":"Succeeded"}]' }
                'show' { if ($args -contains 'servers') { '"serveruser"' } else { '"clientuser"' } }
                'list-instance-public-ips' {
                    if ($args -contains 'servers') { '[{"fqdn":"vm0.servers.example.com"}]' }
                    else { '[{"fqdn":"vm0.clients.example.com"},{"fqdn":"vm2.clients.example.com"},{"fqdn":"vm4.clients.example.com"},{"fqdn":"vm9.clients.example.com"}]' }
                }
                default { throw "Unexpected Azure command: $args" }
            }
        }
        Mock ssh {
            $global:LASTEXITCODE = 0
            $command = $args[-1]
            if ($command -match '-EncodedCommand (\S+)$') {
                $command = [Text.Encoding]::Unicode.GetString([Convert]::FromBase64String($Matches[1]))
            }
            $global:BenchmarkTestRemote.Add([pscustomobject]@{ Host = $args[-2]; Script = $command })
            if ($command -match 'BENCHMARK_ADDRESS=') { 'BENCHMARK_ADDRESS=10.1.0.4' }
            elseif ($global:BenchmarkTestFailClient -and $args[-2] -like '*@vm4.*') { $global:LASTEXITCODE = 2; 'not installed' }
        }
    }

    AfterEach {
        foreach ($name in @('BenchmarkTestInvocation', 'BenchmarkTestRemote', 'BenchmarkTestFailClient', 'BenchmarkTestFailDriver')) {
            Remove-Variable -Name $name -Scope Global
        }
    }

    It 'delegates <System> to its existing driver with exactly the declared clients' -TestCases @(
        @{ System = 'Resp.benchmark'; Driver = 'resp-bench.ps1' }
        @{ System = 'memtier_benchmark'; Driver = 'memtier-bench.ps1' }
    ) {
        param($System, $Driver)
        $script:definition.System = $System
        $path = Join-Path $TestDrive 'benchmark.json'
        $script:definition | ConvertTo-Json -Depth 10 | Set-Content $path
        & (Join-Path $TestDrive '03-workload\benchmark.ps1') -Definition $path -rg test -VmssName clients -SshKey $script:key -NonInteractive
        $global:BenchmarkTestInvocation.Driver | Should Be $Driver
        ($global:BenchmarkTestInvocation.ClientHosts -join ',') | Should Be 'vm2.clients.example.com,vm4.clients.example.com'
        $global:BenchmarkTestInvocation.Server | Should Be '10.1.0.4'
        $global:BenchmarkTestInvocation.Port | Should Be 7000
        $global:BenchmarkTestInvocation.SshUser | Should Be 'clientuser'
        $global:BenchmarkTestInvocation.ConfigFile | Should Be $script:definition.InstanceConfig
        $global:BenchmarkTestRemote.Count | Should Be 3
    }

    It 'supports a separate target VMSS and its Azure username without launching in dry-run mode' {
        $path = Join-Path $TestDrive 'benchmark.json'
        $script:definition | ConvertTo-Json -Depth 10 | Set-Content $path
        & (Join-Path $TestDrive '03-workload\benchmark.ps1') -Definition $path -rg test -VmssName clients `
            -TargetVmssName servers -SshKey $script:key -DryRun
        $global:BenchmarkTestInvocation | Should BeNullOrEmpty
        $global:BenchmarkTestRemote.Count | Should Be 1
        $global:BenchmarkTestRemote[0].Host | Should Be 'serveruser@vm0.servers.example.com'
    }

    It 'maps verbose output to inline capture for <System> (<Enabled>)' -TestCases @(
        @{ System = 'Resp.benchmark'; Enabled = $true }
        @{ System = 'Resp.benchmark'; Enabled = $false }
        @{ System = 'memtier_benchmark'; Enabled = $true }
        @{ System = 'memtier_benchmark'; Enabled = $false }
    ) {
        param($System, $Enabled)
        $script:definition.System = $System
        $path = Join-Path $TestDrive 'benchmark.json'
        $script:definition | ConvertTo-Json -Depth 10 | Set-Content $path
        & (Join-Path $TestDrive '03-workload\benchmark.ps1') -Definition $path -rg test -VmssName clients `
            -SshKey $script:key -NonInteractive -Verbose:$Enabled
        [bool]$global:BenchmarkTestInvocation.PrintOutput | Should Be $Enabled
        [bool]$global:BenchmarkTestInvocation.Verbose | Should Be $false
    }

    It 'does not launch any workloads when a client preflight fails' {
        $global:BenchmarkTestFailClient = $true
        $path = Join-Path $TestDrive 'benchmark.json'
        $script:definition | ConvertTo-Json -Depth 10 | Set-Content $path
        {
            & (Join-Path $TestDrive '03-workload\benchmark.ps1') -Definition $path -rg test -VmssName clients -SshKey $script:key
        } | Should Throw 'Benchmark preflight failed'
        $global:BenchmarkTestInvocation | Should BeNullOrEmpty
    }

    It 'propagates driver failures instead of reporting declarative completion' {
        $global:BenchmarkTestFailDriver = $true
        $path = Join-Path $TestDrive 'benchmark.json'
        $script:definition | ConvertTo-Json -Depth 10 | Set-Content $path
        {
            & (Join-Path $TestDrive '03-workload\benchmark.ps1') -Definition $path -rg test -VmssName clients -SshKey $script:key
        } | Should Throw 'Benchmark driver failure'
    }
}
