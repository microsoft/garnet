$benchmarkDir = Split-Path -Parent $PSScriptRoot
. (Join-Path $benchmarkDir '02-platform\tools\cluster\cluster-utils.ps1')

function Import-ClusterTestFunctions {
    param([string]$Path, [string[]]$Names)
    $tokens = $null
    $errors = $null
    $ast = [System.Management.Automation.Language.Parser]::ParseFile($Path, [ref]$tokens, [ref]$errors)
    if ($errors.Count) { throw ($errors | Out-String) }
    foreach ($name in $Names) {
        $function = $ast.Find({
            param($node)
            $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $name
        }, $true)
        if (-not $function) { throw "Missing function: $name" }
        Set-Item -Path "Function:script:$name" -Value ([scriptblock]::Create($function.Body.Extent.Text.Trim('{', '}')))
    }
}

Import-ClusterTestFunctions -Path (Join-Path $benchmarkDir '03-workload\cluster.ps1') -Names @('Invoke-Remote', 'Invoke-DeclaredCluster')
Import-ClusterTestFunctions -Path (Join-Path $benchmarkDir '02-platform\tools\cluster\cluster-deploy.ps1') -Names @('New-ClusterManual')
Import-ClusterTestFunctions -Path (Join-Path $benchmarkDir '02-platform\tools\cluster\mcluster.ps1') -Names @(
    'Get-SystemProcessIds', 'Stop-System', 'Confirm-Stopped', 'Stop-And-Confirm', 'Clean-System', 'Resolve-Config')

function Write-TestClusterDefinition {
    $path = Join-Path $TestDrive 'cluster.json'
    $script:definition | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath $path
    return $path
}
function ssh { throw 'SSH must be mocked' }
function az { throw 'Azure CLI must be mocked' }
function pgrep { throw 'pgrep must be mocked' }
function Test-ClusterCli { throw 'CLI must be mocked' }
function Get-Eth1Ip { throw 'Get-Eth1Ip must be mocked' }

Describe 'Declarative cluster validation' {
    BeforeEach {
        $script:definition = Get-Content (Join-Path $benchmarkDir 'templates\cluster-explicit.json') -Raw | ConvertFrom-Json
        $script:definition.PSObject.Properties.Remove('Primaries')
        $script:definition.PSObject.Properties.Remove('Replicas')
        $script:definition | Add-Member VmDomain 'd128ldsv6.australiaeast.cloudapp.azure.com'
        $script:definition | Add-Member SshUser 'guser'
        $nodes = '[{"Vm":"vm0","Role":"Primary","Slots":[[0,16383]]},{"Vm":"vm1","Role":"Replica","Primary":"vm0"}]' | ConvertFrom-Json
        $script:definition | Add-Member -NotePropertyName Nodes -NotePropertyValue $nodes
        $script:definition.InstanceConfig = 'instance.conf'
        '{}' | Set-Content (Join-Path $TestDrive 'instance.conf')
    }

    It 'resolves instance paths and only the explicitly declared hostnames' {
        $cluster = Read-ClusterDefinition -Path (Write-TestClusterDefinition) -RequireInstanceConfig
        $cluster.InstanceConfig | Should Be (Join-Path $TestDrive 'instance.conf')
        $cluster.Nodes.Count | Should Be 2
        $cluster.Nodes[0].HostName | Should Be 'vm0.d128ldsv6.australiaeast.cloudapp.azure.com'
        $cluster.Nodes[1].Primary | Should Be 'vm0'
        $cluster.Nodes[0].Slots[0][1] | Should Be 16383
    }

    It 'divides automatic slots deterministically with the remainder assigned first' {
        $nodes = '[{"Vm":"vm0","Role":"Primary","Slots":"auto"},{"Vm":"vm1","Role":"Primary","Slots":"auto"},{"Vm":"vm2","Role":"Primary","Slots":"auto"}]' | ConvertFrom-Json
        $resolved = @(Resolve-ClusterNodes -Nodes $nodes)
        ($resolved[0].Slots[0] -join ',') | Should Be '0,5461'
        ($resolved[1].Slots[0] -join ',') | Should Be '5462,10922'
        ($resolved[2].Slots[0] -join ',') | Should Be '10923,16383'
    }

    It 'supports multiple explicit ranges on a primary' {
        $nodes = '[{"Vm":"vm0","Role":"Primary","Slots":[[0,99],[200,16383]]},{"Vm":"vm1","Role":"Primary","Slots":[[100,199]]}]' | ConvertFrom-Json
        @(Resolve-ClusterNodes -Nodes $nodes).Count | Should Be 2
    }

    It 'allows an explicitly slotless migration target but rejects missing slots and incomplete coverage' {
        $nodes = '[{"Vm":"vm0","Role":"Primary","Slots":[[0,16383]]},{"Vm":"vm1","Role":"Primary","Slots":[]}]' | ConvertFrom-Json
        $resolved = @(Resolve-ClusterNodes -Nodes $nodes)
        $resolved[1].Slots.Count | Should Be 0
        $nodes[1].Slots = $null
        { Resolve-ClusterNodes -Nodes $nodes } | Should Throw 'Missing slot ranges'
        $nodes[0].Slots = @()
        $nodes[1].Slots = @()
        { Resolve-ClusterNodes -Nodes $nodes } | Should Throw 'cover all 16384'
    }

    It 'rejects invalid declarations before any remote work (<Case>)' -TestCases @(
        @{ Case = 'kind'; Expected = 'Cluster definition' }
        @{ Case = 'duplicate'; Expected = 'Duplicate VM' }
        @{ Case = 'role'; Expected = 'Invalid role' }
        @{ Case = 'replica-primary'; Expected = 'declared primary' }
        @{ Case = 'replica-slots'; Expected = 'cannot own slots' }
        @{ Case = 'gap'; Expected = 'cover all 16384' }
        @{ Case = 'overlap'; Expected = 'Overlapping slot' }
        @{ Case = 'range'; Expected = 'between 0 and 16383' }
        @{ Case = 'mixed'; Expected = 'not both' }
        @{ Case = 'domain'; Expected = 'DNS domain' }
        @{ Case = 'port'; Expected = 'Port must be an integer' }
        @{ Case = 'unknown'; Expected = 'Unknown cluster definition property' }
        @{ Case = 'missing-file'; Expected = 'configuration not found' }
    ) {
        param($Case, $Expected)
        switch ($Case) {
            'kind' { $script:definition.Kind = 'Benchmark' }
            'duplicate' { $script:definition.Nodes[1].Vm = 'vm0' }
            'role' { $script:definition.Nodes[1].Role = 'Client' }
            'replica-primary' { $script:definition.Nodes[1].Primary = 'vm9' }
            'replica-slots' { $script:definition.Nodes[1] | Add-Member Slots 'auto' }
            'gap' { $script:definition.Nodes[0].Slots = ,@(0, 100) }
            'overlap' { $script:definition.Nodes[0].Slots = @(@(0, 100), @(100, 16383)) }
            'range' { $script:definition.Nodes[0].Slots = ,@(-1, 16383) }
            'mixed' {
                $script:definition.Nodes += [pscustomobject]@{ Vm = 'vm3'; Role = 'Primary'; Slots = 'auto' }
            }
            'domain' { $script:definition.VmDomain = 'bad;command' }
            'port' { $script:definition.Port = 0 }
            'unknown' { $script:definition | Add-Member Typo $true }
            'missing-file' { $script:definition.InstanceConfig = 'missing.conf' }
        }
        { Read-ClusterDefinition -Path (Write-TestClusterDefinition) -RequireInstanceConfig } | Should Throw $Expected
    }

    It 'can stop when the instance config no longer exists' {
        $script:definition.InstanceConfig = 'missing.conf'
        (Read-ClusterDefinition -Path (Write-TestClusterDefinition)).Nodes.Count | Should Be 2
    }
}

Describe 'Grouped cluster definitions' {
    BeforeEach {
        $script:definition = Get-Content (Join-Path $benchmarkDir 'templates\cluster-explicit.json') -Raw | ConvertFrom-Json
        $script:definition.InstanceConfig = 'instance.conf'
        '{}' | Set-Content (Join-Path $TestDrive 'instance.conf')
    }

    It 'normalizes the <Mode> template to the current primary and replica topology' -TestCases @(
        @{ Mode = 'explicit' }
        @{ Mode = 'auto' }
    ) {
        param($Mode)
        $cluster = Read-ClusterDefinition -Path (Join-Path $benchmarkDir "templates\cluster-$Mode.json") -RequireInstanceConfig
        $cluster.Nodes.Count | Should Be 2
        $cluster.Nodes[0].Vm | Should Be 'vm0'
        $cluster.Nodes[0].Role | Should Be 'Primary'
        ($cluster.Nodes[0].Slots[0] -join ',') | Should Be '0,16383'
        $cluster.Nodes[1].Vm | Should Be 'vm1'
        $cluster.Nodes[1].Primary | Should Be 'vm0'
    }

    It 'divides auto slots across groups in VM order and assigns a shared replica target' {
        $script:definition.Primaries = '[{"VMs":["vm1","vm0"],"Slots":"auto"},{"VMs":["vm2"],"Slots":"auto"}]' | ConvertFrom-Json
        $script:definition.Replicas = @('[{"VMs":["vm3","vm4"],"Primary":"vm1"}]' | ConvertFrom-Json)
        $cluster = Read-ClusterDefinition -Path (Write-TestClusterDefinition)
        ($cluster.Nodes.Vm -join ',') | Should Be 'vm1,vm0,vm2,vm3,vm4'
        ($cluster.Nodes[0].Slots[0] -join ',') | Should Be '0,5461'
        ($cluster.Nodes[1].Slots[0] -join ',') | Should Be '5462,10922'
        ($cluster.Nodes[2].Slots[0] -join ',') | Should Be '10923,16383'
        $cluster.Nodes[3].Primary | Should Be 'vm1'
        $cluster.Nodes[4].Primary | Should Be 'vm1'
    }

    It 'supports separate explicit groups and multiple ranges per primary' {
        $script:definition.Primaries = '[{"VMs":["vm0"],"Slots":[[0,99],[200,16383]]},{"VMs":["vm2"],"Slots":[[100,199]]}]' | ConvertFrom-Json
        $cluster = Read-ClusterDefinition -Path (Write-TestClusterDefinition)
        $cluster.Nodes.Count | Should Be 3
        $cluster.Nodes[0].Slots.Count | Should Be 2
        ($cluster.Nodes[1].Slots[0] -join ',') | Should Be '100,199'
    }

    It 'creates a slotless primary without issuing an empty ADDSLOTSRANGE command' {
        $script:definition.Primaries = '[{"VMs":["vm0"],"Slots":[[0,16383]]},{"VMs":["vm1"],"Slots":[]}]' | ConvertFrom-Json
        $script:definition.Replicas = @()
        $nodes = (Read-ClusterDefinition -Path (Write-TestClusterDefinition)).Nodes
        $nodes[0].Ip = '10.0.0.4'
        $nodes[1].Ip = '10.0.0.5'
        Mock Invoke-ClusterCommand {
            if (($Arguments -join ' ') -eq 'CLUSTER MYID') {
                if ($Node.Vm -eq 'vm0') { 'a' * 40 } else { 'b' * 40 }
            } else { 'OK' }
        }
        Mock Confirm-ClusterTopology {}
        New-ClusterTopology -Nodes $nodes -Cli (Get-ClusterCli -System garnet)
        Assert-MockCalled Invoke-ClusterCommand -Times 1 -Exactly -Scope It -ParameterFilter {
            ($Arguments -join ' ') -eq 'CLUSTER ADDSLOTSRANGE 0 16383' -and $Node.Vm -eq 'vm0'
        }
        Assert-MockCalled Invoke-ClusterCommand -Times 0 -Exactly -Scope It -ParameterFilter {
            $Arguments[1] -eq 'ADDSLOTSRANGE' -and $Node.Vm -eq 'vm1'
        }
        Assert-MockCalled Confirm-ClusterTopology -Times 1 -Exactly -Scope It
    }

    It 'supports <Mode> replicas for a primary-only cluster' -TestCases @(
        @{ Mode = 'empty' }
        @{ Mode = 'omitted' }
    ) {
        param($Mode)
        if ($Mode -eq 'empty') { $script:definition.Replicas = @() }
        else { $script:definition.PSObject.Properties.Remove('Replicas') }
        (Read-ClusterDefinition -Path (Write-TestClusterDefinition)).Nodes.Count | Should Be 1
    }

    It 'rejects invalid grouped definitions (<Case>)' -TestCases @(
        @{ Case = 'primary-array'; Expected = 'Primaries must be an array' }
        @{ Case = 'empty-primary'; Expected = 'at least one group' }
        @{ Case = 'group-object'; Expected = 'group must be an object' }
        @{ Case = 'vm-array'; Expected = 'VMs must be a nonempty array' }
        @{ Case = 'empty-vms'; Expected = 'VMs must be a nonempty array' }
        @{ Case = 'explicit-many'; Expected = 'require one VM' }
        @{ Case = 'duplicate'; Expected = 'Duplicate VM' }
        @{ Case = 'missing-primary'; Expected = 'declared primary' }
        @{ Case = 'target-array'; Expected = 'name one primary' }
        @{ Case = 'unknown'; Expected = 'Unknown Primaries group property' }
        @{ Case = 'mixed-nodes'; Expected = 'not both' }
        @{ Case = 'mixed-slots'; Expected = 'not both' }
    ) {
        param($Case, $Expected)
        switch ($Case) {
            'primary-array' { $script:definition.Primaries = $script:definition.Primaries[0] }
            'empty-primary' { $script:definition.Primaries = @() }
            'group-object' { $script:definition.Primaries = @('vm0') }
            'vm-array' { $script:definition.Primaries[0].VMs = 'vm0' }
            'empty-vms' { $script:definition.Primaries[0].VMs = @() }
            'explicit-many' { $script:definition.Primaries[0].VMs = @('vm0', 'vm2') }
            'duplicate' { $script:definition.Replicas[0].VMs = @('vm0') }
            'missing-primary' { $script:definition.Replicas[0].Primary = 'vm9' }
            'target-array' { $script:definition.Replicas[0].Primary = @('vm0', 'vm2') }
            'unknown' { $script:definition.Primaries[0] | Add-Member Roles @('P') }
            'mixed-nodes' { $script:definition | Add-Member Nodes @() }
            'mixed-slots' { $script:definition.Primaries += [pscustomobject]@{ VMs = @('vm2'); Slots = 'auto' } }
        }
        { Read-ClusterDefinition -Path (Write-TestClusterDefinition) } | Should Throw $Expected
    }
}

Describe 'Declared cluster orchestration' {
    BeforeEach {
        $script:cluster = Read-ClusterDefinition -Path (Join-Path $benchmarkDir 'templates\cluster-explicit.json') -RequireInstanceConfig
        foreach ($node in $script:cluster.Nodes) {
            $node | Add-Member HostName "$($node.Vm).d128ldsv6.australiaeast.cloudapp.azure.com"
        }
        $script:calls = [Collections.Generic.List[object]]::new()
        $script:failurePhase = ''
        Mock Invoke-Remote {
            if ($script:failurePhase -eq 'preflight' -and $TargetHost -like 'vm1.*') { throw 'SSH failure' }
            if ($script:failurePhase -eq 'start' -and $Label -eq 'create') { throw 'Startup failure' }
            $script:calls.Add([pscustomobject]@{ Cmd = $Cmd; TargetHost = $TargetHost; Label = $Label })
            if ($Label -eq 'preflight') {
                if ($TargetHost -like 'vm0.*') { 'CLUSTER_ADDRESS=10.5.128.4' }
                else { 'CLUSTER_ADDRESS=10.5.128.5' }
            }
        }
    }

    It 'preflights all declared VMs, starts clean instances and ships the explicit topology' {
        Invoke-DeclaredCluster -Cluster $script:cluster -Operation create
        ($script:calls.Label -join ',') | Should Be 'preflight,preflight,create,create,setup'
        @($script:calls.TargetHost | Sort-Object -Unique).Count | Should Be 2
        foreach ($call in $script:calls | Where-Object Label -eq 'create') {
            $call.Cmd | Should Match '-Nodes 1 -BasePort 7000 -InstanceOnly'
            $call.Cmd | Should Match '-ConfContent [A-Za-z0-9+/=]+ -ConfName instance.conf -Clean'
        }
        $setup = $script:calls | Where-Object Label -eq 'setup'
        $setup.Cmd -match '-TopologyContent ([A-Za-z0-9+/=]+)' | Should Be $true
        $nodes = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($Matches[1])) | ConvertFrom-Json
        $nodes[0].Ip | Should Be '10.5.128.4'
        $nodes[1].Primary | Should Be 'vm0'
        $nodes[0].Slots[0][1] | Should Be 16383
    }

    It 'aborts before cleaning any VM when a preflight fails' {
        $script:failurePhase = 'preflight'
        { Invoke-DeclaredCluster -Cluster $script:cluster -Operation create } | Should Throw 'SSH failure'
        Assert-MockCalled Invoke-Remote -Times 0 -Exactly -Scope It -ParameterFilter { $Label -eq 'create' }
    }

    It 'does not set up a partially failed start' {
        $script:failurePhase = 'start'
        { Invoke-DeclaredCluster -Cluster $script:cluster -Operation create } | Should Throw 'Startup failure'
        Assert-MockCalled Invoke-Remote -Times 0 -Exactly -Scope It -ParameterFilter { $Label -eq 'setup' }
    }

    It 'stops only declared ports without cleaning, starting or setting up' {
        Invoke-DeclaredCluster -Cluster $script:cluster -Operation stop
        ($script:calls.Label -join ',') | Should Be 'preflight,preflight,stop,stop'
        foreach ($call in $script:calls | Where-Object Label -eq 'stop') {
            $call.Cmd | Should Be 'mcluster.ps1 -Action stop -System garnet -Nodes 1 -BasePort 7000 -InstanceOnly'
        }
    }
}

Describe 'Shared cluster setup and transport' {
    It 'preserves legacy manual slot division and replica placement, including shared VM IPs' {
        Mock New-ClusterTopology { $script:legacyNodes = $Nodes }
        New-ClusterManual -Endpoints @('10.0.0.1:7000', '10.0.0.1:7001', '10.0.0.2:7000', '10.0.0.2:7001') -ReplicaCount 1 -Cli (Get-ClusterCli -System garnet)
        $script:legacyNodes.Count | Should Be 4
        ($script:legacyNodes[0].Slots[0] -join ',') | Should Be '0,8191'
        ($script:legacyNodes[1].Slots[0] -join ',') | Should Be '8192,16383'
        $script:legacyNodes[2].Primary | Should Be 'node0'
        $script:legacyNodes[3].Primary | Should Be 'node1'
    }

    It 'uses encoded PowerShell transport for both command paths and surfaces SSH failures' {
        $script:sshOpts = @()
        $script:SshUser = 'guser'
        Mock ssh { $global:LASTEXITCODE = 3; 'remote failed' }
        { Invoke-Remote -Cmd 'throw "failure"' -TargetHost 'vm0.example.com' -Label create } | Should Throw 'exit code 3'
        Assert-MockCalled ssh -Times 1 -Exactly -Scope It -ParameterFilter { $args[-1] -match '-EncodedCommand ' }
    }

    It 'surfaces server errors even when the CLI exits successfully' {
        Mock Test-ClusterCli { $global:LASTEXITCODE = 0; 'ERR rejected' }
        $cli = [pscustomobject]@{ Name = 'Test-ClusterCli'; Arguments = @() }
        $node = [pscustomobject]@{ Ip = '10.0.0.1'; Port = 7000 }
        { Invoke-ClusterCommand -Cli $cli -Node $node -Arguments @('CLUSTER', 'MEET', '10.0.0.2', '7000') -ExpectOk } | Should Throw 'ERR rejected'
    }
}

Describe 'Cluster health summary' {
    BeforeEach {
        $script:healthNodes = @(
            [pscustomobject]@{ Vm = 'vm0'; Ip = '10.0.0.4'; Port = 7000; Role = 'Primary'; Slots = ,@(0, 16383) }
            [pscustomobject]@{ Vm = 'vm1'; Ip = '10.0.0.5'; Port = 7000; Role = 'Replica'; Primary = 'vm0' }
        )
        $script:healthIds = @{ vm0 = ('a' * 40); vm1 = ('b' * 40) }
        $script:healthState = 'ok'
        $script:syncState = 'False'
        $script:pingReply = 'PONG'
        $script:summaryMessages = [Collections.Generic.List[string]]::new()
        Mock Write-Host { $script:summaryMessages.Add([string]$Object) }
        Mock Invoke-ClusterCommand {
            switch ($Arguments -join ' ') {
                'CLUSTER INFO' { "cluster_state:$script:healthState`r`ncluster_slots_ok:16384" }
                'CLUSTER NODES' {
                    "$($script:healthIds.vm0) 10.0.0.4:7000@17000 master - 0 0 1 connected 0-16383`r`n" +
                        "$($script:healthIds.vm1) 10.0.0.5:7000@17000 slave $($script:healthIds.vm0) 0 0 2 connected"
                }
                'INFO replication' { "master_link_status:up`r`nmaster_sync_in_progress:$script:syncState" }
                'PING' { if ($Node.Vm -eq 'vm1') { $script:pingReply } else { 'PONG' } }
                default { throw "Unexpected cluster command: $Arguments" }
            }
        }
    }

    It 'prints validated roles, endpoints, slots and replication when sync state is <SyncState>' -TestCases @(
        @{ SyncState = 'False' }
        @{ SyncState = '0' }
    ) {
        param($SyncState)
        $script:syncState = $SyncState
        Confirm-ClusterTopology -Nodes $script:healthNodes -Cli (Get-ClusterCli -System garnet) -NodeIds $script:healthIds
        $report = $script:summaryMessages -join "`n"
        $report | Should Match 'vm0\s+10.0.0.4:7000\s+Primary\s+-\s+0-16383\s+UP'
        $report | Should Match 'vm1\s+10.0.0.5:7000\s+Replica\s+vm0\s+-\s+UP\s+up / synced'
        $report | Should Match 'Cluster is UP: 1 primary node\(s\), 1 replica node\(s\), all 16384 slots healthy'
        Assert-MockCalled Invoke-ClusterCommand -Times 2 -Exactly -Scope It -ParameterFilter { ($Arguments -join ' ') -eq 'PING' }
        Assert-MockCalled Invoke-ClusterCommand -Times 0 -Exactly -Scope It -ParameterFilter {
            ($Arguments -join ' ') -match 'ADDSLOTS|MEET|REPLICATE|SET-CONFIG'
        }
    }

    It 'does not report UP when a node fails PING' {
        $script:pingReply = 'ERR not ready'
        {
            Confirm-ClusterTopology -Nodes $script:healthNodes -Cli (Get-ClusterCli -System garnet) -NodeIds $script:healthIds
        } | Should Throw 'PING failed on vm1'
        Assert-MockCalled Write-Host -Times 0 -Exactly -Scope It -ParameterFilter { $Object -like 'Cluster is UP:*' }
    }

    It 'does not report UP when cluster state is unhealthy' {
        $script:healthState = 'fail'
        {
            Confirm-ClusterTopology -Nodes $script:healthNodes -Cli (Get-ClusterCli -System garnet) -NodeIds $script:healthIds -TimeoutSec 0
        } | Should Throw 'Timed out waiting for vm0'
        Assert-MockCalled Write-Host -Times 0 -Exactly -Scope It -ParameterFilter { $Object -like 'Cluster is UP:*' }
    }

    It 'does not report UP while replica full sync is in progress' {
        $script:syncState = '1'
        {
            Confirm-ClusterTopology -Nodes $script:healthNodes -Cli (Get-ClusterCli -System garnet) -NodeIds $script:healthIds -TimeoutSec 0
        } | Should Throw 'Timed out waiting for vm1'
        Assert-MockCalled Write-Host -Times 0 -Exactly -Scope It -ParameterFilter { $Object -like 'Cluster is UP:*' }
    }
}

Describe 'Scoped instance management' {
    BeforeEach {
        $script:BASE_PORT = 7000
        $script:RAMDISK_DIR = '/mnt/ramdisk'
        $script:NVME_DIR = '/mnt/nvme'
        Mock pgrep { $global:LASTEXITCODE = 0; '12345' }
        Mock Stop-Process {}
    }

    It 'selects the exact Garnet config path and terminates only returned process IDs' {
        Stop-System -Sys garnet -Count 1
        Assert-MockCalled pgrep -Times 1 -Exactly -Scope It -ParameterFilter { $args[-1] -match '7000/garnet\\\.conf' }
        Assert-MockCalled Stop-Process -Times 1 -Exactly -Scope It -ParameterFilter { $Id -eq 12345 -and $Force }
    }

    It 'treats no matching processes as an idempotent stop' {
        Mock pgrep { $global:LASTEXITCODE = 1 }
        Stop-System -Sys garnet -Count 1
        Assert-MockCalled Stop-Process -Times 0 -Exactly -Scope It
    }

    It 'does not hide process lookup failures' {
        Mock pgrep { $global:LASTEXITCODE = 2 }
        { Stop-System -Sys garnet -Count 1 } | Should Throw 'Process lookup failed'
    }

    It 'applies the declared port and cluster mode even to a fixed-port instance config' {
        $profile = Join-Path $TestDrive 'fixed-port.conf'
        '{"Port":6379,"EnableCluster":false,"LogMemorySize":"64m","Address":"$eth1"}' |
            Set-Content -LiteralPath $profile
        $script:BasePort = 7100
        $script:BASE_PORT = 7100
        $script:ClusterMode = 'true'
        $script:Tls = $false
        Mock Get-Eth1Ip { '10.0.0.4' }
        Mock New-Item {}
        Mock Set-Content { $script:instanceContent = $Value }
        try {
            Resolve-Config -Sys garnet -Count 1 -ConfPath $profile
            $settings = $script:instanceContent | ConvertFrom-Json
            $settings.Port | Should Be 7100
            $settings.EnableCluster | Should Be $true
            $settings.Address | Should Be '10.0.0.4'
        } finally {
            $script:BasePort = 0
            $script:BASE_PORT = 7000
        }
    }

    It 'cleans only per-port directories, not roots or other instances' {
        Mock Test-Path { $true }
        Mock Remove-Item {}
        Clean-System -Sys garnet -Count 1
        Assert-MockCalled Remove-Item -Times 4 -Exactly -Scope It -ParameterFilter { $LiteralPath -like '*/garnet-cluster/7000' -and $Recurse }
    }
}

Describe 'Cluster driver entry points' {
    BeforeEach {
        $global:ClusterTestWireCommands = [Collections.Generic.List[string]]::new()
        $global:ClusterTestAzureEnabled = $false
        $script:keyPath = Join-Path $TestDrive 'test-key'
        'test fixture, not a private key' | Set-Content -LiteralPath $script:keyPath
        $driverDir = Join-Path $TestDrive '03-workload'
        $toolsDir = Join-Path $TestDrive '02-platform\tools\cluster'
        $securityDir = Join-Path $TestDrive '01-resources\security'
        foreach ($directory in @($driverDir, $toolsDir, $securityDir)) {
            New-Item -ItemType Directory -Path $directory -Force | Out-Null
        }
        Copy-Item (Join-Path $benchmarkDir '03-workload\cluster.ps1') $driverDir
        Copy-Item (Join-Path $benchmarkDir '02-platform\tools\cluster\cluster-utils.ps1') $toolsDir
        Copy-Item (Join-Path $benchmarkDir '02-platform\vmss-utils.ps1') (Join-Path $TestDrive '02-platform')
        Copy-Item (Join-Path $benchmarkDir '01-resources\security\ssh-key-utils.ps1') $securityDir
        Copy-Item (Join-Path $benchmarkDir '01-resources\security\manifest.template.json') $securityDir
        @{
            basePath = [string]$TestDrive
            userKeys = @('missing-key', 'test-key.pub')
            vmKeys = 'test-vm-key'
        } | ConvertTo-Json | Set-Content (Join-Path $securityDir 'manifest.json')
        $legacyDefinition = Get-Content (Join-Path $benchmarkDir 'templates\cluster-explicit.json') -Raw | ConvertFrom-Json
        $legacyDefinition | Add-Member VmDomain 'd128ldsv6.australiaeast.cloudapp.azure.com'
        $legacyDefinition | Add-Member SshUser 'guser'
        $legacyDefinition.InstanceConfig = Join-Path $benchmarkDir '02-platform\tools\config\garnet\garnet-cluster-diskless.conf'
        $legacyDefinition | ConvertTo-Json -Depth 10 | Set-Content (Join-Path $TestDrive 'legacy-cluster.json')
        Mock Read-Host { '0' }
        Mock az {
            if (-not $global:ClusterTestAzureEnabled) { throw 'Azure discovery must not run in this test.' }
            $global:LASTEXITCODE = 0
            switch ($args[1]) {
                'list' { '[{"name":"deployment","state":"Succeeded"}]' }
                'show' { '"azureuser"' }
                'list-instance-public-ips' {
                    '[{"fqdn":"vm0.actual.region.example.com"},{"fqdn":"vm1.actual.region.example.com"},{"fqdn":"vm2.actual.region.example.com"}]'
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
            $global:ClusterTestWireCommands.Add($command)
            if ($command -match 'CLUSTER_ADDRESS=') {
                $terminalPrefix = if ($args -ccontains '-t') { "$([char]27)[?1h$([char]27)=$([char]27)]0;$([char]7)" } else { '' }
                if ($args[-2] -like '*@vm0.*') { "${terminalPrefix}CLUSTER_ADDRESS=10.5.128.4" }
                else { "${terminalPrefix}CLUSTER_ADDRESS=10.5.128.5" }
            } elseif ($command -match '-Action import-peers') {
                'PEER_IMPORT_OK'
            }
        }
    }

    AfterEach {
        Remove-Variable -Name ClusterTestWireCommands -Scope Global
        Remove-Variable -Name ClusterTestAzureEnabled -Scope Global
    }

    It 'runs declarative <Operation> with <KeySource> keys without VMSS discovery or including the client VM' -TestCases @(
        @{ Operation = 'create'; Expected = 5; KeySource = 'explicit' }
        @{ Operation = 'stop'; Expected = 4; KeySource = 'explicit' }
        @{ Operation = 'create'; Expected = 5; KeySource = 'manifest' }
        @{ Operation = 'stop'; Expected = 4; KeySource = 'manifest' }
    ) {
        param($Operation, $Expected, $KeySource)
        $keyArguments = @{}
        if ($KeySource -eq 'explicit') {
            $keyArguments.SshKey = $script:keyPath
            Remove-Item -LiteralPath (Join-Path $TestDrive '01-resources\security\manifest.json')
        }
        & (Join-Path $TestDrive '03-workload\cluster.ps1') -Action $Operation `
            -Definition (Join-Path $TestDrive 'legacy-cluster.json') @keyArguments
        $global:ClusterTestWireCommands.Count | Should Be $Expected
        Assert-MockCalled ssh -Times $Expected -Exactly -Scope It -ParameterFilter { $args[1] -eq (Join-Path $TestDrive 'test-key') }
        Assert-MockCalled az -Times 0 -Exactly -Scope It
        Assert-MockCalled ssh -Times 0 -Exactly -Scope It -ParameterFilter { $args[-2] -like '*@vm2.*' }
    }

    It 'runs VMSS-based <Operation> using <Selection> selection and only declared VMs' -TestCases @(
        @{ Operation = 'create'; Selection = 'explicit'; Expected = 5 }
        @{ Operation = 'stop'; Selection = 'explicit'; Expected = 4 }
    ) {
        param($Operation, $Selection, $Expected)
        $global:ClusterTestAzureEnabled = $true
        $selectionArguments = @{}
        if ($Selection -eq 'explicit') { $selectionArguments.VmssName = 'deployment'; $selectionArguments.NonInteractive = $true }
        & (Join-Path $TestDrive '03-workload\cluster.ps1') -Action $Operation `
            -Definition (Join-Path $benchmarkDir 'templates\cluster-explicit.json') `
            -rg test -SshKey $script:keyPath @selectionArguments
        $global:ClusterTestWireCommands.Count | Should Be $Expected
        Assert-MockCalled az -Times 3 -Exactly -Scope It
        Assert-MockCalled ssh -Times $Expected -Exactly -Scope It -ParameterFilter { $args[-2] -like 'azureuser@vm*.actual.region.example.com' }
        Assert-MockCalled ssh -Times 0 -Exactly -Scope It -ParameterFilter { $args[-2] -like '*@vm2.*' }
        if ($Selection -eq 'explicit') { Assert-MockCalled Read-Host -Times 0 -Exactly -Scope It }
        else { Assert-MockCalled Read-Host -Times 1 -Exactly -Scope It }
    }

    It 'preserves the legacy <Operation> entry point and command sequence with <KeySource> keys' -TestCases @(
        @{ Operation = 'start'; Expected = 'start,setup'; KeySource = 'explicit' }
        @{ Operation = 'stop'; Expected = 'stop'; KeySource = 'explicit' }
        @{ Operation = 'restart'; Expected = 'stop,start,setup'; KeySource = 'explicit' }
        @{ Operation = 'start'; Expected = 'start,setup'; KeySource = 'manifest' }
        @{ Operation = 'stop'; Expected = 'stop'; KeySource = 'manifest' }
        @{ Operation = 'restart'; Expected = 'stop,start,setup'; KeySource = 'manifest' }
    ) {
        param($Operation, $Expected, $KeySource)
        $driverDir = Join-Path $TestDrive '03-workload'
        $cacheDir = Join-Path $TestDrive '.peer-cache'
        New-Item -ItemType Directory -Path $cacheDir -Force | Out-Null
        @{
            schemaVersion = 1; source = 'azure-cli'; vmssName = 'testvmss'; resourceGroup = 'test'
            generatedAt = [datetimeoffset]::UtcNow.ToString('o')
            peers = @(@{ instanceId = '0'; eth1Ip = '10.0.0.4' }, @{ instanceId = '1'; eth1Ip = '10.0.0.5' })
        } | ConvertTo-Json -Depth 10 | Set-Content (Join-Path $cacheDir 'peers.json')
        $instancePath = Join-Path $TestDrive 'instance.conf'
        '{}' | Set-Content -LiteralPath $instancePath
        $keyArguments = @{}
        if ($KeySource -eq 'explicit') {
            $keyArguments.SshKey = $script:keyPath
            Remove-Item -LiteralPath (Join-Path $TestDrive '01-resources\security\manifest.json')
        }
        & (Join-Path $driverDir 'cluster.ps1') -Action $Operation -System garnet -Conf $instancePath `
            -ServerHost 'vm0.testvmss.region.example.com' -ResourceGroup test -VmCount 2 @keyArguments
        $operations = @($global:ClusterTestWireCommands | ForEach-Object {
            if ($_ -match 'cluster-deploy.ps1 -Action (start|setup|stop)') { $Matches[1] }
        })
        ($operations -join ',') | Should Be $Expected
        Assert-MockCalled ssh -Scope It -ParameterFilter { $args[1] -eq (Join-Path $TestDrive 'test-key') }
        Assert-MockCalled az -Times 0 -Exactly -Scope It
    }

    It 'reports a missing manifest before SSH rather than falling back to an undeclared key' {
        Remove-Item -LiteralPath (Join-Path $TestDrive '01-resources\security\manifest.json')
        {
            & (Join-Path $TestDrive '03-workload\cluster.ps1') -Action stop `
                -Definition (Join-Path $TestDrive 'legacy-cluster.json')
        } | Should Throw 'Created'
        Assert-MockCalled ssh -Times 0 -Exactly -Scope It
    }

    It 'reports missing declared keys before SSH rather than falling back to an undeclared key' {
        Remove-Item -LiteralPath $script:keyPath
        {
            & (Join-Path $TestDrive '03-workload\cluster.ps1') -Action stop `
                -Definition (Join-Path $TestDrive 'legacy-cluster.json')
        } | Should Throw 'None of the SSH private keys from the manifest exist'
        Assert-MockCalled ssh -Times 0 -Exactly -Scope It
    }
}
