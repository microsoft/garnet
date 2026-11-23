$benchmarkDir = Split-Path -Parent $PSScriptRoot
$repoDir = Split-Path -Parent (Split-Path -Parent $benchmarkDir)
$profilePath = Join-Path $benchmarkDir '02-platform\tools\config\garnet\garnet-cluster-diskless.conf'
$tokens = $null
$parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    (Join-Path $benchmarkDir '02-platform\tools\cluster\mcluster.ps1'),
    [ref]$tokens, [ref]$parseErrors)
if ($parseErrors.Count) { throw ($parseErrors | Out-String) }
foreach ($name in @('Get-GarnetLogMemorySize', 'Resolve-Config', 'Get-GarnetRuntimeDirectory')) {
    $function = $ast.Find({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $name
    }, $true)
    . ([scriptblock]::Create($function.Extent.Text))
}
function Get-Eth1Ip { throw 'Get-Eth1Ip must be mocked' }
function findmnt { throw 'findmnt must be mocked' }

Describe 'Diskless cluster memory sizing' {
    It 'reserves 20 percent of <MemoryKb> KiB and subtracts the AOF buffer' -TestCases @(
        @{ MemoryKb = 268435456; Expected = '218828583731' }
        @{ MemoryKb = 264123548; Expected = '215296268697' }
    ) {
        param($MemoryKb, $Expected)
        Mock Get-Content { "MemTotal: $MemoryKb kB`nMemFree: 1 kB" } -ParameterFilter { $LiteralPath -eq '/proc/meminfo' }
        Get-GarnetLogMemorySize -AofMemorySize '1g' | Should Be $Expected
    }

    It 'rejects missing memory information instead of supplying a default' {
        Mock Get-Content { 'MemFree: 123 kB' } -ParameterFilter { $LiteralPath -eq '/proc/meminfo' }
        { Get-GarnetLogMemorySize -AofMemorySize '1g' } | Should Throw 'Cannot determine total VM memory'
    }

    It 'rejects a VM too small for the AOF and minimum main log' {
        Mock Get-Content { 'MemTotal: 1048576 kB' } -ParameterFilter { $LiteralPath -eq '/proc/meminfo' }
        { Get-GarnetLogMemorySize -AofMemorySize '1g' } | Should Throw 'below the 64 MiB log minimum'
    }
}

Describe 'Diskless cluster profile resolution' {
    BeforeEach {
        $script:BASE_PORT = 7000
        $script:ClusterMode = 'true'
        $script:Tls = $false
        $script:RAMDISK_DIR = '/mnt/ramdisk'
        $script:NVME_DIR = '/mnt/nvme'
        $script:resolvedContent = $null
        Mock Get-Eth1Ip { '10.5.128.5' }
        Mock New-Item {}
        Mock Set-Content { $script:resolvedContent = $Value }
        Mock Get-Content { 'MemTotal: 264123548 kB' } -ParameterFilter { $LiteralPath -eq '/proc/meminfo' }
    }

    It 'produces a valid null-device cluster configuration with bounded replication lag' {
        Resolve-Config -Sys garnet -Count 1 -ConfPath $profilePath
        $settings = $script:resolvedContent | ConvertFrom-Json
        $options = Get-Content (Join-Path $repoDir 'libs\host\Configuration\Options.cs') -Raw
        $propertyNames = @([regex]::Matches($options, '(?m)^\s*public\s+[\w.?<>\[\]]+\s+(\w+)\s*\{\s*get;') |
            ForEach-Object { $_.Groups[1].Value })
        foreach ($property in $settings.PSObject.Properties) {
            $propertyNames -contains $property.Name | Should Be $true
        }
        $script:resolvedContent | Should Not Match '\$(?:logMemorySize|eth1|port)'
        $settings.Address | Should Be '10.5.128.5'
        $settings.Port | Should Be 7000
        $settings.LogMemorySize | Should Be '215296268697'
        $settings.AofMemorySize | Should Be '1g'
        $settings.AofPhysicalSublogCount | Should Be 1
        $settings.DeviceType | Should Be 'Null'
        $settings.EnableStorageTier | Should Be $false
        $settings.EnableAOF | Should Be $true
        $settings.UseAofNullDevice | Should Be $true
        $settings.FastAofTruncate | Should Be $true
        $settings.ReplicaDisklessSync | Should Be $true
        $settings.ClusterConfigFlushFrequencyMs | Should Be -1
        $settings.AofSyncMaxLagBytes | Should BeGreaterThan 0
        $settings.AofReplayMaxLagBytes | Should BeGreaterThan 0
        ($settings.AofSyncMaxLagBytes + $settings.AofReplayMaxLagBytes) | Should BeLessThan 1GB
        $settings.CheckpointFrequencySecs | Should Be 0
        $settings.OnDemandCheckpoint | Should Be $false
        $settings.Recover | Should Be $false
        $settings.FileLogger | Should BeNullOrEmpty
        $settings.CheckpointDir | Should Be '/dev/shm/garnet-cluster/7000'
    }

    It 'rejects multiple VM-sized instances instead of overcommitting the VM' {
        { Resolve-Config -Sys garnet -Count 2 -ConfPath $profilePath } | Should Throw 'exactly one Garnet instance'
        Assert-MockCalled Set-Content -Times 0 -Exactly -Scope It
    }

    It 'does not probe VM memory for an existing fixed-size profile' {
        $existingProfile = Join-Path $benchmarkDir '02-platform\tools\config\garnet\garnet-cache.conf'
        Resolve-Config -Sys garnet -Count 2 -ConfPath $existingProfile
        ($script:resolvedContent | ConvertFrom-Json).LogMemorySize | Should Be '16g'
        Assert-MockCalled Get-Content -Times 0 -Exactly -Scope It -ParameterFilter { $LiteralPath -eq '/proc/meminfo' }
        Assert-MockCalled Set-Content -Times 2 -Exactly -Scope It
    }
}

Describe 'Diskless runtime output routing' {
    BeforeEach {
        $script:NVME_DIR = '/mnt/nvme'
        Mock Get-Content { '{"CheckpointDir":"/dev/shm/garnet-cluster/7000"}' }
    }

    It 'selects the verified RAM-backed runtime directory' {
        Mock findmnt { $global:LASTEXITCODE = 0; 'tmpfs' }
        Get-GarnetRuntimeDirectory -ConfigPath 'garnet.conf' -DefaultDirectory 'disk-directory' |
            Should Be '/dev/shm/garnet-cluster/7000'
    }

    It 'refuses a missing or non-tmpfs mount (<FileSystem>, exit <ExitCode>)' -TestCases @(
        @{ FileSystem = 'xfs'; ExitCode = 0 }
        @{ FileSystem = ''; ExitCode = 1 }
    ) {
        param($FileSystem, $ExitCode)
        Mock findmnt { $global:LASTEXITCODE = $ExitCode; $FileSystem }
        { Get-GarnetRuntimeDirectory -ConfigPath 'garnet.conf' -DefaultDirectory 'disk-directory' } |
            Should Throw 'refusing to write diskless runtime output to disk'
    }
}
