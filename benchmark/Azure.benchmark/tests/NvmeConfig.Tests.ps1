$benchmarkDir = Split-Path -Parent $PSScriptRoot
$repoDir = Split-Path -Parent (Split-Path -Parent $benchmarkDir)

function ConvertTo-MemoryBytes([string]$Size) {
    if ($Size -notmatch '^(\d+)([kmg])$') { throw "Unsupported memory size: $Size" }
    $units = @{ k = 1KB; m = 1MB; g = 1GB }
    [long]$Matches[1] * $units[$Matches[2]]
}

Describe 'NVMe investigation configuration' {
    $template = Get-Content (Join-Path $benchmarkDir '02-platform\tools\config\garnet\garnet-nvme.conf') -Raw
    $settings = $template.Replace('$nvme', '/mnt/nvme').Replace('$port', '7000').Replace('$eth1', '10.5.128.5') | ConvertFrom-Json

    It 'uses current host option names after template substitution' {
        $options = Get-Content (Join-Path $repoDir 'libs\host\Configuration\Options.cs') -Raw
        $propertyNames = @([regex]::Matches($options, '(?m)^\s*public\s+[\w.?<>\[\]]+\s+(\w+)\s*\{\s*get;') |
            ForEach-Object { $_.Groups[1].Value })
        foreach ($property in $settings.PSObject.Properties) {
            $propertyNames -contains $property.Name | Should Be $true
        }
        $settings.Port | Should Be 7000
        $settings.Address | Should Be '10.5.128.5'
    }

    It 'puts all configured server disk outputs on NVMe with real io_uring storage' {
        foreach ($property in $settings.PSObject.Properties | Where-Object { $_.Name -match 'Dir$|^FileLogger$' }) {
            $property.Value | Should Match '^/mnt/nvme/garnet-cluster/7000(?:/|$)'
        }
        $settings.LogDir | Should Be '/mnt/nvme/garnet-cluster/7000'
        $settings.CheckpointDir | Should Not BeNullOrEmpty
        $settings.FileLogger | Should Not BeNullOrEmpty
        $settings.EnableStorageTier | Should Be $true
        $settings.EnableAOF | Should Be $true
        $settings.UseAofNullDevice | Should Be $false
        $settings.DeviceType | Should Be 'Native'
        $settings.DeviceIoBackend | Should Be 'Uring'
    }

    It 'enables sampling in paired server profiles with otherwise identical controls' {
        $memoryTemplate = Get-Content (Join-Path $benchmarkDir '02-platform\tools\config\garnet\garnet-nvme-memory.conf') -Raw
        $memorySettings = $memoryTemplate.Replace('$nvme', '/mnt/nvme').Replace('$port', '7000').Replace('$eth1', '10.5.128.5') | ConvertFrom-Json
        ($settings.MetricsSamplingFrequency -gt 0) | Should Be $true
        foreach ($property in $settings.PSObject.Properties | Where-Object { $_.Name -ne 'LogMemorySize' }) {
            $memorySettings.($property.Name) | Should Be $property.Value
        }
        @($memorySettings.PSObject.Properties).Count | Should Be @($settings.PSObject.Properties).Count
    }

    It 'uses the same dataset with memory headroom in one profile and disk spill in the other' {
        $memoryTemplate = Get-Content (Join-Path $benchmarkDir '02-platform\tools\config\garnet\garnet-nvme-memory.conf') -Raw
        $memorySettings = $memoryTemplate.Replace('$nvme', '/mnt/nvme').Replace('$port', '7000').Replace('$eth1', '10.5.128.5') | ConvertFrom-Json
        $workload = Get-Content (Join-Path $benchmarkDir '03-workload\bench\bench-ltm-cluster.conf') -Raw | ConvertFrom-Json
        $payloadBytes = [long]$workload.DbSize * [long]$workload.ValueLength
        $diskBudgetBytes = ConvertTo-MemoryBytes $settings.LogMemorySize
        $memoryBudgetBytes = ConvertTo-MemoryBytes $memorySettings.LogMemorySize
        ($payloadBytes -le $memoryBudgetBytes / 2) | Should Be $true
        ($payloadBytes -ge $diskBudgetBytes * 8) | Should Be $true
    }
}

Describe 'Garnet runtime output routing' {
    $tokens = $null
    $parseErrors = $null
    $ast = [System.Management.Automation.Language.Parser]::ParseFile(
        (Join-Path $benchmarkDir '02-platform\tools\cluster\mcluster.ps1'),
        [ref]$tokens, [ref]$parseErrors)
    if ($parseErrors.Count) { throw ($parseErrors | Out-String) }
    foreach ($name in @('Get-GarnetRuntimeDirectory', 'Start-Garnet')) {
        $function = $ast.Find({
            param($node)
            $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $name
        }, $true)
        . ([scriptblock]::Create($function.Extent.Text))
    }
    function mountpoint { throw 'mountpoint must be mocked' }
    function sudo { throw 'sudo must be mocked' }
    function bash { throw 'bash must be mocked' }

    BeforeEach {
        $script:NVME_DIR = '/mnt/nvme'
        $script:BASE_PORT = 7000
        $script:launchCommand = $null
        Mock mountpoint { $global:LASTEXITCODE = 0 }
    }

    It 'selects the NVMe runtime directory without changing other profiles (<LogDir>)' -TestCases @(
        @{ LogDir = '/mnt/nvme/garnet-cluster/7000'; Expected = '/mnt/nvme/garnet-cluster/7000'; Probes = 1 }
        @{ LogDir = '/mnt/nvme-archive/7000'; Expected = 'instance'; Probes = 0 }
        @{ LogDir = '/mnt/ramdisk/garnet-cluster/7000'; Expected = 'instance'; Probes = 0 }
        @{ LogDir = $null; Expected = 'instance'; Probes = 0 }
    ) {
        param($LogDir, $Expected, $Probes)
        $configPath = Join-Path $TestDrive 'garnet.conf'
        @{ LogDir = $LogDir } | ConvertTo-Json | Set-Content $configPath
        Get-GarnetRuntimeDirectory -ConfigPath $configPath -DefaultDirectory 'instance' | Should Be $Expected
        Assert-MockCalled mountpoint -Times $Probes -Exactly -Scope It
    }

    It 'refuses NVMe runtime output when the mount is missing' {
        $configPath = Join-Path $TestDrive 'garnet.conf'
        '{"LogDir":"/mnt/nvme/garnet-cluster/7000"}' | Set-Content $configPath
        Mock mountpoint { $global:LASTEXITCODE = 1 }
        { Get-GarnetRuntimeDirectory -ConfigPath $configPath -DefaultDirectory 'instance' } |
            Should Throw 'refusing to write to the OS disk'
    }

    It 'launches with stdout, stderr and the working directory on <RuntimeDir>' -TestCases @(
        @{ RuntimeDir = '/mnt/nvme/garnet-cluster/7000' }
        @{ RuntimeDir = '/dev/shm/garnet-cluster/7000' }
        @{ RuntimeDir = 'ordinary-instance' }
    ) {
        param($RuntimeDir)
        Mock sudo {}
        Mock Test-Path { $true }
        Mock Get-GarnetRuntimeDirectory { $RuntimeDir }
        Mock New-Item {}
        Mock Set-Location {}
        Mock bash {
            if ($args[1] -like 'nohup *') { $script:launchCommand = $args[1] }
        }
        Start-Garnet -Count 1
        $script:launchCommand | Should Match ([regex]::Escape("> '$RuntimeDir/garnet.log' 2> '$RuntimeDir/garnet.err'"))
        Assert-MockCalled Set-Location -Times 1 -Exactly -Scope It -ParameterFilter { $Path -eq $RuntimeDir }
    }
}
