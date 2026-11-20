$benchmarkDir = Split-Path -Parent $PSScriptRoot

Describe 'NVMe mounting' {
    # Windows defines a mount alias that hides the Linux command mock.
    Remove-Item Alias:mount -Force -ErrorAction SilentlyContinue
    $tokens = $null
    $parseErrors = $null
    $ast = [System.Management.Automation.Language.Parser]::ParseFile(
        (Join-Path $benchmarkDir '02-platform\tools\bootstrap\setup-nvme.ps1'),
        [ref]$tokens, [ref]$parseErrors)
    if ($parseErrors.Count) { throw ($parseErrors | Out-String) }
    foreach ($name in @('Get-NvmeSignatures', 'Assert-NvmeArray', 'Mount-NvmeStorage')) {
        $definition = $ast.Find({
            param($node)
            $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $name
        }, $true)
        . ([scriptblock]::Create($definition.Extent.Text))
    }

    function lsblk { throw 'lsblk must be mocked' }
    function findmnt { throw 'findmnt must be mocked' }
    function readlink { throw 'readlink must be mocked' }
    function mdadm { throw 'mdadm must be mocked' }
    function mkfs.xfs { throw 'mkfs must be mocked' }
    function mkfs.ext4 { throw 'mkfs must be mocked' }
    function mount { throw 'mount must be mocked' }
    function chown { throw 'chown must be mocked' }
    function df { throw 'df must be mocked' }

    BeforeEach {
        $script:DEPLOY_USER = 'guser'
        $script:disks = @([pscustomobject]@{ path = '/dev/nvme1n1'; model = 'Microsoft NVMe Direct Disk v2' })
        $script:types = @{ '/dev/nvme1n1' = @(); '/dev/nvme2n1' = @(); '/dev/md/nvme0' = @() }
        $script:mountedSource = $null
        $script:busy = $false
        $script:arrayExists = $false
        $script:arrayLayout = @('/dev/md127 raid0', '/dev/nvme1n1 disk', '/dev/nvme2n1 disk')
        $script:mdadmExitCode = 0
        Mock lsblk {
            $global:LASTEXITCODE = 0
            if ($args -contains '--nodeps') { @{ blockdevices = $script:disks } | ConvertTo-Json -Depth 4 }
            elseif ($args -contains '--inverse') { $script:arrayLayout }
            elseif ($script:busy) { '/other-mount' }
        }
        Mock Get-NvmeSignatures { param($Device) $script:types[$Device] }
        Mock findmnt {
            $global:LASTEXITCODE = if ($script:mountedSource) { 0 } else { 1 }
            $script:mountedSource
        }
        Mock readlink { $global:LASTEXITCODE = 0; $args[-1] }
        Mock Test-Path { $script:arrayExists } -ParameterFilter { $Path -eq '/dev/md/nvme0' }
        Mock New-Item {}
        Mock mdadm { $global:LASTEXITCODE = $script:mdadmExitCode; $script:arrayExists = $true }
        Mock mkfs.xfs { $global:LASTEXITCODE = 0 }
        Mock mkfs.ext4 { $global:LASTEXITCODE = 0 }
        Mock mount { $global:LASTEXITCODE = 0 }
        Mock chown { $global:LASTEXITCODE = 0 }
        Mock df { $global:LASTEXITCODE = 0 }
    }

    It 'formats and mounts a blank single disk using <FileSystem>' -TestCases @(
        @{ FileSystem = 'xfs' }; @{ FileSystem = 'ext4' }
    ) {
        param($FileSystem)
        Mount-NvmeStorage -MountPath /mnt/nvme -FileSystem $FileSystem
        Assert-MockCalled "mkfs.$FileSystem" -Times 1 -Exactly -Scope It
        Assert-MockCalled mount -Times 1 -Exactly -Scope It
        Assert-MockCalled mdadm -Times 0 -Exactly -Scope It
    }

    It 'creates RAID0 only from blank ephemeral disks' {
        $script:disks += [pscustomobject]@{ path = '/dev/nvme2n1'; model = 'Microsoft NVMe Direct Disk v2' }
        Mount-NvmeStorage -MountPath /mnt/nvme
        Assert-MockCalled mdadm -Times 1 -Exactly -Scope It -ParameterFilter { $args -contains '--create' }
        Assert-MockCalled mkfs.xfs -Times 1 -Exactly -Scope It
    }

    It 'preserves an existing filesystem when remounting' {
        $script:types['/dev/nvme1n1'] = @('ext4')
        Mount-NvmeStorage -MountPath /mnt/nvme
        Assert-MockCalled mkfs.xfs -Times 0 -Exactly -Scope It
        Assert-MockCalled mkfs.ext4 -Times 0 -Exactly -Scope It
        Assert-MockCalled mount -Times 1 -Exactly -Scope It
    }

    It 'assembles and remounts an existing array without formatting' {
        $script:disks += [pscustomobject]@{ path = '/dev/nvme2n1'; model = 'Microsoft NVMe Direct Disk v2' }
        $script:types['/dev/nvme1n1'] = @('linux_raid_member')
        $script:types['/dev/nvme2n1'] = @('linux_raid_member')
        $script:types['/dev/md/nvme0'] = @('xfs')
        Mount-NvmeStorage -MountPath /mnt/nvme
        Assert-MockCalled mdadm -Times 1 -Exactly -Scope It -ParameterFilter { $args -contains '--assemble' }
        Assert-MockCalled mkfs.xfs -Times 0 -Exactly -Scope It
    }

    It 'leaves a correctly mounted volume unchanged' {
        $script:mountedSource = '/dev/nvme1n1'
        Mount-NvmeStorage -MountPath /mnt/nvme
        Assert-MockCalled Get-NvmeSignatures -Times 0 -Exactly -Scope It
        Assert-MockCalled mount -Times 0 -Exactly -Scope It
        Assert-MockCalled mkfs.xfs -Times 0 -Exactly -Scope It
    }

    It 'skips optional setup when no local NVMe exists' {
        $script:disks = @([pscustomobject]@{ path = '/dev/nvme0n1'; model = 'OS disk' })
        Mount-NvmeStorage -MountPath /mnt/nvme -IfPresent
        { Mount-NvmeStorage -MountPath /mnt/nvme } | Should Throw 'No Azure local NVMe disks'
        Assert-MockCalled mount -Times 0 -Exactly -Scope It
    }

    It 'rejects mixed disk signatures before RAID creation' {
        $script:disks += [pscustomobject]@{ path = '/dev/nvme2n1'; model = 'Microsoft NVMe Direct Disk v2' }
        $script:types['/dev/nvme1n1'] = @('xfs')
        { Mount-NvmeStorage -MountPath /mnt/nvme } | Should Throw 'existing data or mixed signatures'
        Assert-MockCalled mdadm -Times 0 -Exactly -Scope It
    }

    It 'rejects an unrelated mount or a disk used by another mount' {
        $script:mountedSource = '/dev/nvme0n1p3'
        { Mount-NvmeStorage -MountPath /mnt/nvme } | Should Throw 'unrelated device'
        $script:mountedSource = $null
        $script:busy = $true
        { Mount-NvmeStorage -MountPath /mnt/nvme } | Should Throw 'in use by another mount'
        Assert-MockCalled mkfs.xfs -Times 0 -Exactly -Scope It
    }

    It 'rejects an existing array with unrelated members' {
        $script:disks += [pscustomobject]@{ path = '/dev/nvme2n1'; model = 'Microsoft NVMe Direct Disk v2' }
        $script:arrayExists = $true
        $script:arrayLayout = @('/dev/md127 raid0', '/dev/nvme0n1 disk', '/dev/nvme2n1 disk')
        { Mount-NvmeStorage -MountPath /mnt/nvme } | Should Throw 'not a RAID0 array'
        Assert-MockCalled mount -Times 0 -Exactly -Scope It
    }

    It 'stops after RAID creation fails' {
        $script:disks += [pscustomobject]@{ path = '/dev/nvme2n1'; model = 'Microsoft NVMe Direct Disk v2' }
        $script:mdadmExitCode = 1
        { Mount-NvmeStorage -MountPath /mnt/nvme } | Should Throw 'Cannot create'
        Assert-MockCalled mkfs.xfs -Times 0 -Exactly -Scope It
    }

    It 'rejects partition signatures rather than overwriting them' {
        $script:types['/dev/nvme1n1'] = @('gpt')
        { Mount-NvmeStorage -MountPath /mnt/nvme } | Should Throw 'unsupported signatures'
        Assert-MockCalled mkfs.xfs -Times 0 -Exactly -Scope It
    }

    It 'requires an unmounted volume for explicit reformatting' {
        $script:mountedSource = '/dev/nvme1n1'
        { Mount-NvmeStorage -MountPath /mnt/nvme -Force } | Should Throw 'requires an unmounted NVMe volume'
        Assert-MockCalled mkfs.xfs -Times 0 -Exactly -Scope It
    }

    It 'reformats a known unmounted filesystem only when explicitly forced' {
        $script:types['/dev/nvme1n1'] = @('ext4')
        Mount-NvmeStorage -MountPath /mnt/nvme -Force
        Assert-MockCalled mkfs.xfs -Times 1 -Exactly -Scope It
    }
}

Describe 'NVMe startup service deployment' {
    $tokens = $null
    $parseErrors = $null
    $ast = [System.Management.Automation.Language.Parser]::ParseFile(
        (Join-Path $benchmarkDir '02-platform\tools\bootstrap\install-nvme-service.ps1'),
        [ref]$tokens, [ref]$parseErrors)
    if ($parseErrors.Count) { throw ($parseErrors | Out-String) }
    $definition = $ast.Find({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
            $node.Name -eq 'Install-NvmeStartupService'
    }, $true)
    . ([scriptblock]::Create($definition.Extent.Text))
    function systemctl { throw 'systemctl must be mocked' }

    BeforeEach {
        $script:commands = @()
        $script:failingCommand = ''
        Mock Test-Path { $true } -ParameterFilter {
            $LiteralPath -in @('/usr/bin/pwsh', '/opt/deploy-actions/setup-nvme.ps1',
                '/etc/systemd/system/azurebench-nvme.service')
        }
        Mock systemctl {
            $script:commands += ($args -join ' ')
            $global:LASTEXITCODE = if ($args[0] -eq $script:failingCommand) { 1 } else { 0 }
        }
    }

    It 'reloads, enables and reruns the idempotent setup service in order' {
        Install-NvmeStartupService
        ($script:commands -join '|') |
            Should Be 'daemon-reload|enable azurebench-nvme.service|restart azurebench-nvme.service'
    }

    It 'fails before activation when <Path> was not deployed' -TestCases @(
        @{ Path = '/usr/bin/pwsh' }
        @{ Path = '/opt/deploy-actions/setup-nvme.ps1' }
        @{ Path = '/etc/systemd/system/azurebench-nvme.service' }
    ) {
        param($Path)
        $script:missingPath = $Path
        Mock Test-Path { $false } -ParameterFilter { $LiteralPath -eq $script:missingPath }
        { Install-NvmeStartupService } | Should Throw 'Required NVMe startup file not found'
        Assert-MockCalled systemctl -Times 0 -Exactly -Scope It
    }

    It 'propagates <Command> failures and stops deployment' -TestCases @(
        @{ Command = 'daemon-reload'; Calls = 1 }
        @{ Command = 'enable'; Calls = 2 }
        @{ Command = 'restart'; Calls = 3 }
    ) {
        param($Command, $Calls)
        $script:failingCommand = $Command
        { Install-NvmeStartupService } | Should Throw "systemctl $Command"
        $script:commands.Count | Should Be $Calls
    }

    It 'ships the service and installer and activates them after native dependencies' {
        $tools = Join-Path $benchmarkDir '02-platform\tools'
        $manifest = Get-Content (Join-Path $tools 'manifest.json') -Raw | ConvertFrom-Json
        foreach ($destination in @('/etc/systemd/system/azurebench-nvme.service',
            '/opt/deploy-actions/install-nvme-service.ps1')) {
            $entries = @($manifest.scripts | Where-Object dst -eq $destination)
            $entries.Count | Should Be 1
            Test-Path -LiteralPath (Join-Path $tools $entries[0].src) -PathType Leaf | Should Be $true
        }
        $steps = @($manifest.runcmd.run)
        ($steps.IndexOf('install-nvme-service.ps1') -gt $steps.IndexOf('install-native-dependencies.ps1')) | Should Be $true
        $install = @($manifest.runcmd | Where-Object run -eq 'install-nvme-service.ps1')
        $install.Count | Should Be 1
        $install[0].sudo | Should Be $true
        $unit = Get-Content (Join-Path $tools 'bootstrap\azurebench-nvme.service') -Raw
        $unit | Should Match '(?m)^ExecStart=.*setup-nvme\.ps1 --if-present\r?$'
        $unit | Should Not Match '--force|(?m)^ExecStop='
    }
}
