$benchmarkDir = Split-Path -Parent $PSScriptRoot

Describe 'Native storage dependencies' {
    $tokens = $null
    $parseErrors = $null
    $installerAst = [System.Management.Automation.Language.Parser]::ParseFile(
        (Join-Path $benchmarkDir '02-platform\tools\bootstrap\install-native-dependencies.ps1'),
        [ref]$tokens, [ref]$parseErrors)
    if ($parseErrors.Count) { throw ($parseErrors | Out-String) }
    $installer = $installerAst.Find({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq 'Install-NativeDependencies'
    }, $true)
    . ([scriptblock]::Create($installer.Extent.Text))
    function apt-get { throw 'apt-get must be mocked' }
    function tdnf { throw 'tdnf must be mocked' }
    function dnf { throw 'dnf must be mocked' }
    function ldconfig { throw 'ldconfig must be mocked' }

    BeforeEach {
        $script:manager = 'tdnf'
        $script:updateExitCode = 0
        $script:installExitCode = 0
        $script:ldconfigExitCode = 0
        $script:packageArguments = @()
        Mock Get-Command {
            if ($Name -eq $script:manager) { return [pscustomobject]@{ Name = $Name } }
        } -ParameterFilter { $CommandType -eq 'Application' }
        Mock apt-get {
            if ($args[0] -eq 'update') {
                $global:LASTEXITCODE = $script:updateExitCode
            } else {
                $script:packageArguments = $args
                $global:LASTEXITCODE = $script:installExitCode
            }
        }
        Mock tdnf {
            $script:packageArguments = $args
            $global:LASTEXITCODE = $script:installExitCode
        }
        Mock dnf {
            $script:packageArguments = $args
            $global:LASTEXITCODE = $script:installExitCode
        }
        Mock ldconfig { $global:LASTEXITCODE = $script:ldconfigExitCode }
    }

    It 'installs runtime libraries and headers using <Manager>' -TestCases @(
        @{ Manager = 'apt-get'; Packages = 'liburing-dev,libaio-dev,libevent-dev,fio,gcc,make,git,pkg-config,mdadm,xfsprogs,e2fsprogs,util-linux' }
        @{ Manager = 'tdnf'; Packages = 'liburing,liburing-devel,libaio,libaio-devel,libevent-devel,fio,gcc,make,git,pkg-config,mdadm,xfsprogs,e2fsprogs,util-linux' }
        @{ Manager = 'dnf'; Packages = 'liburing,liburing-devel,libaio,libaio-devel,libevent-devel,fio,gcc,make,git,pkg-config,mdadm,xfsprogs,e2fsprogs,util-linux' }
    ) {
        param($Manager, $Packages)
        $script:manager = $Manager

        Install-NativeDependencies

        ($script:packageArguments -join ',') | Should Be "install,-y,$Packages"
        Assert-MockCalled ldconfig -Times 1 -Exactly -Scope It
        if ($Manager -eq 'apt-get') {
            Assert-MockCalled apt-get -Times 1 -Exactly -Scope It -ParameterFilter { $args[0] -eq 'update' }
        }

    }

    It 'rejects unsupported package managers' {
        $script:manager = 'unsupported'
        { Install-NativeDependencies } | Should Throw 'requires apt-get, tdnf, or dnf'
        Assert-MockCalled ldconfig -Times 0 -Exactly -Scope It
    }

    It 'stops when refreshing package metadata fails' {
        $script:manager = 'apt-get'
        $script:updateExitCode = 1
        { Install-NativeDependencies } | Should Throw 'apt-get update failed'
        $script:packageArguments.Count | Should Be 0
        Assert-MockCalled ldconfig -Times 0 -Exactly -Scope It
    }

    It 'surfaces package installation failures' {
        $script:installExitCode = 1
        { Install-NativeDependencies } | Should Throw 'tdnf install failed'
        Assert-MockCalled ldconfig -Times 0 -Exactly -Scope It
    }

    It 'surfaces linker cache update failures' {
        $script:ldconfigExitCode = 1
        { Install-NativeDependencies } | Should Throw 'ldconfig failed'
    }
}

Describe 'Hiredis build' {
    $tokens = $null
    $parseErrors = $null
    $buildAst = [System.Management.Automation.Language.Parser]::ParseFile(
        (Join-Path $benchmarkDir '02-platform\tools\bootstrap\build.ps1'),
        [ref]$tokens, [ref]$parseErrors)
    if ($parseErrors.Count) { throw ($parseErrors | Out-String) }
    $builder = $buildAst.Find({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq 'Build-Hiredis'
    }, $true)
    . ([scriptblock]::Create($builder.Extent.Text))
    function Invoke-Git { throw 'git must be mocked' }
    function sudo { throw 'sudo must be mocked' }
    function pkg-config { throw 'pkg-config must be mocked' }

    BeforeEach {
        $script:hiredisDir = $TestDrive
        $script:Branch = 'v1.5.0'
        $script:DEPLOY_USER = 'guser'
        $script:cores = '2'
        $script:buildExitCode = 0
        Mock Invoke-Git {}
        Mock sudo {
            if ($args -contains '-j2') { $global:LASTEXITCODE = $script:buildExitCode }
            else { $global:LASTEXITCODE = 0 }
        }
        Mock pkg-config {
            $global:LASTEXITCODE = 0
            if ($args -contains '--variable=pc_path') { '/usr/lib/pkgconfig:/usr/share/pkgconfig' }
            else { '1.5.0' }
        }
    }

    It 'installs hiredis with discoverable metadata and refreshes the linker cache' {
        Build-Hiredis

        Assert-MockCalled Invoke-Git -Times 1 -Exactly -Scope It -ParameterFilter {
            ($args[0] -join ',') -eq 'checkout,v1.5.0,--'
        }
        Assert-MockCalled sudo -Times 1 -Exactly -Scope It -ParameterFilter {
            ($args -join ',') -eq 'make,install,PREFIX=/usr/local'
        }
        Assert-MockCalled sudo -Times 1 -Exactly -Scope It -ParameterFilter {
            ($args -join ',') -eq 'install,-D,-m,0644,hiredis.pc,/usr/lib/pkgconfig/hiredis.pc'
        }
        Assert-MockCalled sudo -Times 1 -Exactly -Scope It -ParameterFilter { $args[0] -eq 'ldconfig' }
    }

    It 'stops on a build failure and restores the working directory' {
        $original = Get-Location
        $script:buildExitCode = 1

        { Build-Hiredis } | Should Throw 'Hiredis build failed'

        (Get-Location).Path | Should Be $original.Path
        Assert-MockCalled sudo -Times 0 -Exactly -Scope It -ParameterFilter { $args[0] -eq 'make' }
    }
}
