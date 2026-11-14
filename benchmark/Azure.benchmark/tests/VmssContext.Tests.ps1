$benchmarkDir = Split-Path -Parent $PSScriptRoot
. (Join-Path $benchmarkDir '02-platform\vmss-utils.ps1')

function az { throw 'Azure CLI must be mocked' }

Describe 'Management instance selection' {
    BeforeEach {
        $tokens = $null; $errors = $null
        $ast = [Management.Automation.Language.Parser]::ParseFile(
            (Join-Path $benchmarkDir '02-platform\manage-vmss.ps1'), [ref]$tokens, [ref]$errors)
        if ($errors.Count) { throw ($errors | Out-String) }
        $filter = $ast.Find({
            param($node)
            $node -is [Management.Automation.Language.IfStatementAst] -and
                $node.Clauses[0].Item1.Extent.Text -eq '$VmNames.Count'
        }, $true)
        $script:instanceFilter = [scriptblock]::Create($filter.Extent.Text)
        $script:instances = @(
            [pscustomobject]@{ instance = 'id0'; fqdn = 'vm0.example.com' }
            [pscustomobject]@{ instance = 'id2'; fqdn = 'vm2.example.com' }
            [pscustomobject]@{ instance = 'id9'; fqdn = 'vm9.example.com' }
        )
    }

    It 'selects only the requested VM and preserves legacy all-instance selection when omitted' {
        $instances = @($script:instances)
        $VmNames = @()
        . $script:instanceFilter
        $instances.Count | Should Be 3
        $VmNames = @('vm2')
        . $script:instanceFilter
        $instances.Count | Should Be 1
        $instances[0].instance | Should Be 'id2'
    }

    It 'rejects missing, duplicate, or ambiguous VM selections (<Case>)' -TestCases @(
        @{ Case = 'missing'; Names = @('vm8'); Expected = 'found 0' }
        @{ Case = 'duplicate'; Names = @('vm2', 'VM2'); Expected = 'Duplicate VM selection' }
        @{ Case = 'ambiguous'; Names = @('vm2'); Expected = 'found 2' }
    ) {
        param($Case, $Names, $Expected)
        $instances = @($script:instances)
        $VmNames = $Names
        if ($Case -eq 'ambiguous') { $instances += [pscustomobject]@{ instance = 'other'; fqdn = 'vm2.other.example.com' } }
        { . $script:instanceFilter } | Should Throw $Expected
    }
}

Describe 'Shared VMSS selection' {
    BeforeEach {
        $script:azureExit = 0
        $script:azureReply = '[{"name":"servers","state":"Succeeded"},{"name":"clients","state":"Succeeded"}]'
        $script:selection = '0'
        Mock Test-VmssNonInteractive { $false }
        Mock Read-Host { $script:selection }
        Mock az { $global:LASTEXITCODE = $script:azureExit; $script:azureReply }
    }

    It 'selects an explicit VMSS without prompting' {
        Select-Vmss -ResourceGroup test -VmssName servers -Single -NonInteractive | Should Be 'servers'
        Assert-MockCalled Read-Host -Times 0 -Exactly -Scope It
    }

    It 'supports interactive selection by <Selection>' -TestCases @(
        @{ Selection = '1' }
        @{ Selection = 'clients' }
    ) {
        param($Selection)
        $script:selection = $Selection
        Select-Vmss -ResourceGroup test -Single | Should Be 'clients'
        Assert-MockCalled Read-Host -Times 1 -Exactly -Scope It
    }

    It 'preserves management selection of <Selection>' -TestCases @(
        @{ Selection = 'all' }
        @{ Selection = 'servers,clients' }
    ) {
        param($Selection)
        (@(Select-Vmss -ResourceGroup test -VmssName $Selection) -join ',') | Should Be 'servers,clients'
    }

    It 'retains the management power-state provider for interactive listings' {
        $script:providerNames = @()
        $provider = {
            param($names)
            $script:providerNames = $names
            @{ servers = 'running: 2'; clients = 'deallocated: 1' }
        }
        Select-Vmss -ResourceGroup test -PowerStateProvider $provider | Should Be 'servers'
        ($script:providerNames -join ',') | Should Be 'servers,clients'
    }

    It 'rejects <Selection> for a single-VMSS topology' -TestCases @(
        @{ Selection = 'all' }
        @{ Selection = 'servers,clients' }
    ) {
        param($Selection)
        { Select-Vmss -ResourceGroup test -VmssName $Selection -Single } | Should Throw 'exactly one VMSS'
        Assert-MockCalled Read-Host -Times 0 -Exactly -Scope It
    }

    It 'rejects missing selections without prompting in non-interactive mode' {
        { Select-Vmss -ResourceGroup test -Single -NonInteractive } | Should Throw 'VmssName is required'
        Assert-MockCalled Read-Host -Times 0 -Exactly -Scope It
        Assert-MockCalled az -Times 0 -Exactly -Scope It
    }

    It 'rejects missing selections when the PowerShell process is non-interactive' {
        Mock Test-VmssNonInteractive { $true }
        { Select-Vmss -ResourceGroup test -Single } | Should Throw 'VmssName is required'
        Assert-MockCalled Read-Host -Times 0 -Exactly -Scope It
    }

    It 'rejects missing resource groups' {
        { Select-Vmss -VmssName servers -Single } | Should Throw 'ResourceGroup'
        Assert-MockCalled az -Times 0 -Exactly -Scope It
    }

    It 'rejects unknown names and out-of-range interactive indices' {
        { Select-Vmss -ResourceGroup test -VmssName absent -Single } | Should Throw 'not found'
        $script:selection = '2'
        { Select-Vmss -ResourceGroup test -Single } | Should Throw 'Invalid VMSS index'
    }

    It 'surfaces Azure listing failures and empty inventories' {
        $script:azureExit = 1
        $script:azureReply = 'authentication failed'
        { Select-Vmss -ResourceGroup test -VmssName servers } | Should Throw 'authentication failed'
        $script:azureExit = 0
        $script:azureReply = '[]'
        { Select-Vmss -ResourceGroup test -VmssName servers } | Should Throw 'No VMSS found'
    }
}

Describe 'VMSS execution context' {
    BeforeEach {
        $script:user = 'guser'
        $script:inventory = '[{"fqdn":"vm0.custom.region.example.com"},{"fqdn":"vm1.custom.region.example.com"},{"fqdn":"vm2.custom.region.example.com"}]'
        Mock Read-Host { throw 'Explicit selection must not prompt' }
        Mock az {
            $global:LASTEXITCODE = 0
            switch ($args[1]) {
                'list' { '[{"name":"servers","state":"Succeeded"}]' }
                'show' { ConvertTo-Json -InputObject $script:user -Compress }
                'list-instance-public-ips' { $script:inventory }
                default { throw "Unexpected Azure operation: $args" }
            }
        }
    }

    It 'resolves only declared DNS aliases and reads the Azure SSH username' {
        $context = Resolve-VmssExecutionContext -ResourceGroup test -VmssName servers -VmNames vm0,vm1 -NonInteractive
        $context.VmssName | Should Be 'servers'
        $context.SshUser | Should Be 'guser'
        $context.Nodes.Count | Should Be 2
        $context.Nodes[0].HostName | Should Be 'vm0.custom.region.example.com'
        $context.Nodes[1].HostName | Should Be 'vm1.custom.region.example.com'
        Assert-MockCalled az -Times 1 -Exactly -Scope It -ParameterFilter { $args[1] -eq 'show' -and $args -contains 'virtualMachineProfile.osProfile.adminUsername' }
    }

    It 'allows an explicit SSH username without querying the default' {
        $context = Resolve-VmssExecutionContext -ResourceGroup test -VmssName servers -VmNames vm0 -SshUser operator
        $context.SshUser | Should Be 'operator'
        Assert-MockCalled az -Times 0 -Exactly -Scope It -ParameterFilter { $args[1] -eq 'show' }
    }

    It 'rejects missing or ambiguous declared hostnames (<Inventory>)' -TestCases @(
        @{ Inventory = '[]'; Expected = 'found 0' }
        @{ Inventory = '[{"fqdn":"different.custom.example.com"}]'; Expected = 'found 0' }
        @{ Inventory = '[{"fqdn":"vm0.a.example.com"},{"fqdn":"vm0.b.example.com"}]'; Expected = 'found 2' }
        @{ Inventory = '[{"fqdn":"vm0.bad;host.example.com"}]'; Expected = 'Invalid Azure SSH FQDN' }
    ) {
        param($Inventory, $Expected)
        $script:inventory = $Inventory
        { Resolve-VmssExecutionContext -ResourceGroup test -VmssName servers -VmNames vm0 } | Should Throw $Expected
    }

    It 'rejects an invalid Azure SSH username' {
        $script:user = 'invalid user'
        { Resolve-VmssExecutionContext -ResourceGroup test -VmssName servers -VmNames vm0 } | Should Throw 'SSH username'
        Assert-MockCalled az -Times 0 -Exactly -Scope It -ParameterFilter { $args[1] -eq 'list-instance-public-ips' }
    }
}
