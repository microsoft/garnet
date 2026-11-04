$benchmarkDir = Split-Path -Parent $PSScriptRoot

Describe 'Resource group Owner access' {
    . (Join-Path $benchmarkDir '01-resources\storage-permission-utils.ps1')

    BeforeEach {
        $script:groupScope = '/subscriptions/sub/resourceGroups/benchmark'
        $script:assignments = @()
        Mock Get-StoragePermissionJson {
            param([string[]]$Arguments)

            switch ($Arguments[0]) {
                'account' { return [pscustomobject]@{ id = 'sub'; name = 'subscription'; user = @{ type = 'user'; name = 'publisher' } } }
                'ad' { return [pscustomobject]@{ id = 'publisher-id' } }
                'group' { return [pscustomobject]@{ id = $script:groupScope } }
                'role' { return $script:assignments }
                default { throw "Unexpected Azure command: $Arguments" }
            }
        }
        Mock Write-ManualStorageElevationInstructions {}
    }

    It 'accepts unrestricted Owner inherited from <Scope>' -TestCases @(
        @{ Scope = '/providers/Microsoft.Management/managementGroups/parent' }
        @{ Scope = '/subscriptions/sub' }
        @{ Scope = '/subscriptions/sub/resourceGroups/benchmark' }
    ) {
        param($Scope)

        $script:assignments = @([pscustomobject]@{
            scope = $Scope
            roleDefinitionName = 'Owner'
            roleDefinitionId = '/providers/Microsoft.Authorization/roleDefinitions/8e3af657-a8ff-443c-a75c-2fe8c4bcb635'
            condition = $null
        })

        $context = Get-ResourceGroupOwnerContext -ResourceGroup benchmark

        $context.PrincipalId | Should Be 'publisher-id'
        $context.ResourceGroupScope | Should Be $script:groupScope
        Assert-MockCalled Get-StoragePermissionJson -Times 1 -Exactly -Scope It -ParameterFilter {
            $Arguments[0] -eq 'role' -and
            $Arguments -contains '--include-inherited' -and
            $Arguments -contains '--include-groups' -and
            $Arguments[([array]::IndexOf($Arguments, '--scope') + 1)] -eq $script:groupScope
        }
    }

    It 'rejects restricted Owner and non-Owner assignments' {
        $script:assignments = @(
            [pscustomobject]@{
                scope = $script:groupScope
                roleDefinitionName = 'Owner'
                roleDefinitionId = '8e3af657-a8ff-443c-a75c-2fe8c4bcb635'
                condition = 'restricted'
            }
            [pscustomobject]@{
                scope = $script:groupScope
                roleDefinitionName = 'Contributor'
                roleDefinitionId = 'b24988ac-6180-42a0-ab88-20f7382dd24c'
                condition = $null
            }
        )

        Get-ResourceGroupOwnerContext -ResourceGroup benchmark | Should BeNullOrEmpty
        Assert-MockCalled Write-ManualStorageElevationInstructions -Times 1 -Exactly -Scope It
    }
}

Describe 'Existing VM tools bootstrap' {
    # Load only the command builder, without executing VMSS management.
    $tokens = $null
    $parseErrors = $null
    $managementAst = [System.Management.Automation.Language.Parser]::ParseFile(
        (Join-Path $benchmarkDir '02-platform\manage-vmss.ps1'), [ref]$tokens, [ref]$parseErrors)
    $builder = $managementAst.Find({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq 'Get-ToolsUpdateCommand'
    }, $true)
    . ([scriptblock]::Create($builder.Extent.Text))
    function Get-ToolsBlobUrl {}
    function tar { throw 'tar must be mocked' }
    function sudo {
        param([string]$Command, [switch]$D, [string]$m, [string]$Source, [string]$Destination)
        throw 'sudo must be mocked'
    }

    BeforeEach {
        $script:installExitCode = 0
        $script:configSource = $null
        $script:storageConfig = $null
        Mock Get-ToolsBlobUrl { 'https://storage.blob.core.windows.net/tools/tools.tar.gz' }
        Mock Invoke-RestMethod { [pscustomobject]@{ access_token = 'test-token' } }
        Mock Invoke-WebRequest {}
        Mock Test-Path { $false } -ParameterFilter { $Path -like '/tmp/*' }
        Mock Test-Path { $true } -ParameterFilter { $Path -like '/home/*/tools/update.ps1' }
        Mock New-Item {} -ParameterFilter { $Path -like '/home/*' }
        Mock tar { $global:LASTEXITCODE = 0 }
        Mock sudo {
            $script:configSource = $Source
            $script:storageConfig = Get-Content -LiteralPath $Source -Raw
            $global:LASTEXITCODE = $script:installExitCode
        }
    }

    It 'persists a readable tools URL for <Mode> on an existing VM' -TestCases @(
        @{ Mode = 'Run' }
        @{ Mode = 'Copy' }
    ) {
        param($Mode)

        $command = Get-ToolsUpdateCommand -User guser -Mode $Mode
        $bootstrap = [System.Text.Encoding]::Unicode.GetString([Convert]::FromBase64String(($command -split ' ')[-1]))
        $ast = [System.Management.Automation.Language.Parser]::ParseInput($bootstrap, [ref]$tokens, [ref]$parseErrors)
        $parseErrors.Count | Should Be 0
        # Execute the bootstrap with mocked guest operations, excluding updater dispatch and exit.
        & ([scriptblock]::Create($bootstrap.Substring(0, $ast.EndBlock.Statements[-2].Extent.StartOffset)))

        $script:storageConfig | Should Not BeNullOrEmpty
        $script:storageConfig.Trim() | Should Be 'TOOLS_BLOB_URL=https://storage.blob.core.windows.net/tools/tools.tar.gz'
        Test-Path -LiteralPath $script:configSource | Should Be $false
        Assert-MockCalled sudo -Times 1 -Exactly -Scope It -ParameterFilter {
            $Command -eq 'install' -and $D -and $m -eq '0644' -and
            $Destination -eq '/opt/deploy-actions/storage.env'
        }
    }

    It 'reports configuration installation failure and removes its temporary file' {
        $script:installExitCode = 1
        $command = Get-ToolsUpdateCommand -User guser -Mode Copy
        $bootstrap = [System.Text.Encoding]::Unicode.GetString([Convert]::FromBase64String(($command -split ' ')[-1]))
        $ast = [System.Management.Automation.Language.Parser]::ParseInput($bootstrap, [ref]$tokens, [ref]$parseErrors)
        $script = [scriptblock]::Create($bootstrap.Substring(0, $ast.EndBlock.Statements[-2].Extent.StartOffset))

        { & $script } | Should Throw 'Failed to persist tools blob URL'
        Test-Path -LiteralPath $script:configSource | Should Be $false
    }
}

Describe 'VMSS deployment examples' {
    It 'includes storageAccountName in each complete deployment command' {
        $readme = Get-Content -LiteralPath (Join-Path $benchmarkDir '02-platform\README.md') -Raw
        $examples = @([regex]::Matches($readme, '(?s)```powershell\r?\n(az deployment group create .*?)```') |
            Where-Object { $_.Groups[1].Value -match 'vmss.bicep' })
        $examples.Count | Should Be 4
        foreach ($example in $examples) {
            $text = $example.Groups[1].Value -replace '<[^>]+>', 'example'
            $tokens = $null
            $parseErrors = $null
            $ast = [System.Management.Automation.Language.Parser]::ParseInput($text, [ref]$tokens, [ref]$parseErrors)
            $parseErrors.Count | Should Be 0
            $ast.EndBlock.Statements.Count | Should Be 1
            $commands = @($ast.FindAll({
                param($node)
                $node -is [System.Management.Automation.Language.CommandAst]
            }, $true))
            $commands.Count | Should Be 1
            ($commands[0].CommandElements.Extent.Text -contains 'storageAccountName=example') | Should Be $true
        }
    }
}
