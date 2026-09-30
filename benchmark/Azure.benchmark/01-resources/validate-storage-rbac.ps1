#!/usr/bin/env pwsh
[CmdletBinding(PositionalBinding = $false)]
<#
.SYNOPSIS
    Summarizes and validates Azure Storage RBAC readiness for the benchmark deployment.

.DESCRIPTION
    Checks the active Azure CLI identity, storage account authentication settings,
    effective management permissions, Entra-authenticated blob access, and Storage
    Blob Data Reader access for VMSS managed identities.

    The script is read-only by default. Pass -TestWriteAccess to upload and delete a
    temporary validation blob using the active Entra identity.

.PARAMETER rg
    Azure resource group containing the benchmark storage account and VMSS resources.

.PARAMETER StorageAccount
    Storage account name. If omitted, discovers the account tagged app=azurebench.

.PARAMETER ContainerName
    Blob container used for tools delivery (default: tools).

.PARAMETER VmssName
    Optional VMSS names to validate. If omitted, validates every VMSS in the resource group.

.PARAMETER TestWriteAccess
    Uploads and deletes a temporary blob to validate Storage Blob Data Contributor access.

.EXAMPLE
    .\validate-storage-rbac.ps1 -rg vazois-devbox

.EXAMPLE
    .\validate-storage-rbac.ps1 -rg vazois-devbox -TestWriteAccess

.EXAMPLE
    .\validate-storage-rbac.ps1 -rg vazois-devbox -VmssName server,client
#>

param(
    [Alias('ResourceGroup')]
    [string]$rg,

    [string]$StorageAccount = '',

    [string]$ContainerName = 'tools',

    [string[]]$VmssName,

    [switch]$TestWriteAccess,

    [switch]$Help,

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$RemainingArguments
)

foreach ($argument in $RemainingArguments) {
    switch ($argument) {
        { $_ -ieq '--testwriteaccess' -or $_ -ieq '--test-write-access' } {
            $TestWriteAccess = $true
            continue
        }
        '--help' {
            $Help = $true
            continue
        }
        default {
            throw "Unsupported argument '$argument'. PowerShell parameters use a single dash; run with -Help for supported options."
        }
    }
}

if ($Help -or -not $rg) {
    Write-Host "Usage: validate-storage-rbac.ps1 -rg <resource-group> [options]" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Summarizes Azure Storage RBAC readiness for the benchmark deployment."
    Write-Host "The script is read-only unless -TestWriteAccess is supplied."
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  -rg <name>                 Resource group name (required)"
    Write-Host "  -StorageAccount <name>     Storage account (default: discover app=azurebench)"
    Write-Host "  -ContainerName <name>      Tools container (default: tools)"
    Write-Host "  -VmssName <name[]>         VMSS names (default: all VMSS in the resource group)"
    Write-Host "  -TestWriteAccess           Upload and delete a temporary validation blob"
    Write-Host "  -Help                      Show this help message"
    return
}

$ErrorActionPreference = 'Stop'
$checks = [System.Collections.Generic.List[object]]::new()

function Add-Check {
    param(
        [string]$Category,
        [string]$Name,
        [ValidateSet('PASS', 'WARN', 'FAIL', 'INFO')]
        [string]$Status,
        [string]$Details
    )

    $checks.Add([pscustomobject]@{
            Category = $Category
            Check    = $Name
            Status   = $Status
            Details  = $Details
        })
}

function Invoke-AzCommand {
    param(
        [string[]]$Arguments,
        [switch]$AllowFailure
    )

    $errorFile = Join-Path ([System.IO.Path]::GetTempPath()) "azurebench-az-$([guid]::NewGuid().ToString('N')).err"
    try {
        $output = & az @Arguments 2> $errorFile
        $exitCode = $LASTEXITCODE
        $standardOutput = (@($output) | ForEach-Object { "$_" }) -join [Environment]::NewLine
        $standardError = if (Test-Path $errorFile) { [string](Get-Content -LiteralPath $errorFile -Raw) } else { '' }
        $result = [pscustomobject]@{
            ExitCode = $exitCode
            Output   = ([string]$standardOutput).Trim()
            Error    = ([string]$standardError).Trim()
        }

        if ($exitCode -ne 0 -and -not $AllowFailure) {
            $message = if ($result.Error) { $result.Error } elseif ($result.Output) { $result.Output } else { 'No error details returned.' }
            throw "az $($Arguments -join ' ') failed: $message"
        }
        return $result
    }
    finally {
        if (Test-Path $errorFile) {
            Remove-Item -LiteralPath $errorFile -Force
        }
    }
}

function Invoke-AzJson {
    param(
        [string[]]$Arguments,
        [switch]$AllowFailure
    )

    $result = Invoke-AzCommand -Arguments ($Arguments + @('--output', 'json')) -AllowFailure:$AllowFailure
    if ($result.ExitCode -ne 0 -or [string]::IsNullOrWhiteSpace($result.Output)) {
        return [pscustomobject]@{
            ExitCode = $result.ExitCode
            Value    = $null
            Error    = $result.Error
        }
    }

    try {
        $value = $result.Output | ConvertFrom-Json
    }
    catch {
        throw "Could not parse Azure CLI JSON output for 'az $($Arguments -join ' ')': $($_.Exception.Message)"
    }

    return [pscustomobject]@{
        ExitCode = 0
        Value    = $value
        Error    = ''
    }
}

function Get-ErrorSummary {
    param([string]$Text)

    $lines = @($Text -split '\r?\n' | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($lines.Count -eq 0) {
        return 'No error details returned.'
    }

    for ($i = 0; $i -lt $lines.Count; $i++) {
        if ($lines[$i] -match '^ERROR:\s*(.*)$') {
            if (-not [string]::IsNullOrWhiteSpace($Matches[1])) {
                return $Matches[1].Trim()
            }
            if ($i + 1 -lt $lines.Count) {
                return $lines[$i + 1].Trim()
            }
        }
    }
    return $lines[-1].Trim()
}

function Test-WildcardMatch {
    param(
        [string[]]$Patterns,
        [string]$Value
    )

    foreach ($pattern in @($Patterns)) {
        if ([string]::IsNullOrWhiteSpace($pattern)) {
            continue
        }
        $matcher = [System.Management.Automation.WildcardPattern]::new(
            $pattern,
            [System.Management.Automation.WildcardOptions]::IgnoreCase)
        if ($matcher.IsMatch($Value)) {
            return $true
        }
    }
    return $false
}

function Test-EffectivePermission {
    param(
        [object[]]$Permissions,
        [string]$Permission,
        [switch]$DataAction
    )

    foreach ($entry in @($Permissions)) {
        $allowed = if ($DataAction) { @($entry.dataActions) } else { @($entry.actions) }
        $excluded = if ($DataAction) { @($entry.notDataActions) } else { @($entry.notActions) }
        if ((Test-WildcardMatch -Patterns $allowed -Value $Permission) -and
            -not (Test-WildcardMatch -Patterns $excluded -Value $Permission)) {
            return $true
        }
    }
    return $false
}

function Get-PrincipalObjectId {
    param([object]$Account)

    if ($Account.user.type -eq 'user') {
        $user = Invoke-AzJson -Arguments @('ad', 'signed-in-user', 'show') -AllowFailure
        if ($user.ExitCode -eq 0) {
            return [string]$user.Value.id
        }
        Add-Check -Category 'Identity' -Name 'Resolve principal object ID' -Status 'WARN' `
            -Details "Could not query the signed-in user object ID: $(Get-ErrorSummary $user.Error)"
        return ''
    }

    $principal = Invoke-AzJson -Arguments @('ad', 'sp', 'show', '--id', [string]$Account.user.name) -AllowFailure
    if ($principal.ExitCode -eq 0) {
        return [string]$principal.Value.id
    }
    Add-Check -Category 'Identity' -Name 'Resolve principal object ID' -Status 'WARN' `
        -Details "Could not query service principal '$($Account.user.name)': $(Get-ErrorSummary $principal.Error)"
    return ''
}

function Get-RoleAssignments {
    param(
        [string]$PrincipalId,
        [string]$Scope,
        [switch]$IncludeGroups
    )

    $arguments = @(
        'role', 'assignment', 'list',
        '--assignee-object-id', $PrincipalId,
        '--scope', $Scope,
        '--include-inherited',
        '--fill-principal-name', 'false'
    )
    if ($IncludeGroups) {
        $arguments = @(
            'role', 'assignment', 'list',
            '--assignee', $PrincipalId,
            '--scope', $Scope,
            '--include-inherited',
            '--include-groups'
        )
    }

    return Invoke-AzJson -Arguments $arguments -AllowFailure
}

function Test-RoleAssignmentsForDataAction {
    param(
        [object[]]$Assignments,
        [string]$DataAction
    )

    foreach ($assignment in @($Assignments)) {
        $roleDefinitionId = [string]$assignment.roleDefinitionId
        $roleDefinitionName = Split-Path -Leaf $roleDefinitionId
        $definition = Invoke-AzJson -Arguments @('role', 'definition', 'list', '--name', $roleDefinitionName) -AllowFailure
        if ($definition.ExitCode -ne 0) {
            continue
        }

        foreach ($role in @($definition.Value)) {
            if (Test-EffectivePermission -Permissions @($role.permissions) -Permission $DataAction -DataAction) {
                return [pscustomobject]@{
                    HasAccess = $true
                    Role      = [string]$assignment.roleDefinitionName
                    Scope     = [string]$assignment.scope
                    Condition = [string]$assignment.condition
                }
            }
        }
    }

    return [pscustomobject]@{
        HasAccess = $false
        Role      = ''
        Scope     = ''
        Condition = ''
    }
}

Write-Host "`n=== Azure Storage RBAC readiness ===" -ForegroundColor Cyan
Write-Host "  Resource group : $rg"
Write-Host "  Container      : $ContainerName"
Write-Host "  Write test     : $(if ($TestWriteAccess) { 'enabled' } else { 'disabled (read-only)' })"

$accountResult = Invoke-AzJson -Arguments @('account', 'show')
$account = $accountResult.Value
$principalId = Get-PrincipalObjectId -Account $account
$principalDescription = "$($account.user.name) ($($account.user.type))"
Add-Check -Category 'Identity' -Name 'Azure CLI principal' -Status 'PASS' `
    -Details "$principalDescription; subscription $($account.name) ($($account.id))"

if (-not $StorageAccount) {
    $accountsResult = Invoke-AzJson -Arguments @('storage', 'account', 'list', '--resource-group', $rg)
    $taggedAccounts = @($accountsResult.Value | Where-Object { $_.tags.app -eq 'azurebench' })
    if ($taggedAccounts.Count -eq 0) {
        throw "No storage account tagged app=azurebench was found in resource group '$rg'. Pass -StorageAccount explicitly."
    }
    if ($taggedAccounts.Count -gt 1) {
        throw "Multiple storage accounts tagged app=azurebench were found in '$rg': $($taggedAccounts.name -join ', '). Pass -StorageAccount explicitly."
    }
    $StorageAccount = [string]$taggedAccounts[0].name
}

$storageResult = Invoke-AzJson -Arguments @(
    'storage', 'account', 'show',
    '--resource-group', $rg,
    '--name', $StorageAccount
)
$storage = $storageResult.Value
$storageScope = [string]$storage.id
$containerScope = "$storageScope/blobServices/default/containers/$ContainerName"

Write-Host "  Storage account: $StorageAccount"
Write-Host "  Principal      : $principalDescription"
if ($principalId) {
    Write-Host "  Principal ID   : $principalId"
}

if ($storage.allowSharedKeyAccess -eq $false) {
    Add-Check -Category 'Storage' -Name 'Shared Key access disabled' -Status 'PASS' `
        -Details 'allowSharedKeyAccess is explicitly false.'
}
else {
    $value = if ($null -eq $storage.allowSharedKeyAccess) { 'null' } else { [string]$storage.allowSharedKeyAccess }
    Add-Check -Category 'Storage' -Name 'Shared Key access disabled' -Status 'WARN' `
        -Details "allowSharedKeyAccess is $value; SFI-ID4.2.1 treats null or true as non-compliant."
}

$permissionsUrl = "https://management.azure.com$storageScope/providers/Microsoft.Authorization/permissions?api-version=2022-04-01"
$permissionsResult = Invoke-AzJson -Arguments @('rest', '--method', 'get', '--url', $permissionsUrl) -AllowFailure
if ($permissionsResult.ExitCode -eq 0) {
    $permissions = @($permissionsResult.Value.value)
    $canAssignRoles = Test-EffectivePermission -Permissions $permissions `
        -Permission 'Microsoft.Authorization/roleAssignments/write'
    $canUpdateStorage = Test-EffectivePermission -Permissions $permissions `
        -Permission 'Microsoft.Storage/storageAccounts/write'

    Add-Check -Category 'Management' -Name 'Create role assignments' `
        -Status $(if ($canAssignRoles) { 'PASS' } else { 'FAIL' }) `
        -Details $(if ($canAssignRoles) {
            'The active identity has Microsoft.Authorization/roleAssignments/write at the storage scope.'
        }
        else {
            'The active identity cannot create the VMSS Storage Blob Data Reader assignment at this scope.'
        })

    Add-Check -Category 'Management' -Name 'Update storage account' `
        -Status $(if ($canUpdateStorage) { 'PASS' } else { 'FAIL' }) `
        -Details $(if ($canUpdateStorage) {
            'The active identity has Microsoft.Storage/storageAccounts/write.'
        }
        else {
            'The active identity cannot set allowSharedKeyAccess on this account.'
        })
}
else {
    $errorSummary = Get-ErrorSummary $permissionsResult.Error
    Add-Check -Category 'Management' -Name 'Query effective permissions' -Status 'FAIL' `
        -Details $errorSummary
}

if ($principalId) {
    $assignmentResult = Get-RoleAssignments -PrincipalId $principalId -Scope $storageScope -IncludeGroups
    if ($assignmentResult.ExitCode -ne 0) {
        $assignmentResult = Get-RoleAssignments -PrincipalId $principalId -Scope $storageScope
    }

    if ($assignmentResult.ExitCode -eq 0) {
        $roleNames = @($assignmentResult.Value | ForEach-Object { $_.roleDefinitionName } |
            Where-Object { $_ } | Sort-Object -Unique)
        $roleSummary = if ($roleNames.Count -gt 0) { $roleNames -join ', ' } else { 'No role assignments returned.' }
        Add-Check -Category 'Identity' -Name 'Effective role assignments' -Status 'INFO' `
            -Details $roleSummary
    }
    else {
        Add-Check -Category 'Identity' -Name 'Effective role assignments' -Status 'WARN' `
            -Details (Get-ErrorSummary $assignmentResult.Error)
    }
}

$readResult = Invoke-AzCommand -Arguments @(
    'storage', 'blob', 'list',
    '--account-name', $StorageAccount,
    '--container-name', $ContainerName,
    '--auth-mode', 'login',
    '--num-results', '1',
    '--output', 'none'
) -AllowFailure

Add-Check -Category 'Data plane' -Name 'Entra blob read/list' `
    -Status $(if ($readResult.ExitCode -eq 0) { 'PASS' } else { 'FAIL' }) `
    -Details $(if ($readResult.ExitCode -eq 0) {
        "The active identity can access '$ContainerName' with --auth-mode login."
    }
    else {
        Get-ErrorSummary $(if ($readResult.Error) { $readResult.Error } else { $readResult.Output })
    })

if ($TestWriteAccess) {
    $temporaryFile = Join-Path ([System.IO.Path]::GetTempPath()) "azurebench-rbac-$([guid]::NewGuid().ToString('N')).txt"
    $blobName = "rbac-validation/$([guid]::NewGuid().ToString('N')).txt"
    $uploaded = $false
    try {
        Set-Content -LiteralPath $temporaryFile -Value 'Azure benchmark RBAC validation' -NoNewline -Encoding ascii
        $uploadResult = Invoke-AzCommand -Arguments @(
            'storage', 'blob', 'upload',
            '--account-name', $StorageAccount,
            '--container-name', $ContainerName,
            '--name', $blobName,
            '--file', $temporaryFile,
            '--auth-mode', 'login',
            '--overwrite',
            '--output', 'none'
        ) -AllowFailure

        if ($uploadResult.ExitCode -eq 0) {
            $uploaded = $true
            Add-Check -Category 'Data plane' -Name 'Entra blob write' -Status 'PASS' `
                -Details "Uploaded temporary blob '$blobName'."
        }
        else {
            Add-Check -Category 'Data plane' -Name 'Entra blob write' -Status 'FAIL' `
                -Details (Get-ErrorSummary $(if ($uploadResult.Error) { $uploadResult.Error } else { $uploadResult.Output }))
        }
    }
    finally {
        if ($uploaded) {
            $deleteResult = Invoke-AzCommand -Arguments @(
                'storage', 'blob', 'delete',
                '--account-name', $StorageAccount,
                '--container-name', $ContainerName,
                '--name', $blobName,
                '--auth-mode', 'login',
                '--output', 'none'
            ) -AllowFailure
            Add-Check -Category 'Data plane' -Name 'Temporary blob cleanup' `
                -Status $(if ($deleteResult.ExitCode -eq 0) { 'PASS' } else { 'FAIL' }) `
                -Details $(if ($deleteResult.ExitCode -eq 0) {
                    "Deleted temporary blob '$blobName'."
                }
                else {
                    "Could not delete '$blobName': $(Get-ErrorSummary $(if ($deleteResult.Error) { $deleteResult.Error } else { $deleteResult.Output }))"
                })
        }
        if (Test-Path $temporaryFile) {
            Remove-Item -LiteralPath $temporaryFile -Force
        }
    }
}
else {
    Add-Check -Category 'Data plane' -Name 'Entra blob write' -Status 'INFO' `
        -Details 'Not tested. Pass -TestWriteAccess to upload and delete a temporary blob.'
}

$vmssResources = @()
if ($VmssName) {
    foreach ($name in $VmssName) {
        $vmssResult = Invoke-AzJson -Arguments @(
            'vmss', 'show',
            '--resource-group', $rg,
            '--name', $name
        ) -AllowFailure
        if ($vmssResult.ExitCode -ne 0) {
            Add-Check -Category 'VMSS' -Name "$name identity" -Status 'FAIL' `
                -Details (Get-ErrorSummary $vmssResult.Error)
            continue
        }
        $vmssResources += $vmssResult.Value
    }
}
else {
    $vmssList = Invoke-AzJson -Arguments @('vmss', 'list', '--resource-group', $rg)
    $vmssResources = @($vmssList.Value)
}

if ($vmssResources.Count -eq 0) {
    Add-Check -Category 'VMSS' -Name 'Managed identities' -Status 'INFO' `
        -Details "No VMSS resources were found in '$rg'."
}
else {
    $blobReadAction = 'Microsoft.Storage/storageAccounts/blobServices/containers/blobs/read'
    foreach ($vmss in $vmssResources) {
        $name = [string]$vmss.name
        $vmssPrincipalId = [string]$vmss.identity.principalId
        if ([string]::IsNullOrWhiteSpace($vmssPrincipalId)) {
            Add-Check -Category 'VMSS' -Name "$name blob read" -Status 'FAIL' `
                -Details 'The VMSS has no system-assigned managed identity.'
            continue
        }

        $vmssAssignments = Get-RoleAssignments -PrincipalId $vmssPrincipalId -Scope $containerScope
        if ($vmssAssignments.ExitCode -ne 0) {
            Add-Check -Category 'VMSS' -Name "$name blob read" -Status 'FAIL' `
                -Details (Get-ErrorSummary $vmssAssignments.Error)
            continue
        }

        $access = Test-RoleAssignmentsForDataAction -Assignments @($vmssAssignments.Value) `
            -DataAction $blobReadAction
        if ($access.HasAccess) {
            $conditionDetail = if ($access.Condition) { " Condition: $($access.Condition)" } else { '' }
            Add-Check -Category 'VMSS' -Name "$name blob read" `
                -Status $(if ($access.Condition) { 'WARN' } else { 'PASS' }) `
                -Details "Principal $vmssPrincipalId has '$($access.Role)' at '$($access.Scope)'.$conditionDetail"
        }
        else {
            Add-Check -Category 'VMSS' -Name "$name blob read" -Status 'FAIL' `
                -Details "Principal $vmssPrincipalId has no effective blob-read role at '$containerScope' or a parent scope."
        }
    }
}

Write-Host "`n=== Validation summary ===" -ForegroundColor Cyan
$checks | Format-Table -AutoSize -Wrap

$failures = @($checks | Where-Object { $_.Status -eq 'FAIL' })
$warnings = @($checks | Where-Object { $_.Status -eq 'WARN' })
Write-Host ""
if ($failures.Count -gt 0) {
    Write-Host "Overall: NOT READY ($($failures.Count) failed check(s), $($warnings.Count) warning(s))" -ForegroundColor Red
    exit 1
}
if ($warnings.Count -gt 0) {
    Write-Host "Overall: READY WITH WARNINGS ($($warnings.Count) warning(s))" -ForegroundColor Yellow
    exit 0
}

Write-Host "Overall: READY" -ForegroundColor Green
