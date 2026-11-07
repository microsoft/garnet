#!/usr/bin/env pwsh
[CmdletBinding()]
<#
.SYNOPSIS
    Grants the signed-in deployer Storage Blob Data Owner on the tools container.
.DESCRIPTION
    Requires active Owner access on the resource group. Activate it manually
    through Azure Portal PIM before running this script. No automatic elevation occurs.
    Verifies publishing access by uploading and deleting a temporary blob.
.PARAMETER rg
    Resource group containing the benchmark storage account.
.PARAMETER StorageAccount
    Storage account name; otherwise discovers the account tagged app=azurebench.
.PARAMETER ContainerName
    Tools container name (default: tools).
.EXAMPLE
    .\grant-perms.ps1 -rg my-benchmark
#>
param(
    [Alias('ResourceGroup')]
    [string]$rg,
    [string]$StorageAccount,
    [string]$ContainerName = 'tools',
    [switch]$Help
)

if ($Help -or -not $rg) {
    Write-Host 'Usage: grant-perms.ps1 -rg <resource-group> [-StorageAccount <name>] [-ContainerName tools]'
    Write-Host 'Manually activate Owner on the resource group in Azure Portal PIM first. Grants persistent Storage Blob Data Owner on the tools container to the signed-in user.'
    return
}

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'storage-permission-utils.ps1')
$context = Get-ResourceGroupOwnerContext -ResourceGroup $rg
if (-not $context) { exit 1 }

try {
    if (-not $StorageAccount) {
        $accounts = @(Get-StoragePermissionJson -Arguments @(
            'storage', 'account', 'list', '--resource-group', $rg,
            '--query', "[?tags.app=='azurebench']"
        ))
        if ($accounts.Count -ne 1) {
            throw "Expected one storage account tagged app=azurebench in '$rg'; found $($accounts.Count). Specify -StorageAccount."
        }
        $StorageAccount = $accounts[0].name
    }
    $storage = Get-StoragePermissionJson -Arguments @('storage', 'account', 'show', '-g', $rg, '-n', $StorageAccount)
    $scope = "$($storage.id)/blobServices/default/containers/$ContainerName"
    $null = Get-StoragePermissionJson -Arguments @(
        'rest', '--method', 'get', '--url', "https://management.azure.com${scope}?api-version=2023-05-01"
    )
    $assignments = @(Get-StoragePermissionJson -Arguments @(
        'role', 'assignment', 'list', '--assignee', $context.PrincipalId,
        '--include-groups', '--include-inherited', '--scope', $scope
    ))
    $dataRoles = @($assignments | Where-Object {
        -not $_.condition -and ($_.roleDefinitionId -split '/')[-1] -in @(
            'b7e6dc6d-f1e8-4753-8033-0f276bb0955b', # Storage Blob Data Owner
            'ba92f5b4-2d11-453d-a403-e96b0029c9fe'  # Storage Blob Data Contributor
        )
    })
    if ($dataRoles.Count) {
        Write-Host 'An existing unrestricted blob data Owner/Contributor assignment covers publishing; no duplicate grant needed.'
    } else {
        Write-Host "Granting persistent Storage Blob Data Owner to $($context.Account.user.name) at $scope"
        $null = Get-StoragePermissionJson -Arguments @(
            'role', 'assignment', 'create', '--assignee-object-id', $context.PrincipalId,
            '--assignee-principal-type', 'User', '--role', 'Storage Blob Data Owner', '--scope', $scope
        )
    }

    $file = [IO.Path]::GetTempFileName()
    $blob = "rbac-validation/$([guid]::NewGuid().ToString('N')).txt"
    try {
        Set-Content -LiteralPath $file -Value 'Azure benchmark publisher access validation' -Encoding ascii
        $deadline = [DateTime]::UtcNow.AddMinutes(10)
        do {
            $upload = Invoke-StoragePermissionAz -AllowFailure -Arguments @(
                'storage', 'blob', 'upload', '--account-name', $StorageAccount,
                '--container-name', $ContainerName, '--name', $blob, '--file', $file,
                '--auth-mode', 'login', '--overwrite', '--output', 'none'
            )
            if ($upload.ExitCode -eq 0) { break }
            if ($upload.Error -notmatch 'AuthorizationPermissionMismatch|AuthorizationFailure|required permissions') {
                throw "Publisher access validation failed: $($upload.Error)`n$($upload.Output)"
            }
            Write-Host "Waiting for blob data permissions to propagate: $($upload.Error)" -ForegroundColor Yellow
            if ([DateTime]::UtcNow -ge $deadline) {
                throw 'Publisher access could not be verified within ten minutes. The role assignment is not rolled back; rerun the validator with -TestWriteAccess.'
            }
            Start-Sleep -Seconds 15
        } while ($true)
        $null = Invoke-StoragePermissionAz -Arguments @(
            'storage', 'blob', 'delete', '--account-name', $StorageAccount,
            '--container-name', $ContainerName, '--name', $blob, '--auth-mode', 'login', '--output', 'none'
        )
        Write-Host "Publisher upload/delete access verified on '$StorageAccount/$ContainerName'. You can now run publish-tools without PIM while this data-role assignment remains active." -ForegroundColor Green
    } finally {
        Remove-Item -LiteralPath $file -Force
    }
} catch {
    [Console]::Error.WriteLine("Storage permission setup failed: $($_.Exception.Message)")
    Write-Host 'If a validation blob was uploaded but deletion failed, remove the named rbac-validation blob after resolving access.'
    Write-ManualStorageElevationInstructions -Account $context.Account -ResourceGroup $rg
    exit 1
}
