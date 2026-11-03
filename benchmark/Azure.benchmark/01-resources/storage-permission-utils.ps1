function Invoke-StoragePermissionAz {
    param([string[]]$Arguments, [switch]$AllowFailure)

    $errorFile = [IO.Path]::GetTempFileName()
    try {
        $output = & az @Arguments 2> $errorFile
        $exitCode = $LASTEXITCODE
        $errorText = [string](Get-Content -LiteralPath $errorFile -Raw)
        $outputText = (@($output) -join [Environment]::NewLine).Trim()
        if ($exitCode -ne 0 -and -not $AllowFailure) {
            throw "Azure CLI failed: az $($Arguments -join ' ')`n$errorText`n$outputText"
        }
        return [pscustomobject]@{ ExitCode = $exitCode; Output = $outputText; Error = ([string]$errorText).Trim() }
    } finally {
        Remove-Item -LiteralPath $errorFile -Force
    }
}

function Get-StoragePermissionJson {
    param([string[]]$Arguments)

    $result = Invoke-StoragePermissionAz -Arguments ($Arguments + @('--output', 'json'))
    return ($result.Output | ConvertFrom-Json -ErrorAction Stop)
}

function Write-ManualStorageElevationInstructions {
    param([object]$Account, [string]$ResourceGroup)

    Write-Host "`nTo continue:" -ForegroundColor Yellow
    Write-Host '  1. Open Azure Portal > Privileged Identity Management (PIM) > My roles > Azure resources > Eligible assignments.'
    if ($Account) {
        Write-Host "  2. Activate Owner for resource group '$ResourceGroup' in subscription '$($Account.name)' ($($Account.id))."
    } else {
        Write-Host "  2. Activate Owner for resource group '$ResourceGroup'."
    }
    Write-Host '  3. Complete any MFA/approval, wait until the role appears under Active assignments, then rerun your command.'
    Write-Host 'If Owner is not listed as eligible, ask your administrator for access. These scripts do not activate PIM automatically.'
    Write-Host "`nOptional: check your account, roles, and storage access:"
    Write-Host "  pwsh .\01-resources\validate-storage-rbac.ps1 -rg $ResourceGroup"
    Write-Host 'The validator is diagnostic; rerunning your original command checks Owner access on the resource group again.'
    if ($Account) {
        Write-Host "`nOnly if Azure CLI asks you to sign in again:"
        Write-Host "  az login --tenant $($Account.tenantId)"
        Write-Host "  az account set --subscription $($Account.id)"
    } else {
        Write-Host "`nIf Azure CLI authentication failed, run az login and select the intended subscription."
    }
}

function Get-ResourceGroupOwnerContext {
    param([string]$ResourceGroup)

    $account = $null
    try {
        $account = Get-StoragePermissionJson -Arguments @('account', 'show')
        if ($account.user.type -ne 'user') { throw 'This workflow requires a signed-in user account, not a service principal.' }
        $principal = Get-StoragePermissionJson -Arguments @('ad', 'signed-in-user', 'show')
        if (-not $principal.id) { throw 'Azure CLI did not return the signed-in user object ID.' }
        $group = Get-StoragePermissionJson -Arguments @('group', 'show', '--name', $ResourceGroup)
        if (-not $group.id) { throw "Could not resolve resource group '$ResourceGroup'." }
        $scope = $group.id
        Write-Host "`n=== Resource group Owner permission check ===" -ForegroundColor Cyan
        Write-Host "  Account      : $($account.user.name)"
        Write-Host "  Subscription : $($account.name) ($($account.id))"
        Write-Host "  Resource group: $ResourceGroup"
        $assignments = @(Get-StoragePermissionJson -Arguments @(
            'role', 'assignment', 'list', '--assignee', $principal.id,
            '--include-groups', '--include-inherited', '--scope', $scope
        ))
        Write-Host "  Roles        : $(($assignments.roleDefinitionName | Sort-Object -Unique) -join ', ')"
        $owners = @($assignments | Where-Object {
            ($_.scope -eq $scope -or $scope.StartsWith("$($_.scope)/", [StringComparison]::OrdinalIgnoreCase)) -and
            -not $_.condition -and
            ($_.roleDefinitionId -split '/')[-1] -eq '8e3af657-a8ff-443c-a75c-2fe8c4bcb635'
        })
        if (-not $owners.Count) {
            Write-Host "`nCannot continue: Owner access on resource group '$ResourceGroup' is not active, or its assignment has restrictions." -ForegroundColor Yellow
            Write-Host 'This step assigns storage access and requires unrestricted Owner access on the resource group. Your PIM activation may have expired or may not have been activated yet.'
            Write-Host 'No changes were made by this permission check.'
            Write-ManualStorageElevationInstructions -Account $account -ResourceGroup $ResourceGroup
            return $null
        }
        Write-Host 'Active Owner access on the resource group found. Azure still authorizes each operation; propagation or deny assignments can cause failures.' -ForegroundColor Green
        return [pscustomobject]@{ Account = $account; PrincipalId = $principal.id; ResourceGroupScope = $scope }
    } catch {
        [Console]::Error.WriteLine("Permission check failed: $($_.Exception.Message)")
        Write-ManualStorageElevationInstructions -Account $account -ResourceGroup $ResourceGroup
        return $null
    }
}
