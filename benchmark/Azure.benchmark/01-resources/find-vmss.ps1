#!/usr/bin/env pwsh
[CmdletBinding()]
<#
.SYNOPSIS
    Finds Azure regions where a VMSS SKU is available and reports vCPU quota.

.DESCRIPTION
    Searches the Azure catalog for regions where an exact virtual machine SKU
    is offered and shows whether it is available to the selected subscription.
    For each region, reports the SKU-family quota, total regional vCPU quota,
    and an estimate of how many additional instances can be created before
    either quota is exhausted.

    Requires Azure CLI and an authenticated session from az login.

.PARAMETER Sku
    Full or partial VM size/SKU name, such as Standard_D32ds_v5 or Standard_NV6.
    An exact match is preferred; otherwise, all partial matches are reported.

.PARAMETER Region
    Optional Azure region name or names used to narrow the results.

.PARAMETER Subscription
    Optional Azure subscription name or ID. The active subscription is used
    when this parameter is omitted.

.EXAMPLE
    .\find-vmss.ps1 -Sku Standard_D32ds_v5

.EXAMPLE
    .\find-vmss.ps1 -Sku Standard_D32ds_v5 -Region eastus,eastus2

.EXAMPLE
    .\find-vmss.ps1 -Sku Standard_D32ds_v5 -Subscription <subscription-id>
#>

param(
    [Parameter(Mandatory)]
    [ValidateNotNullOrEmpty()]
    [string]$Sku,

    [string[]]$Region,

    [string]$Subscription
)

$ErrorActionPreference = 'Stop'

function Invoke-AzJson {
    param(
        [Parameter(Mandatory)]
        [string[]]$Arguments
    )

    $output = & az @Arguments --output json --only-show-errors 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "Azure CLI command failed: az $($Arguments -join ' ')"
    }
    if ([string]::IsNullOrWhiteSpace(($output -join ''))) {
        throw "Azure CLI returned no data: az $($Arguments -join ' ')"
    }

    return $output | ConvertFrom-Json
}

if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    throw "Azure CLI was not found. Install it and run 'az login' before using this script."
}

$subscriptionArgs = if ($Subscription) { @('--subscription', $Subscription) } else { @() }
$account = Invoke-AzJson -Arguments (@('account', 'show') + $subscriptionArgs)
$publicRegions = @(
    Invoke-AzJson -Arguments (@('account', 'list-locations') + $subscriptionArgs) |
        Where-Object {
            $_.type -eq 'Region' -and
            -not [string]::IsNullOrWhiteSpace($_.metadata.physicalLocation)
        } |
        Select-Object -ExpandProperty name -Unique
)

$skuArgs = @(
    'vm', 'list-skus',
    '--resource-type', 'virtualMachines',
    '--size', $Sku,
    '--all', 'true'
) + $subscriptionArgs

$candidateRecords = @(Invoke-AzJson -Arguments $skuArgs)
$skuRecords = @($candidateRecords | Where-Object { $_.name -ieq $Sku })

if ($skuRecords.Count -eq 0) {
    $skuRecords = $candidateRecords
    if ($skuRecords.Count -eq 0) {
        throw "No Azure VM SKU matched '$Sku'."
    }
}

$regionRecords = @(
    $skuRecords |
        ForEach-Object {
            $record = $_
            $vCpuCapability = $record.capabilities |
                Where-Object { $_.name -eq 'vCPUs' } |
                Select-Object -First 1
            if (-not $record.family -or -not $vCpuCapability) {
                throw "Azure did not return the SKU family or vCPU count for '$($record.name)'."
            }

            foreach ($location in $record.locations) {
                $isRestricted = @(
                    $record.restrictions |
                        Where-Object {
                            $_.type -ieq 'Location' -and (
                                $_.values -icontains $location -or
                                $_.restrictionInfo.locations -icontains $location
                            )
                        }
                ).Count -gt 0

                [PSCustomObject]@{
                    Sku          = $record.name
                    Family       = $record.family
                    VCpus        = [int]$vCpuCapability.value
                    Region       = $location
                    Availability = if ($isRestricted) { 'Restricted' } else { 'Available' }
                }
            }
        } |
        Group-Object Sku, Region |
        ForEach-Object {
            $first = $_.Group[0]
            [PSCustomObject]@{
                Sku          = $first.Sku
                Family       = $first.Family
                VCpus        = $first.VCpus
                Region       = $first.Region
                Availability = if ($_.Group.Availability -contains 'Available') {
                    'Available'
                }
                else {
                    'Restricted'
                }
            }
        } |
        Where-Object { $publicRegions -icontains $_.Region } |
        Sort-Object Region
)

if ($Region) {
    $requestedRegions = @(
        $Region |
            ForEach-Object { $_ -split ',' } |
            ForEach-Object { $_.Trim() } |
            Where-Object { $_ } |
            Sort-Object -Unique
    )
    $regionRecords = @(
        $regionRecords |
            Where-Object { $requestedRegions -icontains $_.Region }
    )

    $notOfferedRegions = @(
        $requestedRegions |
            Where-Object { $regionRecords.Region -inotcontains $_ }
    )
    foreach ($notOfferedRegion in $notOfferedRegions) {
        Write-Warning "SKU '$Sku' is not offered in region '$notOfferedRegion'."
    }
}

if ($regionRecords.Count -eq 0) {
    throw "No matching regions were found for SKU '$Sku'."
}

Write-Host "Subscription : $($account.name) ($($account.id))"
Write-Host "Search       : $Sku"
Write-Host "Matches      : $(($regionRecords.Sku | Sort-Object -Unique).Count)"
Write-Host "Offerings    : $($regionRecords.Count) SKU/region combinations"
Write-Host ""

$usageByRegion = @{}
$results = foreach ($regionRecord in $regionRecords) {
    $location = $regionRecord.Region
    if (-not $usageByRegion.ContainsKey($location)) {
        Write-Verbose "Reading quota for $location"
        try {
            $usageByRegion[$location] = @(
                Invoke-AzJson -Arguments (
                    @('vm', 'list-usage', '--location', $location) + $subscriptionArgs
                )
            )
        }
        catch {
            Write-Warning "Quota is unavailable for region '$location': $($_.Exception.Message)"
            $usageByRegion[$location] = @()
        }
    }
    $usage = $usageByRegion[$location]

    $familyUsage = $usage |
        Where-Object { $_.name.value -ieq $regionRecord.Family } |
        Select-Object -First 1
    $regionalUsage = $usage |
        Where-Object { $_.name.value -ieq 'cores' } |
        Select-Object -First 1

    $familyRemaining = if ($familyUsage) {
        [Math]::Max(0, [int64]$familyUsage.limit - [int64]$familyUsage.currentValue)
    }
    else {
        $null
    }
    $regionalRemaining = if ($regionalUsage) {
        [Math]::Max(0, [int64]$regionalUsage.limit - [int64]$regionalUsage.currentValue)
    }
    else {
        $null
    }

    $maxAdditionalInstances = if (
        $regionRecord.Availability -eq 'Available' -and
        $null -ne $familyRemaining -and
        $null -ne $regionalRemaining
    ) {
        [int64][Math]::Floor([Math]::Min($familyRemaining, $regionalRemaining) / $regionRecord.VCpus)
    }
    else {
        $null
    }
    $status = if ($regionRecord.Availability -eq 'Restricted') {
        'Restricted'
    }
    elseif ($familyUsage -and [int64]$familyUsage.limit -eq 0) {
        'No quota'
    }
    else {
        'Available'
    }

    [PSCustomObject]@{
        Sku           = $regionRecord.Sku
        VCpus         = $regionRecord.VCpus
        Region        = $location
        Status        = $status
        FamilyQuota   = if ($familyUsage) { "$($familyUsage.currentValue)/$($familyUsage.limit)" } else { 'N/A' }
        RegionalQuota = if ($regionalUsage) { "$($regionalUsage.currentValue)/$($regionalUsage.limit)" } else { 'N/A' }
        MaxInstances  = $maxAdditionalInstances
    }
}

$results |
    Sort-Object -Property Sku, @{ Expression = 'MaxInstances'; Descending = $true }, Region |
    Format-Table -AutoSize
