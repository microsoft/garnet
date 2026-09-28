#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Lists eth1 (accelerated NIC) IPs for all instances in the local VMSS.
    Runs from inside a VM using its managed identity to query Azure API.

.EXAMPLE
    list-ips.ps1
    list-ips.ps1 -NicName fs72vmss2-acc-nic
    list-ips.ps1 -Help
#>
param(
    [string]$NicName,
    [switch]$Help
)

if ($Help) {
    Write-Host "Usage: list-ips.ps1 [-NicName <nic-name>]"
    Write-Host ""
    Write-Host "Lists eth1 (accelerated NIC) private IPs for all VMSS instances."
    Write-Host "Queries Azure API using the VM's managed identity (no login required)."
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  -NicName   NIC name to query (default: auto-detect the non-primary/acc NIC)"
    Write-Host "  -Help      Show this help message"
    return
}

$ErrorActionPreference = "Stop"

# Get instance metadata
$metadataUri = "http://169.254.169.254/metadata/instance?api-version=2021-02-01"
$metadata = Invoke-RestMethod -Uri $metadataUri -Headers @{ Metadata = "true" } -TimeoutSec 5

$subscriptionId = $metadata.compute.subscriptionId
$resourceGroup = $metadata.compute.resourceGroupName
$vmssName = $metadata.compute.vmScaleSetName

if (-not $vmssName) {
    throw "ERROR: This VM does not appear to be part of a VMSS."
}

Write-Host "VMSS: $vmssName (RG: $resourceGroup)" -ForegroundColor Cyan

# Get access token using managed identity
$tokenUri = "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://management.azure.com/"
$tokenResponse = Invoke-RestMethod -Uri $tokenUri -Headers @{ Metadata = "true" } -TimeoutSec 5
$token = $tokenResponse.access_token

$headers = @{
    Authorization  = "Bearer $token"
    "Content-Type" = "application/json"
}

# Follow ARM paging (nextLink) so we don't stop at the first ~100 results.
function Get-ArmPagedValues {
    param([string]$Uri, [hashtable]$Headers, [int]$TimeoutSec = 30)
    $all = @()
    $next = $Uri
    while ($next) {
        $page = Invoke-RestMethod -Uri $next -Headers $Headers -TimeoutSec $TimeoutSec
        if ($page.value) { $all += $page.value }
        $next = $page.nextLink
    }
    return $all
}

# List all NICs across the whole VMSS in one paged call (avoids a per-instance
# REST round-trip, which is unusable at thousands of instances).
$nicsUri = "https://management.azure.com/subscriptions/$subscriptionId/resourceGroups/$resourceGroup/providers/Microsoft.Compute/virtualMachineScaleSets/$vmssName/networkInterfaces?api-version=2022-11-01"
$nics = Get-ArmPagedValues -Uri $nicsUri -Headers $headers -TimeoutSec 30

$results = @()
foreach ($nic in $nics) {
    $isAccNic = $false
    if ($NicName) {
        $isAccNic = $nic.name -eq $NicName
    } else {
        # Auto-detect: pick the NIC that is not primary or has an acc-style name.
        $isAccNic = ($nic.properties.primary -eq $false) -or ($nic.name -like "*acc*")
    }

    if ($isAccNic) {
        # NIC id path: .../virtualMachines/<instanceId>/networkInterfaces/<nic>
        $instanceId = if ($nic.id -match '/virtualMachines/(\d+)/') { $matches[1] } else { '' }
        $ip = $nic.properties.ipConfigurations[0].properties.privateIPAddress
        $results += [PSCustomObject]@{
            Instance = $instanceId
            Name     = $nic.name
            IP       = $ip
        }
    }
}

# Sort by IP for readability
$results = $results | Sort-Object { [version]($_.IP -replace '(\d+)\.(\d+)\.(\d+)\.(\d+)', '$1.$2.$3.$4') }

Write-Host ""
Write-Host "Accelerated NIC IPs:" -ForegroundColor Yellow
$results | Format-Table -AutoSize

# Also output just the IPs for easy scripting
Write-Host "IPs only:" -ForegroundColor DarkGray
$results | ForEach-Object { Write-Host $_.IP }
