<#
.SYNOPSIS
    Deploys shared resource-group infrastructure (NSG, VNet, Proximity Group, Storage account, Key Vault) and generates vmss-parameters.json.

.DESCRIPTION
    Step 1: Deploys network/network.bicep to create the shared network resources.
    Step 2: Deploys storage/storage.bicep to create the shared storage account (blob container for tools tarball delivery).
    Step 3: Deploys security/keyvault.bicep to create the shared Key Vault, copies public keys from the manifest basePath
            into security/, and uploads the inter-node VMSS private key as secret 'vmss-ssh-private'.
    Step 4: Publishes a read-only, policy-bound SAS URL for the tools tarball blob into the Key Vault (secret 'tools-sas-url')
            so the VMSS can pull tools.tar.gz at boot without a Storage Blob Data Reader role assignment (no RBAC required).
    Step 5: Reads network deployment outputs and generates vmss-parameters.json for subsequent VMSS deployments.

    The deploy action is idempotent: it discovers existing network, storage, and Key Vault resources and skips
    (re)deploying them. The storage account and Key Vault names are discovered at deploy time by management scripts
    (via their 'app=azurebench' tag / resource-group lookup), so they are NOT persisted to vmss-parameters.json.

    SSH key definitions are read from security/manifest.json (basePath, userKeys, vmKeys).

.PARAMETER rg
    Azure resource group name.

.PARAMETER Action
    deploy      - Deploy shared resources (network, storage, Key Vault + keys, tools SAS) and generate vmss-parameters.json (default)
    stage       - Query existing network resources in the resource group and generate vmss-parameters.json (no deployment)
    refresh-sas - Regenerate the tools tarball SAS and refresh the 'tools-sas-url' Key Vault secret (renews expiry)

.EXAMPLE
    # Deploy shared resources using default resource group (vazois-garnet)
    .\deploy-common-resources.ps1

    # Deploy shared resources to a specific resource group
    .\deploy-common-resources.ps1 -rg my-resource-group

    # Generate vmss-parameters.json from existing resources (no deployment)
    .\deploy-common-resources.ps1 -Action stage -rg my-resource-group

    # Deploy to a specific resource group with a custom deployment name
    .\deploy-common-resources.ps1 -rg my-resource-group -Action deploy -DeploymentName my-deploy
#>

param(
    [Alias('ResourceGroup')]
    [string]$rg = 'vazois-garnet',

    [Alias('Location')]
    [string]$Region = '',

    [ValidateSet('deploy', 'stage', 'refresh-sas')]
    [string]$Action = 'deploy',

    [string]$DeploymentName = 'network-deploy',

    [string]$ContainerName = 'tools',

    [string]$ToolsBlobName = 'tools.tar.gz',

    [string]$SasPolicyName = 'vmss-tools-read',

    [string]$SasSecretName = 'tools-sas-url',

    [int]$SasExpiryDays = 365,

    [switch]$Help
)

if ($Help) {
    Write-Host "Usage: deploy-common-resources.ps1 [options]" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Deploys shared resource-group infrastructure (NSG, VNet, Proximity Group, Storage account, Key Vault) and generates vmss-parameters.json."
    Write-Host ""
    Write-Host "Parameters:"
    Write-Host "  -rg <name>              Resource group name (default: vazois-garnet)"
    Write-Host "  -Region <name>          Azure region (default: resource group location)"
    Write-Host "  -Action <action>        Action to perform (default: deploy)"
    Write-Host "                          deploy  - Deploy shared resources (network, storage, Key Vault + keys, tools SAS)"
    Write-Host "                                    and generate parameters (idempotent: skips resources that already exist)"
    Write-Host "                          stage   - Query existing network resources and generate parameters (no deployment)"
    Write-Host "                          refresh-sas - Regenerate the tools tarball SAS and refresh the 'tools-sas-url' Key Vault secret"
    Write-Host "  -SasExpiryDays <n>      Stored access policy / SAS expiry in days (default: 365)"
    Write-Host "  -DeploymentName <name>  Deployment name (default: network-deploy)"
    Write-Host "  -Help                   Show this help message"
    Write-Host ""
    Write-Host "Examples:"
    Write-Host "  .\deploy-common-resources.ps1"
    Write-Host "  .\deploy-common-resources.ps1 -rg my-rg -Action deploy"
    Write-Host "  .\deploy-common-resources.ps1 -rg my-rg -Action stage"
    Write-Host ""
    Write-Host "For detailed help: Get-Help .\deploy-common-resources.ps1 -Detailed"
    Write-Host ""
    return
}

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$networkDir = Join-Path $scriptDir 'network'
$storageDir = Join-Path $scriptDir 'storage'
$repoRoot = Split-Path -Parent $scriptDir
$vmssParamsFile = Join-Path $repoRoot '02-platform\vmss-parameters.json'
$securityDir = Join-Path $scriptDir 'security'
$manifestFile = Join-Path $securityDir 'manifest.json'
. (Join-Path $securityDir 'ssh-key-utils.ps1')

function Write-VmssParams {
    param(
        [string]$Location, [string]$NsgId, [string]$VnetName,
        [string]$SubnetName, [string]$AccSubnetName, [string]$ProximityId
    )

    Write-Host "  nsgId         : $NsgId"
    Write-Host "  vnetName      : $VnetName"
    Write-Host "  subnetName    : $SubnetName"
    Write-Host "  accSubnetName : $AccSubnetName"
    Write-Host "  proximityId   : $ProximityId"
    Write-Host "  location      : $Location"

    $vmssParams = @{
        '$schema'      = 'https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#'
        contentVersion = '1.0.0.0'
        parameters     = @{
            location      = @{ value = $Location }
            subnetName    = @{ value = $SubnetName }
            accSubnetName = @{ value = $AccSubnetName }
            nsgId         = @{ value = $NsgId }
            proximityId   = @{ value = $ProximityId }
            vnetName      = @{ value = $VnetName }
        }
    }

    $vmssParams | ConvertTo-Json -Depth 4 | Set-Content -Path $vmssParamsFile -Encoding utf8

    Write-Host "`n=== Generated $vmssParamsFile ===" -ForegroundColor Green
    Get-Content $vmssParamsFile
    Write-Host ""
}

# Resolves the deployment region: -Region argument takes precedence, then an
# optional fallback location (e.g. from an existing-resource inventory, which
# avoids an extra az call), then the resource group's own location.
function Resolve-Region {
    param([string]$ResourceGroup, [string]$Region, [string]$FallbackLocation = '')
    if ($Region) {
        Write-Host "  Region         : $Region (from -Region)"
        return $Region
    }
    if ($FallbackLocation) {
        Write-Host "  Region         : $FallbackLocation (from existing resources)"
        return $FallbackLocation
    }
    $Region = (az group show --name $ResourceGroup --query location -o tsv)
    if ($LASTEXITCODE -ne 0 -or -not $Region) {
        Write-Error "Could not determine region for resource group '$ResourceGroup'. Pass -Region explicitly."
        exit 1
    }
    Write-Host "  Region         : $Region (from resource group)"
    return $Region
}

# Deploys the shared storage account (blob container for tools tarball delivery).
# The account name is derived deterministically from the resource group id in
# storage/storage.bicep; management scripts discover it later by its 'app=azurebench'
# tag, so it is intentionally NOT written to vmss-parameters.json.
function Deploy-Storage {
    param([string]$ResourceGroup, [string]$Region, [string]$StorageDir)

    Write-Host "`n=== Deploying storage account ===" -ForegroundColor Cyan
    Write-Host "  Resource Group : $ResourceGroup"
    Write-Host "  Template       : storage/storage.bicep"
    Write-Host "  Region         : $Region"
    Write-Host ""

    az deployment group create `
        --resource-group $ResourceGroup `
        --name 'storage-deploy' `
        --template-file (Join-Path $StorageDir 'storage.bicep') `
        --parameters location=$Region `
        --output none

    if ($LASTEXITCODE -ne 0) {
        Write-Error "Storage deployment failed."
        exit 1
    }

    $stName = az deployment group show `
        --resource-group $ResourceGroup `
        --name 'storage-deploy' `
        --query 'properties.outputs.storageAccountName.value' -o tsv

    Write-Host "Storage deployment succeeded (account: $stName)." -ForegroundColor Green
    return $stName
}

# Inventories the shared resources in the resource group with a SINGLE az call
# (az resource list) instead of several per-type calls, since each az invocation
# pays a slow CLI cold-start. Returns existence flags, identifiers, location, and
# the app=azurebench storage account name. Subnet names are not top-level resources,
# so they are fetched separately (Get-VnetSubnets) only when needed.
# JMESPath avoids parentheses/pipes: az is a cmd.exe batch wrapper (az.cmd) and
# PowerShell only auto-quotes args containing spaces, so bare '(' ')' '|' would
# leak to cmd and break parsing.
function Get-ResourceInventory {
    param([string]$ResourceGroup)

    $json = az resource list --resource-group $ResourceGroup `
        --query "[].{name:name, type:type, id:id, location:location, tags:tags}" -o json 2>$null
    $resources = if ($json) { @($json | ConvertFrom-Json) } else { @() }

    $nsg = $resources | Where-Object { $_.type -eq 'Microsoft.Network/networkSecurityGroups' } | Select-Object -First 1
    $vnet = $resources | Where-Object { $_.type -eq 'Microsoft.Network/virtualNetworks' } | Select-Object -First 1
    $ppg = $resources | Where-Object { $_.type -eq 'Microsoft.Compute/proximityPlacementGroups' } | Select-Object -First 1
    $storage = $resources | Where-Object { $_.type -eq 'Microsoft.Storage/storageAccounts' -and $_.tags.app -eq 'azurebench' } | Select-Object -First 1

    return @{
        HasNetwork  = [bool]($nsg -and $vnet -and $ppg)
        HasStorage  = [bool]$storage
        NsgId       = $nsg.id
        VnetName    = $vnet.name
        VnetId      = $vnet.id
        ProximityId = $ppg.id
        StorageName = $storage.name
        Location    = if ($vnet) { $vnet.location } elseif ($nsg) { $nsg.location } else { '' }
    }
}

# Returns the subnet names of a VNet (one az call). Errors if fewer than 2 exist.
function Get-VnetSubnets {
    param([string]$VnetId)
    $subnets = @(az network vnet show --ids $VnetId --query 'subnets[].name' -o tsv 2>$null | Where-Object { $_ })
    if ($subnets.Count -lt 2) {
        Write-Error "Expected at least 2 subnets in VNet '$VnetId', found $($subnets.Count)."
        exit 1
    }
    return $subnets
}

# Ensures a Key Vault exists in the resource group and returns its name. Non-interactive
# and idempotent: discovers an existing vault (preferring the app=azurebench tag, else the
# first vault) and reuses it; otherwise creates one from security/keyvault.bicep, purging a
# soft-deleted namesake if present. Paren-free JMESPath for az.cmd safety.
# Resolves the signed-in user's AAD object ID. Prefers Microsoft Graph
# (az ad signed-in-user show); if that is blocked by a Continuous Access
# Evaluation challenge, falls back to the 'oid' claim in the ARM access token
# (refreshed by 'az login'), which does not require a Graph call.
function Get-DeployerObjectId {
    $oid = az ad signed-in-user show --query id -o tsv 2>$null
    if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($oid)) {
        return $oid.Trim()
    }

    Write-Host "  Graph lookup unavailable (CAE challenge); reading object ID from access token..." -ForegroundColor DarkGray
    $token = az account get-access-token --query accessToken -o tsv 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($token)) {
        return $null
    }

    $parts = $token.Split('.')
    if ($parts.Count -lt 2) { return $null }
    $payload = $parts[1].Replace('-', '+').Replace('_', '/')
    switch ($payload.Length % 4) { 2 { $payload += '==' } 3 { $payload += '=' } }
    try {
        $json = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($payload)) | ConvertFrom-Json
    } catch {
        return $null
    }
    if ($json.oid) { return $json.oid }
    return $null
}

function Deploy-KeyVault {
    param([string]$ResourceGroup, [string]$Region, [string]$SecurityDir)

    $kvName = az keyvault list --resource-group $ResourceGroup --query "[?tags.app=='azurebench'].name" -o tsv 2>$null |
        Where-Object { $_ } | Select-Object -First 1
    if (-not $kvName) {
        $kvName = az keyvault list --resource-group $ResourceGroup --query "[].name" -o tsv 2>$null |
            Where-Object { $_ } | Select-Object -First 1
    }
    if ($kvName) {
        Write-Host "Key Vault already exists in '$ResourceGroup' (vault: $kvName); skipping Key Vault deployment." -ForegroundColor Yellow
        return $kvName
    }

    $kvName = "kv-$(Get-Date -Format 'yyyyMMddHHmmss')"
    Write-Host "`n=== Deploying Key Vault ===" -ForegroundColor Cyan
    Write-Host "  Vault      : $kvName"

    $softDeleted = az keyvault show-deleted --name $kvName --query name -o tsv 2>$null
    if ($softDeleted) {
        Write-Host "  Found soft-deleted vault '$kvName'. Purging..." -ForegroundColor Yellow
        az keyvault purge --name $kvName --output none 2>$null
    }

    $kvBicep = Join-Path $SecurityDir 'keyvault.bicep'
    if (-not (Test-Path $kvBicep)) {
        Write-Error "keyvault.bicep not found in $SecurityDir"
        exit 1
    }

    $deployerOid = Get-DeployerObjectId
    if ([string]::IsNullOrWhiteSpace($deployerOid)) {
        Write-Error ("Could not determine the signed-in user's object ID (Microsoft Graph and the access-token fallback both failed). " +
            "This usually means your session needs re-authentication (e.g. a Continuous Access Evaluation challenge). " +
            "Run 'az login --scope https://graph.microsoft.com//.default' and try again.")
        exit 1
    }

    az deployment group create `
        --resource-group $ResourceGroup `
        --name 'keyvault-deploy' `
        --template-file $kvBicep `
        --parameters keyVaultName=$kvName location=$Region deployerPrincipalId=$deployerOid `
        --output none

    if ($LASTEXITCODE -ne 0) {
        Write-Error "Key Vault deployment failed."
        exit 1
    }
    Write-Host "  Key Vault '$kvName' created." -ForegroundColor Green
    Write-Host "  Waiting for access policy propagation..." -ForegroundColor DarkGray
    Start-Sleep -Seconds 15
    return $kvName
}

# Uploads the inter-node VMSS private key (manifest vmKeys) into the Key Vault as
# secret 'vmss-ssh-private'. Idempotent: skips if the secret already exists.
function Set-VmssPrivateKeySecret {
    param([object]$Manifest, [string]$VaultName)

    $secretName = 'vmss-ssh-private'
    $existing = az keyvault secret show --vault-name $VaultName --name $secretName --query id -o tsv 2>$null
    if ($existing) {
        Write-Host "  Secret '$secretName' already exists in '$VaultName'; skipping upload." -ForegroundColor Yellow
        return
    }

    $privateKeyPath = Join-Path $Manifest.BasePath $Manifest.VmKeys
    if (-not (Test-Path $privateKeyPath)) {
        Write-Error "VMSS private key not found: $privateKeyPath"
        exit 1
    }

    Write-Host "  Uploading '$($Manifest.VmKeys)' private key to Key Vault '$VaultName'..."
    az keyvault secret set --vault-name $VaultName --name $secretName --file $privateKeyPath --output none 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to upload secret '$secretName' to Key Vault."
        exit 1
    }
    Write-Host "  Secret '$secretName' uploaded." -ForegroundColor Green
}

# Publishes a read-only, policy-bound SAS URL for the tools tarball blob into the Key Vault
# as a secret (default 'tools-sas-url'). This is the no-RBAC delivery path: the VMSS reads
# the secret via its Key Vault access at boot and curls the tarball, so no Storage Blob
# Data Reader role assignment (roleAssignments/write) is required.
#   1. Retrieves an account key (listKeys) — data-plane ops use the key, not blob RBAC.
#   2. Ensures a container stored access policy (read+list) with a bounded expiry; a stored
#      policy lets us extend/revoke centrally without re-issuing SAS.
#   3. Generates a SAS bound to that policy and stores the full blob URL as a KV secret.
# Idempotent for -Action deploy (skips if the secret exists); -Action refresh-sas forces
# regeneration (-Force) and refreshes the policy expiry.
function Publish-ToolsSas {
    param(
        [string]$ResourceGroup, [string]$StorageAccount, [string]$VaultName,
        [string]$ContainerName, [string]$PolicyName, [string]$BlobName,
        [string]$SecretName, [int]$ExpiryDays, [switch]$Force
    )

    Write-Host "`n=== Publishing tools SAS to Key Vault ===" -ForegroundColor Cyan

    if (-not $Force) {
        $existing = az keyvault secret show --vault-name $VaultName --name $SecretName --query id -o tsv 2>$null
        if ($existing) {
            Write-Host "  Secret '$SecretName' already exists in '$VaultName'; skipping (use -Action refresh-sas to regenerate)." -ForegroundColor Yellow
            return
        }
    }

    $accountKey = az storage account keys list --account-name $StorageAccount --resource-group $ResourceGroup --query "[0].value" -o tsv 2>$null
    if (-not $accountKey) {
        Write-Error "Could not retrieve a key for storage account '$StorageAccount'."
        exit 1
    }

    $expiry = (Get-Date).AddDays($ExpiryDays).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')

    # Ensure the stored access policy exists (create or update its expiry). policy list
    # returns an object keyed by policy name, so membership is a property-name check.
    $policiesJson = az storage container policy list --account-name $StorageAccount --account-key $accountKey `
        --container-name $ContainerName -o json 2>$null
    $policies = if ($policiesJson) { $policiesJson | ConvertFrom-Json } else { $null }
    $policyExists = $policies -and (($policies.PSObject.Properties.Name) -contains $PolicyName)

    if ($policyExists) {
        Write-Host "  Updating stored access policy '$PolicyName' (expiry $expiry)..."
        az storage container policy update --account-name $StorageAccount --account-key $accountKey `
            --container-name $ContainerName --name $PolicyName --permissions rl --expiry $expiry --output none 2>$null
    } else {
        Write-Host "  Creating stored access policy '$PolicyName' (expiry $expiry)..."
        az storage container policy create --account-name $StorageAccount --account-key $accountKey `
            --container-name $ContainerName --name $PolicyName --permissions rl --expiry $expiry --output none 2>$null
    }
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to set stored access policy '$PolicyName' on container '$ContainerName'."
        exit 1
    }

    # SAS bound to the policy (permissions/expiry inherited from the policy).
    $sas = az storage container generate-sas --account-name $StorageAccount --account-key $accountKey `
        --name $ContainerName --policy-name $PolicyName -o tsv 2>$null
    if ($sas) { $sas = $sas.Trim() }
    if (-not $sas) {
        Write-Error "Failed to generate SAS for container '$ContainerName'."
        exit 1
    }

    $blobEndpoint = (az storage account show --name $StorageAccount --resource-group $ResourceGroup --query "primaryEndpoints.blob" -o tsv 2>$null)
    if (-not $blobEndpoint) {
        Write-Error "Could not determine the blob endpoint for storage account '$StorageAccount'."
        exit 1
    }
    $blobEndpoint = $blobEndpoint.TrimEnd('/')
    $sasUrl = "$blobEndpoint/$ContainerName/${BlobName}?$sas"

    # Store the ready-to-use SAS URL as a KV secret via a temp file: the SAS contains '&',
    # which az.cmd would misinterpret as a command separator if passed inline as --value.
    $tmp = Join-Path ([System.IO.Path]::GetTempPath()) "tools-sas-$([guid]::NewGuid().ToString('N')).txt"
    try {
        Set-Content -Path $tmp -Value $sasUrl -NoNewline -Encoding ascii
        az keyvault secret set --vault-name $VaultName --name $SecretName --file $tmp --output none 2>$null
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Failed to store secret '$SecretName' in Key Vault '$VaultName'."
            exit 1
        }
    } finally {
        if (Test-Path $tmp) { Remove-Item $tmp -Force }
    }

    Write-Host "  Stored SAS URL as secret '$SecretName' (expires $expiry)." -ForegroundColor Green
    Write-Host "    $blobEndpoint/$ContainerName/$BlobName?<sas>" -ForegroundColor DarkGray
}

# Check if resource group exists
$rgExists = az group exists --name $rg 2>$null
if ($rgExists -ne 'true') {
    if ($Action -eq 'stage') {
        Write-Error "Resource group '$rg' does not exist."
        exit 1
    }
    Write-Host "Resource group '$rg' does not exist." -ForegroundColor Yellow
    $create = Read-Host "Would you like to create it? (y/N)"
    if ($create -eq 'y' -or $create -eq 'Y') {
        if ($Region) {
            # Honor the region passed on the command line; don't prompt.
            $location = $Region
        } else {
            $location = Read-Host "Location (default: southcentralus)"
            if (-not $location) { $location = 'southcentralus' }
        }
        az group create --name $rg --location $location --output none
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Failed to create resource group '$rg'."
            exit 1
        }
        Write-Host "Resource group '$rg' created in '$location'." -ForegroundColor Green
    } else {
        Write-Error "Resource group '$rg' does not exist. Aborting."
        exit 1
    }
}

if ($Action -eq 'stage') {
    Write-Host "`n=== Querying resources in '$rg' ===" -ForegroundColor Cyan

    $inv = Get-ResourceInventory -ResourceGroup $rg
    if (-not $inv.HasNetwork) {
        Write-Error "Network resources (NSG, VNet, proximity group) not found in resource group '$rg'."
        exit 1
    }
    $subnets = Get-VnetSubnets -VnetId $inv.VnetId

    Write-Host "`n=== Staging SSH public keys ===" -ForegroundColor Cyan
    try {
        $manifest = Get-SshKeyManifest -ManifestPath $manifestFile
        $keySync = Sync-SshPublicKeys -Manifest $manifest -SecurityDir $securityDir
    } catch {
        Write-Error $_.Exception.Message
        exit 1
    }
    if ($keySync.ResolvedNames.Count -eq 0) {
        Write-Error "No SSH public keys could be staged from '$($manifest.BasePath)'."
        exit 1
    }
    Write-Host "  Source     : $($manifest.BasePath)"
    Write-Host "  Staged     : $($keySync.ResolvedNames -join ', ')"

    Write-VmssParams -Location $inv.Location -NsgId $inv.NsgId -VnetName $inv.VnetName `
        -SubnetName $subnets[0] -AccSubnetName $subnets[1] -ProximityId $inv.ProximityId
    exit 0
}

if ($Action -eq 'refresh-sas') {
    Write-Host "`n=== Refreshing tools SAS in '$rg' ===" -ForegroundColor Cyan

    $inv = Get-ResourceInventory -ResourceGroup $rg
    if (-not $inv.HasStorage) {
        Write-Error "No storage account (app=azurebench) found in '$rg'. Run -Action deploy first."
        exit 1
    }

    $kvName = az keyvault list --resource-group $rg --query "[?tags.app=='azurebench'].name" -o tsv 2>$null |
        Where-Object { $_ } | Select-Object -First 1
    if (-not $kvName) {
        $kvName = az keyvault list --resource-group $rg --query "[].name" -o tsv 2>$null |
            Where-Object { $_ } | Select-Object -First 1
    }
    if (-not $kvName) {
        Write-Error "No Key Vault found in '$rg'. Run -Action deploy first."
        exit 1
    }

    Publish-ToolsSas -ResourceGroup $rg -StorageAccount $inv.StorageName -VaultName $kvName `
        -ContainerName $ContainerName -PolicyName $SasPolicyName -BlobName $ToolsBlobName `
        -SecretName $SasSecretName -ExpiryDays $SasExpiryDays -Force
    exit 0
}

# Action = deploy (idempotent: skips resources that already exist)
Write-Host "`n=== Deploying shared resources ===" -ForegroundColor Cyan
Write-Host "  Resource Group : $rg"

# One inventory call up front; reused for every existence check + identifiers below.
$inv = Get-ResourceInventory -ResourceGroup $rg
$Region = Resolve-Region -ResourceGroup $rg -Region $Region -FallbackLocation $inv.Location
Write-Host ""

# --- Network resources ---
if ($inv.HasNetwork) {
    Write-Host "Network resources (NSG, VNet, proximity group) already exist in '$rg'; skipping network deployment." -ForegroundColor Yellow
    $subnets = Get-VnetSubnets -VnetId $inv.VnetId
    $location = $inv.Location
    $nsgId = $inv.NsgId
    $vnetName = $inv.VnetName
    $subnetName = $subnets[0]
    $accSubnetName = $subnets[1]
    $proximityId = $inv.ProximityId
} else {
    Write-Host "=== Deploying network resources ===" -ForegroundColor Cyan
    Write-Host "  Template   : network/network.bicep"
    Write-Host ""

    az deployment group create `
        --resource-group $rg `
        --name $DeploymentName `
        --template-file (Join-Path $networkDir 'network.bicep') `
        --parameters (Join-Path $networkDir 'network-parameters.json') `
        --parameters location=$Region `
        --output none

    if ($LASTEXITCODE -ne 0) {
        Write-Error "Network deployment failed."
        exit 1
    }
    Write-Host "Network deployment succeeded." -ForegroundColor Green

    $outputs = az deployment group show `
        --resource-group $rg `
        --name $DeploymentName `
        --query 'properties.outputs' `
        --output json | ConvertFrom-Json

    if (-not $outputs) {
        Write-Error "Could not read deployment outputs. Ensure deployment '$DeploymentName' exists in resource group '$rg'."
        exit 1
    }

    # Use the region we actually deployed to (from -Region or the RG-location default).
    $location = $Region
    $nsgId = $outputs.nsgId.value
    $vnetName = $outputs.vnetName.value
    $subnetName = $outputs.subnetName.value
    $accSubnetName = $outputs.accSubnetName.value
    $proximityId = $outputs.proximityId.value
}

# --- Storage account ---
if ($inv.HasStorage) {
    Write-Host "Storage account already exists in '$rg' (account: $($inv.StorageName)); skipping storage deployment." -ForegroundColor Yellow
    $storageName = $inv.StorageName
} else {
    $storageName = Deploy-Storage -ResourceGroup $rg -Region $Region -StorageDir $storageDir
}

# --- Key Vault + SSH keys ---
$manifest = Get-SshKeyManifest -ManifestPath $manifestFile
Write-Host "`n=== SSH key manifest ===" -ForegroundColor Cyan
Write-Host "  BasePath   : $($manifest.BasePath)"
Write-Host "  User Keys  : $($manifest.UserKeys -join ', ')"
Write-Host "  VM Keys    : $($manifest.VmKeys)"
$keySync = Sync-SshPublicKeys -Manifest $manifest -SecurityDir $securityDir
Write-Host "  Resolved   : $($keySync.ResolvedNames -join ', ')"
$kvName = Deploy-KeyVault -ResourceGroup $rg -Region $Region -SecurityDir $securityDir
Set-VmssPrivateKeySecret -Manifest $manifest -VaultName $kvName

# --- Tools tarball SAS (stored in Key Vault for no-RBAC blob delivery) ---
Publish-ToolsSas -ResourceGroup $rg -StorageAccount $storageName -VaultName $kvName `
    -ContainerName $ContainerName -PolicyName $SasPolicyName -BlobName $ToolsBlobName `
    -SecretName $SasSecretName -ExpiryDays $SasExpiryDays

# --- Generate vmss-parameters.json ---
Write-VmssParams -Location $location -NsgId $nsgId -VnetName $vnetName `
    -SubnetName $subnetName -AccSubnetName $accSubnetName -ProximityId $proximityId
