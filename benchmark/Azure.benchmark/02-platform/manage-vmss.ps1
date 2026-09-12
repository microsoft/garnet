#!/usr/bin/env pwsh
[CmdletBinding()]
<#
.SYNOPSIS
    Manages VMSS nodes: SSH-based software updates plus power (start/stop) operations.

.DESCRIPTION
    Connects to VMSS instances via SSH to refresh repos, rebuild systems, or re-run initialization.
    Also performs Azure power operations (start / deallocate) on entire VMSS.
    Supports multiple VMSS in a resource group with auto-discovery and validation.

.PARAMETER rg
    Azure resource group name (required).

.PARAMETER VmssName
    VMSS name(s) to target. Accepts single name or comma-separated list.
    If omitted, script lists all VMSS and prompts for selection.

.PARAMETER Action
    Action to perform:
    - refresh (default): git pull all repos
    - rebuild: git pull + rebuild specified system (requires -System)
    - deploy: fetch fresh tools tarball + run full deployment workflow
    - install: fetch fresh tools tarball + copy scripts only
    - ping: probe SSH port (TCP 22) reachability of each instance and print FQDN
    - start: power on all instances in the VMSS (az vmss start, fire-and-forget)
    - stop: deallocate all instances in the VMSS, stopping compute billing (az vmss deallocate, fire-and-forget)
    - restart: restart only failed/unhealthy instances in the VMSS (az vmss restart, fire-and-forget)
    - discover: force cluster peer rescan on each instance (runs cluster-deploy.ps1 -Action discover)

.PARAMETER System
    System to rebuild (required when Action=rebuild).
    Valid: garnet, valkey, resp-bench, memtier

.PARAMETER SshUser
    SSH username (default: guser)

.PARAMETER NoPull
    Skip the git pull step on rebuild and build only the current local changes.

.PARAMETER Ref
    Rebuild from a specific commit hash, tag, or branch. Fetches all refs then
    hard-resets the system repo onto <Ref> before building. Only valid with
    -Action rebuild; takes precedence over -Force/-NoPull pull behavior.

.EXAMPLE
    # List VMSS and prompt for selection, then refresh all repos
    .\manage-vmss.ps1 -rg vazois-garnet

    # Refresh specific VMSS
    .\manage-vmss.ps1 -rg vazois-garnet -VmssName server

    # Rebuild garnet on multiple VMSS
    .\manage-vmss.ps1 -rg vazois-garnet -VmssName server,client -Action rebuild -System garnet

    # Verbose output (show per-instance command output)
    .\manage-vmss.ps1 -rg vazois-garnet -VmssName server -Action rebuild -System garnet -Verbose

    # Power on a VMSS
    .\manage-vmss.ps1 -rg vazois-garnet -VmssName server -Action start

    # Deallocate (stop billing for) a VMSS
    .\manage-vmss.ps1 -rg vazois-garnet -VmssName server -Action stop

    # Restart only the failed instances in a VMSS
    .\manage-vmss.ps1 -rg vazois-garnet -VmssName server -Action restart
#>

param(
    [string]$rg,

    [string]$VmssName,

    [ValidateSet('refresh', 'rebuild', 'deploy', 'install', 'ping', 'start', 'stop', 'restart', 'list', 'discover', 'create', 'push-keys', 'publish-tools')]
    [string]$Action = 'refresh',

    [ValidateSet('garnet', 'valkey', 'resp-bench', 'memtier')]
    [string]$System,

    [int]$MaxScan = 0,

    [string]$SshUser = 'guser',

    [string]$Ref,

    [string]$ParametersFile = 'vmss-parameters.json',

    [string]$TemplateFile = 'vmss.bicep',

    [string]$DeploymentName,

    [string]$ToolsPath,

    [string]$ContainerName = 'tools',

    [string]$ToolsBlobName = 'tools.tar.gz',

    [switch]$GrantStorageAccess,

    [switch]$Force,

    [switch]$NoPull,

    [switch]$Help
)

if ($Help -or -not $rg) {
    Write-Host "Usage: manage-vmss.ps1 -rg <resource-group> [options]" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Manages VMSS nodes: SSH software updates plus power (start/stop) operations."
    Write-Host ""
    Write-Host "Required Parameters:"
    Write-Host "  -rg <name>          Azure resource group name"
    Write-Host ""
    Write-Host "Optional Parameters:"
    Write-Host "  -VmssName <name>    VMSS name(s) to target (comma-separated or 'all')"
    Write-Host "                      If omitted, script prompts for selection"
    Write-Host "  -Action <action>    Action to perform (default: refresh)"
    Write-Host "                      refresh        - git pull all repos"
    Write-Host "                      rebuild        - git pull + rebuild system (requires -System)"
    Write-Host "                      deploy         - fetch fresh tools tarball + run full deployment workflow"
    Write-Host "                      install        - fetch fresh tools tarball + copy scripts only"
    Write-Host "                      ping           - probe SSH port (TCP 22) reachability + FQDN"
    Write-Host "                      start          - power on all VMSS instances (fire-and-forget)"
    Write-Host "                      stop           - deallocate all VMSS instances (fire-and-forget)"
    Write-Host "                      restart        - restart only failed instances (fire-and-forget)"
    Write-Host "                      list           - list instance status for selected VMSS"
    Write-Host "                      discover       - force cluster peer rescan (runs cluster-deploy.ps1 -Action discover)"
    Write-Host "                      create         - deploy the VMSS from $TemplateFile (auto-discovers KV + SSH keys; storage RBAC via -GrantStorageAccess)"
    Write-Host "                      push-keys      - push authorized_keys (security/*.pub) to running instances via run-command"
    Write-Host "                      publish-tools  - package tools/ into a tarball and upload it to the shared storage account (blob for SAS delivery)"
    Write-Host "  -System <name>      System to rebuild: garnet, valkey, resp-bench, memtier"
    Write-Host "                      Required when -Action rebuild"
    Write-Host "  -MaxScan <n>        Cap subnet-scan probes for -Action discover (0 = unlimited)"
    Write-Host "  -Ref <commit>       Rebuild from a specific commit/tag/branch (fetch + git reset --hard)"
    Write-Host "                      Only valid with -Action rebuild; overrides -Force/-NoPull pull behavior"
    Write-Host "  -SshUser <user>     SSH username (default: guser)"
    Write-Host "  -ParametersFile <p> Bicep parameters file for -Action create (default: vmss-parameters.json)"
    Write-Host "  -TemplateFile <p>   Bicep template for -Action create (default: vmss.bicep)"
    Write-Host "  -DeploymentName <n> Deployment name for -Action create (default: timestamp yyyy-MM-dd-HH-mm)"
    Write-Host "  -ToolsPath <p>      tools/ directory to package for -Action publish-tools (default: .\tools)"
    Write-Host "  -ContainerName <c>  Blob container for -Action publish-tools (default: tools)"
    Write-Host "  -ToolsBlobName <n>  Blob name for -Action publish-tools (default: tools.tar.gz)"
    Write-Host "  -GrantStorageAccess Grant the VMSS identity Storage Blob Data Reader during -Action create"
    Write-Host "                      (requires Owner/User Access Administrator; skipped by default)"
    Write-Host "  -Verbose            Show per-instance command output"
    Write-Host "  -Force              Force pull (git reset --hard) instead of fast-forward"
    Write-Host "  -NoPull             Skip git pull on rebuild (build local changes only)"
    Write-Host "  -Help               Show this help message"
    Write-Host ""
    Write-Host "Examples:"
    Write-Host "  .\manage-vmss.ps1 -rg vazois-garnet"
    Write-Host "  .\manage-vmss.ps1 -rg vazois-garnet -VmssName server -Action rebuild -System garnet"
    Write-Host "  .\manage-vmss.ps1 -rg vazois-garnet -VmssName server,client -Action refresh"
    Write-Host "  .\manage-vmss.ps1 -rg vazois-garnet -VmssName server -Action start"
    Write-Host "  .\manage-vmss.ps1 -rg vazois-garnet -VmssName server -Action stop"
    Write-Host "  .\manage-vmss.ps1 -rg vazois-garnet -VmssName server -Action restart"
    Write-Host "  .\manage-vmss.ps1 -rg vazois-garnet -VmssName server -Action ping"
    Write-Host "  .\manage-vmss.ps1 -rg vazois-garnet -VmssName server -Action rebuild -System garnet -Ref a1b2c3d"
    Write-Host "  .\manage-vmss.ps1 -rg vazois-garnet -Action create"
    Write-Host "  .\manage-vmss.ps1 -rg vazois-garnet -Action publish-tools"
    Write-Host "  .\manage-vmss.ps1 -rg vazois-garnet -VmssName server -Action push-keys"
    Write-Host ""
    Write-Host "For detailed help: Get-Help .\manage-vmss.ps1 -Detailed"
    Write-Host ""
    return
}

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$securityDir = Join-Path $scriptDir '..\01-resources\security'
. (Join-Path $securityDir 'ssh-key-utils.ps1')

function Write-CreateError {
    param([string]$Message)
    [Console]::Error.WriteLine("Error: $Message")
}

function Confirm-CreateAction {
    param([string]$Prompt)
    $answer = Read-Host "$Prompt [y/N]"
    return $answer -in @('y', 'yes')
}

function Get-AzureCreateContext {
    param([string]$ResourceGroup)

    $accountJson = az account show `
        --query '{subscriptionName:name,subscriptionId:id,tenantId:tenantId}' -o json 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $accountJson) {
        Write-CreateError "Unable to read the active Azure account. Run 'az login' and select the intended subscription."
        return $null
    }

    $groupJson = az group show --name $ResourceGroup `
        --query '{name:name,location:location,id:id}' -o json 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $groupJson) {
        Write-CreateError "Unable to access resource group '$ResourceGroup' in the active subscription. Confirm that it exists and that you have permission to read it."
        return $null
    }

    try {
        $account = $accountJson | ConvertFrom-Json
        $group = $groupJson | ConvertFrom-Json
    } catch {
        Write-CreateError "Azure CLI returned invalid account or resource-group data: $($_.Exception.Message)"
        return $null
    }

    return [pscustomobject]@{
        SubscriptionName = $account.subscriptionName
        SubscriptionId   = $account.subscriptionId
        TenantId         = $account.tenantId
        ResourceGroup    = $group.name
        ResourceGroupId  = $group.id
        Location         = $group.location
    }
}

function Invoke-ParameterStaging {
    param([string]$ResourceGroup)

    $stageScript = Join-Path $scriptDir '..\01-resources\deploy-common-resources.ps1'
    $pwshPath = (Get-Process -Id $PID).Path
    & $pwshPath -NoProfile -File $stageScript -rg $ResourceGroup -Action stage
    return $LASTEXITCODE -eq 0
}

function Test-StagedInfrastructure {
    param(
        [string]$ParametersPath,
        [string]$ResourceGroup,
        [object]$AzureContext
    )

    $errors = [System.Collections.Generic.List[string]]::new()
    try {
        $document = Get-Content -LiteralPath $ParametersPath -Raw | ConvertFrom-Json
    } catch {
        $errors.Add("Parameters file is not valid JSON: $($_.Exception.Message)")
        return [pscustomobject]@{ IsValid = $false; Errors = $errors }
    }

    $requiredNames = @('location', 'nsgId', 'vnetName', 'subnetName', 'accSubnetName')
    $values = @{}
    foreach ($name in $requiredNames + 'proximityId') {
        $property = $document.parameters.PSObject.Properties[$name]
        $values[$name] = if ($property) { $property.Value.value } else { $null }
    }
    foreach ($name in $requiredNames) {
        if ([string]::IsNullOrWhiteSpace([string]$values[$name])) {
            $errors.Add("Required staged parameter '$name' is missing or empty.")
        }
    }
    if ($errors.Count -gt 0) {
        return [pscustomobject]@{ IsValid = $false; Errors = $errors }
    }

    $resourcePrefix = "/subscriptions/$($AzureContext.SubscriptionId)/resourceGroups/$ResourceGroup/"
    if (-not ([string]$values.nsgId).StartsWith($resourcePrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        $errors.Add("nsgId does not belong to resource group '$ResourceGroup' in the active subscription.")
    }
    if ($values.proximityId -and
        -not ([string]$values.proximityId).StartsWith($resourcePrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        $errors.Add("proximityId does not belong to resource group '$ResourceGroup' in the active subscription.")
    }

    $nsgJson = az network nsg show --ids $values.nsgId `
        --query '{id:id,location:location}' -o json 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $nsgJson) {
        $errors.Add("The staged NSG no longer exists or is not accessible.")
    } else {
        $nsg = $nsgJson | ConvertFrom-Json
        if ($nsg.location -ne $values.location) {
            $errors.Add("The staged NSG location '$($nsg.location)' does not match '$($values.location)'.")
        }
    }

    $vnetJson = az network vnet show --resource-group $ResourceGroup --name $values.vnetName `
        --query '{id:id,location:location,subnets:subnets[].name}' -o json 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $vnetJson) {
        $errors.Add("The staged VNet '$($values.vnetName)' no longer exists or is not accessible.")
    } else {
        $vnet = $vnetJson | ConvertFrom-Json
        if ($vnet.location -ne $values.location) {
            $errors.Add("The staged VNet location '$($vnet.location)' does not match '$($values.location)'.")
        }
        $subnets = @($vnet.subnets)
        foreach ($subnetName in @($values.subnetName, $values.accSubnetName)) {
            if ($subnetName -notin $subnets) {
                $errors.Add("Subnet '$subnetName' does not exist in VNet '$($values.vnetName)'.")
            }
        }
        if ($values.subnetName -eq $values.accSubnetName) {
            $errors.Add("Management and accelerated-networking subnet names must be different.")
        }
    }

    if ($values.proximityId) {
        $ppgJson = az ppg show --ids $values.proximityId `
            --query '{id:id,location:location}' -o json 2>$null
        if ($LASTEXITCODE -ne 0 -or -not $ppgJson) {
            $errors.Add("The staged proximity placement group no longer exists or is not accessible.")
        } else {
            $ppg = $ppgJson | ConvertFrom-Json
            if ($ppg.location -ne $values.location) {
                $errors.Add("The staged proximity placement group location '$($ppg.location)' does not match '$($values.location)'.")
            }
        }
    }

    return [pscustomobject]@{
        IsValid = $errors.Count -eq 0
        Errors  = $errors
    }
}

# --- Load shared utilities ---
. "$scriptDir\..\03-workload\bench\utils.ps1"

# Returns a synchronized hashtable mapping VMSS name -> power-state summary
# (e.g. "running: 6" or "running: 4, deallocated: 2"). Queries all VMSS in
# parallel for speed. Value is "unknown" if a query fails, "no instances" if empty.
function Get-VmssPowerStates {
    param([string]$ResourceGroup, [string[]]$VmssNames)

    $map = [hashtable]::Synchronized(@{})

    $VmssNames | ForEach-Object -Parallel {
        $name = $_
        $summary = 'unknown'
        $codesJson = az vmss list-instances `
            --resource-group $using:ResourceGroup `
            --name $name `
            --expand instanceView `
            --query "[].instanceView.statuses[?starts_with(code, 'PowerState')].code | []" -o json 2>$null

        if ($LASTEXITCODE -eq 0 -and $null -ne $codesJson) {
            $codes = @($codesJson | ConvertFrom-Json)
            if ($codes.Count -eq 0) {
                $summary = 'no instances'
            } else {
                $summary = ($codes |
                    ForEach-Object { $_ -replace 'PowerState/', '' } |
                    Group-Object |
                    Sort-Object Name |
                    ForEach-Object { "$($_.Name): $($_.Count)" }) -join ', '
            }
        }

        ($using:map)[$name] = $summary
    } -ThrottleLimit 16

    return $map
}

# --- Action: create (provision the VMSS scale set) ---
# Runs before VMSS discovery because the scale set does not exist yet. Uses Approach B:
# inline-discover the Key Vault and storage account in the resource group (no persisted
# names) and pass them to the vmss.bicep deployment so the VMSS managed identity is
# granted Key Vault (secret get) and Storage Blob Data Reader access during deployment.
# vmssName/instanceCount/etc. come from the parameters file or are prompted by az.
if ($Action -eq 'create') {
    $templatePath = if ([System.IO.Path]::IsPathRooted($TemplateFile)) {
        $TemplateFile
    } else {
        Join-Path $scriptDir $TemplateFile
    }
    $paramsPath = if ([System.IO.Path]::IsPathRooted($ParametersFile)) {
        $ParametersFile
    } else {
        Join-Path $scriptDir $ParametersFile
    }
    if (-not (Test-Path $templatePath)) { Write-Error "Template not found: $templatePath"; exit 1 }

    $usingDefaultParameters = -not $PSBoundParameters.ContainsKey('ParametersFile')
    if (-not (Test-Path -LiteralPath $paramsPath -PathType Leaf)) {
        if (-not $usingDefaultParameters) {
            Write-CreateError "Parameters file not found: $paramsPath"
            exit 1
        }
    }

    $azureContext = Get-AzureCreateContext -ResourceGroup $rg
    if (-not $azureContext) { exit 1 }

    Write-Host "`n=== Azure deployment context ===" -ForegroundColor Cyan
    Write-Host "  Tenant          : $($azureContext.TenantId)"
    Write-Host "  Subscription    : $($azureContext.SubscriptionName) ($($azureContext.SubscriptionId))"
    Write-Host "  Resource group  : $($azureContext.ResourceGroup)"
    Write-Host "  Group location  : $($azureContext.Location)"
    if (-not (Confirm-CreateAction "Use this Azure context for VMSS creation?")) {
        Write-CreateError "VMSS creation cancelled because the Azure context was not approved."
        exit 1
    }

    if (-not (Test-Path -LiteralPath $paramsPath -PathType Leaf)) {
        Write-Host "`nThe generated parameters file is machine-local and is not stored in Git:" -ForegroundColor Yellow
        Write-Host "  $paramsPath"
        if (-not (Confirm-CreateAction "Stage it from resource group '$rg' now?")) {
            Write-CreateError "Parameters staging was declined."
            exit 1
        }
        if (-not (Invoke-ParameterStaging -ResourceGroup $rg) -or
            -not (Test-Path -LiteralPath $paramsPath -PathType Leaf)) {
            Write-CreateError "Failed to stage parameters from resource group '$rg'."
            exit 1
        }
    }

    $stagedValidation = Test-StagedInfrastructure -ParametersPath $paramsPath `
        -ResourceGroup $rg -AzureContext $azureContext
    if (-not $stagedValidation.IsValid) {
        Write-Host "`nThe staged infrastructure parameters are invalid or stale:" -ForegroundColor Yellow
        $stagedValidation.Errors | ForEach-Object { Write-Host "  - $_" -ForegroundColor Yellow }

        if (-not $usingDefaultParameters -or
            -not (Confirm-CreateAction "Restage the default parameters from resource group '$rg' now?")) {
            Write-CreateError "Valid staged infrastructure parameters are required for VMSS creation."
            exit 1
        }
        if (-not (Invoke-ParameterStaging -ResourceGroup $rg)) {
            Write-CreateError "Failed to restage parameters from resource group '$rg'."
            exit 1
        }

        $stagedValidation = Test-StagedInfrastructure -ParametersPath $paramsPath `
            -ResourceGroup $rg -AzureContext $azureContext
        if (-not $stagedValidation.IsValid) {
            $stagedValidation.Errors | ForEach-Object { Write-CreateError $_ }
            exit 1
        }
    }

    Write-Host "`nStaged infrastructure validated for '$rg'." -ForegroundColor Green
    Write-Host "`n=== Creating VMSS in resource group '$rg' ===" -ForegroundColor Cyan

    # Inline-discover the Key Vault (prefer the app=azurebench tag, fall back to first).
    # Paren-free JMESPath so az.cmd does not choke on unquoted parentheses/pipes.
    $kv = az keyvault list --resource-group $rg --query "[?tags.app=='azurebench'].name" -o tsv 2>$null |
        Where-Object { $_ } | Select-Object -First 1
    if (-not $kv) {
        $kv = az keyvault list --resource-group $rg --query "[].name" -o tsv 2>$null |
            Where-Object { $_ } | Select-Object -First 1
    }
    if (-not $kv) {
        Write-Error "No Key Vault found in '$rg'. Run 01-resources/deploy-common-resources.ps1 first."
        exit 1
    }

    # Inline-discover the shared storage account (app=azurebench tag). Only needed when
    # granting the VMSS identity blob access, which requires the deployer to have
    # Owner / User Access Administrator (roleAssignments/write). Opt in via
    # -GrantStorageAccess; otherwise the RBAC grant is skipped so the deployment
    # succeeds without elevated rights.
    $st = $null
    if ($GrantStorageAccess) {
        $st = az storage account list --resource-group $rg --query "[?tags.app=='azurebench'].name" -o tsv 2>$null |
            Where-Object { $_ } | Select-Object -First 1
        if (-not $st) {
            Write-Error "No storage account found in '$rg'. Run 01-resources/deploy-common-resources.ps1 first."
            exit 1
        }
    }

    if (-not $DeploymentName) { $DeploymentName = Get-Date -Format 'yyyy-MM-dd-HH-mm' }

    # Resolve SSH public keys from the manifest's canonical basePath (normally
    # %USERPROFILE%\.ssh). The git-ignored security directory is only a fallback cache
    # populated by deploy-common-resources.ps1.
    $manifestPath = Join-Path $securityDir 'manifest.json'
    $sshParamsFile = $null
    $sshKeyCount = 0
    try {
        $sshManifest = Get-SshKeyManifest -ManifestPath $manifestPath
        $keySync = Sync-SshPublicKeys -Manifest $sshManifest -SecurityDir $securityDir
    } catch {
        Write-CreateError $_.Exception.Message
        exit 1
    }

    $pubKeys = @()
    foreach ($pubPath in $keySync.ResolvedPaths) {
        $publicKey = (Get-Content -LiteralPath $pubPath -Raw).Trim()
        if ($publicKey -notmatch '^(ssh-|ecdsa-|sk-)') {
            Write-CreateError "SSH public key file has an unsupported format: $pubPath"
            exit 1
        }
        $pubKeys += $publicKey
    }

    if ($pubKeys.Count -eq 0) {
        Write-CreateError "No SSH public keys were found. Checked manifest basePath '$($sshManifest.BasePath)' and fallback '$securityDir'."
        exit 1
    }

    $sshKeyCount = $pubKeys.Count
    $sshParamsFile = Join-Path ([System.IO.Path]::GetTempPath()) "vmss-sshkeys-$([guid]::NewGuid().ToString('N')).json"
    $sshPayload = [ordered]@{
        '$schema'      = 'https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#'
        contentVersion = '1.0.0.0'
        parameters     = [ordered]@{ sshPublicKeys = [ordered]@{ value = @($pubKeys) } }
    }
    $sshPayload | ConvertTo-Json -Depth 5 | Set-Content -Path $sshParamsFile -Encoding utf8

    Write-Host "  Key Vault       : $kv"
    if ($GrantStorageAccess) {
        Write-Host "  Storage account : $st (Blob Data Reader RBAC grant)"
    } else {
        Write-Host "  Storage account : RBAC grant skipped (use -GrantStorageAccess; needs Owner/User Access Administrator)" -ForegroundColor Yellow
    }
    Write-Host "  SSH keys        : $sshKeyCount ($($keySync.ResolvedNames -join ', '))"
    Write-Host "  SSH key source  : $($sshManifest.BasePath) (synced to security cache)"
    Write-Host "  Template        : $TemplateFile"
    Write-Host "  Parameters      : $ParametersFile"
    Write-Host "  Deployment name : $DeploymentName"
    Write-Host ""

    $deployArgs = @(
        'deployment', 'group', 'create',
        '--resource-group', $rg,
        '--name', $DeploymentName,
        '--template-file', $templatePath,
        '--parameters', "@$paramsPath",
        '--parameters', "keyVaultName=$kv"
    )
    if ($st) { $deployArgs += "storageAccountName=$st" }
    if ($sshParamsFile) { $deployArgs += @('--parameters', "@$sshParamsFile") }

    try {
        az @deployArgs
        $deployExit = $LASTEXITCODE
    } finally {
        if ($sshParamsFile -and (Test-Path $sshParamsFile)) { Remove-Item $sshParamsFile -Force }
    }

    if ($deployExit -ne 0) {
        Write-Error "VMSS deployment '$DeploymentName' failed."
        exit 1
    }

    Write-Host "`nVMSS deployment '$DeploymentName' succeeded." -ForegroundColor Green
    exit 0
}

# --- Action: publish-tools ---
# Package the tools/ bootstrap bundle into tools.tar.gz and upload it to the shared
# storage account's blob container. VMSS instances pull this tarball at boot via the
# policy-bound SAS URL stored in Key Vault (secret 'tools-sas-url'), so the blob must
# exist before any instance provisions. Uploads with the storage account key (the
# deployer role lacks data-plane RBAC), matching the SAS delivery model.
if ($Action -eq 'publish-tools') {
    Write-Host "`n=== Publishing tools bundle to storage in '$rg' ===" -ForegroundColor Cyan

    # Resolve the tools/ directory (default: sibling tools\ next to this script).
    if (-not $ToolsPath) { $ToolsPath = Join-Path $scriptDir 'tools' }
    if (-not (Test-Path $ToolsPath)) {
        Write-Error "Tools directory not found: $ToolsPath"
        exit 1
    }
    $toolsFull = (Resolve-Path $ToolsPath).Path
    $toolsParent = Split-Path -Parent $toolsFull
    $toolsLeaf = Split-Path -Leaf $toolsFull

    # Inline-discover the shared storage account (app=azurebench tag, fall back to first).
    $st = az storage account list --resource-group $rg --query "[?tags.app=='azurebench'].name" -o tsv 2>$null |
        Where-Object { $_ } | Select-Object -First 1
    if (-not $st) {
        $st = az storage account list --resource-group $rg --query "[].name" -o tsv 2>$null |
            Where-Object { $_ } | Select-Object -First 1
    }
    if (-not $st) {
        Write-Error "No storage account found in '$rg'. Run 01-resources/deploy-common-resources.ps1 first."
        exit 1
    }

    $accountKey = az storage account keys list --account-name $st --resource-group $rg --query "[0].value" -o tsv 2>$null
    if (-not $accountKey) {
        Write-Error "Could not retrieve a key for storage account '$st'."
        exit 1
    }

    Write-Host "  Storage account : $st"
    Write-Host "  Container       : $ContainerName"
    Write-Host "  Blob            : $ToolsBlobName"
    Write-Host "  Source          : $toolsFull"

    # Build the tarball as <parent>/tools (a single top-level 'tools/' directory) so it
    # extracts predictably on the VM. Requires tar (present on Windows 10+ and Linux).
    $tarball = Join-Path ([System.IO.Path]::GetTempPath()) "tools-$([guid]::NewGuid().ToString('N')).tar.gz"
    Write-Host "  Packaging '$toolsLeaf' -> $tarball ..."
    tar -czf $tarball -C $toolsParent $toolsLeaf
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path $tarball)) {
        Write-Error "Failed to create tools tarball."
        exit 1
    }

    try {
        Write-Host "  Uploading to blob '$ContainerName/$ToolsBlobName' ..."
        az storage blob upload `
            --account-name $st --account-key $accountKey `
            --container-name $ContainerName --name $ToolsBlobName `
            --file $tarball --content-type 'application/gzip' `
            --overwrite --output none
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Blob upload failed."
            exit 1
        }
    } finally {
        if (Test-Path $tarball) { Remove-Item $tarball -Force }
    }

    Write-Host "  Published tools bundle to '$st/$ContainerName/$ToolsBlobName'." -ForegroundColor Green
    Write-Host "  VMSS instances fetch it at boot via the 'tools-sas-url' Key Vault secret." -ForegroundColor DarkGray
    exit 0
}

# Validate Action and System parameter combination
if ($Action -eq 'rebuild' -and -not $System) {
    Write-Error "Action 'rebuild' requires -System parameter (garnet, valkey, resp-bench, or memtier)"
    exit 1
}

if ($Ref -and $Action -ne 'rebuild') {
    Write-Error "-Ref is only supported with -Action rebuild"
    exit 1
}

# --- VMSS Discovery & Selection ---
Write-Host "`n=== Discovering VMSS in resource group '$rg' ===" -ForegroundColor Cyan

$allVmssJson = az vmss list --resource-group $rg --query '[].{name:name, state:provisioningState}' -o json 2>$null
if ($LASTEXITCODE -ne 0 -or -not $allVmssJson) {
    Write-Error "Failed to list VMSS in resource group '$rg'. Ensure the resource group exists and you're logged in."
    exit 1
}

$allVmssInfo = @($allVmssJson | ConvertFrom-Json)
if ($allVmssInfo.Count -eq 0) {
    Write-Error "No VMSS found in resource group '$rg'."
    exit 1
}

$allVmss = @($allVmssInfo | ForEach-Object { $_.name })

$targetVmss = @()

if (-not $VmssName) {
    # Prompt user to select
    Write-Host "`nAvailable VMSS:"
    Write-Host "  (querying power state...)" -ForegroundColor DarkGray
    $powerMap = Get-VmssPowerStates -ResourceGroup $rg -VmssNames $allVmss

    # Build rows and compute column widths for aligned table output
    $rows = for ($i = 0; $i -lt $allVmssInfo.Count; $i++) {
        [PSCustomObject]@{
            Idx   = "[$i]"
            Name  = $allVmssInfo[$i].name
            Prov  = $allVmssInfo[$i].state
            Power = $powerMap[$allVmssInfo[$i].name]
        }
    }

    $wIdx   = [Math]::Max('#'.Length, (($rows | ForEach-Object { $_.Idx.Length }   | Measure-Object -Maximum).Maximum))
    $wName  = [Math]::Max('NAME'.Length, (($rows | ForEach-Object { $_.Name.Length }  | Measure-Object -Maximum).Maximum))
    $wProv  = [Math]::Max('PROVISIONING'.Length, (($rows | ForEach-Object { $_.Prov.Length }  | Measure-Object -Maximum).Maximum))
    $wPower = [Math]::Max('POWER'.Length, (($rows | ForEach-Object { "$($_.Power)".Length } | Measure-Object -Maximum).Maximum))

    $header = "  {0}  {1}  {2}  {3}" -f `
        '#'.PadRight($wIdx), 'NAME'.PadRight($wName), 'PROVISIONING'.PadRight($wProv), 'POWER'.PadRight($wPower)
    Write-Host $header -ForegroundColor Cyan
    Write-Host ("  {0}  {1}  {2}  {3}" -f `
        ('-' * $wIdx), ('-' * $wName), ('-' * $wProv), ('-' * $wPower)) -ForegroundColor DarkGray

    foreach ($row in $rows) {
        $provColor  = if ($row.Prov -eq 'Succeeded') { 'Green' } elseif ($row.Prov -eq 'Failed') { 'Red' } else { 'Yellow' }
        $powerColor = if ($row.Power -match 'running') { 'Green' } elseif ($row.Power -match 'deallocated|stopped') { 'DarkGray' } else { 'Yellow' }
        Write-Host ("  {0}  {1}  " -f $row.Idx.PadRight($wIdx), $row.Name.PadRight($wName)) -NoNewline
        Write-Host $row.Prov.PadRight($wProv) -NoNewline -ForegroundColor $provColor
        Write-Host '  ' -NoNewline
        Write-Host $row.Power.PadRight($wPower) -ForegroundColor $powerColor
    }
    Write-Host ""
    $selection = Read-Host "Select VMSS (comma-separated indices/names, or 'all')"

    if ($selection -eq 'all') {
        $targetVmss = $allVmss
    } else {
        $parts = $selection -split ',' | ForEach-Object { $_.Trim() }
        foreach ($part in $parts) {
            if ($part -match '^\d+$') {
                $idx = [int]$part
                if ($idx -ge 0 -and $idx -lt $allVmss.Count) {
                    $targetVmss += $allVmss[$idx]
                } else {
                    Write-Error "Invalid index: $idx"
                    exit 1
                }
            } else {
                if ($part -in $allVmss) {
                    $targetVmss += $part
                } else {
                    Write-Error "VMSS '$part' not found in resource group '$rg'"
                    exit 1
                }
            }
        }
    }
} else {
    # Validate provided VMSS names
    if ($VmssName -eq 'all') {
        $targetVmss = $allVmss
    } else {
        $targetVmss = $VmssName -split ',' | ForEach-Object { $_.Trim() }
        foreach ($vmss in $targetVmss) {
            if ($vmss -notin $allVmss) {
                Write-Error "VMSS '$vmss' not found in resource group '$rg'. Available: $($allVmss -join ', ')"
                exit 1
            }
        }
    }
}

if ($targetVmss.Count -eq 0) {
    Write-Error "No VMSS selected."
    exit 1
}

Write-Host "Target VMSS: $($targetVmss -join ', ')" -ForegroundColor Green

# --- List Action: show per-instance status ---
if ($Action -eq 'list') {
    foreach ($vmss in $targetVmss) {
        Write-Host "`n=== VMSS Instance Status: $vmss ===" -ForegroundColor Cyan

        $instJson = az vmss list-instances --resource-group $rg --name $vmss `
            --expand instanceView -o json 2>$null

        if ($LASTEXITCODE -ne 0 -or -not $instJson) {
            Write-Warning "Failed to query instances for '$vmss'."
            continue
        }

        $instances = @($instJson | ConvertFrom-Json)
        if ($instances.Count -eq 0) {
            Write-Host "  No instances found." -ForegroundColor DarkGray
            continue
        }

        # Also get public IPs for FQDN
        $ipsJson = az vmss list-instance-public-ips --resource-group $rg --name $vmss `
            --query '[].{id:id, ip:ipAddress, fqdn:dnsSettings.fqdn}' -o json 2>$null
        $ipMap = @{}
        if ($LASTEXITCODE -eq 0 -and $ipsJson) {
            $ips = @($ipsJson | ConvertFrom-Json)
            foreach ($ipEntry in $ips) {
                # Extract instance ID from the resource ID path
                if ($ipEntry.id -match '/virtualMachines/(\d+)/') {
                    $ipMap[$Matches[1]] = @{ ip = $ipEntry.ip; fqdn = $ipEntry.fqdn }
                }
            }
        }

        # Build rows
        $rows = foreach ($inst in $instances) {
            $instId = $inst.instanceId
            $provState = $inst.provisioningState
            $powerCode = ($inst.instanceView.statuses | Where-Object { $_.code -match '^PowerState/' } | Select-Object -First 1).code
            $powerState = if ($powerCode) { $powerCode -replace 'PowerState/', '' } else { 'unknown' }
            $ipInfo = $ipMap[$instId]
            $fqdn = if ($ipInfo) { $ipInfo.fqdn } else { '' }
            $ip = if ($ipInfo) { $ipInfo.ip } else { '' }

            [PSCustomObject]@{
                Idx   = $instId
                Power = $powerState
                Prov  = $provState
                IP    = $ip
                FQDN  = $fqdn
            }
        }

        $rows = @($rows | Sort-Object { [int]$_.Idx })

        # Compute column widths
        $wIdx   = [Math]::Max('ID'.Length, (($rows | ForEach-Object { "$($_.Idx)".Length }) | Measure-Object -Maximum).Maximum)
        $wPower = [Math]::Max('POWER'.Length, (($rows | ForEach-Object { $_.Power.Length }) | Measure-Object -Maximum).Maximum)
        $wProv  = [Math]::Max('PROVISIONING'.Length, (($rows | ForEach-Object { $_.Prov.Length }) | Measure-Object -Maximum).Maximum)
        $wIP    = [Math]::Max('IP'.Length, (($rows | ForEach-Object { $_.IP.Length }) | Measure-Object -Maximum).Maximum)

        # Header
        $header = "  {0}  {1}  {2}  {3}  {4}" -f 'ID'.PadRight($wIdx), 'POWER'.PadRight($wPower), 'PROVISIONING'.PadRight($wProv), 'IP'.PadRight($wIP), 'FQDN'
        Write-Host $header -ForegroundColor Cyan
        Write-Host ("  {0}  {1}  {2}  {3}  {4}" -f ('-' * $wIdx), ('-' * $wPower), ('-' * $wProv), ('-' * $wIP), '----') -ForegroundColor DarkGray

        foreach ($row in $rows) {
            $powerColor = switch -Regex ($row.Power) {
                'running'     { 'Green' }
                'deallocated' { 'DarkGray' }
                'stopped'     { 'Yellow' }
                default       { 'Red' }
            }
            $provColor = if ($row.Prov -eq 'Succeeded') { 'Green' } elseif ($row.Prov -eq 'Failed') { 'Red' } else { 'Yellow' }

            Write-Host ("  {0}  " -f "$($row.Idx)".PadRight($wIdx)) -NoNewline
            Write-Host $row.Power.PadRight($wPower) -NoNewline -ForegroundColor $powerColor
            Write-Host '  ' -NoNewline
            Write-Host $row.Prov.PadRight($wProv) -NoNewline -ForegroundColor $provColor
            Write-Host "  $($row.IP.PadRight($wIP))  $($row.FQDN)" -ForegroundColor DarkGray
        }

        # Summary
        $powerSummary = ($rows | Group-Object Power | Sort-Object Name | ForEach-Object { "$($_.Name): $($_.Count)" }) -join ', '
        $provSummary = ($rows | Group-Object Prov | Sort-Object Name | ForEach-Object { "$($_.Name): $($_.Count)" }) -join ', '
        Write-Host "`n  Summary: $powerSummary | $provSummary" -ForegroundColor DarkGray
    }
    Write-Host ""
    return
}

# --- Push-keys: distribute authorized_keys to running instances ---
# Ports the former deploy-keys.ps1 'update' action: reads the public keys from
# 01-resources/security (userKeys + vmKeys per manifest.json) and writes them to
# each selected instance's ~/.ssh/authorized_keys via az vmss run-command.
if ($Action -eq 'push-keys') {
    $securityDir = Join-Path $scriptDir '..\01-resources\security'
    $mfPath = Join-Path $securityDir 'manifest.json'
    if (-not (Test-Path $mfPath)) {
        Write-Error "manifest.json not found: $mfPath"
        exit 1
    }
    $mf = Get-Content $mfPath -Raw | ConvertFrom-Json
    $keyNames = @(@($mf.userKeys) + @($mf.vmKeys) | Where-Object { $_ })

    $pubKeys = @()
    foreach ($name in $keyNames) {
        $pubPath = Join-Path $securityDir "$name.pub"
        if (Test-Path $pubPath) {
            $pubKeys += (Get-Content $pubPath -Raw).Trim()
        } else {
            Write-Warning "Public key not found: $pubPath (run 01-resources/deploy-common-resources.ps1 first)"
        }
    }
    if ($pubKeys.Count -eq 0) {
        Write-Error "No public keys found in $securityDir. Run 01-resources/deploy-common-resources.ps1 first."
        exit 1
    }

    $authorizedKeys = $pubKeys -join "`n"
    $authorizedKeysB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($authorizedKeys))
    $script = "echo '$authorizedKeysB64' | base64 -d > /home/$SshUser/.ssh/authorized_keys && chmod 600 /home/$SshUser/.ssh/authorized_keys && chown $SshUser`:$SshUser /home/$SshUser/.ssh/authorized_keys"

    $anyFail = $false
    foreach ($vmss in $targetVmss) {
        Write-Host "`n=== Pushing keys to VMSS: $vmss ===" -ForegroundColor Cyan
        Write-Host "  Keys: $($pubKeys.Count) ($($keyNames -join ', '))"

        $instancesJson = az vmss list-instances --resource-group $rg --name $vmss `
            --query '[].{id:instanceId, name:osProfile.computerName}' -o json 2>$null | ConvertFrom-Json

        if (-not $instancesJson -or $instancesJson.Count -eq 0) {
            Write-Warning "No instances found for '$vmss'."
            continue
        }

        Write-Host "  Found $($instancesJson.Count) instance(s)"
        foreach ($instance in $instancesJson) {
            Write-Host "  Updating $($instance.name) (instance $($instance.id))..." -NoNewline
            az vmss run-command invoke `
                --resource-group $rg `
                --name $vmss `
                --instance-id $instance.id `
                --command-id RunShellScript `
                --scripts $script `
                --output none 2>$null
            if ($LASTEXITCODE -eq 0) {
                Write-Host " OK" -ForegroundColor Green
            } else {
                Write-Host " FAILED" -ForegroundColor Red
                $anyFail = $true
            }
        }
    }

    Write-Host "`n=== Key push complete ===" -ForegroundColor Green
    exit $(if ($anyFail) { 1 } else { 0 })
}

# --- Ping (accessibility probe) ---
# Probes each instance's SSH port (TCP 22) in parallel and prints a reachability
# table with the instance's public IP and FQDN. Does not log in or run commands.
if ($Action -eq 'ping') {
    $pingPort = 22
    $pingTimeoutMs = 3000
    $anyFailed = $false

    foreach ($vmss in $targetVmss) {
        Write-Host "`n=== VMSS Ping: $vmss ===" -ForegroundColor Cyan

        $ipsJson = az vmss list-instance-public-ips --resource-group $rg --name $vmss `
            --query '[].{id:id, ip:ipAddress, fqdn:dnsSettings.fqdn}' -o json 2>$null

        if ($LASTEXITCODE -ne 0 -or -not $ipsJson) {
            Write-Warning "Failed to get public IPs for VMSS '$vmss'."
            continue
        }

        $instances = @($ipsJson | ConvertFrom-Json)
        if ($instances.Count -eq 0) {
            Write-Host "  No instances with public IPs found." -ForegroundColor DarkGray
            continue
        }

        Write-Host "Probing $($instances.Count) instance(s) on SSH port $pingPort..." -ForegroundColor Yellow

        # Parallel TCP connect probe
        $rows = $instances | ForEach-Object -Parallel {
            $inst = $_
            $ip = $inst.ip
            # Extract numeric instance ID from the resource ID path
            $instId = if ($inst.id -match '/virtualMachines/(\d+)/') { $Matches[1] } else { '?' }

            $reachable = $false
            $latency = ''
            if ($ip) {
                $sw = [System.Diagnostics.Stopwatch]::StartNew()
                try {
                    $tcp = [System.Net.Sockets.TcpClient]::new()
                    $task = $tcp.ConnectAsync($ip, $using:pingPort)
                    if ($task.Wait([TimeSpan]::FromMilliseconds($using:pingTimeoutMs)) -and $tcp.Connected) {
                        $reachable = $true
                        $latency = "$([math]::Round($sw.Elapsed.TotalMilliseconds)) ms"
                    }
                    $tcp.Close()
                } catch {
                    $reachable = $false
                }
                $sw.Stop()
            }

            [PSCustomObject]@{
                Idx       = $instId
                Reachable = $reachable
                Latency   = if ($reachable) { $latency } else { '--' }
                IP        = if ($ip) { $ip } else { '(none)' }
                FQDN      = if ($inst.fqdn) { $inst.fqdn } else { '' }
            }
        } -ThrottleLimit $instances.Count

        $rows = @($rows | Sort-Object { [int]($_.Idx -replace '\D', '0') })

        # Column widths
        $wIdx = [Math]::Max('ID'.Length, (($rows | ForEach-Object { "$($_.Idx)".Length }) | Measure-Object -Maximum).Maximum)
        $wLat = [Math]::Max('LATENCY'.Length, (($rows | ForEach-Object { $_.Latency.Length }) | Measure-Object -Maximum).Maximum)
        $wIP  = [Math]::Max('IP'.Length, (($rows | ForEach-Object { $_.IP.Length }) | Measure-Object -Maximum).Maximum)

        $header = "  {0}  {1}  {2}  {3}  {4}" -f 'ID'.PadRight($wIdx), 'REACHABLE', 'LATENCY'.PadRight($wLat), 'IP'.PadRight($wIP), 'FQDN'
        Write-Host $header -ForegroundColor Cyan
        Write-Host ("  {0}  {1}  {2}  {3}  {4}" -f ('-' * $wIdx), '---------', ('-' * $wLat), ('-' * $wIP), '----') -ForegroundColor DarkGray

        foreach ($row in $rows) {
            $reachLabel = if ($row.Reachable) { 'yes' } else { 'NO' }
            $reachColor = if ($row.Reachable) { 'Green' } else { 'Red' }
            Write-Host ("  {0}  " -f "$($row.Idx)".PadRight($wIdx)) -NoNewline
            Write-Host $reachLabel.PadRight(9) -NoNewline -ForegroundColor $reachColor
            Write-Host ("  {0}  {1}  {2}" -f $row.Latency.PadRight($wLat), $row.IP.PadRight($wIP), $row.FQDN) -ForegroundColor DarkGray
        }

        $reachCount = ($rows | Where-Object { $_.Reachable }).Count
        $unreachCount = $rows.Count - $reachCount
        $summaryColor = if ($unreachCount -gt 0) { 'Yellow' } else { 'Green' }
        Write-Host "`n  Summary: $reachCount/$($rows.Count) reachable$(if ($unreachCount -gt 0) { " ($unreachCount unreachable)" })" -ForegroundColor $summaryColor

        if ($unreachCount -gt 0) {
            $anyFailed = $true
            Write-Host "  Unreachable:" -ForegroundColor Red
            $rows | Where-Object { -not $_.Reachable } | ForEach-Object {
                $label = if ($_.FQDN) { $_.FQDN } else { "instance $($_.Idx)" }
                Write-Host "    $label ($($_.IP))" -ForegroundColor Red
            }
        }
    }

    Write-Host ""
    exit $(if ($anyFailed) { 1 } else { 0 })
}

# --- Power Operations (start / stop / restart) ---
# start/stop/restart are Azure control-plane operations. They do not use SSH,
# instance IPs, or the parallel SSH loop. Executed concurrently across the
# selected VMSS, then summarized. restart targets only failed instances.
if ($Action -in 'start', 'stop', 'restart') {
    $verbLabel = switch ($Action) {
        'start'   { 'Starting' }
        'stop'    { 'Deallocating' }
        'restart' { 'Restarting failed instances in' }
    }

    Write-Host "`n=== Power Operation: $verbLabel VMSS ===" -ForegroundColor Cyan
    Write-Host "  (running across $($targetVmss.Count) VMSS in parallel...)" -ForegroundColor DarkGray

    $powerStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

    $powerResults = $targetVmss | ForEach-Object -Parallel {
        $vmss = $_
        $action = $using:Action
        $rg = $using:rg

        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        $success = $false
        $detail = ''
        $skipped = $false

        if ($action -eq 'restart') {
            # Identify unhealthy instances, then restart only those. An instance is
            # considered failed if any of these hold:
            #   - top-level provisioningState == 'Failed'
            #   - its instanceView has an Error-level status
            #   - an extension reports no status (agent not reporting) or a 'failed' code
            #   - the Application Health extension reports a non-healthy vmHealth state
            $instJson = az vmss list-instances --resource-group $rg --name $vmss `
                --expand instanceView -o json 2>$null

            if ($LASTEXITCODE -ne 0 -or -not $instJson) {
                $detail = 'failed to list instances'
            } else {
                $instances = @($instJson | ConvertFrom-Json)

                $failed = @(foreach ($inst in $instances) {
                    $isFailed = $false

                    if ($inst.provisioningState -eq 'Failed') { $isFailed = $true }

                    $iv = $inst.instanceView
                    if (-not $isFailed -and $iv.statuses) {
                        if ($iv.statuses | Where-Object { $_.level -eq 'Error' }) { $isFailed = $true }
                    }

                    if (-not $isFailed -and $iv.extensions) {
                        foreach ($ext in $iv.extensions) {
                            if (-not $ext.statuses -or ($ext.statuses | Where-Object { $_.code -match 'failed' })) {
                                $isFailed = $true
                                break
                            }
                        }
                    }

                    # Application Health: vmHealth.status.code is 'HealthState/healthy'
                    # when healthy; anything else (unhealthy/unknown) means the instance
                    # is reporting itself as not healthy and should be restarted.
                    if (-not $isFailed -and $iv.vmHealth.status.code -and
                        $iv.vmHealth.status.code -ne 'HealthState/healthy') {
                        $isFailed = $true
                    }

                    if ($isFailed) { $inst.instanceId }
                })

                if ($failed.Count -eq 0) {
                    $success = $true
                    $skipped = $true
                    $detail = 'no failed/unhealthy instances'
                } else {
                    # Map instance IDs -> FQDNs for a friendlier informational message
                    $fqdnMap = @{}
                    $ipsJson = az vmss list-instance-public-ips --resource-group $rg --name $vmss `
                        --query "[].{fqdn:dnsSettings.fqdn, id:id}" -o json 2>$null
                    if ($LASTEXITCODE -eq 0 -and $ipsJson) {
                        foreach ($p in @($ipsJson | ConvertFrom-Json)) {
                            if ($p.id -match '/virtualMachines/(\d+)/') { $fqdnMap[$matches[1]] = $p.fqdn }
                        }
                    }
                    $labels = $failed | ForEach-Object { if ($fqdnMap[$_]) { $fqdnMap[$_] } else { "instance $_" } }

                    Write-Host "  [$vmss] restarting $($failed.Count) failed/unhealthy instance(s):" -ForegroundColor Yellow
                    $labels | ForEach-Object { Write-Host "    $_" -ForegroundColor Yellow }

                    # Fire-and-forget: --no-wait returns immediately instead of blocking
                    # on the restart LRO (instances with a non-reporting agent may never
                    # converge, which would otherwise hang the command).
                    $output = az vmss restart --resource-group $rg --name $vmss --instance-ids $failed --no-wait 2>&1
                    $success = ($LASTEXITCODE -eq 0)
                    $detail = "restart requested for: $($labels -join ', ')"
                    if (-not $success) { $detail += " | $($output -join ' ')" }
                }
            }
        } else {
            $azVerb = if ($action -eq 'start') { 'start' } else { 'deallocate' }
            # Fire-and-forget: --no-wait returns immediately instead of blocking on
            # the power LRO (which can hang if instances fail to converge).
            $output = az vmss $azVerb --resource-group $rg --name $vmss --no-wait 2>&1
            $success = ($LASTEXITCODE -eq 0)
            if ($success) { $detail = "$azVerb requested" }
            if (-not $success) { $detail = ($output -join ' ') }
        }

        $sw.Stop()

        [PSCustomObject]@{
            Vmss     = $vmss
            Success  = $success
            Skipped  = $skipped
            Detail   = $detail
            Duration = $sw.Elapsed.ToString('mm\:ss\.ff')
        }
    } -ThrottleLimit $targetVmss.Count

    $powerStopwatch.Stop()

    # Per-VMSS report
    foreach ($r in ($powerResults | Sort-Object Vmss)) {
        if ($r.Success -and $r.Skipped) {
            Write-Host "  $($r.Vmss): $($r.Detail) [$($r.Duration)]" -ForegroundColor DarkGray
        } elseif ($r.Success) {
            $suffix = if ($r.Detail) { " ($($r.Detail))" } else { '' }
            Write-Host "  $($r.Vmss): done$suffix [$($r.Duration)]" -ForegroundColor Green
        } else {
            Write-Host "  $($r.Vmss): FAILED [$($r.Duration)]" -ForegroundColor Red
            if ($r.Detail) { Write-Host "    $($r.Detail)" -ForegroundColor DarkGray }
        }
    }

    $pOk = ($powerResults | Where-Object { $_.Success }).Count
    $pFail = $powerResults.Count - $pOk

    Write-Host "`n=== Summary ===" -ForegroundColor Cyan
    Write-Host "Elapsed: $($powerStopwatch.Elapsed.ToString('mm\:ss\.ff'))" -ForegroundColor DarkGray
    Write-Host "Total VMSS: $($powerResults.Count)"
    Write-Host "  Success: $pOk" -ForegroundColor Green
    Write-Host "  Failed:  $pFail" -ForegroundColor $(if ($pFail -gt 0) { 'Red' } else { 'Gray' })

    if ($pFail -gt 0) {
        Write-Host "`nFailed VMSS:"
        $powerResults | Where-Object { -not $_.Success } | ForEach-Object {
            Write-Host "  $($_.Vmss)" -ForegroundColor Red
        }
    }

    Write-Host ""
    exit $(if ($pFail -gt 0) { 1 } else { 0 })
}

# --- SSH Key Resolution ---
$sshKey = $null
$manifestPath = Join-Path $scriptDir "..\01-resources\security\manifest.json"

if (Test-Path $manifestPath) {
    $manifest = Get-SshKeyManifest -ManifestPath $manifestPath
    $sshKey = Resolve-SshUserPrivateKey -Manifest $manifest -AllowMissing
}

if (-not $sshKey) {
    # Fallback to default
    $defaultKey = Join-Path $env:USERPROFILE ".ssh\id_ed25519"
    if (Test-Path $defaultKey) {
        $sshKey = $defaultKey
    } else {
        Write-Error "No SSH key found. Checked manifest.json and $defaultKey"
        exit 1
    }
}

Write-Host "Using SSH key: $sshKey" -ForegroundColor DarkGray

# --- Build SSH Command based on Action ---
# Git pull strategy: fast-forward only or force reset
# $branch param is optional; when provided, force uses that explicit branch instead of detecting HEAD
function Get-GitPull([string]$branch = '', [string]$ref = '') {
    if ($ref) {
        # Explicit commit/tag/branch: fetch everything then hard-reset onto it.
        $fetch = "for i in 1 2 3; do git fetch --all && break || sleep 2; done"
        return "$fetch && git reset --hard $ref"
    }
    if ($Force) {
        $target = if ($branch) { "origin/$branch" } else { 'origin/$(git rev-parse --abbrev-ref HEAD)' }
        $fetch = "for i in 1 2 3; do git fetch --all && break || sleep 2; done"
        return "$fetch && git reset --hard $target"
    } else {
        return "for i in 1 2 3; do git pull --ff-only && break || sleep 2; done"
    }
}

function Get-ToolsUpdateCommand {
    param(
        [string]$User,
        [ValidateSet('Copy', 'Run')][string]$Mode
    )

    $bootstrap = @'
$ErrorActionPreference = 'Stop'
$homeDir = '/home/__USER__'
$toolsDir = "$homeDir/tools"
$updateScript = "$toolsDir/update.ps1"

if (Test-Path $updateScript) {
    if ('__MODE__' -eq 'Run') { & $updateScript -Fetch -Run }
    else { & $updateScript -Fetch -Copy }
    exit $LASTEXITCODE
}

Write-Host 'Updater missing; bootstrapping tools bundle from Key Vault...'
$envFile = '/opt/deploy-actions/keyvault.env'
if (-not (Test-Path $envFile)) { throw "Missing $envFile" }
$vaultLine = Select-String -Path $envFile -Pattern '^VAULT_NAME=' | Select-Object -First 1
if (-not $vaultLine) { throw "VAULT_NAME is missing from $envFile" }
$vaultName = ($vaultLine.Line -split '=', 2)[1].Trim()

$token = Invoke-RestMethod `
    -Uri 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://vault.azure.net' `
    -Headers @{ Metadata = 'true' } -TimeoutSec 15
$secret = Invoke-RestMethod `
    -Uri "https://$vaultName.vault.azure.net/secrets/tools-sas-url?api-version=7.4" `
    -Headers @{ Authorization = "Bearer $($token.access_token)" } -TimeoutSec 15
if (-not $secret.value) { throw "Key Vault secret 'tools-sas-url' is empty" }

$tarball = "/tmp/tools-$([Environment]::UserName).tar.gz"
Invoke-WebRequest -Uri $secret.value -OutFile $tarball -TimeoutSec 120
New-Item -ItemType Directory -Path $homeDir -Force | Out-Null
tar -xzf $tarball -C $homeDir
if ($LASTEXITCODE -ne 0 -or -not (Test-Path $updateScript)) {
    throw 'Tools bundle extraction did not restore update.ps1'
}

if ('__MODE__' -eq 'Run') { & $updateScript -Run }
else { & $updateScript -Copy }
exit $LASTEXITCODE
'@

    $bootstrap = $bootstrap.Replace('__USER__', $User).Replace('__MODE__', $Mode)
    $encoded = [Convert]::ToBase64String([System.Text.Encoding]::Unicode.GetBytes($bootstrap))
    return "pwsh -NoProfile -EncodedCommand $encoded"
}

function Get-SshCommand {
    param([string]$Action, [string]$System)

    switch ($Action) {
        'refresh' {
            # Read repos from manifest.json
            $manifestFile = Join-Path $scriptDir "tools\manifest.json"
            if (-not (Test-Path $manifestFile)) {
                Write-Error "manifest.json not found at $manifestFile"
                exit 1
            }
            $nodeManifest = Get-Content $manifestFile -Raw | ConvertFrom-Json

            if (-not $nodeManifest.repos) {
                Write-Error "No repos section in manifest.json"
                exit 1
            }

            $pullCommands = $nodeManifest.repos | ForEach-Object {
                $branch = if ($_.branch -is [array]) { $_.branch[0] } else { $_.branch }
                $pull = Get-GitPull -branch $branch
                "cd $($_.path) && echo '[$($_.name)]:' && $pull"
            }

            $dnsRestart = "sudo resolvectl flush-caches 2>/dev/null; sudo systemctl restart systemd-resolved 2>/dev/null; nslookup github.com >/dev/null 2>&1 || sleep 3"
            return "$dnsRestart; " + ($pullCommands -join '; ')
        }

        'rebuild' {
            # Read manifest.json to get build arguments
            $manifestFile = Join-Path $scriptDir "tools\manifest.json"
            if (-not (Test-Path $manifestFile)) {
                Write-Error "tools/manifest.json not found"
                exit 1
            }

            $nodeManifest = Get-Content $manifestFile -Raw | ConvertFrom-Json
            $buildEntry = $nodeManifest.runcmd | Where-Object {
                $_.run -eq 'build.ps1' -and $_.args -match "^$System\b"
            } | Select-Object -First 1

            if (-not $buildEntry) {
                Write-Error "No build entry found in manifest for system '$System'"
                exit 1
            }

            $buildArgs = $buildEntry.args

            # Map system to repo (resp-bench is built from garnet)
            $repoName = switch ($System) {
                'resp-bench' { 'garnet' }  # resp-bench is part of garnet repo
                default      { $System }
            }

            # Get repo entry
            $repoEntry = $nodeManifest.repos | Where-Object { $_.name -eq $repoName } | Select-Object -First 1
            if (-not $repoEntry) {
                Write-Error "Could not find repo '$repoName' in manifest.json repos section"
                exit 1
            }

            # Resolve branch: index into repos[].branch array, or use string directly
            $branchField = $repoEntry.branch
            if ($buildEntry.PSObject.Properties['branch'] -and $null -ne $buildEntry.branch -and $branchField -is [array]) {
                $buildBranch = $branchField[$buildEntry.branch]
            } elseif ($branchField -is [array]) {
                $buildBranch = $branchField[0]
            } else {
                $buildBranch = $branchField
            }

            # Append branch to build args (skipped for -NoPull/-Ref so build.ps1 does
            # not fetch/checkout/reset --hard, which would discard local changes or
            # override the explicit ref we just checked out)
            if (-not $NoPull -and -not $Ref -and $buildArgs -match '^\s*\S+\s*$') {
                $buildArgs = "$($buildArgs.Trim()) $buildBranch"
            }

            $repoPath = $repoEntry.path
            $dnsWarmup = "nslookup github.com >/dev/null 2>&1 || sleep 3"
            if ($Ref) {
                # Fetch + hard-reset to an explicit commit/tag/branch, then build the
                # checked-out tree (build.ps1 gets no branch arg so it won't re-checkout).
                $pull = Get-GitPull -ref $Ref
                return "$dnsWarmup; cd $repoPath && echo '[git checkout $Ref]' && $pull && echo '[build]' && sudo /opt/deploy-actions/build.ps1 $buildArgs"
            }
            if ($NoPull) {
                return "cd $repoPath && echo '[build (no pull)]' && sudo /opt/deploy-actions/build.ps1 $buildArgs"
            }
            $pull = Get-GitPull -branch $buildBranch
            return "$dnsWarmup; cd $repoPath && echo '[git pull]' && $pull && echo '[build]' && sudo /opt/deploy-actions/build.ps1 $buildArgs"
        }

        'deploy' {
            return Get-ToolsUpdateCommand -User $SshUser -Mode Run
        }

        'install' {
            return Get-ToolsUpdateCommand -User $SshUser -Mode Copy
        }

        'discover' {
            $maxScanFlag = if ($MaxScan -gt 0) { " -MaxScan $MaxScan" } else { "" }
            return "pwsh /home/$SshUser/tools/cluster/cluster-deploy.ps1 -Action discover$maxScanFlag"
        }
    }
}

$sshCommand = Get-SshCommand -Action $Action -System $System
$sshCommandDisplay = if ($Action -eq 'install') {
    'bootstrap/fetch tools bundle, then copy deployed scripts'
} elseif ($Action -eq 'deploy') {
    'bootstrap/fetch tools bundle, then run the deployment workflow'
} else {
    $sshCommand
}

Write-Host "`n=== SSH Command ===" -ForegroundColor Cyan
Write-Host $sshCommandDisplay -ForegroundColor DarkGray
Write-Host ""

# --- Execute on all VMSS ---
$allResults = @()
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

foreach ($vmss in $targetVmss) {
    Write-Host "`n=== Processing VMSS: $vmss ===" -ForegroundColor Cyan

    # Get instance public IPs
    $ipsJson = az vmss list-instance-public-ips `
        --resource-group $rg `
        --name $vmss `
        --query '[].{instance:name, ip:ipAddress, fqdn:dnsSettings.fqdn}' -o json 2>$null

    if ($LASTEXITCODE -ne 0 -or -not $ipsJson) {
        Write-Warning "Failed to get public IPs for VMSS '$vmss'. Skipping."
        continue
    }

    $instances = $ipsJson | ConvertFrom-Json

    if ($instances.Count -eq 0) {
        Write-Warning "No instances with public IPs found in VMSS '$vmss'. Skipping."
        continue
    }

    Write-Host "Found $($instances.Count) instance(s) with public IPs" -ForegroundColor Yellow

    # Execute SSH command on each instance
    $sshOpts = @('-n', '-o', 'ConnectTimeout=10', '-o', 'StrictHostKeyChecking=no', '-o', 'BatchMode=yes')
    $totalInstances = $instances.Count

    # Parallel execution with progress bar
    $parallelStopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $parallelProgress = [hashtable]::Synchronized(@{ done = 0; failed = 0 })

    $parallelJob = $instances | ForEach-Object -Parallel {
        $instance = $_
        $ip = $instance.ip
        $instanceName = $instance.instance
        $fqdn = $instance.fqdn

        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        $outputText = ""
        $success = $false

        try {
            $dnsCmd = "sudo systemctl restart systemd-resolved 2>/dev/null; "
            $output = ssh -i $using:sshKey $using:sshOpts "${using:SshUser}@${ip}" "$dnsCmd$($using:sshCommand)" 2>&1
            $exitCode = $LASTEXITCODE
            $outputText = $output -join "`n"
            $success = ($exitCode -eq 0)
        } catch {
            $outputText = $_.Exception.Message
        }
        $sw.Stop()
        $duration = $sw.Elapsed.ToString('mm\:ss\.ff')

        $p = $using:parallelProgress
        $p.done++
        if (-not $success) { $p.failed++ }

        [PSCustomObject]@{
            Vmss     = $using:vmss
            Instance = $instanceName
            IP       = $ip
            FQDN     = $fqdn
            Success  = $success
            Output   = $outputText
            Duration = $duration
        }
    } -ThrottleLimit $totalInstances -AsJob

    Wait-ParallelJob -Job $parallelJob -Progress $parallelProgress -Total $totalInstances -Stopwatch $parallelStopwatch -Label "Processing"

    $results = $parallelJob | Receive-Job -Wait
    Remove-Job $parallelJob
    $parallelStopwatch.Stop()

    $pSuccess = ($results | Where-Object { $_.Success }).Count
    $pFailed = $results.Count - $pSuccess
    Write-Host "  ✓ Complete: $pSuccess/$($results.Count) succeeded | Elapsed: $($parallelStopwatch.Elapsed.ToString('mm\:ss\.ff'))" -ForegroundColor $(if ($pFailed -gt 0) { 'Yellow' } else { 'Green' })

    if ($pFailed -gt 0) {
        Write-Host "  Failed:" -ForegroundColor Red
        $results | Where-Object { -not $_.Success } | ForEach-Object {
            $label = if ($_.FQDN) { $_.FQDN } else { $_.Instance }
            Write-Host "    $label ($($_.IP)) [$($_.Duration)]" -ForegroundColor Red
            $reason = $_.Output -split "`n" | Where-Object {
                $_ -match 'Permission denied|not recognized|ERROR:|Error:|failed|timed out|Connection refused|No route|Host key'
            } | Select-Object -First 1
            if ($reason) { Write-Host "      $($reason.Trim())" -ForegroundColor DarkRed }
        }
    }

    # Verbose: show per-instance output
    $isVerbose = $VerbosePreference -ne 'SilentlyContinue'
    if ($isVerbose) {
        Write-Host ""
        $idx = 0
        foreach ($r in $results) {
            $idx++
            $color = if ($r.Success) { 'Green' } else { 'Red' }
            $label = if ($r.FQDN) { $r.FQDN } else { $r.Instance }
            Write-Host "  $idx/$totalInstances >>> $label ($($r.IP)) [$($r.Duration)]" -ForegroundColor $color
            if ($r.Output) {
                $r.Output -split "`n" | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }
            }
        }
    }

    $allResults += $results
}

# --- Summary ---
$stopwatch.Stop()
$elapsed = $stopwatch.Elapsed
Write-Host "`n=== Summary ===" -ForegroundColor Cyan
Write-Host "Elapsed: $($elapsed.ToString('mm\:ss\.ff'))" -ForegroundColor DarkGray

$successCount = ($allResults | Where-Object { $_.Success }).Count
$failCount = $allResults.Count - $successCount

Write-Host "Total instances: $($allResults.Count)"
Write-Host "  Success: $successCount" -ForegroundColor Green
Write-Host "  Failed:  $failCount" -ForegroundColor $(if ($failCount -gt 0) { 'Red' } else { 'Gray' })

if ($failCount -gt 0) {
    Write-Host "`nFailed instances:"
    $allResults | Where-Object { -not $_.Success } | ForEach-Object {
        $label = if ($_.FQDN) { $_.FQDN } else { "$($_.Vmss)/$($_.Instance)" }
        Write-Host "  $label ($($_.IP))" -ForegroundColor Red
    }
}

Write-Host ""

exit $(if ($failCount -gt 0) { 1 } else { 0 })
