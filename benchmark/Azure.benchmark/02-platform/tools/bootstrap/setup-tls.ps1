#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Install role-specific AzureBench TLS material from Key Vault.
#>
param(
    [string]$VaultName,
    [string]$OutputDirectory = '/opt/azurebench/tls'
)

$ErrorActionPreference = 'Stop'

if ([Environment]::UserName -ne 'root') {
    throw 'setup-tls.ps1 must run as root.'
}

$config = @{}
foreach ($path in @('/opt/deploy-actions/config.env', '/opt/deploy-actions/keyvault.env', '/opt/deploy-actions/deployment.env')) {
    if (-not (Test-Path $path)) { continue }
    Get-Content $path | ForEach-Object {
        if ($_ -match '^\s*([A-Z_]+)="?([^"]*)"?\s*$' -and $_ -notmatch '^\s*#') {
            $config[$Matches[1]] = $Matches[2]
        }
    }
}

if (-not $VaultName) { $VaultName = $config.VAULT_NAME }
$role = $config.DEPLOYMENT_ROLE
$deployUser = $config.DEPLOY_USER
if (-not $VaultName) { throw 'VAULT_NAME is missing from /opt/deploy-actions/keyvault.env.' }
if ($role -notin @('server', 'client')) { throw "DEPLOYMENT_ROLE must be 'server' or 'client'." }
if (-not $deployUser) { $deployUser = 'guser' }

$token = Invoke-RestMethod `
    -Uri 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://vault.azure.net' `
    -Headers @{ Metadata = 'true' } -TimeoutSec 15
if (-not $token.access_token) { throw 'Managed identity did not return a Key Vault access token.' }
$headers = @{ Authorization = "Bearer $($token.access_token)" }

function Get-KeyVaultSecretValue {
    param([string]$Name)
    $response = Invoke-RestMethod `
        -Uri "https://$VaultName.vault.azure.net/secrets/$Name`?api-version=7.4" `
        -Headers $headers -TimeoutSec 30
    if ([string]::IsNullOrWhiteSpace([string]$response.value)) {
        throw "Key Vault secret '$Name' is empty."
    }
    return [string]$response.value
}

$metadataJson = Get-KeyVaultSecretValue 'azurebench-tls-metadata'
$metadata = $metadataJson | ConvertFrom-Json
if ($metadata.schemaVersion -ne 1 -or $metadata.status -ne 'complete' -or -not $metadata.targetHost) {
    throw 'Key Vault TLS metadata is incomplete or unsupported.'
}

$pfxSecret = "azurebench-tls-$role-pfx"
$passwordSecret = "azurebench-tls-$role-password"
$pfxBytes = [Convert]::FromBase64String((Get-KeyVaultSecretValue $pfxSecret))
$password = Get-KeyVaultSecretValue $passwordSecret
$caCertificate = Get-KeyVaultSecretValue 'azurebench-tls-ca-certificate'

New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
$pfxPath = Join-Path $OutputDirectory "$role.pfx"
$passwordPath = Join-Path $OutputDirectory "$role.password"
$caPath = Join-Path $OutputDirectory 'ca.crt'
$metadataPath = Join-Path $OutputDirectory 'metadata.json'

[System.IO.File]::WriteAllBytes("$pfxPath.tmp", $pfxBytes)
[System.IO.File]::WriteAllText("$passwordPath.tmp", $password, [System.Text.UTF8Encoding]::new($false))
[System.IO.File]::WriteAllText("$caPath.tmp", $caCertificate, [System.Text.UTF8Encoding]::new($false))
[System.IO.File]::WriteAllText("$metadataPath.tmp", $metadataJson, [System.Text.UTF8Encoding]::new($false))
Move-Item "$pfxPath.tmp" $pfxPath -Force
Move-Item "$passwordPath.tmp" $passwordPath -Force
Move-Item "$caPath.tmp" $caPath -Force
Move-Item "$metadataPath.tmp" $metadataPath -Force

& chown -R "root:$deployUser" $OutputDirectory
if ($LASTEXITCODE -ne 0) { throw "Failed to set ownership on '$OutputDirectory'." }
& chmod 0750 $OutputDirectory
& chmod 0640 $pfxPath $passwordPath $caPath $metadataPath
if ($LASTEXITCODE -ne 0) { throw "Failed to set permissions on TLS material." }

Write-Host "Installed $role TLS material for target '$($metadata.targetHost)' in $OutputDirectory."
