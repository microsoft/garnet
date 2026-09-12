#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Clone repositories defined in manifest.json.
    Optionally fetches a GitHub PAT from Azure Key Vault for private repos.

.EXAMPLE
    clone-repos.ps1
    clone-repos.ps1 -Vault "my-keyvault"
    clone-repos.ps1 -Vault "my-keyvault" -SecretName "github-pat"
#>
param(
    [string]$Vault = '',
    [string]$SecretName = 'github-pat'
)

$ErrorActionPreference = "Stop"

# Read DEPLOY_USER from config.env
$configEnv = "/opt/deploy-actions/config.env"
$deployUser = "guser"
if (Test-Path $configEnv) {
    Get-Content $configEnv | ForEach-Object {
        if ($_ -match '^DEPLOY_USER="?([^"]+)"?$') { $deployUser = $Matches[1] }
    }
}

# Read manifest
$ManifestPath = "/home/$deployUser/tools/manifest.json"
if (-not (Test-Path $ManifestPath)) {
    Write-Host "ERROR: manifest.json not found at $ManifestPath" -ForegroundColor Red
    exit 1
}
$manifest = Get-Content $ManifestPath -Raw | ConvertFrom-Json

if (-not $manifest.repos) {
    Write-Host "No repos section in manifest.json" -ForegroundColor Yellow
    exit 0
}

# Fetch PAT from Key Vault if vault name provided
$PAT = ''
if ($Vault) {
    try {
        $tokenResponse = Invoke-RestMethod -Uri 'http://169.254.169.254/metadata/identity/oauth2/token?resource=https://vault.azure.net&api-version=2018-02-01' -Headers @{ Metadata = 'true' }
        $secretResponse = Invoke-RestMethod -Uri "https://$Vault.vault.azure.net/secrets/$SecretName`?api-version=7.4" -Headers @{ Authorization = "Bearer $($tokenResponse.access_token)" }
        $PAT = $secretResponse.value
    } catch {
        Write-Host "WARNING: Failed to fetch PAT from Key Vault: $_" -ForegroundColor Yellow
    }
}

foreach ($repo in $manifest.repos) {
    $target = $repo.path
    $url = $repo.url
    $branch = if ($repo.branch -is [array]) { $repo.branch[0] } else { $repo.branch }
    $name = $repo.name

    if (Test-Path $target) {
        Write-Host "Skipping $name ($target already exists)"
        continue
    }

    $branchArgs = @()
    if ($branch) { $branchArgs = @("--branch", $branch) }

    $cloneUrl = $url
    if ($repo.PSObject.Properties['visibility'] -and $repo.visibility -eq 'private' -and $PAT) {
        $cloneUrl = $url -replace 'https://', "https://x-access-token:${PAT}@"
    }

    Write-Host "Cloning $name -> $target (branch: $branch)"
    sudo -u $deployUser git clone @branchArgs $cloneUrl $target
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  ERROR: Failed to clone $name" -ForegroundColor Red
    }
}

Write-Host "Clone complete." -ForegroundColor Green
