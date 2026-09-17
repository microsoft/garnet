#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Clone the public repositories defined in manifest.json.

.EXAMPLE
    clone-repos.ps1
#>

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
$deploymentEnv = "/opt/deploy-actions/deployment.env"
$workloadProfile = "benchmark"
if (Test-Path $deploymentEnv) {
    Get-Content $deploymentEnv | ForEach-Object {
        if ($_ -match '^WORKLOAD_PROFILE="?([^"]+)"?$') { $workloadProfile = $Matches[1] }
    }
}

if (-not $manifest.repos) {
    Write-Host "No repos section in manifest.json" -ForegroundColor Yellow
    exit 0
}

foreach ($repo in $manifest.repos) {
    $repoProfiles = @(
        if ($repo.PSObject.Properties['profiles']) { $repo.profiles } else { 'benchmark' }
    )
    if ($repoProfiles -notcontains $workloadProfile) {
        continue
    }

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

    Write-Host "Cloning $name -> $target (branch: $branch)"
    sudo -u $deployUser git clone @branchArgs $url $target
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  ERROR: Failed to clone $name" -ForegroundColor Red
    }
}

Write-Host "Clone complete." -ForegroundColor Green
