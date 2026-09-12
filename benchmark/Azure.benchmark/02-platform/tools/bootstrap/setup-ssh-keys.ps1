#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Install the VMSS SSH private key from Azure Key Vault (managed identity).

.DESCRIPTION
    PowerShell port of setup-ssh-keys.sh. Targets Linux VMSS instances.
    The public key is already in authorized_keys via osProfile.ssh.publicKeys;
    this installs the private key so VMs can SSH to each other. Runs with a
    delay + retry to allow Key Vault access policy to propagate.
#>

$ErrorActionPreference = 'Stop'

# --- Load shared config.env ---
$configFile = '/opt/deploy-actions/config.env'
if (Test-Path $configFile) {
    Get-Content $configFile | ForEach-Object {
        if ($_ -match '^\s*([A-Z_]+)="?([^"]*)"?\s*$' -and $_ -notmatch '^\s*#') {
            Set-Variable -Name $Matches[1] -Value $Matches[2] -Scope Script
        }
    }
}

# Override VAULT_NAME from cloud-init injected file (set at deploy time by Bicep)
$kvFile = '/opt/deploy-actions/keyvault.env'
if (Test-Path $kvFile) {
    Get-Content $kvFile | ForEach-Object {
        if ($_ -match '^\s*([A-Z_]+)="?([^"]*)"?\s*$' -and $_ -notmatch '^\s*#') {
            Set-Variable -Name $Matches[1] -Value $Matches[2] -Scope Script
        }
    }
}

$sshDir = "$USER_HOME/.ssh"
$maxRetries = 5
$retryDelay = 15

Start-Sleep -Seconds $retryDelay

for ($attempt = 1; $attempt -le $maxRetries; $attempt++) {
    Write-Host "Attempt $attempt/${maxRetries}: Fetching SSH key from Key Vault..."

    $tokenUrl = 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://vault.azure.net'
    $tokenResp = & curl -s -H 'Metadata:true' $tokenUrl
    $token = $null
    if ($tokenResp) {
        try { $token = ($tokenResp | ConvertFrom-Json).access_token } catch { $token = $null }
    }

    if (-not $token -or $token -eq 'null') {
        Write-Host "WARNING: Failed to get managed identity token. Retrying in ${retryDelay}s..."
        Start-Sleep -Seconds $retryDelay
        continue
    }

    $secretUrl = "https://${VAULT_NAME}.vault.azure.net/secrets/${SSH_SECRET_NAME}?api-version=7.4"
    $secretResp = & curl -s -H "Authorization: Bearer $token" $secretUrl
    $privateKey = $null
    if ($secretResp) {
        try { $privateKey = ($secretResp | ConvertFrom-Json).value } catch { $privateKey = $null }
    }

    if ($privateKey -and $privateKey -ne 'null') {
        $keyPath = "$sshDir/id_ed25519"
        $privateKey.TrimEnd("`n") + "`n" | Set-Content -Path $keyPath -NoNewline
        & chown "${DEPLOY_USER}:${DEPLOY_USER}" $keyPath
        & chmod 600 $keyPath

        # Derive public key and ensure it's in authorized_keys
        $pubKey = (& ssh-keygen -y -f $keyPath 2>$null) -join "`n"
        if ($pubKey) {
            $authFile = "$sshDir/authorized_keys"
            $existing = if (Test-Path $authFile) { Get-Content $authFile -Raw } else { '' }
            if ($existing -notmatch [regex]::Escape($pubKey)) {
                Add-Content -Path $authFile -Value $pubKey
            }
            & chown "${DEPLOY_USER}:${DEPLOY_USER}" $authFile
            & chmod 600 $authFile
        }

        # Derive SSH Host pattern from VNET_PREFIX (e.g., 10.5.0.0/16 -> 10.5.*)
        $vnetBase = ($VNET_PREFIX -split '/')[0]
        $octets = $vnetBase -split '\.'
        $vnetCidr = [int](($VNET_PREFIX -split '/')[1])
        if ($vnetCidr -le 16) {
            $sshHostPattern = "$($octets[0]).$($octets[1]).*"
        }
        else {
            $sshHostPattern = "$($octets[0]).$($octets[1]).$($octets[2]).*"
        }

        $configContent = @(
            "Host $sshHostPattern",
            '  StrictHostKeyChecking no',
            '  UserKnownHostsFile /dev/null'
        ) -join "`n"
        Set-Content -Path "$sshDir/config" -Value $configContent
        & chown "${DEPLOY_USER}:${DEPLOY_USER}" "$sshDir/config"
        & chmod 644 "$sshDir/config"

        Write-Host 'SSH key setup complete. Inter-VM SSH enabled.'
        exit 0
    }

    Write-Host "WARNING: Failed to fetch key. Retrying in ${retryDelay}s..."
    Start-Sleep -Seconds $retryDelay
}

Write-Host "ERROR: Could not fetch SSH key after $maxRetries attempts. Run post-deploy.ps1 manually."
exit 0
