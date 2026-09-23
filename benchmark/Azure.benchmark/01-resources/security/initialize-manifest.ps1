<#
.SYNOPSIS
    Creates your per-user SSH key manifest from the tracked template.

.DESCRIPTION
    Copies manifest.template.json (in this folder) to the git-ignored manifest.json
    and fills in your SSH key names. The manifest declares the desktop-to-VM key
    (userKeys) and the VM-to-VM key (vmKeys) that the deploy scripts consume. Run
    once after cloning.

    manifest.json is git-ignored, so your personal key names are never committed.

.PARAMETER SshUserKey
    Personal SSH public key name(s), desktop -> VM. Accepts multiple, comma-separated
    or as an array. If omitted, you are prompted (default id_ed25519_user).

.PARAMETER SshVmKey
    VMSS inter-node SSH key name (VM -> VM). If omitted, you are prompted
    (default id_ed25519_vmss).

.PARAMETER Force
    Overwrite an existing manifest.json. Without it, the script refuses to clobber
    a manifest you have already personalized.

.EXAMPLE
    .\initialize-manifest.ps1

.EXAMPLE
    .\initialize-manifest.ps1 -SshUserKey id_ed25519_alice -SshVmKey id_ed25519_vmss

.EXAMPLE
    .\initialize-manifest.ps1 -SshUserKey id_ed25519_desktop,id_ed25519_laptop
#>
[CmdletBinding()]
param(
    [string[]]$SshUserKey,
    [string]$SshVmKey,
    [switch]$Force
)

$ErrorActionPreference = 'Stop'
$template = Join-Path $PSScriptRoot 'manifest.template.json'
$manifest = Join-Path $PSScriptRoot 'manifest.json'

if (-not (Test-Path -LiteralPath $template -PathType Leaf)) {
    throw "Template not found: $template"
}
if ((Test-Path -LiteralPath $manifest -PathType Leaf) -and -not $Force) {
    throw "manifest.json already exists: $manifest. Edit it directly, or pass -Force to overwrite."
}

if (-not $SshUserKey) {
    $suggest = 'id_ed25519_user'
    $answer = Read-Host "Personal SSH public key name(s), comma-separated (desktop -> VM) [$suggest]"
    $SshUserKey = if ([string]::IsNullOrWhiteSpace($answer)) { $suggest } else { $answer }
}
# Normalize: split any comma-separated entries so each key becomes its own array element.
$SshUserKey = @($SshUserKey | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ })
if ($SshUserKey.Count -eq 0) {
    throw "At least one personal SSH key name is required."
}
if (-not $SshVmKey) {
    $suggestVm = 'id_ed25519_vmss'
    $answerVm = Read-Host "VMSS inter-node SSH key name (VM -> VM) [$suggestVm]"
    $SshVmKey = if ([string]::IsNullOrWhiteSpace($answerVm)) { $suggestVm } else { $answerVm }
}
$SshVmKey = $SshVmKey.Trim()

# Start from the template so basePath (and any future fields) are preserved, then
# override the key entries. userKeys is written as a JSON array (one entry per key).
$manifestObj = Get-Content -Raw -LiteralPath $template | ConvertFrom-Json
$manifestObj.userKeys = @($SshUserKey)
$manifestObj.vmKeys = $SshVmKey
$json = $manifestObj | ConvertTo-Json -Depth 5
[System.IO.File]::WriteAllText($manifest, $json + [Environment]::NewLine)

Write-Host "Wrote $manifest" -ForegroundColor Green
Write-Host "  userKeys -> $($SshUserKey -join ', ')"
Write-Host "  vmKeys   -> $SshVmKey"
Write-Host ""
Write-Host "manifest.json is git-ignored; your key names are not committed." -ForegroundColor Cyan
