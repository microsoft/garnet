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
    Personal SSH public key name (desktop -> VM). If omitted, you are prompted
    (default id_ed25519_user).

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
#>
[CmdletBinding()]
param(
    [string]$SshUserKey,
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
    $answer = Read-Host "Personal SSH public key name (desktop -> VM) [$suggest]"
    $SshUserKey = if ([string]::IsNullOrWhiteSpace($answer)) { $suggest } else { $answer }
}
if (-not $SshVmKey) {
    $suggestVm = 'id_ed25519_vmss'
    $answerVm = Read-Host "VMSS inter-node SSH key name (VM -> VM) [$suggestVm]"
    $SshVmKey = if ([string]::IsNullOrWhiteSpace($answerVm)) { $suggestVm } else { $answerVm }
}

$content = Get-Content -Raw -LiteralPath $template
$content = $content.Replace('__SSH_USER_KEY__', $SshUserKey).Replace('__SSH_VM_KEY__', $SshVmKey)
[System.IO.File]::WriteAllText($manifest, $content)

Write-Host "Wrote $manifest" -ForegroundColor Green
Write-Host "  userKeys -> $SshUserKey"
Write-Host "  vmKeys   -> $SshVmKey"
Write-Host ""
Write-Host "manifest.json is git-ignored; your key names are not committed." -ForegroundColor Cyan
