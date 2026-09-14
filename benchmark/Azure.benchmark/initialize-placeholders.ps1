<#
.SYNOPSIS
    Fills the placeholder tokens in the Azure.benchmark tree with your own values.

.DESCRIPTION
    The Azure.benchmark suite ships with neutral placeholder tokens instead of any
    personal or account-specific identifiers, so nothing tied to a specific person
    or subscription is committed to the public repository. Run this script once,
    after cloning, to substitute the placeholders with values for your environment.

    Supported placeholders:
      __SSH_USER_KEY__   Name of your personal SSH public key in the manifest
                         basePath (normally %USERPROFILE%\.ssh), e.g. id_ed25519_user.
      __SSH_VM_KEY__     Name of the VMSS inter-node SSH key in the manifest
                         basePath, used for VM-to-VM SSH (default id_ed25519_vmss).

    The script rewrites files in place. Run inside a clean git working tree so the
    changes are easy to review with `git diff` before committing.

.PARAMETER SshUserKey
    Value for __SSH_USER_KEY__. If omitted, you are prompted.

.PARAMETER SshVmKey
    Value for __SSH_VM_KEY__. If omitted, you are prompted (default id_ed25519_vmss).

.PARAMETER Check
    Dry run: report every file that still contains a placeholder and exit without
    writing anything. Useful in CI to fail if a tree was committed un-initialized.

.EXAMPLE
    .\initialize-placeholders.ps1 -SshUserKey id_ed25519_alice

.EXAMPLE
    .\initialize-placeholders.ps1 -Check
#>
[CmdletBinding()]
param(
    [string]$SshUserKey,
    [string]$SshVmKey,
    [switch]$Check
)

$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot

# Directories and file globs that must never be rewritten.
$excludeDirs = @('.git', '.peer-cache', 'results')
$excludeExt = @('.pub', '.png', '.jpg', '.jpeg', '.gif', '.ico', '.zip', '.gz', '.dll', '.so', '.exe')
# Files that reference the tokens as documentation and must keep them literal.
$excludeFiles = @('initialize-placeholders.ps1', 'SECURITY-REVIEW.md')

function Get-CandidateFiles {
    Get-ChildItem -Path $root -Recurse -File | Where-Object {
        $rel = $_.FullName.Substring($root.Length).TrimStart('\', '/')
        $parts = $rel -split '[\\/]'
        (-not ($parts | Where-Object { $excludeDirs -contains $_ })) -and
        ($excludeExt -notcontains $_.Extension.ToLowerInvariant()) -and
        ($excludeFiles -notcontains $_.Name)
    }
}

$placeholders = @('__SSH_USER_KEY__', '__SSH_VM_KEY__')

if ($Check) {
    $hits = @()
    foreach ($file in Get-CandidateFiles) {
        $content = Get-Content -Raw -LiteralPath $file.FullName
        foreach ($ph in $placeholders) {
            if ($content -and $content.Contains($ph)) {
                $hits += [pscustomobject]@{
                    File = $file.FullName.Substring($root.Length).TrimStart('\', '/')
                    Placeholder = $ph
                }
            }
        }
    }
    if ($hits.Count -eq 0) {
        Write-Host "No placeholders remain. Tree is initialized." -ForegroundColor Green
        exit 0
    }
    Write-Host "Un-filled placeholders found:" -ForegroundColor Yellow
    $hits | Sort-Object File, Placeholder | Format-Table -AutoSize
    exit 1
}

if (-not $SshUserKey) {
    $suggest = 'id_ed25519_user'
    $answer = Read-Host "Personal SSH public key name in your manifest basePath [$suggest]"
    $SshUserKey = if ([string]::IsNullOrWhiteSpace($answer)) { $suggest } else { $answer }
}
if (-not $SshVmKey) {
    $suggestVm = 'id_ed25519_vmss'
    $answerVm = Read-Host "VMSS inter-node SSH key name in your manifest basePath [$suggestVm]"
    $SshVmKey = if ([string]::IsNullOrWhiteSpace($answerVm)) { $suggestVm } else { $answerVm }
}

$replacements = @{
    '__SSH_USER_KEY__' = $SshUserKey
    '__SSH_VM_KEY__'   = $SshVmKey
}

$changed = 0
foreach ($file in Get-CandidateFiles) {
    $content = Get-Content -Raw -LiteralPath $file.FullName
    if (-not $content) { continue }
    $updated = $content
    foreach ($ph in $replacements.Keys) {
        $updated = $updated.Replace($ph, $replacements[$ph])
    }
    if ($updated -ne $content) {
        [System.IO.File]::WriteAllText($file.FullName, $updated)
        $changed++
        Write-Host "  updated $($file.FullName.Substring($root.Length).TrimStart('\','/'))"
    }
}

Write-Host ""
Write-Host "Done. $changed file(s) updated." -ForegroundColor Green
Write-Host "  SSH user key -> $SshUserKey"
Write-Host "  SSH VM key   -> $SshVmKey"
Write-Host ""
Write-Host "Review the changes with 'git diff' before committing." -ForegroundColor Cyan
