<#
.SYNOPSIS
    Fills the placeholder tokens in the Azure.benchmark tree with your own values.

.DESCRIPTION
    The Azure.benchmark suite ships with neutral placeholder tokens instead of any
    personal or account-specific identifiers, so nothing tied to a specific person
    or subscription is committed to the public repository. Run this script once,
    after cloning, to substitute the placeholders with values for your environment.

    Supported placeholders:
      __OWNER__          Owner/alias used for resource tags and to derive the
                         default resource group (`<owner>-garnet`) and Key Vault
                         (`<owner>-garnet-kv`) names.
      __SSH_USER_KEY__   Name of your personal SSH public key in the manifest
                         basePath (normally %USERPROFILE%\.ssh), e.g. id_ed25519_user.

    The script rewrites files in place. Run inside a clean git working tree so the
    changes are easy to review with `git diff` before committing.

.PARAMETER Owner
    Value for __OWNER__. If omitted, you are prompted.

.PARAMETER SshUserKey
    Value for __SSH_USER_KEY__. If omitted, you are prompted.

.PARAMETER Check
    Dry run: report every file that still contains a placeholder and exit without
    writing anything. Useful in CI to fail if a tree was committed un-initialized.

.EXAMPLE
    .\initialize-placeholders.ps1 -Owner alice -SshUserKey id_ed25519_alice

.EXAMPLE
    .\initialize-placeholders.ps1 -Check
#>
[CmdletBinding()]
param(
    [string]$Owner,
    [string]$SshUserKey,
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

$placeholders = @('__OWNER__', '__SSH_USER_KEY__')

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

if (-not $Owner) {
    $Owner = Read-Host "Owner alias (resource tag; derives '<owner>-garnet' RG and '<owner>-garnet-kv' vault)"
}
if (-not $SshUserKey) {
    $suggest = 'id_ed25519_user'
    $answer = Read-Host "Personal SSH public key name in your manifest basePath [$suggest]"
    $SshUserKey = if ([string]::IsNullOrWhiteSpace($answer)) { $suggest } else { $answer }
}

if ([string]::IsNullOrWhiteSpace($Owner)) {
    throw "Owner is required."
}
if ($Owner -notmatch '^[a-z0-9][a-z0-9-]{1,40}$') {
    throw "Owner '$Owner' is invalid. Use lowercase letters, digits and hyphens (Azure resource-name safe)."
}

$replacements = @{
    '__OWNER__'        = $Owner
    '__SSH_USER_KEY__' = $SshUserKey
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
Write-Host "  Owner        -> $Owner"
Write-Host "  SSH user key -> $SshUserKey"
Write-Host ""
Write-Host "Review the changes with 'git diff' before committing." -ForegroundColor Cyan
