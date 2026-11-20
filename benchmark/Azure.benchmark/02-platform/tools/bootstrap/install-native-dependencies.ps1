#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Install Linux native storage and benchmark build dependencies.
#>

$ErrorActionPreference = 'Stop'

function Install-NativeDependencies {
    $packageManager = @('apt-get', 'tdnf', 'dnf') |
        Where-Object { Get-Command $_ -CommandType Application -ErrorAction SilentlyContinue } |
        Select-Object -First 1
    if (-not $packageManager) {
        throw 'Native I/O dependency installation requires apt-get, tdnf, or dnf.'
    }

    if ($packageManager -eq 'apt-get') {
        & apt-get update
        if ($LASTEXITCODE -ne 0) { throw "apt-get update failed (exit $LASTEXITCODE)." }
        # Development packages also install the distribution's matching runtime libraries.
        $packages = @('liburing-dev', 'libaio-dev', 'libevent-dev', 'fio', 'gcc', 'make', 'git', 'pkg-config', 'mdadm', 'xfsprogs', 'e2fsprogs', 'util-linux')
    } else {
        $packages = @('liburing', 'liburing-devel', 'libaio', 'libaio-devel', 'libevent-devel', 'fio', 'gcc', 'make', 'git', 'pkg-config', 'mdadm', 'xfsprogs', 'e2fsprogs', 'util-linux')
    }

    Write-Host "Installing native I/O dependencies with $packageManager..."
    & $packageManager install -y @packages
    if ($LASTEXITCODE -ne 0) {
        throw "$packageManager install failed (exit $LASTEXITCODE)."
    }

    & ldconfig
    if ($LASTEXITCODE -ne 0) { throw "ldconfig failed (exit $LASTEXITCODE)." }
    Write-Host 'Native I/O dependencies are installed.'
}

if ($IsLinux) {
    Install-NativeDependencies
} else {
    Write-Host 'Native I/O dependency installation is Linux-only; skipping.'
}
