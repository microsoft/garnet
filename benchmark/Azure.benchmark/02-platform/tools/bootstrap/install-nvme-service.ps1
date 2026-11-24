#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Enable local NVMe initialization and remounting at every VM boot.
#>

$ErrorActionPreference = 'Stop'

function Install-NvmeStartupService {
    foreach ($path in @('/usr/bin/pwsh', '/opt/deploy-actions/setup-nvme.ps1',
        '/etc/systemd/system/azurebench-nvme.service')) {
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
            throw "Required NVMe startup file not found: $path"
        }
    }

    foreach ($arguments in @(@('daemon-reload'), @('enable', 'azurebench-nvme.service'),
        @('restart', 'azurebench-nvme.service'))) {
        & systemctl @arguments
        if ($LASTEXITCODE -ne 0) {
            throw "systemctl $($arguments -join ' ') failed (exit $LASTEXITCODE)."
        }
    }
    Write-Host 'NVMe setup completed and is enabled for subsequent VM boots.'
}

if ($IsLinux) {
    Install-NvmeStartupService
} else {
    throw 'NVMe startup service installation requires Linux with systemd.'
}
