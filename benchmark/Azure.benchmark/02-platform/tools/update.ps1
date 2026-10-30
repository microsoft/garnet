#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Pull latest repo (or fetch a fresh tools tarball), copy scripts, and
    optionally run deploy commands. Reads manifest.json for source->destination
    mapping and runcmd definitions.

.EXAMPLE
    update.ps1 -Pull
    update.ps1 -Fetch -Run
    update.ps1 -Run
    update.ps1 -Pull -Run
#>
param(
    [switch]$Pull,
    [switch]$Fetch,
    [switch]$Copy,
    [switch]$Run,
    [switch]$RunOnly,
    [switch]$Force,
    [string]$ToolsBlobUrl,
    [switch]$Help
)

if ($Help -or (-not $Pull -and -not $Fetch -and -not $Copy -and -not $Run -and -not $RunOnly)) {
    Write-Host "Usage: update.ps1 [-Pull] [-Fetch] [-Copy] [-Run] [-RunOnly] [-Force]"
    Write-Host ""
    Write-Host "Pull latest repo, copy scripts, and optionally run deploy commands."
    Write-Host "Reads manifest.json for source->destination mapping and runcmd definitions."
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  -Pull         Pull latest changes from git before copying"
    Write-Host "  -Fetch        Re-download and unpack a fresh tools tarball from storage"
    Write-Host "                using the VM managed identity (no VM redeploy needed)"
    Write-Host "  -Copy         Copy scripts to deployed locations"
    Write-Host "  -Run          Copy scripts and execute the runcmd section from manifest"
    Write-Host "  -RunOnly      Execute runcmd without copying scripts"
    Write-Host "  -Force        Force pull (git reset --hard) instead of fast-forward"
    Write-Host "  -ToolsBlobUrl Tools tarball blob URL for -Fetch (default: read from /opt/deploy-actions/storage.env)"
    Write-Host "  -Help         Show this help message"
    return
}

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoDir = Split-Path -Parent $ScriptDir
$Manifest = "$ScriptDir/manifest.json"

if ($Pull) {
    Write-Host "Pulling latest from repo..."
    # Restart DNS resolver to avoid transient resolution failures
    bash -c "sudo systemctl restart systemd-resolved" 2>$null
    if ($Force) {
        # Read branch from manifest.json for this repo, fall back to HEAD detection
        $manifestPath = "$ScriptDir/manifest.json"
        $branch = $null
        if (Test-Path $manifestPath) {
            $manifestData = Get-Content $manifestPath -Raw | ConvertFrom-Json
            $repoEntry = $manifestData.repos | Where-Object { $_.path -eq $RepoDir } | Select-Object -First 1
            if ($repoEntry) {
                $branch = if ($repoEntry.branch -is [array]) { $repoEntry.branch[0] } else { $repoEntry.branch }
            }
        }
        if (-not $branch) { $branch = git -C $RepoDir rev-parse --abbrev-ref HEAD 2>$null }
        Write-Host "  Fetching (branch: $branch)..."
        git -C $RepoDir fetch --all -q 2>&1
        git -C $RepoDir reset --hard "origin/$branch" 2>&1
        if ($LASTEXITCODE -ne 0) { Write-Host "  WARNING: git force pull failed" -ForegroundColor Yellow }
    } else {
        git -C $RepoDir pull --ff-only 2>&1
        if ($LASTEXITCODE -ne 0) { Write-Host "  WARNING: git pull failed (use -Force to reset)" -ForegroundColor Yellow }
    }
}

if ($Fetch) {
    Write-Host "Fetching fresh tools tarball from storage with the VM managed identity..."

    if (-not $ToolsBlobUrl) {
        $envFile = '/opt/deploy-actions/storage.env'
        if (Test-Path $envFile) {
            $line = Select-String -Path $envFile -Pattern '^TOOLS_BLOB_URL=' | Select-Object -First 1
            if ($line) { $ToolsBlobUrl = ($line.Line -split '=', 2)[1].Trim() }
        }
    }
    if (-not $ToolsBlobUrl) {
        throw "ERROR: -Fetch requires a tools blob URL (pass -ToolsBlobUrl or set TOOLS_BLOB_URL in /opt/deploy-actions/storage.env)"
    }

    $extractDir = Split-Path -Parent $ScriptDir
    # Use a per-user tarball path so a root-owned /tmp/tools.tar.gz left by
    # cloud-init at boot cannot block the download.
    $tarball = "/tmp/tools-$([Environment]::UserName).tar.gz"
    if (Test-Path $tarball) { Remove-Item $tarball -Force -ErrorAction SilentlyContinue }

    # Storage has Shared Key/SAS disabled: request a Storage token from IMDS and
    # download with it (requires Storage Blob Data Reader on the VMSS identity).
    $imdsUri = 'http://169.254.169.254/metadata/identity/oauth2/token' +
        '?api-version=2018-02-01&resource=https://storage.azure.com/'

    $fetched = $false
    for ($i = 1; $i -le 12; $i++) {
        try {
            $tokenResp = Invoke-RestMethod -Uri $imdsUri -Headers @{ Metadata = 'true' } -NoProxy -TimeoutSec 10
            $accessToken = $tokenResp.access_token
            if ($accessToken) {
                Invoke-WebRequest -Uri $ToolsBlobUrl -OutFile $tarball -TimeoutSec 60 -Headers @{
                    Authorization  = "Bearer $accessToken"
                    'x-ms-version' = '2023-11-03'
                }
                if (Test-Path $tarball) { $fetched = $true; Write-Host "  tools bundle fetched"; break }
            }
        } catch {
            Write-Host "  attempt ${i}: $($_.Exception.Message)" -ForegroundColor DarkYellow
        }
        Write-Host "  tools fetch attempt $i failed; retrying in 5s..." -ForegroundColor Yellow
        Start-Sleep -Seconds 5
    }

    if (-not $fetched) { throw "ERROR: -Fetch failed to download the tools tarball from $ToolsBlobUrl" }

    tar -xzf $tarball -C $extractDir
    if ($LASTEXITCODE -ne 0) { throw "ERROR: -Fetch failed to unpack the tools tarball" }
    Write-Host "Tools tarball unpacked to $extractDir." -ForegroundColor Green

    # This process is still executing the pre-fetch version of update.ps1.
    # Re-exec the downloaded version so manifest/schema changes in the new bundle
    # take effect immediately instead of one deployment later.
    if ($RunOnly) {
        & pwsh -NoProfile -File "$ScriptDir/update.ps1" -RunOnly
    } elseif ($Run) {
        & pwsh -NoProfile -File "$ScriptDir/update.ps1" -Run
    } else {
        & pwsh -NoProfile -File "$ScriptDir/update.ps1" -Copy
    }
    exit $LASTEXITCODE
}

if (-not (Test-Path $Manifest)) {
    throw "ERROR: $Manifest not found"
}

$entries = Get-Content $Manifest -Raw | ConvertFrom-Json

# Copy scripts to deployed locations (skip with -RunOnly)
if (-not $RunOnly) {
    Write-Host "Copying scripts to deployed locations..."

    # Ensure target directories exist (derive from manifest destinations)
    $dirs = $entries.scripts | ForEach-Object { Split-Path $_.dst -Parent } | Sort-Object -Unique
    foreach ($dir in $dirs) {
        sudo mkdir -p $dir 2>$null
    }

    foreach ($entry in $entries.scripts) {
        $src = "$ScriptDir/$($entry.src)"
        $dst = $entry.dst
        $mode = $entry.mode

        if (Test-Path $src) {
            sudo cp $src $dst
            sudo chmod $mode $dst
            Write-Host "  $dst" -ForegroundColor DarkGray
        } else {
            Write-Host "  SKIP: $($entry.src) (not found)" -ForegroundColor Yellow
        }
    }

    Write-Host "Scripts updated." -ForegroundColor Green
}

# Execute runcmd section if -Run or -RunOnly is passed
if ($Run -or $RunOnly) {
    if (-not $entries.runcmd) {
        Write-Host "No runcmd section in manifest. Skipping."
        return
    }

    Write-Host ""
    Write-Host "Executing runcmd from manifest..."

    # Background runcmd steps redirect output to a log file. /var/log is not
    # writable by the non-root deploy user, so use a world-writable (sticky) dir.
    $logDir = "/var/log/deploy-actions"
    bash -c "sudo mkdir -p $logDir && sudo chmod 1777 $logDir" 2>$null

    foreach ($cmd in $entries.runcmd) {
        $scriptName = $cmd.run
        $useSudo = $cmd.sudo
        $cmdArgs = $cmd.args
        $background = if ($cmd.PSObject.Properties['background']) { $cmd.background } else { $false }

        # For build.ps1: resolve branch from runcmd index into repos[].branch, or repos[].branch directly
        if ($scriptName -eq 'build.ps1' -and $cmdArgs -match '^\s*(\S+)\s*$') {
            $buildSystem = $Matches[1]
            $repoName = switch ($buildSystem) { 'resp-bench' { 'garnet' }; default { $buildSystem } }
            $repoEntry = $entries.repos | Where-Object { $_.name -eq $repoName } | Select-Object -First 1
            $resolvedBranch = $null

            if ($repoEntry -and $repoEntry.branch) {
                $branchField = $repoEntry.branch
                if ($cmd.PSObject.Properties['branch'] -and $null -ne $cmd.branch -and $branchField -is [array]) {
                    # Index into branch array
                    $resolvedBranch = $branchField[$cmd.branch]
                } elseif ($branchField -is [array]) {
                    # Default to first element
                    $resolvedBranch = $branchField[0]
                } else {
                    $resolvedBranch = $branchField
                }
            }

            if ($resolvedBranch) { $cmdArgs = "$buildSystem $resolvedBranch" }
            if ($cmd.PSObject.Properties['tls'] -and $cmd.tls) {
                $cmdArgs = "$cmdArgs tls"
            }
        }

        # Resolve script path from the scripts section by matching filename
        $scriptEntry = $entries.scripts | Where-Object { $_.src -like "*$scriptName" } | Select-Object -First 1
        if (-not $scriptEntry -or -not (Test-Path $scriptEntry.dst)) {
            Write-Host "  ERROR: Cannot resolve script '$scriptName' from manifest" -ForegroundColor Red
            continue
        }

        $scriptPath = $scriptEntry.dst
        $runCmd = if ($useSudo) { "sudo $scriptPath $cmdArgs" } else { "$scriptPath $cmdArgs" }

        Write-Host "  -> $runCmd"
        if ($background) {
            $logFile = "$logDir/$($scriptName -replace '\.(sh|ps1)$','').log"
            bash -c "nohup $runCmd > $logFile 2>&1 &"
        } else {
            bash -c $runCmd
            if ($LASTEXITCODE -ne 0) {
                throw "runcmd step failed (exit $LASTEXITCODE): $runCmd"
            }
        }
    }

    Write-Host "All runcmd steps complete." -ForegroundColor Green
}
