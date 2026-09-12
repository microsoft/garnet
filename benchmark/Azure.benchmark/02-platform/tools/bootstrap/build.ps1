#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Build and install a benchmark system: redis, valkey, garnet, resp-bench, or memtier.

.DESCRIPTION
    PowerShell port of build.sh. Targets Linux VMSS instances (invokes native
    git/make/dotnet/autoconf toolchains). Usage: build.ps1 <system> [branch] [tls]

.EXAMPLE
    build.ps1 valkey
    build.ps1 valkey 9.0
    build.ps1 valkey 9.0 tls
    build.ps1 garnet main
    build.ps1 resp-bench
    build.ps1 memtier
#>
param(
    [Parameter(Position = 0, Mandatory = $true)][string]$System,
    [Parameter(Position = 1)][string]$Branch = '',
    [Parameter(Position = 2)][string]$Tls = ''
)

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

$manifest = "$USER_HOME/tools/manifest.json"
$cores = (& nproc).Trim()

function Get-RepoPath([string]$Name) {
    $data = Get-Content $manifest -Raw | ConvertFrom-Json
    ($data.repos | Where-Object { $_.name -eq $Name } | Select-Object -First 1).path
}

function Invoke-Git([string[]]$GitArgs) {
    & sudo -u $DEPLOY_USER git @GitArgs
    if ($LASTEXITCODE -ne 0) { throw "git $($GitArgs -join ' ') failed" }
}

$garnetDir = Get-RepoPath 'garnet'
$valkeyDir = Get-RepoPath 'valkey'
$redisDir = Get-RepoPath 'redis'
$memtierDir = Get-RepoPath 'memtier'

function Get-Rid {
    $arch = (& uname -m).Trim()
    if ($arch -eq 'aarch64') { 'linux-arm64' } else { 'linux-x64' }
}

function Build-ValkeyRedis([string]$dir) {
    if (-not (Test-Path $dir)) { Write-Host "ERROR: $dir not found. Clone the repo first."; exit 1 }
    Set-Location $dir

    if ($Branch) {
        Write-Host "==== Checking out $System $Branch ===="
        Invoke-Git @('fetch', '--tags')
        Invoke-Git @('checkout', $Branch, '--')
        Invoke-Git @('reset', '--hard', $Branch, '--')
    }

    & make distclean 2>$null
    $global:LASTEXITCODE = 0

    if ($Tls -eq 'tls') {
        Write-Host "==== Building $System with TLS ===="
        & sudo -u $DEPLOY_USER make "-j$cores" BUILD_TLS=yes
    }
    else {
        Write-Host "==== Building $System ===="
        & sudo -u $DEPLOY_USER make "-j$cores"
    }
    if ($LASTEXITCODE -ne 0) { throw "make failed" }

    Write-Host "==== Installing $System ===="
    & sudo make install
    if ($LASTEXITCODE -ne 0) { throw "make install failed" }

    Write-Host "==== Build complete ===="
    # Print the built server version. Valkey produces valkey-server (older
    # forks redis-server); guard with Test-Path so a missing binary does not
    # raise a terminating "not recognized" error and fail an otherwise-good build.
    foreach ($bin in @('./src/valkey-server', './src/redis-server')) {
        if (Test-Path $bin) { & $bin --version; break }
    }
}

function Build-Garnet {
    if (-not (Test-Path $garnetDir)) { Write-Host "ERROR: $garnetDir not found. Clone the garnet repo first."; exit 1 }
    Set-Location $garnetDir

    if ($Branch) {
        Write-Host "==== Checking out $Branch ===="
        Invoke-Git @('fetch', '--all', '--tags')
        Invoke-Git @('checkout', $Branch, '--')
        Invoke-Git @('reset', '--hard', $Branch, '--')
    }

    $rid = Get-Rid
    Write-Host "==== Building GarnetServer (Release, $rid) ===="
    & sudo -u $DEPLOY_USER dotnet publish $GARNET_PROJECT -c Release -r $rid -f net10.0 -o "$garnetDir/publish"
    if ($LASTEXITCODE -ne 0) { throw "dotnet publish failed" }

    & mkdir -p "$INSTALL_DIR/garnet"
    & cp -r "$garnetDir/publish/." "$INSTALL_DIR/garnet/"
    & ln -sf "$INSTALL_DIR/garnet/GarnetServer" "$INSTALL_DIR/GarnetServer"
    & chmod +x "$INSTALL_DIR/garnet/GarnetServer"

    Write-Host "==== Build complete ===="
    & GarnetServer --version 2>$null
    if ($LASTEXITCODE -ne 0) { Write-Host "GarnetServer installed at $INSTALL_DIR/GarnetServer" }
}

function Build-Memtier {
    if (-not (Test-Path $memtierDir)) { Write-Host "ERROR: $memtierDir not found. Clone the repo first."; exit 1 }
    Set-Location $memtierDir

    if ($Branch) {
        Write-Host "==== Checking out memtier $Branch ===="
        Invoke-Git @('fetch', '--tags')
        Invoke-Git @('checkout', $Branch, '--')
        Invoke-Git @('reset', '--hard', $Branch, '--')
    }

    Write-Host "==== Building memtier_benchmark ===="
    & autoreconf -ivf
    if ($LASTEXITCODE -ne 0) { throw "autoreconf failed" }
    & ./configure
    if ($LASTEXITCODE -ne 0) { throw "configure failed" }
    & make "-j$cores"
    if ($LASTEXITCODE -ne 0) { throw "make failed" }
    & sudo make install
    if ($LASTEXITCODE -ne 0) { throw "make install failed" }

    Write-Host "==== Build complete ===="
    & memtier_benchmark --version
}

function Build-RespBench {
    if (-not (Test-Path $garnetDir)) { Write-Host "ERROR: $garnetDir not found. Clone the garnet repo first."; exit 1 }
    Set-Location $garnetDir

    if ($Branch) {
        Write-Host "==== Checking out $Branch ===="
        Invoke-Git @('fetch', '--all')
        Invoke-Git @('checkout', $Branch, '--')
        Invoke-Git @('reset', '--hard', $Branch, '--')
    }

    $rid = Get-Rid
    Write-Host "==== Building Resp.benchmark (Release, $rid) ===="
    & sudo -u $DEPLOY_USER dotnet publish $RESP_BENCH_PROJECT -c Release -r $rid -f net10.0 -o "$garnetDir/resp-bench-publish"
    if ($LASTEXITCODE -ne 0) { throw "dotnet publish failed" }

    & mkdir -p "$INSTALL_DIR/resp-bench"
    & cp -r "$garnetDir/resp-bench-publish/." "$INSTALL_DIR/resp-bench/"
    & ln -sf "$INSTALL_DIR/resp-bench/Resp.benchmark" "$INSTALL_DIR/Resp.benchmark"
    & chmod +x "$INSTALL_DIR/resp-bench/Resp.benchmark"

    Write-Host "==== Resp.benchmark build complete ===="
    Write-Host "Installed at $INSTALL_DIR/resp-bench/Resp.benchmark"
}

switch ($System) {
    'redis' { Build-ValkeyRedis $redisDir }
    'valkey' { Build-ValkeyRedis $valkeyDir }
    'garnet' { Build-Garnet }
    'resp-bench' { Build-RespBench }
    'memtier' { Build-Memtier }
    default {
        Write-Host "Unknown system: $System (use redis, valkey, garnet, resp-bench, or memtier)"
        exit 1
    }
}
