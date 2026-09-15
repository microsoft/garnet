#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Runs memtier_benchmark with load and benchmark phases.

.EXAMPLE
    memtier-bench.ps1 -Address 10.5.0.4 -Port 6379
    memtier-bench.ps1 -Address 10.5.0.4 -Port 7000 -Cluster
    memtier-bench.ps1 -Address 10.5.0.4 -Port 7000 -Cluster -Threads 64 -Clients 32 -SkipLoad
    memtier-bench.ps1 -Address 10.5.0.4 -Port 7000 -Cluster -Pipeline 512 -DbSize 1000000 -DataSize 128 -TestTime 30
#>
param(
    [string]$Address,
    [int]$Port,
    [int]$Threads = 128,
    [int]$Clients = 64,
    [int]$Pipeline = 1024,
    [long]$DbSize = 268435456,
    [int]$DataSize = 8,
    [int]$TestTime = 15,
    [switch]$Cluster,
    [switch]$Tls,
    [string]$TlsHost = 'azurebench-server',
    [switch]$SkipLoad,
    [switch]$Help
)

if ($Help -or (-not $Address -and -not $Port)) {
    Write-Host "Usage: memtier-bench.ps1 -Address <ip> -Port <n> [-Threads <n>] [-Clients <n>] [-Pipeline <n>] [-DbSize <n>] [-DataSize <n>] [-TestTime <n>] [-Cluster] [-Tls] [-TlsHost <name>] [-SkipLoad]"
    Write-Host ""
    Write-Host "Runs memtier_benchmark with load and benchmark phases."
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  -Address    Target server address (required)"
    Write-Host "  -Port       Target server port (required)"
    Write-Host "  -Threads    Number of threads (default: 128)"
    Write-Host "  -Clients    Number of clients per thread (default: 64)"
    Write-Host "  -Pipeline   Pipeline depth (default: 1024)"
    Write-Host "  -DbSize     Number of keys to load (default: 268435456)"
    Write-Host "  -DataSize   Value size in bytes (default: 8)"
    Write-Host "  -TestTime   Benchmark duration in seconds (default: 15)"
    Write-Host "  -Cluster    Enable cluster mode"
    Write-Host "  -Tls        Enable TLS with CA validation"
    Write-Host "  -TlsHost    Expected TLS server identity (default: azurebench-server)"
    Write-Host "  -SkipLoad   Skip the key loading phase"
    Write-Host "  -Help       Show this help message"
    return
}

if (-not $Address -or -not $Port) {
    throw "ERROR: -Address and -Port are required. Use -Help for usage."
}

$ErrorActionPreference = "Stop"
$clusterFlag = if ($Cluster) { "--cluster-mode" } else { "" }
$tlsArgs = @()

if ($Tls) {
    if ($TlsHost -notmatch '^[a-zA-Z0-9](?:[a-zA-Z0-9.-]*[a-zA-Z0-9])?$') {
        throw "TlsHost is not a valid DNS name: '$TlsHost'."
    }
    if ($Address -notmatch '^[a-zA-Z0-9](?:[a-zA-Z0-9.:-]*[a-zA-Z0-9])?$') {
        throw "Address is not a valid host or IP address: '$Address'."
    }
    $deploymentFile = '/opt/deploy-actions/deployment.env'
    if (-not (Test-Path $deploymentFile)) { throw "Deployment role file not found: $deploymentFile" }
    $roleLine = Select-String -Path $deploymentFile -Pattern '^DEPLOYMENT_ROLE=' | Select-Object -First 1
    $role = if ($roleLine) { ($roleLine.Line -replace '^DEPLOYMENT_ROLE=', '').Trim('"') } else { '' }
    if ($role -ne 'client') { throw "memtier TLS requires DEPLOYMENT_ROLE=client; this node is '$role'." }

    foreach ($path in @('/opt/azurebench/tls/ca.crt', '/opt/azurebench/tls/metadata.json')) {
        if (-not (Test-Path $path -PathType Leaf)) { throw "Required TLS file not found: $path" }
    }
    $metadata = Get-Content /opt/azurebench/tls/metadata.json -Raw | ConvertFrom-Json
    if ($metadata.status -ne 'complete') { throw 'TLS metadata is incomplete.' }
    if ($metadata.targetHost -ne $TlsHost) {
        throw "TLS metadata target '$($metadata.targetHost)' does not match '$TlsHost'."
    }

    $memtierHelp = memtier_benchmark --help 2>&1 | Out-String
    foreach ($option in @('--tls', '--cacert', '--sni')) {
        if ($memtierHelp -notmatch [regex]::Escape($option)) {
            throw "memtier_benchmark does not support $option; rebuild it with TLS support."
        }
    }
    if (-not (Get-Command openssl -ErrorAction SilentlyContinue)) {
        throw 'openssl is not installed.'
    }
    $verification = '' | openssl s_client -connect "${Address}:${Port}" -servername $TlsHost `
        -CAfile /opt/azurebench/tls/ca.crt -verify_hostname $TlsHost -verify_return_error 2>&1
    if ($LASTEXITCODE -ne 0 -or ($verification -join "`n") -notmatch 'Verification: OK') {
        throw "TLS identity validation failed for ${Address}:${Port}: $($verification -join [Environment]::NewLine)"
    }
    $tlsArgs = @('--tls', '--cacert=/opt/azurebench/tls/ca.crt', "--sni=$TlsHost")
}

# Print parameters for this run
Write-Host "==== Parameters ====" -ForegroundColor Yellow
Write-Host "  Address:    $Address"
Write-Host "  Port:       $Port"
Write-Host "  Threads:    $Threads"
Write-Host "  Clients:    $Clients"
Write-Host "  Pipeline:   $Pipeline"
Write-Host "  DbSize:     $DbSize"
Write-Host "  DataSize:   $DataSize"
Write-Host "  TestTime:   $TestTime"
Write-Host "  SkipLoad:   $SkipLoad"
Write-Host "  Cluster:    $Cluster"
Write-Host "  TLS:        $(if ($Tls) { "enabled ($TlsHost)" } else { "disabled" })"
Write-Host ""

# Phase 1: Load keys
if (-not $SkipLoad) {
    Write-Host "==== Loading keys ====" -ForegroundColor Yellow
    $loadArgs = @("-s", $Address, "--port=$Port", "--ratio=1:0", "--pipeline=$Pipeline",
        "--data-size=$DataSize", "--clients=$Clients", "--threads=$Threads",
        "--key-minimum=1", "--key-maximum=$DbSize", "--key-pattern=P:P",
        "--run-count=1", "--hide-histogram", "--requests=allkeys")
    if ($Cluster) { $loadArgs += "--cluster-mode" }
    $loadArgs += $tlsArgs
    & memtier_benchmark @loadArgs
    if ($LASTEXITCODE -ne 0) { throw "Load phase failed" }
} else {
    Write-Host "==== Skipping load phase ====" -ForegroundColor DarkGray
}

# Phase 2: Benchmark
Write-Host ""
Write-Host "==== Running benchmark ====" -ForegroundColor Yellow
$allResults = @()

for ($i = $Threads; $i -le $Threads; $i *= 2) {
    $benchArgs = @("-s", $Address, "--port=$Port", "--ratio=1:9", "--pipeline=$Pipeline",
        "--data-size=$DataSize", "--clients=$Clients", "--threads=$i",
        "--test-time=$TestTime", "--run-count=1", "--hide-histogram",
        "--key-minimum=1", "--key-maximum=$DbSize", "--key-pattern=R:R")
    if ($Cluster) { $benchArgs += "--cluster-mode" }
    $benchArgs += $tlsArgs

    # Stream output live and capture for summary
    $outputFile = "/tmp/memtier-last-run.txt"
    & memtier_benchmark @benchArgs 2>&1 | Tee-Object -FilePath $outputFile
    $rawOutput = Get-Content $outputFile

    $totals = $rawOutput | Where-Object { $_ -match "Totals" } | Select-Object -Last 1
    if ($totals) {
        Write-Host "$i $totals" -ForegroundColor Cyan
        $allResults += "$i $totals"
    }
}

Write-Host ""
Write-Host "==== Final Summary ====" -ForegroundColor Green
Write-Host "pipeline: $Pipeline, threads: $Threads, clients: $Clients, payload: $DataSize, dbSize: $DbSize, testTime: $TestTime"
$allResults | ForEach-Object { Write-Host $_ }
