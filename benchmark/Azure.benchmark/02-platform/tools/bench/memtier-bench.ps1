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
    [switch]$SkipLoad,
    [switch]$Help
)

if ($Help -or (-not $Address -and -not $Port)) {
    Write-Host "Usage: memtier-bench.ps1 -Address <ip> -Port <n> [-Threads <n>] [-Clients <n>] [-Pipeline <n>] [-DbSize <n>] [-DataSize <n>] [-TestTime <n>] [-Cluster] [-SkipLoad]"
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
    Write-Host "  -SkipLoad   Skip the key loading phase"
    Write-Host "  -Help       Show this help message"
    return
}

if (-not $Address -or -not $Port) {
    throw "ERROR: -Address and -Port are required. Use -Help for usage."
}

$ErrorActionPreference = "Stop"
$clusterFlag = if ($Cluster) { "--cluster-mode" } else { "" }

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
Write-Host ""

# Phase 1: Load keys
if (-not $SkipLoad) {
    Write-Host "==== Loading keys ====" -ForegroundColor Yellow
    $loadArgs = @("-s", $Address, "--port=$Port", "--ratio=1:0", "--pipeline=$Pipeline",
        "--data-size=$DataSize", "--clients=$Clients", "--threads=$Threads",
        "--key-minimum=1", "--key-maximum=$DbSize", "--key-pattern=P:P",
        "--run-count=1", "--hide-histogram", "--requests=allkeys")
    if ($Cluster) { $loadArgs += "--cluster-mode" }
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
