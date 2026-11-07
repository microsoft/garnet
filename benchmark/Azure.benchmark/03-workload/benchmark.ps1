#!/usr/bin/env pwsh
[CmdletBinding()]
param(
    [string]$Definition,
    [Alias('rg')][string]$ResourceGroup,
    [string]$VmssName,
    [string]$TargetVmssName,
    [string]$SshUser,
    [string]$TargetSshUser,
    [string]$SshKey,
    [switch]$NonInteractive,
    [switch]$DryRun,
    [switch]$Detail,
    [switch]$Monitor,
    [switch]$SkipLoad,
    [switch]$Tls,
    [switch]$Help
)

if ($Help) {
    Write-Host 'Usage: benchmark.ps1 -Definition <benchmark.json> -rg <resource-group> [-VmssName <clients>] [-TargetVmssName <servers>]'
    Write-Host 'Runs one workload per declared VM through the existing RESP or memtier driver.'
    Write-Host 'Omit -VmssName for selection; -NonInteractive requires it. Target VMSS defaults to the client VMSS.'
    Write-Host '-DryRun resolves and prints the plan without launching workloads.'
    Write-Host '-SshUser/-TargetSshUser override Azure usernames; -SshKey overrides the security manifest.'
    Write-Host '-Detail/-Monitor apply to RESP; -SkipLoad applies to memtier; -Tls enables TLS.'
    Write-Host '-Verbose prints captured benchmark-client output inline after each phase, not Garnet server logs.'
    return
}
$ErrorActionPreference = 'Stop'
if (-not $Definition) { throw '-Definition is required.' }
. "$PSScriptRoot\bench\utils.ps1"
. "$PSScriptRoot\..\02-platform\vmss-utils.ps1"
. "$PSScriptRoot\..\01-resources\security\ssh-key-utils.ps1"

$benchmark = Read-BenchmarkDefinition -Path $Definition
if ($benchmark.System -eq 'Resp.benchmark' -and $SkipLoad) { throw '-SkipLoad applies only to memtier_benchmark.' }
if ($benchmark.System -eq 'memtier_benchmark' -and ($Detail -or $Monitor)) { throw '-Detail/-Monitor apply only to Resp.benchmark.' }
if (-not $SshKey) {
    $manifest = Get-SshKeyManifest -ManifestPath "$PSScriptRoot\..\01-resources\security\manifest.json"
    $SshKey = Resolve-SshUserPrivateKey -Manifest $manifest
}
if (-not (Test-Path -LiteralPath $SshKey -PathType Leaf)) { throw "SSH key not found: $SshKey" }

$sameTarget = -not $TargetVmssName -or ($VmssName -and $VmssName -eq $TargetVmssName)
$vmNames = @($benchmark.Instances.Vm)
if ($sameTarget -and $benchmark.Target.Vm -notin $vmNames) { $vmNames += $benchmark.Target.Vm }
$clients = Resolve-VmssExecutionContext -ResourceGroup $ResourceGroup -VmssName $VmssName `
    -VmNames $vmNames -SshUser $SshUser -NonInteractive:$NonInteractive
if ($sameTarget) {
    $target = $clients.Nodes | Where-Object Vm -eq $benchmark.Target.Vm
    if (-not $TargetSshUser) { $TargetSshUser = $clients.SshUser }
} else {
    $servers = Resolve-VmssExecutionContext -ResourceGroup $ResourceGroup -VmssName $TargetVmssName `
        -VmNames $benchmark.Target.Vm -SshUser $TargetSshUser -NonInteractive
    $target = $servers.Nodes[0]
    $TargetSshUser = $servers.SshUser
}
if ($TargetSshUser -notmatch '^[a-zA-Z_][a-zA-Z0-9_.-]*$') { throw 'Invalid target SSH username.' }
$ipScript = @'
$deploy = Get-Command cluster-deploy.ps1 -ErrorAction Stop
. (Join-Path (Split-Path -Parent $deploy.Source) 'cluster-utils.ps1')
Write-Output "BENCHMARK_ADDRESS=$(Get-ClusterDataPlaneIp)"
'@
$output = @(Invoke-WorkloadRemoteScript -HostName $target.HostName -SshUser $TargetSshUser -SshKey $SshKey -Script $ipScript)
$markers = @($output | Where-Object { $_ -match '^BENCHMARK_ADDRESS=' })
$address = $null
$ip = if ($markers.Count -eq 1) { ($markers[0] -replace '^BENCHMARK_ADDRESS=', '').Trim() } else { '' }
if (-not [Net.IPAddress]::TryParse($ip, [ref]$address) -or $address.AddressFamily -ne [Net.Sockets.AddressFamily]::InterNetwork) {
    throw "Invalid target data-plane address from $($target.HostName): $ip"
}
$hosts = @($benchmark.Instances | ForEach-Object { ($clients.Nodes | Where-Object Vm -eq $_.Vm).HostName })
$driver = if ($benchmark.System -eq 'Resp.benchmark') { 'resp-bench.ps1' } else { 'memtier-bench.ps1' }
Write-Host '==== Declarative benchmark plan ====' -ForegroundColor Cyan
Write-Host "System: $($benchmark.System); config: $($benchmark.InstanceConfig)"
Write-Host "Target: $($benchmark.Target.Vm) at ${ip}:$($benchmark.Target.Port)"
Write-Host "Clients: $($hosts -join ', '); one workload instance per VM"
if ($DryRun) { Write-Host 'Dry run: no load or benchmark launched.'; return }

$preflight = "if (-not (Get-Command '$($benchmark.System)' -ErrorAction SilentlyContinue)) { throw 'Benchmark executable is not installed.' }"
foreach ($hostName in $hosts) {
    Invoke-WorkloadRemoteScript -HostName $hostName -SshUser $clients.SshUser -SshKey $SshKey -Script $preflight | ForEach-Object { Write-Host $_ }
}
$arguments = @{
    ConfigFile = $benchmark.InstanceConfig
    ClientHosts = $hosts
    SshUser = $clients.SshUser
    SshKey = $SshKey
    Server = $ip
    Port = [int]$benchmark.Target.Port
    Tls = $Tls
    PrintOutput = ($VerbosePreference -ne 'SilentlyContinue')
    Verbose = $false
}
if ($benchmark.System -eq 'Resp.benchmark') { $arguments.Detail = $Detail; $arguments.Monitor = $Monitor }
else { $arguments.SkipLoad = $SkipLoad }
& (Join-Path "$PSScriptRoot\bench" $driver) @arguments
if (-not $?) { throw 'Benchmark driver failed.' }
Write-Host '==== Declarative benchmark complete ====' -ForegroundColor Green
