#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Configure secondary NIC (eth1) networking for VMSS benchmark instances.

.DESCRIPTION
    PowerShell port of setup-network.sh. Targets Linux VMSS instances (uses
    ethtool, ip, iptables, sysctl, systemctl). Configures RSS/IRQ affinity,
    policy-based routing, firewall rules, TCP tuning and fd limits.

    Usage: setup-network.ps1 [engine] [nodes]
      engine - "valkey" or "garnet" (default: garnet)
      nodes  - number of instances (default: 1, used for valkey IRQ scaling)
#>
param(
    [Parameter(Position = 0)][string]$Engine = 'garnet',
    [Parameter(Position = 1)][int]$Nodes = 1
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

$totalCores = [int](& nproc).Trim()

function Get-CombinedQueues([string]$nic) {
    # Returns @(max, current) parsed from ethtool -l output.
    $vals = @(& ethtool -l $nic 2>$null | ForEach-Object {
            if ($_ -match 'Combined:\s*(\d+)') { [int]$Matches[1] }
        })
    return $vals
}

function Get-NicIrqs([string]$nic) {
    @(Get-Content /proc/interrupts | Where-Object { $_ -match "\b$nic\b" } | ForEach-Object {
            (($_ -split '\s+') | Where-Object { $_ })[0].TrimEnd(':')
        })
}

function Set-IrqAffinity([string]$irq, [int]$cpu) {
    & bash -c "echo '$cpu' > /proc/irq/$irq/smp_affinity_list"
}

function Configure-IrqValkey {
    $irqCores = $Nodes * 2
    if ($irqCores -ge $totalCores) {
        Write-Host "ERROR: IRQ cores ($irqCores) >= total cores ($totalCores). Reduce node count."
        exit 1
    }
    Write-Host "[valkey] Configuring $irqCores RSS queues for $Nodes instances on $totalCores cores"

    if (Test-Path "/sys/class/net/$IFACE") {
        $q = Get-CombinedQueues $IFACE
        $maxQ = if ($q.Count -gt 0) { $q[0] } else { 0 }
        $targetQ = $irqCores
        if ($targetQ -gt $maxQ) {
            Write-Host "  WARNING: Requested $targetQ queues but max is $maxQ, using $maxQ"
            $targetQ = $maxQ
        }
        Write-Host "  [$IFACE] Setting RSS queues to $targetQ"
        & ethtool -L $IFACE combined $targetQ
    }

    $irqs = Get-NicIrqs $IFACE
    $cpu = 0
    foreach ($irq in $irqs) {
        if ($cpu -ge $irqCores) { break }
        Set-IrqAffinity $irq $cpu
        Write-Host "  IRQ $irq -> CPU $cpu"
        $cpu++
    }

    & systemctl stop irqbalance 2>$null; $global:LASTEXITCODE = 0
    & systemctl disable irqbalance 2>$null; $global:LASTEXITCODE = 0
    Write-Host "  irqbalance stopped and disabled"
}

function Configure-IrqGarnet {
    Write-Host "[garnet] Maximizing RSS queues for inline processing on $totalCores cores"

    foreach ($nic in @('eth0', $IFACE)) {
        if (Test-Path "/sys/class/net/$nic") {
            $q = Get-CombinedQueues $nic
            $maxQ = if ($q.Count -gt 0) { $q[0] } else { 0 }
            if ($maxQ -gt 0) {
                $currentQ = if ($q.Count -gt 1) { $q[1] } else { 0 }
                if ($currentQ -lt $maxQ) {
                    Write-Host "  [$nic] Setting RSS queues from $currentQ to $maxQ"
                    & ethtool -L $nic combined $maxQ
                }
                else {
                    Write-Host "  [$nic] Already at max RSS queues ($maxQ)"
                }
            }
        }
    }

    $irqs = Get-NicIrqs $IFACE
    $cpu = 0
    foreach ($irq in $irqs) {
        Set-IrqAffinity $irq $cpu
        Write-Host "  IRQ $irq -> CPU $cpu"
        $cpu = ($cpu + 1) % $totalCores
    }
}

switch ($Engine) {
    'valkey' { Configure-IrqValkey }
    'garnet' { Configure-IrqGarnet }
    default {
        Write-Host "Unknown engine: $Engine (expected 'valkey' or 'garnet')"
        exit 1
    }
}

# -------------------------------------------------------------
# 2. Policy-Based Routing for eth1
# -------------------------------------------------------------
$ipOut = (& ip -4 addr show dev $IFACE) -join "`n"
$ETH1_IP = if ($ipOut -match 'inet\s+([\d.]+)') { $Matches[1] } else { '' }
$ETH1_CIDR = if ($ipOut -match 'inet\s+([\d./]+)') { $Matches[1] } else { '' }
$SUBNET_CIDR = $ETH1_CIDR -replace '\.\d+/', '.0/'

if (-not $ETH1_IP) {
    Write-Host "ERROR: No IP found on $IFACE, skipping routing setup"
    exit 0
}

Write-Host "Configuring policy routing for $IFACE (IP: $ETH1_IP, Subnet: $SUBNET_CIDR)"

& sysctl -w net.ipv4.conf.all.rp_filter=0
& sysctl -w "net.ipv4.conf.$IFACE.rp_filter=0"

& ip route add $SUBNET_CIDR dev $IFACE src $ETH1_IP table 100 2>$null; $global:LASTEXITCODE = 0

$VNET_CIDR = if ($VNET_PREFIX) { $VNET_PREFIX } else { '10.5.0.0/16' }
& ip route add $VNET_CIDR dev $IFACE src $ETH1_IP table 100 2>$null; $global:LASTEXITCODE = 0

& ip rule add from $ETH1_IP table 100 priority 100 2>$null; $global:LASTEXITCODE = 0

# -------------------------------------------------------------
# 3. Iptables Rules (check-or-insert)
# -------------------------------------------------------------
function Ensure-IptablesRule([string[]]$Rule) {
    $check = @('-C') + $Rule
    & iptables @check 2>$null
    if ($LASTEXITCODE -ne 0) {
        $insert = @('-I') + $Rule
        & iptables @insert
    }
    $global:LASTEXITCODE = 0
}

Ensure-IptablesRule @('INPUT', '-p', 'icmp', '--icmp-type', 'echo-request', '-j', 'ACCEPT')
Ensure-IptablesRule @('INPUT', '-p', 'icmp', '--icmp-type', 'echo-reply', '-j', 'ACCEPT')
Ensure-IptablesRule @('INPUT', '-i', $IFACE, '-p', 'tcp', '--dport', '6379', '-j', 'ACCEPT')
Ensure-IptablesRule @('INPUT', '-i', $IFACE, '-p', 'tcp', '--dport', '7000:7099', '-j', 'ACCEPT')
Ensure-IptablesRule @('INPUT', '-i', $IFACE, '-p', 'tcp', '--dport', '17000:17099', '-j', 'ACCEPT')
Ensure-IptablesRule @('INPUT', '-p', 'tcp', '--dport', '22', '-s', $SUBNET, '-j', 'ACCEPT')

# -------------------------------------------------------------
# 4. TCP Tuning for High-Throughput Benchmarking
# -------------------------------------------------------------
& sysctl -w net.core.wmem_max=67108864
& sysctl -w net.core.netdev_max_backlog=250000
& sysctl -w net.core.somaxconn=262144
& sysctl -w net.ipv4.tcp_max_syn_backlog=262144
& sysctl -w 'net.ipv4.tcp_rmem=4096 87380 33554432'
& sysctl -w 'net.ipv4.tcp_wmem=4096 87380 33554432'

# -------------------------------------------------------------
# 5. File Descriptor Limits
# -------------------------------------------------------------
$limits = @'
* soft nofile 1048576
* hard nofile 1048576
root soft nofile 1048576
root hard nofile 1048576
'@
Add-Content -Path /etc/security/limits.conf -Value $limits
& sudo sysctl -w fs.nr_open=1048576
& sudo sysctl -w fs.file-max=2097152

Write-Host "Network setup complete ($Engine mode, $Nodes nodes): RSS/IRQ, routing, iptables, TCP tuning, fd limits."
