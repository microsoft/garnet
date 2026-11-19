function Resolve-ClusterNodes {
    param([object[]]$Nodes)

    if (-not $Nodes.Count) { throw 'Cluster definition must contain Nodes.' }
    $byVm = @{}
    $resolved = @()
    foreach ($node in $Nodes) {
        if ($node.Vm -isnot [string] -or $node.Vm -notmatch '^[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$') {
            throw 'Each node must have a valid short VM name.'
        }
        if ($byVm.ContainsKey($node.Vm)) { throw "Duplicate VM: $($node.Vm)" }
        if ($node.Role -notin @('Primary', 'Replica')) { throw "Invalid role for $($node.Vm)." }
        $allowed = @('Vm', 'Role', 'Slots', 'Primary', 'Ip', 'Port')
        foreach ($property in $node.PSObject.Properties.Name) {
            if ($property -notin $allowed) { throw "Unknown node property: $property" }
        }
        $copy = [pscustomobject]@{
            Vm = $node.Vm
            Role = $node.Role
            Slots = $node.Slots
            Primary = $node.Primary
            Ip = $node.Ip
            Port = $node.Port
        }
        if ($copy.Role -eq 'Primary' -and $copy.Primary) { throw "Primary $($copy.Vm) cannot replicate another node." }
        if ($copy.Role -eq 'Replica' -and $null -ne $copy.Slots) { throw "Replica $($copy.Vm) cannot own slots." }
        $byVm[$copy.Vm] = $copy
        $resolved += $copy
    }

    $primaries = @($resolved | Where-Object Role -eq 'Primary')
    if ($primaries.Count -lt 1 -or $primaries.Count -gt 16384) { throw 'A cluster needs between 1 and 16384 primaries.' }
    foreach ($replica in $resolved | Where-Object Role -eq 'Replica') {
        if (-not $replica.Primary -or -not $byVm.ContainsKey($replica.Primary) -or $byVm[$replica.Primary].Role -ne 'Primary') {
            throw "Replica $($replica.Vm) must reference a declared primary."
        }
    }

    $automatic = @($primaries | Where-Object { $_.Slots -is [string] -and $_.Slots -eq 'auto' })
    if ($automatic.Count -gt 0 -and $automatic.Count -ne $primaries.Count) {
        throw 'Use either explicit ranges for every primary or auto for every primary, not both.'
    }
    if ($automatic.Count) {
        $start = 0
        $width = [int][math]::Floor(16384 / $primaries.Count)
        $remainder = 16384 % $primaries.Count
        for ($i = 0; $i -lt $primaries.Count; $i++) {
            $length = $width + [int]($i -lt $remainder)
            $primaries[$i].Slots = ,@($start, ($start + $length - 1))
            $start += $length
        }
    }

    $assigned = [System.Collections.Generic.HashSet[int]]::new()
    foreach ($primary in $primaries) {
        if ($primary.Slots -isnot [array]) { throw "Missing slot ranges for $($primary.Vm)." }
        foreach ($range in $primary.Slots) {
            if ($range -isnot [array] -or $range.Count -ne 2) { throw "Invalid slot range for $($primary.Vm)." }
            foreach ($value in $range) {
                if (($value -isnot [int] -and $value -isnot [long]) -or $value -lt 0 -or $value -gt 16383) {
                    throw "Slots must be integers between 0 and 16383 ($($primary.Vm))."
                }
            }
            if ($range[0] -gt $range[1]) { throw "Reversed slot range for $($primary.Vm)." }
            foreach ($slot in $range[0]..$range[1]) {
                if (-not $assigned.Add($slot)) { throw "Overlapping slot assignment: $slot" }
            }
        }
    }
    if ($assigned.Count -ne 16384) { throw "Slot assignments must cover all 16384 slots; found $($assigned.Count)." }
    return $resolved
}

function Expand-ClusterDefinitionNodes {
    param([object]$Definition)

    $properties = $Definition.PSObject.Properties.Name
    if ($properties -contains 'Nodes') {
        if ($properties -contains 'Primaries' -or $properties -contains 'Replicas') {
            throw 'Use Nodes or Primaries/Replicas, not both.'
        }
        if ($Definition.Nodes -isnot [array]) { throw 'Nodes must be an array.' }
        return $Definition.Nodes
    }

    foreach ($role in @('Primary', 'Replica')) {
        $name = if ($role -eq 'Primary') { 'Primaries' } else { 'Replicas' }
        if ($role -eq 'Replica' -and $properties -notcontains $name) { continue }
        $groups = $Definition.$name
        if ($groups -isnot [array] -or ($role -eq 'Primary' -and $groups.Count -eq 0)) {
            throw "$name must be an array$(if ($role -eq 'Primary') { ' with at least one group' })."
        }
        foreach ($group in $groups) {
            if ($group -isnot [pscustomobject]) { throw "Each $name group must be an object." }
            $allowed = if ($role -eq 'Primary') { @('VMs', 'Slots') } else { @('VMs', 'Primary') }
            foreach ($property in $group.PSObject.Properties.Name) {
                if ($property -notin $allowed) { throw "Unknown $name group property: $property" }
            }
            if ($group.VMs -isnot [array] -or $group.VMs.Count -eq 0) {
                throw "$name group VMs must be a nonempty array."
            }
            if ($role -eq 'Primary') {
                $automatic = $group.Slots -is [string] -and $group.Slots -eq 'auto'
                if (-not $automatic -and $group.Slots -isnot [array]) {
                    throw 'Primary group Slots must be auto or explicit ranges.'
                }
                if (-not $automatic -and $group.VMs.Count -ne 1) {
                    throw 'Explicit slot assignments require one VM per primary group.'
                }
            } elseif ($group.Primary -isnot [string] -or -not $group.Primary) {
                throw 'Replica groups must name one primary using Primary.'
            }
            foreach ($vm in $group.VMs) {
                $node = [pscustomobject]@{ Vm = $vm; Role = $role }
                if ($role -eq 'Primary') { $node | Add-Member -NotePropertyName Slots -NotePropertyValue $group.Slots }
                else { $node | Add-Member -NotePropertyName Primary -NotePropertyValue $group.Primary }
                $node
            }
        }
    }
}

function Read-ClusterDefinition {
    param([string]$Path, [switch]$RequireInstanceConfig)

    $definitionPath = (Resolve-Path -LiteralPath $Path -ErrorAction Stop).Path
    $json = Get-Content -LiteralPath $definitionPath -Raw
    $document = [System.Text.Json.JsonDocument]::Parse($json)
    $document.Dispose()
    $definition = $json | ConvertFrom-Json
    $allowed = @('SchemaVersion', 'Kind', 'System', 'InstanceConfig', 'VmDomain', 'SshUser', 'Port', 'Nodes', 'Primaries', 'Replicas')
    foreach ($property in $definition.PSObject.Properties.Name) {
        if ($property -notin $allowed) { throw "Unknown cluster definition property: $property" }
    }
    if (($definition.SchemaVersion -isnot [int] -and $definition.SchemaVersion -isnot [long]) -or
        $definition.SchemaVersion -ne 1 -or $definition.Kind -ne 'Cluster') { throw 'Expected a SchemaVersion 1 Cluster definition.' }
    if ($definition.System -notin @('garnet', 'valkey')) { throw 'System must be garnet or valkey.' }
    if ($definition.PSObject.Properties.Name -contains 'VmDomain' -and ($definition.VmDomain -isnot [string] -or
        [Uri]::CheckHostName($definition.VmDomain) -ne [UriHostNameType]::Dns -or
        $definition.VmDomain -match '[^a-zA-Z0-9.-]')) {
        throw 'VmDomain must be a DNS domain name.'
    }
    if ($definition.PSObject.Properties.Name -contains 'SshUser' -and
        ($definition.SshUser -isnot [string] -or $definition.SshUser -notmatch '^[a-zA-Z_][a-zA-Z0-9_.-]*$')) {
        throw 'SshUser must be a valid SSH username.'
    }
    if (($definition.Port -isnot [int] -and $definition.Port -isnot [long]) -or $definition.Port -lt 1 -or $definition.Port -gt 65535) {
        throw 'Port must be an integer between 1 and 65535.'
    }
    if ($definition.InstanceConfig -isnot [string] -or -not $definition.InstanceConfig) { throw 'InstanceConfig is required.' }
    $instancePath = $definition.InstanceConfig
    if (-not [IO.Path]::IsPathRooted($instancePath)) {
        $instancePath = Join-Path (Split-Path -Parent $definitionPath) $instancePath
    }
    if ($RequireInstanceConfig -and -not (Test-Path -LiteralPath $instancePath -PathType Leaf)) {
        throw "Instance configuration not found: $instancePath"
    }
    $nodes = @(Resolve-ClusterNodes -Nodes @(Expand-ClusterDefinitionNodes -Definition $definition))
    foreach ($node in $nodes) {
        if ($node.Ip -or $node.Port) { throw 'Node-level Ip and Port overrides are not supported; use VmDomain and the cluster Port.' }
        $node.Port = [int]$definition.Port
        if ($definition.VmDomain) {
            $node | Add-Member -NotePropertyName HostName -NotePropertyValue "$($node.Vm).$($definition.VmDomain)"
        }
    }
    $definition.InstanceConfig = [IO.Path]::GetFullPath($instancePath)
    $definition | Add-Member -NotePropertyName Nodes -NotePropertyValue $nodes -Force
    return $definition
}

function Get-ClusterDataPlaneIp {
    $output = & ip -j -4 addr show eth1
    if ($LASTEXITCODE -ne 0) { throw 'Unable to query eth1.' }
    $interfaces = ($output -join "`n") | ConvertFrom-Json
    $addresses = @($interfaces.addr_info | Where-Object { $_.family -eq 'inet' } | ForEach-Object local)
    if ($addresses.Count -ne 1) { throw 'Expected exactly one IPv4 data-plane address on eth1.' }
    return $addresses[0]
}

function Get-ClusterCli {
    param([string]$System, [switch]$UseTls)
    if ($UseTls) {
        $metadata = Get-Content '/opt/azurebench/tls/metadata.json' -Raw | ConvertFrom-Json
        if ($metadata.targetHost -notmatch '^[a-zA-Z0-9][a-zA-Z0-9.-]*$') { throw 'Invalid TLS target host in certificate metadata.' }
        return [pscustomobject]@{
            Name = 'valkey-cli-tls'
            Arguments = @('--tls', '--cacert', '/opt/azurebench/tls/ca.crt', '--sni', $metadata.targetHost)
        }
    }
    return [pscustomobject]@{ Name = $(if ($System -eq 'valkey') { 'valkey-cli' } else { 'redis-cli' }); Arguments = @() }
}

function Invoke-ClusterCommand {
    param([object]$Cli, [object]$Node, [string[]]$Arguments, [switch]$ExpectOk)
    $cliArguments = @($Cli.Arguments) + @('--raw', '-h', $Node.Ip, '-p', [string]$Node.Port) + $Arguments
    $output = & $Cli.Name @cliArguments 2>&1
    $code = $LASTEXITCODE
    $reply = ($output | Out-String).Trim()
    if ($code -ne 0 -or ($ExpectOk -and $reply -ne 'OK')) {
        throw "Cluster command '$($Arguments -join ' ')' on $($Node.Ip):$($Node.Port) failed (exit $code): $reply"
    }
    return $reply
}

function Wait-ClusterCondition {
    param([scriptblock]$Condition, [string]$Description, [int]$TimeoutSec = 60)
    $deadline = [datetime]::UtcNow.AddSeconds($TimeoutSec)
    do {
        if (& $Condition) { return }
        Start-Sleep -Milliseconds 250
    } while ([datetime]::UtcNow -lt $deadline)
    throw "Timed out waiting for $Description."
}

function New-ClusterTopology {
    param([object[]]$Nodes, [object]$Cli, [int]$TimeoutSec = 60)

    $Nodes = @(Resolve-ClusterNodes -Nodes $Nodes)
    $ips = @{}
    foreach ($node in $Nodes) {
        $address = $null
        if (-not [Net.IPAddress]::TryParse([string]$node.Ip, [ref]$address) -or
            $address.AddressFamily -ne [Net.Sockets.AddressFamily]::InterNetwork -or
            $node.Port -lt 1 -or $node.Port -gt 65535 -or $ips.ContainsKey("$($node.Ip):$($node.Port)")) {
            throw "Invalid or duplicate endpoint for $($node.Vm)."
        }
        $ips["$($node.Ip):$($node.Port)"] = $true
    }
    $ids = @{}
    foreach ($node in $Nodes) {
        $id = Invoke-ClusterCommand -Cli $Cli -Node $node -Arguments @('CLUSTER', 'MYID')
        if ($id -notmatch '^[a-fA-F0-9]{40}$' -or $id -in $ids.Values) { throw "Invalid or duplicate node ID for $($node.Vm): $id" }
        $ids[$node.Vm] = $id
    }
    foreach ($node in $Nodes | Where-Object { $_.Role -eq 'Primary' -and $_.Slots.Count -gt 0 }) {
        $arguments = @('CLUSTER', 'ADDSLOTSRANGE')
        foreach ($range in $node.Slots) { $arguments += @([string]$range[0], [string]$range[1]) }
        Invoke-ClusterCommand -Cli $Cli -Node $node -Arguments $arguments -ExpectOk | Out-Null
    }
    for ($i = 0; $i -lt $Nodes.Count; $i++) {
        Invoke-ClusterCommand -Cli $Cli -Node $Nodes[$i] -Arguments @('CLUSTER', 'SET-CONFIG-EPOCH', [string]($i + 1)) -ExpectOk | Out-Null
    }
    for ($i = 1; $i -lt $Nodes.Count; $i++) {
        Invoke-ClusterCommand -Cli $Cli -Node $Nodes[0] -Arguments @('CLUSTER', 'MEET', $Nodes[$i].Ip, [string]$Nodes[$i].Port) -ExpectOk | Out-Null
    }
    foreach ($node in $Nodes | Where-Object Role -eq 'Replica') {
        $primaryId = $ids[$node.Primary]
        Wait-ClusterCondition -TimeoutSec $TimeoutSec -Description "$($node.Vm) to discover primary $($node.Primary)" -Condition {
            $known = Invoke-ClusterCommand -Cli $Cli -Node $node -Arguments @('CLUSTER', 'NODES')
            return $known -match "(?m)^$primaryId\s"
        }
        Invoke-ClusterCommand -Cli $Cli -Node $node -Arguments @('CLUSTER', 'REPLICATE', $primaryId) -ExpectOk | Out-Null
    }
    Confirm-ClusterTopology -Nodes $Nodes -Cli $Cli -NodeIds $ids -TimeoutSec $TimeoutSec
}

function Confirm-ClusterTopology {
    param([object[]]$Nodes, [object]$Cli, [hashtable]$NodeIds, [int]$TimeoutSec = 60)

    foreach ($node in $Nodes) {
        Wait-ClusterCondition -TimeoutSec $TimeoutSec -Description "$($node.Vm) cluster state, role, slots and replication" -Condition {
            $info = Invoke-ClusterCommand -Cli $Cli -Node $node -Arguments @('CLUSTER', 'INFO')
            if ($info -notmatch '(?m)^cluster_state:ok\r?$' -or $info -notmatch '(?m)^cluster_slots_ok:16384\r?$') { return $false }
            $members = Invoke-ClusterCommand -Cli $Cli -Node $node -Arguments @('CLUSTER', 'NODES')
            $self = @($members -split '\r?\n' | Where-Object { ($_ -split '\s+')[0] -eq $NodeIds[$node.Vm] })
            if ($self.Count -ne 1) { return $false }
            $fields = $self[0] -split '\s+'
            $flags = $fields[2] -split ','
            if ($node.Role -eq 'Replica') {
                if ('slave' -notin $flags -or $fields[3] -ne $NodeIds[$node.Primary]) { return $false }
                $replication = Invoke-ClusterCommand -Cli $Cli -Node $node -Arguments @('INFO', 'replication')
                return $replication -match '(?m)^master_link_status:up\r?$' -and
                    $replication -match '(?mi)^master_sync_in_progress:(?:0|false)\r?$'
            }
            if ('master' -notin $flags) { return $false }
            $actual = [System.Collections.Generic.HashSet[int]]::new()
            foreach ($token in $fields | Select-Object -Skip 8) {
                if ($token -notmatch '^(\d+)(?:-(\d+))?$') { return $false }
                $start = [int]$Matches[1]
                $end = if ($Matches[2]) { [int]$Matches[2] } else { $start }
                foreach ($slot in $start..$end) { $null = $actual.Add($slot) }
            }
            $expected = @($node.Slots | ForEach-Object { $_[0]..$_[1] })
            return $actual.SetEquals([int[]]$expected)
        }
    }

    $summary = foreach ($node in $Nodes) {
        $ping = Invoke-ClusterCommand -Cli $Cli -Node $node -Arguments @('PING')
        if ($ping -ne 'PONG') { throw "PING failed on $($node.Vm) ($($node.Ip):$($node.Port)): $ping" }
        [pscustomobject]@{
            VM = $node.Vm
            Endpoint = "$($node.Ip):$($node.Port)"
            Role = $node.Role
            Primary = $(if ($node.Role -eq 'Replica') { $node.Primary } else { '-' })
            Slots = $(if ($node.Role -eq 'Primary') { ($node.Slots | ForEach-Object { "$($_[0])-$($_[1])" }) -join ', ' } else { '-' })
            Status = 'UP'
            Replication = $(if ($node.Role -eq 'Replica') { 'up / synced' } else { '-' })
        }
    }
    $primaryCount = @($Nodes | Where-Object Role -eq 'Primary').Count
    $replicaCount = @($Nodes | Where-Object Role -eq 'Replica').Count
    Write-Host ''
    Write-Host '==== Cluster setup summary ====' -ForegroundColor Cyan
    Write-Host (($summary | Format-Table -AutoSize -Wrap | Out-String -Width 200).TrimEnd())
    Write-Host "Cluster is UP: $primaryCount primary node(s), $replicaCount replica node(s), all 16384 slots healthy." -ForegroundColor Green
    Write-Host 'Validated: PING on every node, cluster state, declared roles and slot ownership, replica links and full-sync completion.' -ForegroundColor Green
}
