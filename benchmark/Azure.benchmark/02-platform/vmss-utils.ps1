function Invoke-VmssAzureJson {
    param([string[]]$Arguments)

    $output = @(& az @Arguments 2>&1)
    $code = $LASTEXITCODE
    $json = ($output | Out-String).Trim()
    if ($code -ne 0 -or -not $json) {
        throw "Azure command 'az $($Arguments -join ' ')' failed (exit $code): $json"
    }
    return ($json | ConvertFrom-Json)
}

function Test-VmssNonInteractive {
    return [Environment]::GetCommandLineArgs() -contains '-NonInteractive'
}

function Select-Vmss {
    param(
        [string]$ResourceGroup,
        [string]$VmssName,
        [switch]$Single,
        [switch]$NonInteractive,
        [scriptblock]$PowerStateProvider
    )

    if (-not $ResourceGroup) { throw '-ResourceGroup (alias -rg) is required for VMSS selection.' }
    if ($Single -and $VmssName -and ($VmssName -eq 'all' -or $VmssName.Contains(','))) {
        throw 'Select exactly one VMSS; all and multiple VMSS names are not supported.'
    }
    if (-not $VmssName -and ($NonInteractive -or (Test-VmssNonInteractive))) {
        throw '-VmssName is required for non-interactive execution.'
    }
    Write-Host "`n=== Discovering VMSS in resource group '$ResourceGroup' ===" -ForegroundColor Cyan
    $info = @(Invoke-VmssAzureJson -Arguments @('vmss', 'list', '--resource-group', $ResourceGroup,
        '--query', '[].{name:name, state:provisioningState}', '-o', 'json'))
    if (-not $info.Count) { throw "No VMSS found in resource group '$ResourceGroup'." }
    $names = @($info | ForEach-Object { $_.name })
    if (@($names | Where-Object { $_ -isnot [string] -or -not $_ }).Count) {
        throw 'Azure VMSS inventory contains an invalid name.'
    }

    $prompted = -not $VmssName
    if ($prompted) {
        Write-Host "`nAvailable VMSS:"
        $powerMap = @{}
        if ($PowerStateProvider) {
            Write-Host '  (querying power state...)' -ForegroundColor DarkGray
            $powerMap = & $PowerStateProvider $names
        }
        $rows = for ($i = 0; $i -lt $info.Count; $i++) {
            [pscustomobject]@{
                Idx = "[$i]"
                Name = $info[$i].name
                Prov = [string]$info[$i].state
                Power = $(if ($PowerStateProvider) { [string]$powerMap[$info[$i].name] } else { 'not queried' })
            }
        }
        $wIdx = [Math]::Max('#'.Length, (($rows | ForEach-Object { $_.Idx.Length } | Measure-Object -Maximum).Maximum))
        $wName = [Math]::Max('NAME'.Length, (($rows | ForEach-Object { $_.Name.Length } | Measure-Object -Maximum).Maximum))
        $wProv = [Math]::Max('PROVISIONING'.Length, (($rows | ForEach-Object { $_.Prov.Length } | Measure-Object -Maximum).Maximum))
        $wPower = [Math]::Max('POWER'.Length, (($rows | ForEach-Object { $_.Power.Length } | Measure-Object -Maximum).Maximum))
        Write-Host ("  {0}  {1}  {2}  {3}" -f '#'.PadRight($wIdx), 'NAME'.PadRight($wName),
            'PROVISIONING'.PadRight($wProv), 'POWER'.PadRight($wPower)) -ForegroundColor Cyan
        Write-Host ("  {0}  {1}  {2}  {3}" -f ('-' * $wIdx), ('-' * $wName),
            ('-' * $wProv), ('-' * $wPower)) -ForegroundColor DarkGray
        foreach ($row in $rows) {
            $provColor = if ($row.Prov -eq 'Succeeded') { 'Green' } elseif ($row.Prov -eq 'Failed') { 'Red' } else { 'Yellow' }
            $powerColor = if ($row.Power -match 'running') { 'Green' } elseif ($row.Power -match 'deallocated|stopped') { 'DarkGray' } else { 'Yellow' }
            Write-Host ("  {0}  {1}  " -f $row.Idx.PadRight($wIdx), $row.Name.PadRight($wName)) -NoNewline
            Write-Host $row.Prov.PadRight($wProv) -NoNewline -ForegroundColor $provColor
            Write-Host '  ' -NoNewline
            Write-Host $row.Power.PadRight($wPower) -ForegroundColor $powerColor
        }
        Write-Host ''
        $prompt = if ($Single) { 'Select VMSS (one index or name)' } else { "Select VMSS (comma-separated indices/names, or 'all')" }
        $VmssName = Read-Host $prompt
    }
    if ([string]::IsNullOrWhiteSpace($VmssName)) { throw 'No VMSS selected.' }
    if ($Single -and ($VmssName -eq 'all' -or $VmssName.Contains(','))) {
        throw 'Select exactly one VMSS; all and multiple VMSS names are not supported.'
    }

    $selected = @()
    if ($VmssName -eq 'all') {
        $selected = $names
    } else {
        foreach ($part in ($VmssName -split ',' | ForEach-Object { $_.Trim() })) {
            if ($prompted -and $part -match '^\d+$') {
                $index = [int]$part
                if ($index -ge $names.Count) { throw "Invalid VMSS index: $index" }
                $selected += $names[$index]
            } elseif ($part -in $names) {
                $selected += $part
            } else {
                throw "VMSS '$part' not found in resource group '$ResourceGroup'. Available: $($names -join ', ')"
            }
        }
    }
    Write-Host "Target VMSS: $($selected -join ', ')" -ForegroundColor Green
    return $selected
}

function Resolve-VmssExecutionContext {
    param(
        [string]$ResourceGroup,
        [string]$VmssName,
        [string[]]$VmNames,
        [string]$SshUser,
        [switch]$NonInteractive
    )

    if (-not $VmNames.Count) { throw 'At least one declared VM is required.' }
    $selected = Select-Vmss -ResourceGroup $ResourceGroup -VmssName $VmssName -Single -NonInteractive:$NonInteractive
    if (-not $SshUser) {
        $SshUser = Invoke-VmssAzureJson -Arguments @('vmss', 'show', '--resource-group', $ResourceGroup,
            '--name', $selected, '--query', 'virtualMachineProfile.osProfile.adminUsername', '-o', 'json')
    }
    if ($SshUser -notmatch '^[a-zA-Z_][a-zA-Z0-9_.-]*$') { throw 'VMSS SSH username is missing or invalid; use -SshUser to override.' }
    $inventory = @(Invoke-VmssAzureJson -Arguments @('vmss', 'list-instance-public-ips', '--resource-group', $ResourceGroup,
        '--name', $selected, '--query', '[].{id:id, fqdn:dnsSettings.fqdn}', '-o', 'json'))
    $nodes = foreach ($vm in $VmNames) {
        $matches = @($inventory | Where-Object {
            $_.fqdn -is [string] -and ($_.fqdn -split '\.')[0] -eq $vm
        })
        if ($matches.Count -ne 1) {
            throw "Expected exactly one SSH FQDN for declared VM '$vm' in VMSS '$selected'; found $($matches.Count). VM names must match DNS labels."
        }
        $hostName = $matches[0].fqdn
        if ([Uri]::CheckHostName($hostName) -ne [UriHostNameType]::Dns -or $hostName -match '[^a-zA-Z0-9.-]') {
            throw "Invalid Azure SSH FQDN for '$vm': $hostName"
        }
        [pscustomobject]@{ Vm = $vm; HostName = $hostName }
    }
    return [pscustomobject]@{
        ResourceGroup = $ResourceGroup
        VmssName = $selected
        SshUser = $SshUser
        Nodes = @($nodes)
    }
}
