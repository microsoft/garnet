function Invoke-MigrationMeasuredPasses([string]$Cell, [int]$ValueBytes) {
    $null = Send-WorkloadControl "$Cell-baseline" $true
    Wait-WorkloadDuration $BaselineSeconds
    $baseline = Send-WorkloadControl "$Cell-baseline-paused"
    $baseline | ConvertTo-Json -Depth 30 | Set-Content "$OutputDirectory/foreground-baseline.json"
    $measured = [Collections.Generic.List[object]]::new()
    $seconds = 0.0
    for ($pass = 1; $pass -le $MaximumPasses; $pass++) {
        $transfer = Invoke-Transfer $SourceAddress $TargetAddress "$Cell-pass$pass" $ValueBytes $pass $seconds
        $measured.Add($transfer)
        $passes.Add($transfer)
        $passes | Export-Csv (Join-Path $OutputDirectory 'passes.csv') -NoTypeInformation
        $seconds += $transfer.Seconds
        $null = Invoke-Transfer $TargetAddress $SourceAddress "$Cell-return$pass" $ValueBytes
        if ($pass -ge $MinimumPasses -and $seconds -ge $MinimumSeconds) { break }
    }
    if ($seconds -lt $MinimumSeconds -or $measured.Count -lt $MinimumPasses) {
        throw "Measurement too short after $MaximumPasses passes: $Cell."
    }
    [pscustomobject]@{ Measured = $measured; Seconds = $seconds }
}

function Invoke-MigrationRecovery([int]$PassNumber, [double]$MigrationSeconds) {
    if ($PassNumber -lt $MinimumPasses -or $MigrationSeconds -lt $MinimumSeconds) { return $false }
    $null = Send-WorkloadControl "$cell-recovery" $true
    Wait-WorkloadDuration $RecoverySeconds
    return $true
}

function Merge-Distribution([object[]]$Values) {
    $bins = [Collections.Generic.SortedDictionary[long,long]]::new()
    $count = 0L; $errors = 0L
    foreach ($value in $Values) {
        $count += [long]$value.Count
        $errors += [long]$value.Errors
        $checked = 0L
        foreach ($bin in $value.Bins) {
            $key = [long]$bin.Value
            if (-not $bins.ContainsKey($key)) { $bins.Add($key, 0) }
            $bins[$key] += [long]$bin.Count
            $checked += [long]$bin.Count
        }
        if ($checked -ne [long]$value.Count) { throw 'Histogram bins do not match actual operation counts.' }
    }
    if ($count -eq 0) { throw 'No actual workload operations.' }
    $percentiles = @{}
    foreach ($p in @(50, 95, 99, 99.9, 100)) {
        $rank = [long][math]::Ceiling($count * $p / 100.0)
        $cumulative = 0L
        foreach ($bin in $bins.GetEnumerator()) {
            $cumulative += $bin.Value
            if ($cumulative -ge $rank) { $percentiles["$p"] = $bin.Key / 1000.0; break }
        }
    }
    [pscustomobject]@{
        Count = $count; Successes = $count - $errors; Errors = $errors
        P50Ms = $percentiles['50']; P95Ms = $percentiles['95']; P99Ms = $percentiles['99']
        P999Ms = $percentiles['99.9']; MaximumMs = $percentiles['100']
    }
}

function Get-ForegroundSummary([string]$Directory) {
    $replies = @(Get-ChildItem "$Directory/*-pass*-foreground.json" | ForEach-Object {
        Get-Content $_.FullName -Raw | ConvertFrom-Json
    })
    if (-not $replies.Count) { throw 'Missing migration-overlap foreground replies.' }
    $mode = 'PerPass'
    $policyPath = Join-Path $Directory 'measurement-windows.json'
    if (Test-Path $policyPath) {
        $policy = Get-Content $policyPath -Raw | ConvertFrom-Json
        if ($policy.Mode -ne 'OncePerCase' -or -not $policy.Cell -or
            $policy.BaselineSeconds -le 0 -or $policy.RecoverySeconds -le 0) {
            throw 'Invalid measurement-window policy.'
        }
        $mode = $policy.Mode
    }
    $records = foreach ($phase in @('baseline', 'migration', 'recovery')) {
        $metrics = @()
        $seconds = 0.0
        if ($mode -eq 'OncePerCase' -and $phase -ne 'migration') {
            $path = Join-Path $Directory "foreground-$phase.json"
            if (-not (Test-Path $path)) { throw "Missing once-per-case $phase evidence." }
            $reply = Get-Content $path -Raw | ConvertFrom-Json
            $selected = @($reply.Phases | Where-Object Name -eq "$($policy.Cell)-$phase")
            $requiredSeconds = if ($phase -eq 'baseline') { $policy.BaselineSeconds } else { $policy.RecoverySeconds }
            if ($selected.Count -ne 1 -or $selected[0].Measurements.Seconds -lt $requiredSeconds) {
                throw "Missing or incomplete once-per-case $phase phase."
            }
            $metrics = @($selected[0].Measurements)
            $seconds = [double]$selected[0].Measurements.Seconds
        } else {
            foreach ($reply in $replies) {
                $tag = $reply.Phase -replace '-paused$', ''
                if ($phase -eq 'migration') {
                    if ($reply.Window.Seconds -le 0) { throw 'Invalid migration-overlap duration.' }
                    $metrics += $reply.Window.Measurements
                    $seconds += [double]$reply.Window.Seconds
                } else {
                    $selected = @($reply.Phases | Where-Object Name -eq "$tag-$phase")
                    if ($selected.Count -ne 1) { throw "Missing unique $phase phase: $tag" }
                    $metrics += $selected[0].Measurements
                    $seconds += [double]$selected[0].Measurements.Seconds
                }
            }
        }
        if ($seconds -le 0) { throw 'Invalid workload measurement duration.' }
        $get = Merge-Distribution @($metrics | ForEach-Object GET)
        $set = Merge-Distribution @($metrics | ForEach-Object SET)
        if ($get.Errors -or $set.Errors) { throw 'Foreground errors invalidate a successful result.' }
        [pscustomobject]@{
            Phase = $phase; Seconds = $seconds; GET = $get; SET = $set
            GetOps = $get.Successes / $seconds; SetOps = $set.Successes / $seconds
            Ops = ($get.Successes + $set.Successes) / $seconds
            ActualSetPercent = 100.0 * $set.Count / ($get.Count + $set.Count)
            UsefulRxGBps = $get.Successes * 2048 / $seconds / 1e9
            UsefulTxGBps = $set.Successes * 2048 / $seconds / 1e9
            MinimumRespRxGBps = ($get.Successes * 2057 + $set.Successes * 5) / $seconds / 1e9
            MinimumRespTxGBps = ($get.Successes * 27 + $set.Successes * 2084) / $seconds / 1e9
        }
    }
    $sampled = @($replies | ForEach-Object {
        $tag = $_.Phase -replace '-paused$', ''
        $_.Phases | Where-Object Name -eq "$tag-migration"
    } | ForEach-Object Measurements)
    if ($sampled.Count -ne $replies.Count) { throw 'Missing unique migration phase evidence.' }
    [pscustomobject]@{
        Passes = $replies.Count; Phases = @($records); MeasurementWindowMode = $mode
        ProfiledMigrationPhaseOperations = ($sampled | Measure-Object ProfiledOperations -Sum).Sum
        SampledMigrationPhaseAsk = ($sampled | Measure-Object SampledAsk -Sum).Sum
        SampledMigrationPhaseMoved = ($sampled | Measure-Object SampledMoved -Sum).Sum
        VerifiedWritesAtLastPass = ($replies | Measure-Object VerifiedWrites -Maximum).Maximum
    }
}
