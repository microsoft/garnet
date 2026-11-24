function Get-MigrationValidationPolicy(
    [ValidateRange(1, [long]::MaxValue)][long]$KeyCount,
    [ValidateRange(1, [int]::MaxValue)][int]$ValueBytes
) {
    $full = $KeyCount -le [math]::Floor(1GB / $ValueBytes)
    [pscustomobject]@{
        Mode = $(if ($full) { 'Full' } else { 'DbSizeOnly' })
        KeyCount = $KeyCount
        ValueBytes = $ValueBytes
        FullVerificationMaxBytes = 1GB
    }
}

function Assert-MigrationValidationResult(
    [string]$Directory,
    [long]$KeyCount,
    [int]$ValueBytes,
    [ValidateRange(1, 100)][int]$Passes,
    [long]$VerifiedWrites
) {
    $path = Join-Path $Directory 'validation-policy.json'
    if (Test-Path $path) {
        $policy = Get-Content $path -Raw | ConvertFrom-Json
        $expected = Get-MigrationValidationPolicy $KeyCount $ValueBytes
        if ($policy.Mode -ne $expected.Mode -or $policy.KeyCount -ne $KeyCount -or
            $policy.ValueBytes -ne $ValueBytes) {
            throw 'Incorrect recorded migration validation policy.'
        }
        if ($policy.Mode -eq 'DbSizeOnly' -and $VerifiedWrites -ne 0) {
            throw 'DBSIZE-only validation must not claim verified write values.'
        }
    } else {
        $policy = [pscustomobject]@{ Mode = 'Full' }
    }
    if ($policy.Mode -eq 'Full' -and $VerifiedWrites -le 0) {
        throw 'Missing acknowledged-write verification.'
    }
    $counts = @(Import-Csv (Join-Path $Directory 'key-counts.csv'))
    foreach ($pass in 1..$Passes) {
        foreach ($direction in 'pass', 'return') {
            foreach ($side in 'source', 'target') {
                $entries = @($counts | Where-Object Tag -Like "*-$direction$pass-$side")
                $expected = if ($side -eq 'source') { 0L } else { $KeyCount }
                if ($entries.Count -ne 1 -or [long]$entries[0].Expected -ne $expected -or
                    [long]$entries[0].Actual -ne $expected) {
                    throw "Missing or incorrect DBSIZE evidence: $direction$pass-$side."
                }
            }
        }
    }
    return $policy.Mode
}
