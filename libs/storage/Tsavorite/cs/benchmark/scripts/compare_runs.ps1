<#
.SYNOPSIS
    Compares two directories of output from Tsavorite.benchmark.exe, usually run by run_benchmark.ps1.

.DESCRIPTION
    See run_benchmark.ps1 for instructions on setting up a perf directory and running the benchmark parameter permutations.
    Once run_benchmark.ps1 has completed, you can either run this script from the perf directory if it was copied there, or from
    another machine that has access to the perf directory on the perf machine.

    This script will:
    1. Compare result files of the same name in the two directories (the names are set by run_benchmark.ps1 to identify the parameter permutation used in that file).
    2. List any result files that were not matched.
    3. Display two Grids, one showing the results of the comparison for Loading times, and one for Experiment Run times. These differences are:
        a. The difference in Mean throughput in inserts or transactions (operations) per second.
        b. The percentage difference in throughput. The initial ordering of the grid is by this column, descending; thus the files for which the best performance
        improvement was made are shown first.
        c. The difference in Standard Deviation.
        d. The difference in Standard Deviation as a percentage of Mean.
        e. All other parameters of the run (these are the same between the two files).
    
.PARAMETER OldDir
    The directory containing the results of the baseline run; the result comparison is "NewDir throughput minus OldDir throughput".

.PARAMETER RunSeconds
    The directory containing the results of the new run, with the changes to be tested for impact; the result comparison is "NewDir throughput minus OldDir throughput".

.PARAMETER NoLock
    Do not include results with locking

.EXAMPLE
    pwsh -c "./compare_runs.ps1 './baseline' './locktable-revision'""

    Single (or double) quotes are optional and may be omitted if the directory paths do not contain spaces. Normally 'baseline' is 'main'.

.EXAMPLE
    ./compare_runs.ps1 ./main ./locktable-revision -NoLock

    Does not compare results that have locking specified.
#>
param (
  [Parameter(Mandatory)] [String]$OldDir,
  [Parameter(Mandatory)] [String]$NewDir,
  [Parameter(Mandatory=$false)] [switch]$NoLock
)

class Result : System.IComparable, System.IEquatable[Object] {
    # To make things work in one class, name the properties "Left", "Right", and "Diff"--they aren't displayed until the Diff is calculated.
    [double]$BaselineMean
    [double]$BaselineStdDev
    [double]$CurrentMean
    [double]$CurrentStdDev
    [double]$MeanDiff
    [double]$MeanDiffPercent
    [double]$StdDevDiff
    [double]$StdDevDiffPercent
    [double]$Overlap
    [double]$OverlapPercent
    [double]$Separation
    [uint]$Numa
    [string]$Distribution
    [int]$ReadPercent
    [uint]$ThreadCount
    [uint]$LockMode
    [uint]$Iterations
    [bool]$SmallData
    [bool]$SmallMemory
    [bool]$SyntheticData
    [bool]$NoAff
    [int]$ChkptMs
    [string]$ChkptType
    [bool]$ChkptIncr

    Result([string]$line) {
        $fields = $line.Split(';')
        foreach($field in ($fields | Select-Object -skip 1)) {
            $arg, $value = $field.Split(':')
            $value = $value.Trim()
            switch ($arg.Trim()) {
                "ins/sec" { $this.MeanDiff = $this.BaselineMean = $value }
                "ops/sec" { $this.MeanDiff = $this.BaselineMean = $value }
                "stdev" { $this.StdDevDiff = $this.BaselineStdDev = $value }
                "stdev%" { $this.StdDevDiffPercent = $value }
                "n" { $this.Numa = $value }
                "d" { $this.Distribution = $value }
                "r" { $this.ReadPercent = $value }
                "t" { $this.ThreadCount = $value }
                "z" { $this.LockMode = $value }
                "i" { $this.Iterations = $value }
                "sd" { $this.SmallData = $value -eq "y" }
                "sm" { $this.SmallMemory = $value -eq "y"  }
                "sy" { $this.SyntheticData = $value -eq "y"  }
                "noaff" { $this.NoAff = $value -eq "y"  }
                "chkptms" { $this.ChkptMs = $value }
                "chkpttype" { $this.ChkptType = $value }
                "chkptincr" { $this.ChkptIncr = $value -eq "y"  }
            }
        }
    }

    Result([Result]$other) {
        $this.Numa = $other.Numa
        $this.Distribution = $other.Distribution
        $this.ReadPercent = $other.ReadPercent
        $this.ThreadCount = $other.ThreadCount
        $this.LockMode = $other.LockMode
        $this.Iterations = $other.Iterations
        $this.SmallData = $other.SmallData
        $this.SmallMemory = $other.SmallMemory
        $this.SyntheticData = $other.SyntheticData
        $this.NoAff = $other.NoAff
        $this.ChkptMs = $other.ChkptMs
        $this.ChkptType = $other.ChkptType
        $this.ChkptIncr = $other.ChkptIncr
    }

    [Result] CalculateDifference([Result]$newResult) {
        $result = [Result]::new($newResult)
        $result.MeanDiff = $newResult.MeanDiff - $this.MeanDiff
        $result.MeanDiffPercent = [System.Math]::Round(($result.MeanDiff / $this.MeanDiff) * 100, 1)
        $result.StdDevDiff = $newResult.StdDevDiff - $this.StdDevDiff
        $result.StdDevDiffPercent = $newResult.StdDevDiffPercent - $this.StdDevDiffPercent
        $result.BaselineMean = $this.BaselineMean
        $result.BaselineStdDev = $this.BaselineStdDev
        $result.CurrentMean = $newResult.BaselineMean
        $result.CurrentStdDev = $newResult.BaselineStdDev
        
        $oldMin = $result.BaselineMean - $result.BaselineStdDev
        $oldMax = $result.BaselineMean + $result.BaselineStdDev
        $newMin = $result.CurrentMean - $result.CurrentStdDev
        $newMax = $result.CurrentMean + $result.CurrentStdDev
        
        $lowestMax = [System.Math]::Min($oldMax, $newMax)
        $highestMin = [System.Math]::Max($oldMin, $newMin)
        $result.Overlap = $lowestMax - $highestMin
        
        # Overlap % is the percentage of the new stddev range covered by the overlap, or 0 if there is no overlap.
        # Separation is how many new stddevs separates the two stddev spaces (positive for perf gain, negative for
        # perf loss), or 0 if they overlap. TODO: Calculate significance.
        if ($result.Overlap -le 0) {
            $result.OverlapPercent = 0
            $adjustedOverlap = ($oldMax -lt $newMin) ? -$result.Overlap : $result.Overlap
            $result.Separation = [System.Math]::Round($adjustedOverlap / $result.CurrentStdDev, 1)
        } else {
            $result.OverlapPercent = [System.Math]::Round(($result.Overlap / ($result.CurrentStdDev * 2)) * 100, 1)
            $result.Separation = 0
        }
        
        return $result
    }

    [int] CompareTo($other)
    {
        If (-Not($other -is [Result])) {
            Throw "'other' is not Result"
        }

        # Sort in descending order
        $cmp = $other.MeanDiffPercent.CompareTo($this.MeanDiffPercent)
        return ($cmp -eq 0) ? $other.MeanDiff.CompareTo($this.MeanDiff) : $cmp
    }

    [bool] Equals($other)
    {
        Write-Host "in Equals"
        If (-Not($other -is [Result])) {
            Throw "'other' is not Result"
        }
        return $this.Numa -eq $other.Numa
         -and $this.Distribution -eq $other.Distribution
         -and $this.ReadPercent -eq $other.ReadPercent
         -and $this.ThreadCount -eq $other.ThreadCount
         -and $this.LockMode -eq $other.LockMode
         -and $this.Iterations -eq $other.Iterations
         -and $this.SmallData -eq $other.SmallData
         -and $this.SmallMemory -eq $other.SmallMemory
         -and $this.SyntheticData -eq $other.SyntheticData
         -and $this.NoAff -eq $other.NoAff
         -and $this.ChkptMs -eq $other.ChkptMs
         -and $this.ChkptType -eq $other.ChkptType
         -and $this.ChkptIncr -eq $other.ChkptIncr
    }

    [int] GetHashCode() {
        return ($this.Numa, $this.Distribution, $this.ReadPercent, $this.ThreadCount, $this.LockMode,
                $this.Iterations, $this.SmallData, $this.SmallMemory, $this.SyntheticData,
                $this.NoAff, $this.ChkptMs, $this.ChkptType, $this.ChkptIncr).GetHashCode();
    }
}

# These have the same name format in each directory, qualified by parameters.
$oldOnlyFileNames = New-Object Collections.Generic.List[String]
$newOnlyFileNames = New-Object Collections.Generic.List[String]

$LoadResults = New-Object Collections.Generic.List[Result]
$RunResults = New-Object Collections.Generic.List[Result]

function ParseResultFile([String]$fileName) {
    $loadResult = $null
    $runResult = $null
    foreach($line in Get-Content($fileName)) {
        if ($line.StartsWith("##20;")) {
            $loadResult = [Result]::new($line)
            continue
        }
        if ($line.StartsWith("##21;")) {
            $runResult = [Result]::new($line)
            continue
        }
    }
    if ($null -eq $loadResult) {
        Throw "$fileName has no Load Result"
    }
    if ($null -eq $runResult) {
        Throw "$fileName has no Run Result"
    }
    return ($loadResult, $runResult)
}

foreach($oldFile in Get-ChildItem "$OldDir/results_*") {
    $newName = "$NewDir/$($oldFile.Name)";
    if (!(Test-Path $newName)) {
        $oldOnlyFileNames.Add($oldFile)
        continue
    }
    $newFile = Get-ChildItem $newName

    $oldLoadResult, $oldRunResult = ParseResultFile $oldFile.FullName
    $newLoadResult, $newRunResult = ParseResultFile $newFile.FullName

    $LoadResults.Add($oldLoadResult.CalculateDifference($newLoadResult))
    $RunResults.Add($oldRunResult.CalculateDifference($newRunResult))
}

foreach($newFile in Get-ChildItem "$NewDir/results_*") {
    $oldName = "$OldDir/$($newFile.Name)";
    if (!(Test-Path $oldName)) {
        $newOnlyFileNames.Add($newFile)
        continue
    }
}

if ($oldOnlyFileNames.Count -gt 0) {
    Write-Host "The following files were found only in $OldDir"
    foreach ($fileName in $oldOnlyFileNames) {
        Write-Host "    $fileName"
    }
}
if ($newOnlyFileNames.Count -gt 0) {
    Write-Host "The following files were found only in $NewDir"
    foreach ($fileName in $newOnlyFileNames) {
        Write-Host "    $fileName"
    }
}

if ($oldOnlyFileNames.Count -gt 0 -or $newOnlyFileNames.Count -gt 0) {
    Start-Sleep -Seconds 3
}

$LoadResults.Sort()
$RunResults.Sort()

function RenameProperties([System.Object[]]$results) {
    if ($NoLock) {
        $results = $results | Where-Object {$_.LockMode -eq 0}
    }
    
    # Use this to rename "Percent" suffix to "%"
    $results | Select-Object `
                BaselineMean,
                BaselineStdDev,
                @{N='BStDev %';E={[System.Math]::Round(($_.BaselineStdDev / $_.BaselineMean) * 100, 1)}},
                CurrentMean,
                CurrentStdDev,
                @{N='CStDev %';E={[System.Math]::Round(($_.CurrentStdDev / $_.CurrentMean) * 100, 1)}},
                MeanDiff,
                @{N='MeanDiff %';E={$_.MeanDiffPercent}},
                StdDevDiff,
                @{N='StdDevDiff %';E={$_.StdDevDiffPercent}},
                Overlap,
                @{N='Overlap %';E={$_.OverlapPercent}},
                Separation,
                Numa,
                Distribution,
                ReadPercent,
                ThreadCount,
                LockMode,
                Iterations,
                SmallData,
                SmallMemory,
                SyntheticData,
                NoAff,
                ChkptMs,
                ChkptType,
                ChkptIncr
}

RenameProperties $LoadResults | Out-GridView -Title "Loading Comparison (Inserts Per Second): $OldDir -vs- $NewDir"
RenameProperties $RunResults | Out-GridView  -Title "Experiment Run Comparison(Operations Per Second): $OldDir -vs- $NewDir"
