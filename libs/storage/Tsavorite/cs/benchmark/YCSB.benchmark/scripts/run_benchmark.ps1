<#$f
.SYNOPSIS
    Runs one or more builds of Tsavorite.benchmark.exe with multiple parameter permutations and generates corresponding directories of result files named for those permutations.

.DESCRIPTION
    This is intended to run performance-testing parameter permutations on one or more builds of Tsavorite.benchmark.exe, to be compared by compare_runs.ps1.
    The default execution of this script does a performance run on all Tsavorite.benchmark.exes identified in ExeDirs, and places their output into correspondingly-named
    result directories, to be evaluated with compare_runs.ps1.

    This script functions best if you have a dedicated performance-testing machine that is not your build machine. Use the following steps:
    1. Create a directory on the perf machine for your test
    2. You may either copy already-built binaries (e.g. containing changes you don't want to push) to the performance directory, or supply branch names to be git-cloned and built:
        A. Copy existing build: Xcopy the baseline build's Release directory to your perf folder, as well as all comparison builds. This script will start at the $framework 
            directory to traverse to Tsavorite.benchmark.exe. Name these folders something that indicates their role, such as 'baseline', 'main' / 'branch', etc.
            -or-
        B. Supply branch names to be built: In the ExeDirs argument, pass the names of all branches you want to run. For each branch name, this script will clone that branch into
           a directory named as that branch, build Tsavorite.slnx for Release, and run the Tsavorite.benchmark.exe from its built location.
    3. Copy this script and, if you will want to compare runs on the perf machine, compare_runs.ps1 to the perf folder.
    4. In a remote desktop on the perf machine, change to your folder, and run this file with those directory names. See .EXAMPLE for details.

.PARAMETER ExeDirs
    One or more directories from which to run Tsavorite.benchmark.exe builds. This is a Powershell array of strings; thus from the windows command line 
    the directory names should be joined by , (comma) with no spaces:
        pwsh -c ./run_benchmark.ps1 './baseline','./locktable-revision'

    Single (or double) quotes are optional and may be omitted if the directory paths do not contain spaces. Normally 'baseline' is 'main'.

.PARAMETER RunSeconds
    Number of seconds to run the experiment.
    Used primarily to debug changes to this script or do a quick one-off run; the default is 30 seconds.

.PARAMETER ThreadCount
    Number of threads to use.
    Used primarily to debug changes to this script or do a quick one-off run; the default is multiple counts as defined in the script.

.PARAMETER LockMode
    Locking mode to use: 0 = No locking, 1 = RecordInfo locking, 2 = Manual locking
    Used primarily to debug changes to this script or do a quick one-off run; the default is multiple counts as defined in the script.

.PARAMETER ReadPercentages
    Keys the Operation to perform: An array of one or more of:
        0 = No read (Upsert workload only)
        100 = All reads
        Between 0 and 100 = mix of reads and upserts
        -1 = All RMWs
    The default is 0,100: one pass with all upserts, and one pass with all reads

.PARAMETER UseRecover
    Recover the TsavoriteKV from a checkpoint of a previous run rather than loading it from data.
    Used primarily to debug changes to this script or do a quick one-off run; the default is false.

.PARAMETER CloneAndBuild
    Clone the repo and switch to the branches in ExeDirs, then build these.

.EXAMPLE
    pwsh -c "./run_benchmark.ps1 ./main,./locktable-revision"

    If run from your perf directory using the setup from .DESCRIPTION, this will create and populate the following folders:
        ./results_$framework/main
        ./results_$framework/locktable-revision
    You can then run compare.ps1 on those two directories.

.EXAMPLE
    pwsh -c "./run_benchmark.ps1 './baseline','./locktable-revision' -RunSeconds 3 -NumThreads 8 -UseRecover"

    Does a quick run (e.g. test changes to this file).

.EXAMPLE
    pwsh -c "./run_benchmark.ps1 './baseline','./one_local_change','./another_local_change' <other args>"

    Runs 3 directories.

.EXAMPLE
    pwsh -c "./run_benchmark.ps1 main,branch_with_my_changes -ReadPercentages -1 <other args>"

    Runs an RMW-only workload

.EXAMPLE
    pwsh -c "./run_benchmark.ps1 main,branch_with_my_changes -CloneAndBuild <other args>"

    Clones the main branch to the .\main folder, the branch_with_my_changes to the branch_with_my_changes folder, and runs those with any <other args> specified.

.EXAMPLE
    pwsh -c "./run_benchmark.ps1 main,branch_with_my_changes -CloneAndBuild -LockMode 0"

    Clones the main branch to the .\main folder, the branch_with_my_changes to the branch_with_my_changes folder, and runs those with no locking operations;
    this is for best performance.
#>
param (
  [Parameter(Mandatory=$true)] [string[]]$ExeDirs,
  [Parameter(Mandatory=$false)] [int]$RunSeconds = 30,
  [Parameter(Mandatory=$false)] [int]$ThreadCount = -1,
  [Parameter(Mandatory=$false)] [int]$LockMode = -1,
  [Parameter(Mandatory=$false)] [int[]]$ReadPercentages,
  [Parameter(Mandatory=$false)] [switch]$UseRecover,
  [Parameter(Mandatory=$false)] [switch]$CloneAndBuild
)

if (-not(Test-Path d:/data)) {
    throw "Cannot find d:/data"
}

$framework = "net7.0"
$benchmarkExe = "$framework/Tsavorite.benchmark.exe"

if ($CloneAndBuild) {
    $exeNames = [String[]]($ExeDirs | ForEach-Object{"$_/cs/benchmark/bin/x64/Release/$benchmarkExe"})

    Foreach ($branch in $exeDirs) {
        git clone https://github.com/microsoft/Garnet.git $branch
        cd $branch
        git checkout $branch
        dotnet build libs\storage\Tsavorite\cs\Tsavorite.slnx -c Release
        cd ..
    }
} else {
    $exeNames = [String[]]($ExeDirs | ForEach-Object{"$_/cs/benchmark/bin/x64/Release/$benchmarkExe"})
}

Foreach ($exeName in $exeNames) {
    if (Test-Path "$exeName") {
        Write-Host "Found: $exeName"
        continue
    }
    throw "Cannot find: $exeName"
}

$resultDirs = [String[]]($ExeDirs | ForEach-Object{"./results_$framework/" + (Get-Item $_).Name})
Foreach ($resultDir in $resultDirs) {
    Write-Host $resultDir
    if (Test-Path $resultDir) {
        throw "$resultDir already exists (or possible duplication of leaf name in ExeDirs)"
    }
    New-Item "$resultDir" -ItemType Directory
}

$iterations = 7
$distributions = ("uniform", "zipf")
$readPercents = (0, 100)
$threadCounts = (1, 20, 40, 60, 80)
$lockModes = (0, 1)
$smallDatas = (0) #, 1)
$smallMemories = (0) #, 1)
$syntheticDatas = (0) #, 1)
$k = ""

if ($ThreadCount -ge 0) {
    $threadCounts = ($ThreadCount)
}
if ($LockMode -ge 0) {
    $lockModes = ($LockMode)
}
if ($ReadPercentages) {
    $readPercents = $ReadPercentages
}
if ($UseRecover) {
    $k = "-k"
}

# Numa will always be set in the internal loop body to either 0 or 1, so "Numas.Count" is effectively 1
$permutations = $distributions.Count *
                $readPercents.Count *
                $threadCounts.Count *
                $lockModes.Count *
                $smallDatas.Count *
                $smallMemories.Count *
                $syntheticDatas.Count

$permutation = 1
foreach ($d in $distributions) {
    foreach ($r in $readPercents) {
        foreach ($t in $threadCounts) {
            foreach ($z in $lockModes) {
                foreach ($sd in $smallDatas) {
                    foreach ($sm in $smallMemories) {
                        foreach ($sy in $syntheticDatas) {
                            Write-Host
                            Write-Host "Permutation $permutation of $permutations"

                            # Only certain combinations of Numa/Threads are supported
                            $n = ($t -lt 48) ? 0 : 1;

                            for($ii = 0; $ii -lt $exeNames.Count; ++$ii) {
                                $exeName = $exeNames[$ii]
                                $resultDir = $resultDirs[$ii]

                                Write-Host
                                Write-Host "Permutation $permutation/$permutations generating results $($ii + 1)/$($exeNames.Count) to $resultDir for: -n $n -d $d -r $r -t $t -z $z -i $iterations --runsec $RunSeconds $k"

                                # RunSec and Recover are for one-off operations and are not recorded in the filenames.
                                & "$exeName" -b 0 -n $n -d $d -r $r -t $t -z $z -i $iterations --runsec $RunSeconds $k | Tee-Object "$resultDir/results_n-$($n)_d-$($d)_r-$($r)_t-$($t)_z-$($z).txt"
                            }
                            ++$permutation
                        }
                    }
                }
            }
        }
    }
}
