<#$f
.SYNOPSIS
    This script is designed for performance regression testing and not for reporting performance results. The parameters in this script are tuned for reproducibility, and not for showcasing performance of the system.

.DESCRIPTION

    Script to test for performance regressions using RESP Benchmark tool.  There are 8 configuration files associated with this test. Any of these can be sent as the parameter to the file.
    
        ConfigFiles/CI_Config_Online_GetSet_1Thr.json
        ConfigFiles/CI_Config_Online_GetSet_MaxThr.json
        ConfigFiles/CI_Config_Online_ZADDZREM_1Thr.json
        ConfigFiles/CI_Config_Online_ZADDZREM_MaxThr.json
        ConfigFiles/CI_Config_Offline_Get_1Thr.json
        ConfigFiles/CI_Config_Offline_Get_MaxThr.json
        ConfigFiles/CI_Config_Offline_ZADDREM_1Thr.json
        ConfigFiles/CI_Config_Offline_ZADDREM_MaxThr.json

    NOTE: The expected values are specific for the CI Machine. If you run these on your machine, you will need to change the expected values.
    NOTE: The acceptablerange* parameters in the config file is how far +/- X% the found value can be from the expected value and still say it is pass. Defaulted to 10% 
    
.EXAMPLE
    ./run_benchmark.ps1 
    ./run_benchmark.ps1 ConfigFiles/CI_Config_Online_GetSet_1Thr.json
#>

# Send the config file for the benchmark. Defaults to a simple one
param (
  [string]$configFile = "ConfigFiles/CI_Config_Online_GetSet_1Thr.json"
)

# ******** FUNCTION DEFINITIONS  *********

################## AnalyzeResult ##################### 
#  
#  Takes the result and verifies it falls in the acceptable range (tolerance) based on the percentage. 
#  
######################################################
function AnalyzeResult {
    param ($foundResultValue, $expectedResultValue, $acceptablePercentRange, $warnonly)

    # Calculate the lower and upper bounds of the expected value
    [double] $Tolerance = $acceptablePercentRange / 100
    [double] $LowerBound = $expectedResultValue * (1 - $Tolerance)
    [double] $UpperBound = $expectedResultValue * (1 + $Tolerance)
    [double] $dblfoundResultValue = $foundResultValue
    
    # Check if the actual value is within the bounds
    if ($dblfoundResultValue -ge $LowerBound -and $dblfoundResultValue -le $UpperBound) {
        Write-Host "**   ** PASS! **  The performance result ($dblfoundResultValue) is in the acceptable range +/-$acceptablePercentRange% ($LowerBound -> $UpperBound) of expected value: $expectedResultValue " -ForegroundColor Green
        Write-Host "** "
        return $true # the values are close enough
    }
    else {
        if ($warnonly) {
            Write-Host "**   << PERF REGRESSION WARNING! >>  The performance result ($dblfoundResultValue) is OUT OF RANGE +/-$acceptablePercentRange% ($LowerBound -> $UpperBound) of expected value: $expectedResultValue" -ForegroundColor Yellow
            Write-Host "** "
        }
        else {
            Write-Host "**   << PERF REGRESSION FAIL! >>  The performance result ($dblfoundResultValue) is OUT OF ACCEPTABLE RANGE +/-$acceptablePercentRange% ($LowerBound -> $UpperBound) of expected value: $expectedResultValue" -ForegroundColor Red
            Write-Host "** "
        }
        return $false # the values are too different
    }
  }
######### ParseThroughPutValuefrom Results ###########
#  
# Takes the "Throughput" output line that is in a file and 
# strips off all the characters and just to return the actual value
#  
######################################################
function ParseThroughPutValueFromResults {
param ($foundThroughPutLine)

    $foundThroughputValue = $foundThroughPutLine -replace "^.{12}"  # gets rid of label "Throughput:"
    $foundThroughputValue = $foundThroughputValue.Remove($foundThroughputValue.Length - 8)  # gets rid of the "ops/sec" 
    $foundThroughputValue = $foundThroughputValue -replace ",", ""  # gets rid of the "," in the number

    return $foundThroughputValue
}

# ******** BEGIN MAIN  *********
# Get base path since paths can differ from machine to machine
$string = $pwd.Path
$position = $string.IndexOf("test")
$basePath = $string.Substring(0,$position-1)  # take off slash off end as well

# Read the test config file and convert the JSON to a PowerShell object
if (-not (Test-Path -Path $configFile)) {
    Write-Error -Message "The test config file $configFile does not exist." -Category ObjectNotFound
    exit
}
$json = Get-Content -Raw $configFile
$object = $json | ConvertFrom-Json

# net6.0, net7.0 or net8.0 can be used. Prefer to just use most recent
$framework = "net8.0"

# Use this in the file name to separate outputs when running in ADO
$CurrentOS = "Windows"
if ($IsLinux) {
    $CurrentOS = "Linux"
}

# Calculate number of cores on the test machine - used for max thread and to verify the config file settings
if ($IsLinux) {
    $sockets = [int]$(lscpu | grep -E '^Socket' | awk '{print $2}')
    $coresPerSocket = [int]$(lscpu | grep -E '^Core' | awk '{print $4}')
    $NumberOfCores = ($sockets * $coresPerSocket)

    $ExpectedCoresToTestOn = $object.ExpectedCoresToTestOn_linux
}
else {
    $NumberOfCores = (Get-CimInstance -ClassName Win32_Processor).NumberOfCores | Measure-Object -Sum | Select-Object -ExpandProperty Sum

    $ExpectedCoresToTestOn = $object.ExpectedCoresToTestOn_win
}

# To get accurate comparison of found vs expected values, double check to make sure config settings for Number of Cores of the test machine are what is specified in the test config file
if ($ExpectedCoresToTestOn -ne $NumberOfCores) {
    Write-Error -Message "The Number of Cores on this machine ($NumberOfCores) are not the same as the Expected Cores ($ExpectedCoresToTestOn) found in the test config file: $configFile."
    exit
}

Write-Host "************** Start Resp.Benchmark ********************" 
Write-Host " "

# Set all the config options (args to benchmark app) based on values from config json file
$op = $object.op
$op_workload = $object.op_workload
$onlineMode = $object.onlineMode
$batchsize = $object.batchsize
$op_percent = $object.op_percent
$threads = $object.threads
$client = $object.client
$dbsize = $object.dbsize
$runtime = $object.runtime
$keyLength = $object.keyLength
$sscardinality =$object.sscardinality

# Set the expected values based on the OS
if ($IsLinux) {
    # Linux expected values
    $expectedMedianValue = $object.expectedMedianValue_linux
    $expected99Value = $object.expected99Value_linux
    $expected99_9Value = $object.expected99_9Value_linux
    $expectedTPTValue = $object.expectedTPTValue_linux
    $expected_MSET_ThroughputValue = $object.expectedMSETThroughputValue_linux
    $expected_GET_ThroughputValue = $object.expectedGETThroughputValue_linux 
}
else {
    # Windows expected values
    $expectedMedianValue = $object.expectedMedianValue_win
    $expected99Value = $object.expected99Value_win
    $expected99_9Value = $object.expected99_9Value_win
    $expectedTPTValue = $object.expectedTPTValue_win
    $expected_MSET_ThroughputValue = $object.expectedMSETThroughputValue_win 
    $expected_GET_ThroughputValue = $object.expectedGETThroughputValue_win
}

# percent allowed variance when comparing expected vs actual found value - same for linux and windows. 
#Probably didn't need to be this granualar (maybe just have one acceptable range percentage for all settings) but just in case for future
$acceptableRangeMedian = $object.acceptableRangeMedian  
$acceptableRange99 = $object.acceptableRange99  
$acceptableRange99_9 = $object.acceptableRange99_9 
$acceptableRangeTPT = $object.acceptableRangeTPT   
$acceptable_MSET_ThroughputRange = $object.acceptableMSETThroughputRange  
$acceptable_GET_ThroughputRange = $object.acceptableGETThroughputRange   


# Build up the args based on what values were in the json file
if ($op) {
    $benchmarkArgs = "$benchmarkArgs --op $op"
}

if ($op_workload) {
    $benchmarkArgs = "$benchmarkArgs --op-workload $op_workload"
}

if ($onlineMode) {
    $benchmarkArgs = "$benchmarkArgs $onlineMode"
}

if ($batchsize) {
    $benchmarkArgs = "$benchmarkArgs -b $batchsize"
}

if ($op_percent) {
    $benchmarkArgs = "$benchmarkArgs --op-percent $op_percent"
}

if ($threads) {
    if ($threads -eq "MAX")
    {
        $threads = $NumberOfCores / 2
    }
    $benchmarkArgs = "$benchmarkArgs -t $threads"
}

if ($client) {
    $benchmarkArgs = "$benchmarkArgs --client $client"
}

if ($dbsize) {
    $benchmarkArgs = "$benchmarkArgs --dbsize $dbsize"
}

if ($runtime) {
    $benchmarkArgs = "$benchmarkArgs --runtime $runtime"
}

if ($keyLength) {
    $benchmarkArgs = "$benchmarkArgs --keylength $keyLength"
}

if ($sscardinality) {
    $benchmarkArgs = "$benchmarkArgs --sscardinality $sscardinality"
}

# Set up the results dir and errorlog dir
$resultsDir = "$basePath/test/PerfRegressionTesting/results" 
if (-not (Test-Path -Path $resultsDir)) {
    New-Item -Path $resultsDir -ItemType Directory
}
$errorLogDir = "$basePath/test/PerfRegressionTesting/errorlog" 
if (-not (Test-Path -Path $errorLogDir)) {
    New-Item -Path $errorLogDir -ItemType Directory
}

# Start the Garnet Server
$garnetServerPath = "$basePath/main/GarnetServer" 
$dotNetArgs = "run -f $framework -c release --no-restore --no-build"
$dotNetExe = "dotnet" 

Write-Host "** Start Garnet Server: Start-Process -FilePath $dotNetExe -ArgumentList $dotNetArgs -WorkingDirectory "$garnetServerPath" -RedirectStandardError "$errorLogDir/GarnetServerErrorOut_$CurrentOS.log" -RedirectStandardOutput $resultsDir/GarnetServerStandardOut_$CurrentOS.log"
Start-Process -FilePath $dotNetExe -ArgumentList $dotNetArgs -WorkingDirectory "$garnetServerPath" -RedirectStandardError "$errorLogDir/GarnetServerErrorOut_$CurrentOS.log" -RedirectStandardOutput "$resultsDir/GarnetServerStandardOut_$CurrentOS.log"
Write-Host "** Garnet Server started ..."

# Probably don't need a sleep because benchmark will wait until server is running but just give it a bit to start up
Start-Sleep -Seconds 3

# Run the Resp.benchmark
$respBenchMarkPath = "$basePath/benchmark/Resp.benchmark"  
$dotNetArgs = "run -f $framework -c Release --no-restore --no-build"
$dotNetExe = "dotnet" 

# Create Results and all the log files using the the config file name as part of the name of the results \ logs
$resultsFileName = $configFile.Substring(12)
$justResultsFileNameNoExt = $resultsFileName -replace ".{5}$"
$resultsFileName = $justResultsFileNameNoExt + "_" + $CurrentOS + ".results"
$resultsFile = "$resultsDir/$resultsFileName"
$respBenchMarkErrorFile = "$errorLogDir/$justResultsFileNameNoExt" + "_StandardError_" +$CurrentOS+".log"

Write-Host " "
Write-Host "** Start:  Start-Process -FilePath $dotNetExe -ArgumentList "$dotNetArgs -- $benchmarkArgs" -WorkingDirectory "$respBenchMarkPath" -RedirectStandardError "$respBenchMarkErrorFile" -RedirectStandardOutput "$resultsFile" -Wait"
Start-Process -FilePath $dotNetExe -ArgumentList "$dotNetArgs -- $benchmarkArgs" -WorkingDirectory "$respBenchMarkPath" -RedirectStandardError "$respBenchMarkErrorFile" -RedirectStandardOutput "$resultsFile" -Wait

Write-Host "** Benchmark test finished"
Write-Host " "

Stop-Process -Name GarnetServer -Force
Write-Host " "

Write-Host "**** EVALUATE THE RESULTS FILE $resultsFile ****"

# First check if file is there and if not, error out gracefully
if (-not (Test-Path -Path $resultsFile)) {
    Write-Error -Message "The test results file $resultsFile does not exist. Check to make sure the test was ran." -Category ObjectNotFound
    exit
}

# Check see if results file size 0
$resultsFileSizeBytes = (Get-Item -Path $resultsFile).Length
if ($resultsFileSizeBytes -eq 0) {
    Write-Error -Message "The test results file $resultsFile is empty."
    exit
}

# Set the test suite to pass and if any one fails, then mark the suite as fail - just one result failure will mark the whole test as failed
$testSuiteResult = $true

Write-Host "************************"
Write-Host "**   RESULTS  "
Write-Host "**   "

# Parse the file for test results - offline has different output format than online
if ($onlineMode -eq "--online")
{
    # Results to monitor \ analyze: Median, 99, 99.9 and tpt
    # The columns are separated by spaces but the problem is that each column number of spaces differs so can't just easily do a split. 
    # However, can remove a chunk of them and put in a ; in and then use that to delimit. If only do a few spaces, then get multiple ";" between the columns where only want 1 ";"
    $resultsLine = Get-Content -Tail 1 $resultsFile
    $resultsLine = $resultsLine -replace  " {7}", ";"

    # Get the column values that are wanted (Median, 99 percent, 99.9 percent and TPT)
    $results = $resultsLine -split ";"
    $resultsMedian = $results[2].trim()
    $results99 = $results[5].trim()
    $results99_9 = $results[6].trim()
    $resultsTPT = $results[9].trim()
   
    # Median results verification
    Write-Host "**  Median "
    $currentResults = AnalyzeResult $resultsMedian $expectedMedianValue $acceptableRangeMedian
    if ($currentResults -eq $false) {
        $testSuiteResult = $false
    }

    # 99 Percent results verification
    Write-Host "**  99 Percent "
    $currentResults = AnalyzeResult $results99 $expected99Value $acceptableRange99 $true
    if ($currentResults -eq $false) {
        Write-Host "**   NOTE: due to variance of P99 results, only showing a warning when it is out of range and not causing entire script to show as fail."
        Write-Host "**"
        #        $testSuiteResult = $false
    }

    # 99.9 Percent results verification
    Write-Host "**  99.9 Percent "
    $currentResults = AnalyzeResult $results99_9 $expected99_9Value $acceptableRange99_9 $true
    if ($currentResults -eq $false) {
        Write-Host "**   NOTE: due to variance of P99.9 results, only showing a warning when it is out of range and not causing entire script to show as fail."
        Write-Host "**"

        #        $testSuiteResult = $false
    }
   
    # tpt (kops/sec) results verification
    Write-Host "**  tpt (kops / sec) "
    $currentResults = AnalyzeResult $resultsTPT $expectedTPTValue $acceptableRangeTPT
    if ($currentResults -eq $false) {
        $testSuiteResult = $false
    }
}
else {   # Offline mode

    # Parse out MSET Throughput only if it exists - only in GET not ZADDREM
    $MSETLineNumber = Select-String -Path $resultsFile -Pattern "Operation type: MSET" | Select-Object -ExpandProperty LineNumber
    if ($MSETLineNumber)
    {
        $MSETResultsLine = Get-Content -Path $resultsFile | Select-Object -Index ($MSETLineNumber + 2)
        $foundMSETThroughputValue = ParseThroughPutValueFromResults $MSETResultsLine

        Write-Host "**  MSET Throughput (ops / sec) "
        $currentMSETResults = AnalyzeResult $foundMSETThroughputValue $expected_MSET_ThroughputValue $acceptable_MSET_ThroughputRange
        if ($currentMSETResults -eq $false) {
            $testSuiteResult = $false
        }
    }

    # Parse out GET Throughput (ops/sec) - known that it is the last "Throughput" in file
    $getResultsLine = Select-String -Path $resultsFile -Pattern 'ops/sec' | Select-Object -Last 1 -ExpandProperty Line
    $foundOpsSecValue = ParseThroughPutValueFromResults $getResultsLine

    Write-Host "**  GET Throughput (ops / sec) "
    $currentGetResults = AnalyzeResult $foundOpsSecValue $expected_GET_ThroughputValue $acceptable_GET_ThroughputRange
    if ($currentGetResults -eq $false) {
        $testSuiteResult = $false
    }
}

Write-Host "**  "
Write-Host "************************"
Write-Host "**  Final summary:"
Write-Host "**  "
if ($testSuiteResult) {
    Write-Host "**   PASS!  All tests passed  " -ForegroundColor Green
} else {
    Write-Error -Message "**   PERFORMANCE REGRESSION FAIL!  At least one test had performance outside of expected range. NOTE: Expected results are based on CI machine and may differ from the machine that this was ran on."
}
Write-Host "**  "
Write-Host "************************"
