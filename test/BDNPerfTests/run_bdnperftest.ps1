<#$f
.SYNOPSIS
    This script is designed for to run BDN Benchmarking and use the results as a gate in the GitHub CIs to ensure performance isn't declining 

.DESCRIPTION

    Script to test for performance regressions using BDN Benchmark tool.  There are configuration files associated with each test that contains name and expected values of the BDN benchmark. Any of these can be sent as the parameter to the file.
    
        ConfigFiles/CI_BDN_Config_RespParseStress.json
        

    NOTE: The expected values are specific for the CI Machine. If you run these on your machine, you will need to change the expected values.
    NOTE: The acceptablerange* parameters in the config file is how far +/- X% the found value can be from the expected value and still say it is pass. Defaulted to 10% 
    
.EXAMPLE
    ./run_bdnperftest.ps1 
    ./run_bdnperftest.ps1 ConfigFiles/CI_BDN_Config_RespParseStress.json
#>

# Send the config file for the benchmark. Defaults to a simple one
param (
  [string]$configFile = "ConfigFiles/CI_BDN_Config_RespParseStress.json"
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
            Write-Host "**   << PERF REGRESSION WARNING! >>  The BDN benchmark result ($dblfoundResultValue) is OUT OF RANGE +/-$acceptablePercentRange% ($LowerBound -> $UpperBound) of expected value: $expectedResultValue" -ForegroundColor Yellow
            Write-Host "** "
        }
        else {
            Write-Host "**   << PERF REGRESSION FAIL! >>  The  BDN benchmark ($dblfoundResultValue) is OUT OF ACCEPTABLE RANGE +/-$acceptablePercentRange% ($LowerBound -> $UpperBound) of expected value: $expectedResultValue" -ForegroundColor Red
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
# TO DO: NOT WWORKING YET
#  
######################################################
function ParseThroughPutValueFromResults {
param ($foundThroughPutLine)

    #$foundThroughputValue = $foundThroughPutLine -replace "^.{12}"  # gets rid of label "Throughput:"
    #$foundThroughputValue = $foundThroughputValue.Remove($foundThroughputValue.Length - 8)  # gets rid of the "ops/sec" 
    #$foundThroughputValue = $foundThroughputValue -replace ",", ""  # gets rid of the "," in the number

    $foundThroughputValue = "0"

    return $foundThroughputValue
}

# ******** BEGIN MAIN  *********
# Get base path since paths can differ from machine to machine
$pathstring = $pwd.Path
Write-Host "-----------------------------------------------DEBUG Path String: $pathstring" 
$position = $pathstring.IndexOf("test")
Write-Host "-----------------------------------------------DEBUG Test Position: $position" 
if ( 0 -eq $position )
{
    Write-Host "-----------------------------------------------DEBUG Going GitHub" 
    $position = $pathstring.IndexOf(".github")
    Write-Host "-----------------------------------------------DEBUG GitHub Position: $position" 

}
Write-Host "-----------------------------------------------DEBUG Position: $position" 
$basePath = $pathstring.Substring(0,$position-1)  # take off slash off end as well

Write-Host "------------ DEBUG Basepath: $basePath" 


# Read the test config file and convert the JSON to a PowerShell object
if (-not (Test-Path -Path $configFile)) {
    Write-Error -Message "The test config file $configFile does not exist." -Category ObjectNotFound
    exit
}
$json = Get-Content -Raw $configFile
$object = $json | ConvertFrom-Json

# Use this in the file name to separate outputs when running in ADO
$CurrentOS = "Windows"
if ($IsLinux) {
    $CurrentOS = "Linux"
}

# Don't Think Need this 
# Calculate number of cores on the test machine - used for max thread and to verify the config file settings
#if ($IsLinux) {
#    $sockets = [int]$(lscpu | grep -E '^Socket' | awk '{print $2}')
#    $coresPerSocket = [int]$(lscpu | grep -E '^Core' | awk '{print $4}')
#    $NumberOfCores = ($sockets * $coresPerSocket)

#    $ExpectedCoresToTestOn = $object.ExpectedCoresToTestOn_linux
#}
#else {
#    $NumberOfCores = (Get-CimInstance -ClassName Win32_Processor).NumberOfCores | Measure-Object -Sum | Select-Object -ExpandProperty Sum

#    $ExpectedCoresToTestOn = $object.ExpectedCoresToTestOn_win
#}

# To get accurate comparison of found vs expected values, double check to make sure config settings for Number of Cores of the test machine are what is specified in the test config file
#if ($ExpectedCoresToTestOn -ne $NumberOfCores) {
#    Write-Error -Message "The Number of Cores on this machine ($NumberOfCores) are not the same as the Expected Cores ($ExpectedCoresToTestOn) found in the test config file: $configFile."
#    exit
#}

Write-Host "************** Start BDN.benchmark ********************" 
Write-Host " "

# Set all the config options (args to benchmark app) based on values from config json file
$configuration = $object.configuration
$framework = $object.framework
$filter = $object.filter

# Set the expected values based on the OS
if ($IsLinux) {
    # Linux expected values
    $expectedGetMeanValue = $object.expectedGETMeanValue_linux
    $expectedSetMeanValue = $object.expectedSETMeanValue_linux
    $expectedMGetMeanValue = $object.expectedMGETMeanValue_linux
    $expectedMSetMeanValue = $object.expectedMSETMeanValue_linux
    $expectedInLinePingMeanValue = $object.expectedInLinePingMeanValue_linux
    $expectedSETEXMeanValue = $object.expectedSETEXMeanValue_linux
    $expectedZAddRemMeanValue = $object.expectedZAddRemMeanValue_linux
    $expectedLPushPopMeanValu = $object.expectedLPushPopMeanValue_linux
    $expectedSAddRemMeanValue = $object.expectedSAddRemMeanValue_linux
    $expectedHSetDelMeanValue = $object.expectedHSetDelMeanValue_linux
    $expectedMyDictSetGetMeanValue = $object.expectedMyDictSetGetMeanValue_linux
    $expectedIncrMeanValue = $object.expectedIncrMeanValue_linux
}
else {
    # Windows expected values
    $expectedGetMeanValue = $object.expectedGETMeanValue_win
    $expectedSetMeanValue = $object.expectedSETMeanValue_win
    $expectedMGetMeanValue = $object.expectedMGETMeanValue_win
    $expectedMSetMeanValue = $object.expectedMSETMeanValue_win
    $expectedInLinePingMeanValue = $object.expectedInLinePingMeanValue_win
    $expectedSETEXMeanValue = $object.expectedSETEXMeanValue_win
    $expectedZAddRemMeanValue = $object.expectedZAddRemMeanValue_win
    $expectedLPushPopMeanValu = $object.expectedLPushPopMeanValue_win
    $expectedSAddRemMeanValue = $object.expectedSAddRemMeanValue_win
    $expectedHSetDelMeanValue = $object.expectedHSetDelMeanValue_win
    $expectedMyDictSetGetMeanValue = $object.expectedMyDictSetGetMeanValue_win
    $expectedIncrMeanValue = $object.expectedIncrMeanValue_win
}

# percent allowed variance when comparing expected vs actual found value - same for linux and windows. 
# TO DO: Figure out Error and Std Dev range ... have % off of .1 or we have 
$acceptableError = $object.acceptableError  
$acceptableStdDev = $object.acceptableStdDev  
$acceptableMeanRange = $object.acceptableMeanRange 


# Set up the results dir and errorlog dir
$resultsDir = "$basePath/test/BDNPerfTests/results" 
if (-not (Test-Path -Path $resultsDir)) {
    New-Item -Path $resultsDir -ItemType Directory
}
$errorLogDir = "$basePath/test/BDNPerfTests/errorlog" 
if (-not (Test-Path -Path $errorLogDir)) {
    New-Item -Path $errorLogDir -ItemType Directory
}

# Run the BDN.benchmark
$BDNbenchmarkPath = "$basePath/benchmark/BDN.benchmark"  

# Create Results and all the log files using the the config file name as part of the name of the results \ logs
$resultsFileName = $configFile.Substring(12)
$justResultsFileNameNoExt = $resultsFileName -replace ".{5}$"
$resultsFileName = $justResultsFileNameNoExt + "_" + $CurrentOS + ".results"
$resultsFile = "$resultsDir/$resultsFileName"
$BDNbenchmarkErrorFile = "$errorLogDir/$justResultsFileNameNoExt" + "_StandardError_" +$CurrentOS+".log"


Write-Host "*#*#*################################# DEBUG #########################"
Write-Host "*#*#* Configuration $configuration"
Write-Host "*#*#* framework $framework"
Write-Host "*#*#* filter $filter"
Write-Host "*#*#* Standard Out $resultsFile"
Write-Host "*#*#* Standard Error $BDNbenchmarkErrorFile"
Write-Host "*#*#* Working Dir (BDNBM Path) $BDNbenchmarkPath"
Write-Host "*#*#*################################# DEBUG #########################"

Write-Host "** Start BDN Benchmark: $filter"
Write-Host " "
 Write-Host "** Start:  dotnet run -c $configuration -f $framework --filter $filter --project $BDNbenchmarkPath  > $resultsFile 2> $BDNbenchmarkErrorFile"
dotnet run -c $configuration -f $framework --filter $filter --project $BDNbenchmarkPath  > $resultsFile 2> $BDNbenchmarkErrorFile


# TO DO ###########################
# Failed on GH Action regarding base path -- line 85 (see message below) ... looks like not finding "test" ... guess because it is in the yml -- maybe have it find "workflows" or ".github" if "test" is not found
# For YML files (ADO and GH) - do we need "build" Tsav and Garnet before?  Guessing yes, but worth a test to see. Maybe the run of benchmark builds everything it needs
# Parse output
# Analyze?
# Add "CI" only switch so can run on GH (default to CI?  If so - add full run switch to not analyze but gather and push data somewhere)
# TO DO ###########################

### ERROR WHEN RUNNING GH ACTIONS ###
#ParentContainsErrorRecordException: /home/runner/work/garnet/garnet/test/BDNPerfTests/run_bdnperftest.ps1:85
#Line |
#  85 |  $basePath = $string.Substring(0,$position-1)  # take off slash off en …
#     |  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#     | Exception calling "Substring" with "2" argument(s): "length ('-2') must
     #| be a non-negative value. (Parameter 'length') Actual value was -2."
#Error: Process completed with exit code 1.



Write-Host "** BDN Benchmark for $filter finished"
Write-Host " "

<#
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
#>

Write-Host "**  "
Write-Host "************************"
Write-Host "**  Final summary:"
Write-Host "**  "
if ($testSuiteResult) {
    Write-Host "**   PASS!  All tests passed  " -ForegroundColor Green
} else {
    Write-Error -Message "**   BDN Benchmark PERFORMANCE REGRESSION FAIL!  At least one test had performance outside of expected range. NOTE: Expected results are based on CI machine and may differ from the machine that this was ran on."
}
Write-Host "**  "
Write-Host "************************"
