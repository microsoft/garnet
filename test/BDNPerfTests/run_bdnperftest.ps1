<#$f
.SYNOPSIS
    This script is designed for to run BDN Benchmarking and use the results as a gate in the GitHub CIs to ensure performance isn't declining 

.DESCRIPTION

    Script to test for performance regressions using BDN Benchmark tool.  There are configuration files (in /ConfigFiles dir) associated with each test that contains name and expected values of the BDN benchmark. Any of these can be sent as the parameter to the file.
    
        CI_BDN_Config_RespParseStress.json
        

    NOTE: The expected values are specific for the CI Machine. If you run these on your machine, you will need to change the expected values.
    NOTE: The acceptablerange* parameters in the config file is how far +/- X% the found value can be from the expected value and still say it is pass. Defaulted to 10% 
    
.EXAMPLE
    ./run_bdnperftest.ps1 
    ./run_bdnperftest.ps1 CI_BDN_Config_RespParseStress.json
#>

# Send the config file for the benchmark. Defaults to a simple one
param (
  [string]$configFile = "CI_BDN_Config_RespParseStress.json"
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
        Write-Output "**   ** PASS! **  The performance result ($dblfoundResultValue) is in the acceptable range +/-$acceptablePercentRange% ($LowerBound -> $UpperBound) of expected value: $expectedResultValue " -ForegroundColor Green
        Write-Output "** "
        return $true # the values are close enough
    }
    else {
        if ($warnonly) {
            Write-Output "**   << PERF REGRESSION WARNING! >>  The BDN benchmark result ($dblfoundResultValue) is OUT OF RANGE +/-$acceptablePercentRange% ($LowerBound -> $UpperBound) of expected value: $expectedResultValue" -ForegroundColor Yellow
            Write-Output "** "
        }
        else {
            Write-Output "**   << PERF REGRESSION FAIL! >>  The  BDN benchmark ($dblfoundResultValue) is OUT OF ACCEPTABLE RANGE +/-$acceptablePercentRange% ($LowerBound -> $UpperBound) of expected value: $expectedResultValue" -ForegroundColor Red
            Write-Output "** "
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
if ($pathstring.Contains("test")) {
    Write-Output "------------ DEBUG ***********************************"
    $position = $pathString.IndexOf("test")
    $basePath = $pathstring.Substring(0,$position-1)  # take off slash off end as well
    Write-Output "------------ DEBUG The position of 'test' is: $position"
} else {
    $basePath = $pathstring  # already in base as not in test
    Set-Location .\test\BDNPerfTests\
    Write-Output "------------ DEBUG New Location:" $pwd.Path 
}
Write-Output "------------ DEBUG Basepath: $basePath" 


# Read the test config file and convert the JSON to a PowerShell object
$fullConfiFileAndPath = "ConfigFiles/$configFile"
if (-not (Test-Path -Path $fullConfiFileAndPath)) {
    Write-Error -Message "The test config file $fullConfiFileAndPath does not exist." -Category ObjectNotFound
    exit
}
$json = Get-Content -Raw $fullConfiFileAndPath
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
#    Write-Error -Message "The Number of Cores on this machine ($NumberOfCores) are not the same as the Expected Cores ($ExpectedCoresToTestOn) found in the test config file: $fullConfiFileAndPath."
#    exit
#}

Write-Output "************** Start BDN.benchmark ********************" 
Write-Output " "

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
$justResultsFileNameNoExt = $configFile -replace ".{5}$"   # strip off the .json
$resultsFileName = $justResultsFileNameNoExt + "_" + $CurrentOS + ".results"
$resultsFile = "$resultsDir/$resultsFileName"
$BDNbenchmarkErrorFile = "$errorLogDir/$justResultsFileNameNoExt" + "_StandardError_" +$CurrentOS+".log"


Write-Output "------------ DEBUG #########################"
Write-Output "*#*#* Configuration $configuration"
Write-Output "*#*#* framework $framework"
Write-Output "*#*#* filter $filter"
Write-Output "*#*#* Standard Out (Results) $resultsFile"
Write-Output "*#*#* Standard Error $BDNbenchmarkErrorFile"
Write-Output "*#*#* Working Dir (BDNBM Path) $BDNbenchmarkPath"
Write-Output "*------------ DEBUG #########################"

Write-Output "** Start BDN Benchmark: $filter"
Write-Output " "
Write-Output "** Start:  dotnet run -c $configuration -f $framework --filter $filter --project $BDNbenchmarkPath  > $resultsFile 2> $BDNbenchmarkErrorFile"
dotnet run -c $configuration -f $framework --filter $filter --project $BDNbenchmarkPath  > $resultsFile 2> $BDNbenchmarkErrorFile


# TO DO ###########################
# Get Upload of artifacts working (YML file changes) -- Might be fixed, just need to check in and try
# Parse output  -- WHERE LEFT OFF!!!!  After ran once, comment out the dotnet run and just work on parsing stuff
# Analyze?
# Add "CI" only switch so can run on GH (default to CI?  If so - add full run switch to not analyze but gather and push data somewhere)
# For YML files (ADO and GH) - do we need "build" Tsav and Garnet before?  Guessing yes, but worth a test to see. Maybe the run of benchmark builds everything it needs
# TO DO ###########################


Write-Output "** BDN Benchmark for $filter finished"
Write-Output " "


Write-Output "**** EVALUATE THE RESULTS FILE $resultsFile ****"

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

Write-Output "************************"
Write-Output "**   RESULTS  "
Write-Output "**   "

# Results to monitor \ analyze: Median, 99, 99.9 and tpt
# The columns are separated by spaces but the problem is that each column number of spaces differs so can't just easily do a split. 
# However, can remove a chunk of them and put in a ; in and then use that to delimit. If only do a few spaces, then get multiple ";" between the columns where only want 1 ";"
$resultsLine = Get-Content -Tail 1 $resultsFile
$resultsLine = $resultsLine -replace  " {7}", ";"
Write-Output "-- Debug --  $resultsLine"

# Get the column values that are wanted (Median, 99 percent, 99.9 percent and TPT)
$results = $resultsLine -split ";"
$resultsMethod = $results[2].trim()
$resultsMean = $results[5].trim()
$resultsError = $results[6].trim()
$resultsStdDev = $results[9].trim()
$resultsAllocated = $results[12].trim()

# Median results verification
Write-Output "**  Median "
#$currentResults = AnalyzeResult $resultsMedian $expectedMedianValue $acceptableRangeMedian
#if ($currentResults -eq $false) {
#    $testSuiteResult = $false
#}



Write-Output "**  "
Write-Output "************************"
Write-Output "**  Final summary:"
Write-Output "**  "
if ($testSuiteResult) {
    Write-Output "**   PASS!  All tests passed  "
} else {
    Write-Error -Message "**   BDN Benchmark PERFORMANCE REGRESSION FAIL!  At least one test had performance outside of expected range. NOTE: Expected results are based on CI machine and may differ from the machine that this was ran on."
}
Write-Output "**  "
Write-Output "************************"
