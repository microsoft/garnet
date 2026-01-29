#Requires -Version 7

<#$f
.SYNOPSIS
    This script is designed for to run BDN Benchmarking and use the results as a gate in the GitHub CIs to ensure performance isn't declining. 

.DESCRIPTION

    Script to test for performance regressions in Allocated Memory using BDN Benchmark tool. The various tests are in the configuration file (BDN_Benchmark_Config.json) and are associated with each test that contains name and expected values of the BDN benchmark.
    Any of the BDN benchmark tests (ie BDN.benchmark.Operations.BasicOperations.*) can be sent as the parameter to the file.

    NOTE: The expected values are specific for the CI Machine. If you run these on your machine, you will need to change the expected values.

    NOTE: If adding a new BDN perf test to the BDN_Benchmark_Config.json, then need to add to the "test: [..." line in the ci-bdnbenchmark.yml
    
.EXAMPLE
    ./run_bdnperftest.ps1
    ./run_bdnperftest.ps1 BDN.benchmark.Operations.BasicOperations.*
    ./run_bdnperftest.ps1 Operations.BasicOperations    <-- can run this way but this is how specify in ci-bdnbenchmark.yml
    ./run_bdnperftest.ps1 Operations.BasicOperations net8.0
#>

param (
  [string]$currentTest = "BDN.benchmark.Operations.BasicOperations.*",
  [string]$framework = "net8.0"
)

$OFS = "`r`n"

# ******** FUNCTION DEFINITIONS  *********

################## AnalyzeResult ##################### 
#  
#  Takes the result and verifies it falls in the acceptable range (tolerance) based on the percentage.
#  
######################################################
function AnalyzeResult {
    param ($foundResultValue, $expectedResultValue, $acceptablePercentRange, $warnonly)

    # Calculate the upper bounds of the expected value
    $Tolerance = [double]$acceptablePercentRange / 100
    $UpperBound = [double]$expectedResultValue * (1 + $Tolerance)
    $dblfoundResultValue = [double]$foundResultValue

    # Check if the actual value is within the bounds
    if ($dblfoundResultValue -le $UpperBound) {
        Write-Host "**             ** PASS! **  The found Allocated value ($dblfoundResultValue) is below the acceptable threshold of $UpperBound (Expected value $expectedResultValue + $acceptablePercentRange%)"
        Write-Host "** "
        return $true # the values are close enough
    }
    else {
        if ($warnonly) {
            Write-Host "**   << WARNING! >>  The BDN benchmark found Allocated value ($dblfoundResultValue) is above the acceptable threshold of $UpperBound (Expected value $expectedResultValue + $acceptablePercentRange%) but will only be a warning and not cause the test to show as a fail."
            Write-Host "** "
            return $true # Since it is warning, don't want to cause a fail
        }
        else {
            Write-Host "**   << PERF **FAIL!** >> The BDN benchmark found Allocated value ($dblfoundResultValue) is above the acceptable threshold of $UpperBound (Expected value $expectedResultValue + $acceptablePercentRange%)"
            Write-Host "** "
        }
        return $false # the values are too different
    }
  }

######### ParseValueFromResults ###########
#
# Takes the line from the results file and returns the value from the requested column
# strips off all the characters and just to return the actual value
#
# NOTE: Example of ResultsLine from BDN benchmark: "| ZAddRem  | ACL    |  96.05 us | 1.018 us | 0.952 us |  17.97 KB |"
#
######################################################
function ParseValueFromResults {
param ($ResultsLine, $columnNum)

    # Remove the leading and trailing pipes and split the string by '|'
    $columns = $ResultsLine.Trim('|').Split('|')
    $column = $columns | ForEach-Object { $_.Trim() }
    $foundValue = $column[$columnNum]
    if ($foundValue -eq "NA") {
        Write-Error -Message "The value for the column was NA which means that the BDN test failed and didn't generate performance metrics. Verify the BDN test ran successfully."
        exit
    }

    if ($foundValue -eq "-") {
        $foundValue = "0"
    }
   
    $foundValue = $foundValue.Trim(' B')
    $foundValue = $foundValue.Trim(' K')
    $foundValue = $foundValue.Trim(' m')

    return $foundValue
}


# ******** BEGIN MAIN  *********
# Set all the config options
$configFile = "BDN_Benchmark_Config.json"
$configuration = "Release"
$allocatedColumn = "-1"   # last one is allocated, just to ensure in case other column gets added
$acceptableAllocatedRange = "10"   # percent allowed variance when comparing expected vs actual found value - same for linux and windows.

# For Actions the test in the yml does not get specified with .* because it used for file names there, so add .* here if not already on there.
if ($currentTest -notmatch "\.\*$") {
    $currentTest += ".*"
}
# Actions also don't have BDN on beginning due to what files are saved
if ($currentTest -notlike "BDN.benchmark.*") {
    $currentTest = "BDN.benchmark." + $currentTest
}

# Get base path since paths can differ from machine to machine
$pathstring = $pwd.Path
if ($pathstring.Contains("test")) {
    $position = $pathString.IndexOf("test")
    $basePath = $pathstring.Substring(0,$position-1)  # take off slash off end as well
} else {
    $basePath = $pathstring  # already in base and not in test
    Set-Location .\test\BDNPerfTests\
}

# Read the test config file and convert the JSON to a PowerShell object
if (-not (Test-Path -Path $configFile)) {
    Write-Error -Message "The test config file $configFile does not exist." -Category ObjectNotFound
    exit
}

# Use this in the file name to separate outputs when running in ADO
$CurrentOS = "Windows"
if ($IsLinux) {
    $CurrentOS = "Linux"
}

Write-Host "************** Start BDN.benchmark Test Run ********************"
Write-Host " "
Write-Host "** Current Test: $currentTest"
Write-Host " "

# Access the properties under the specific test in the config file
$json = Get-Content -Raw -Path "$configFile" | ConvertFrom-Json
$testProperties = $json.$currentTest

# create a matrix of expected results for specific test
$splitTextArray = New-Object 'string[]' 3
$expectedResultsArray = New-Object 'string[,]' 100, 4

[int]$currentRow = 0
$WARN_ON_FAIL = $false

# Iterate through each property for the expected value - getting method, params and value
foreach ($property in $testProperties.PSObject.Properties) {

    # Split the property name into parts
    $splitTextArray = $property.Name -split '_'

    # get the expected Method, Parameter and value
    $expectedResultsArray[$currentRow,0] = "| " + $splitTextArray[1]  # Add | at beginning so it is part of summary block and not every other line in the file that matches the Method name
    $expectedResultsArray[$currentRow,1] = $splitTextArray[2]
    $expectedResultsArray[$currentRow,2] = $property.value
    $expectedResultsArray[$currentRow,3] = $splitTextArray[0]  # meta data to keep track if warn on fail or not

    $currentRow += 1
}

$totalExpectedResultValues = $currentRow

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

# Create Results and all the log files using the the config file name as part of the name of the results \ logs - strip off the prefix and postfix
$prefix = "BDN.benchmark."
$justResultsFileNameNoExt = $currentTest -replace ".{2}$"   # strip off the .* for log files
$currentTestStripped = $justResultsFileNameNoExt -replace [regex]::Escape($prefix), ""
$resultsFileName = $currentTestStripped + "_" + $CurrentOS + ".results"
$resultsFile = "$resultsDir/$resultsFileName"
$BDNbenchmarkErrorFile = "$errorLogDir/$currentTestStripped" + "_StandardError_" +$CurrentOS+".log"
$filter = $currentTest
$exporter = "json" 

Write-Output " "
Write-Output "** Start:  dotnet run -c $configuration -f $framework --project $BDNbenchmarkPath --filter $filter --exporters $exporter -e BDNRUNPARAM=$framework > $resultsFile 2> $BDNbenchmarkErrorFile"
dotnet run -c $configuration -f $framework --project $BDNbenchmarkPath --filter $filter --exporters $exporter -e BDNRUNPARAM=$framework > $resultsFile 2> $BDNbenchmarkErrorFile

Write-Output "** BDN Benchmark for $filter finished"
Write-Output " "

Write-Output "**** ANALYZE THE RESULTS FILE $resultsFile ****"
# First check if results file is there and if not, error out gracefully
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

Write-Output " "
Write-Output "************************"
Write-Output "**       RESULTS for test: $currentTest  "
Write-Output "**"

# Set the test suite to pass and if any one fails, then mark the suite as fail - just one result failure will mark the whole test as failed
$testSuiteResult = $true

# Go through the results and verify the summary found results value vs the expected results values
Get-Content $resultsFile | ForEach-Object {
    $line = $_

    # Skip lines that don't start with |
    if (-not $line.StartsWith("|")) {
        return
    }

    # Get a value
    for ($currentExpectedProp = 0; $currentExpectedProp -lt $totalExpectedResultValues; $currentExpectedProp++) {

        # Check if the line contains the exact method name by using word boundaries
        if ($line -match [regex]::Escape($expectedResultsArray[$currentExpectedProp, 0]) + "\b") {

            # Found the method in the results, now check the param we looking for is in the line
            if ($line -match [regex]::Escape($expectedResultsArray[$currentExpectedProp, 1])) {

                # Found Method and Param so know this is one we want, so get value
                $foundValue = ParseValueFromResults $line $allocatedColumn

                # Check if the test is a warn only if there or is a fail
                $WARN_ON_FAIL = $expectedResultsArray[$currentExpectedProp, 3] -like "WARN-ON-FAIL*"

                Write-Host "** Config: " $expectedResultsArray[$currentExpectedProp, 0].Substring(2) $expectedResultsArray[$currentExpectedProp, 1]"   Warn only on fail:"$WARN_ON_FAIL
                $currentResults = AnalyzeResult $foundValue $expectedResultsArray[$currentExpectedProp, 2] $acceptableAllocatedRange $WARN_ON_FAIL
                if ($currentResults -eq $false) {
                    $testSuiteResult = $false
                }
            }
        }
    }
}

Write-Output "************************"
Write-Output "**  Final summary:"
Write-Output "**"
if ($testSuiteResult) {
    Write-Output "**   PASS!  No tests failed but that does not rule out warnings for test that were set for WARN-ON-FAIL."
} else {
    Write-Error -Message "**   BDN Benchmark PERFORMANCE REGRESSION FAIL!  At least one test had benchmark Allocated Memory value outside of expected range. NOTE: Expected results are based on CI machine and may differ from the machine that this was ran on."
}
Write-Output "**"
Write-Output "************************"