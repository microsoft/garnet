<#$f
.SYNOPSIS
    This script is designed for to run BDN Benchmarking and use the results as a gate in the GitHub CIs to ensure performance isn't declining

.DESCRIPTION

    Script to test for performance regressions in Allocated Memory using BDN Benchmark tool. The various tests are in the configuration file (BDN_Benchmark_Config.json) and are associated with each test that contains name and expected values of the BDN benchmark.
    Any of these (ie BDN.benchmark.Operations.BasicOperations.*) can be sent as the parameter to the file.

    NOTE: The expected values are specific for the CI Machine. If you run these on your machine, you will need to change the expected values.

    NOTE: If adding a new BDN perf test to the BDN_Benchmark_Config.json, then need to add to the "test: [..." line in the ce-bdnbenchmark.yml
    
.EXAMPLE
    ./run_bdnperftest.ps1
    ./run_bdnperftest.ps1 BDN.benchmark.Operations.BasicOperations.*
    ./run_bdnperftest.ps1 Operations.BasicOperations    <-- this is how specify in ci-bdnbenchmark.yml
#>

param (
  [string]$currentTest = "BDN.benchmark.Operations.BasicOperations.*"
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
    [double] $Tolerance = $acceptablePercentRange / 100
    [double] $UpperBound = $expectedResultValue * (1 + $Tolerance)
    [double] $dblfoundResultValue = $foundResultValue

    # Check if the actual value is within the bounds
    if ($dblfoundResultValue -le $UpperBound) {
        Write-Host "**             ** PASS! **  The Allocated Value result ($dblfoundResultValue) is under the acceptable threshold of $UpperBound (Expected value $expectedResultValue + $acceptablePercentRange%)"
        Write-Host "** "
        return $true # the values are close enough
    }
    else {
        if ($warnonly) {
            Write-Host "**   << PERF REGRESSION WARNING! >>  The BDN benchmark Allocated Value result ($dblfoundResultValue) is above the acceptable threshold of $UpperBound (Expected value $expectedResultValue + $acceptablePercentRange%)"
            Write-Host "** "
        }
        else {
            Write-Host "**   << PERF REGRESSION FAIL! >> The BDN benchmark Allocated Value result ($dblfoundResultValue) is above the acceptable threshold of $UpperBound (Expected value $expectedResultValue + $acceptablePercentRange%)"
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
$framework = "net8.0"
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
$basicOperations = $json.$currentTest

# create a matrix of expected results for specific test
$splitTextArray = New-Object 'string[]' 3
$expectedResultsArray = New-Object 'string[,]' 20, 3

[int]$currentRow = 0

# Iterate through each property for the expected value - getting method, params and value
foreach ($property in $basicOperations.PSObject.Properties) {
    # Split the property name into parts
    $splitTextArray = $property.Name -split '_'
   
    # get the expected Method, Parameter and value
    $expectedResultsArray[$currentRow,0] = "| " + $splitTextArray[1]  # Add | at beginning so it is part of summary block and not every other line in the file that matches the Method name
    $expectedResultsArray[$currentRow,1] = $splitTextArray[2]
    $expectedResultsArray[$currentRow,2] = $property.value

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

Write-Output " "
Write-Output "** Start:  dotnet run -c $configuration -f $framework --filter $filter --project $BDNbenchmarkPath --exporters json > $resultsFile 2> $BDNbenchmarkErrorFile"
dotnet run -c $configuration -f $framework --filter $filter --project $BDNbenchmarkPath --exporters json  > $resultsFile 2> $BDNbenchmarkErrorFile

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

    # Get a value
    for ($currentExpectedProp = 0; $currentExpectedProp -lt $totalExpectedResultValues; $currentExpectedProp++) {

        # Check if the line contains the method name
        if ($line -match [regex]::Escape($expectedResultsArray[$currentExpectedProp, 0])) {

            # Found the method in the results, now check the param we looking for is in the line
            if ($line -match [regex]::Escape($expectedResultsArray[$currentExpectedProp, 1])) {

                # Found Method and Param so know this is one we want, so get value
                $foundValue = ParseValueFromResults $line $allocatedColumn

                # Check if found value is not equal to expected value
                Write-Host "** Config: "$expectedResultsArray[$currentExpectedProp, 0].Substring(2) $expectedResultsArray[$currentExpectedProp, 1]
                $currentResults = AnalyzeResult $foundValue $expectedResultsArray[$currentExpectedProp, 2] $acceptableAllocatedRange $true
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
    Write-Output "**   PASS!  All tests in the suite passed."
} else {
    Write-Error -Message "**   BDN Benchmark PERFORMANCE REGRESSION FAIL!  At least one test had benchmark value outside of expected range. NOTE: Expected results are based on CI machine and may differ from the machine that this was ran on."
}
Write-Output "**"
Write-Output "************************"