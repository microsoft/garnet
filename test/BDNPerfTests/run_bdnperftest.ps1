<#$f
.SYNOPSIS
    This script is designed for to run BDN Benchmarking and use the results as a gate in the GitHub CIs to ensure performance isn't declining 

.DESCRIPTION

    Script to test for performance regressions in Allocated Memory using BDN Benchmark tool.  There are configuration files (in /ConfigFiles dir) associated with each test that contains name and expected values of the BDN benchmark. Any of these can be sent as the parameter to the file.
    
        CI_BDN_Config_Resp.RespParseStress.json

    NOTE: The expected values are specific for the CI Machine. If you run these on your machine, you will need to change the expected values.
    NOTE: The acceptablerange* parameters in the config file is how far +/- X% the found value can be from the expected value and still say it is pass. Defaulted to 10% 
    
.EXAMPLE
    ./run_bdnperftest.ps1 
    ./run_bdnperftest.ps1 CI_BDN_Config_Resp.RespParseStress.json
#>


# Send the config file for the benchmark. Defaults to a simple one
param (
  [string]$configFile = "CI_BDN_Config_Resp.RespParseStress.json"
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
        Write-Host "**             ** PASS! **  The Allocated Value result ($dblfoundResultValue) is under the acceptable threshold of $UpperBound (Expected value $expectedResultValue + $acceptablePercentRange%.)"  
        Write-Host "** "
        return $true # the values are close enough
    }
    else {
        if ($warnonly) {
            Write-Host "**   << PERF REGRESSION WARNING! >>  The BDN benchmark Allocated Value result ($dblfoundResultValue) is above the acceptable threshold of $UpperBound (Expected value $expectedResultValue + $acceptablePercentRange%.)" 
            Write-Host "** "
        }
        else {
            Write-Host "**   << PERF REGRESSION FAIL! >> The BDN benchmark Allocated Value result ($dblfoundResultValue) is above the acceptable threshold of $UpperBound (Expected value $expectedResultValue + $acceptablePercentRange%.)" 
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
# NOTE: Example of ResultsLine from BDN benchmark: "| InlinePing   |   2.343 us | 0.0135 us | 0.0113 us |         - |"
# NOTE: columnNum is zero based
#
######################################################
function ParseValueFromResults {
param ($ResultsLine, $columnNum)

    # Remove the leading and trailing pipes and split the string by '|'
    $columns = $ResultsLine.Trim('|').Split('|') 
    $column = $columns | ForEach-Object { $_.Trim() }
    $foundValue = $column[$columnNum] 

    $foundValue = $foundValue.Trim(' B')  
    $foundValue = $foundValue.Trim(' m')  

    return $foundValue
}


# ******** BEGIN MAIN  *********
# Get base path since paths can differ from machine to machine
$pathstring = $pwd.Path
if ($pathstring.Contains("test")) {
    $position = $pathString.IndexOf("test")
    $basePath = $pathstring.Substring(0,$position-1)  # take off slash off end as well
} else {
    $basePath = $pathstring  # already in base as not in test
    Set-Location .\test\BDNPerfTests\
}

# Add .json to end if not already there
if ($configFile -notlike "*.json") {
    $configFile += ".json"
}

# This is special case that allows passing test without specifying CI_CONFIG_BDN_ at the beginning - need for perf test
if ($configFile -notlike "CI_CONFIG_BDN_*") {
    $configFile = "CI_CONFIG_BDN_" + $configFile  
}

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

Write-Host "************** Start BDN.benchmark Test Run ********************" 
Write-Host " "

# Set all the config options (args to benchmark app) based on values from config json file
$configuration = $object.configuration
$framework = $object.framework
$filter = $object.filter
$allocatedColumn = "-1"  # Number of columns can differ a bit, but know the allocated one is always the last one

# Set the expected values based on the OS
if ($IsLinux) {
    # Linux expected values
    $expectedZAddRemAllocatedValue = $object.expectedZAddRemAllocatedValue_linux
    $expectedLPushPopAllocatedValue = $object.expectedLPushPopAllocatedValue_linux
    $expectedSAddRemAllocatedValue = $object.expectedSAddRemAllocatedValue_linux
    $expectedHSetDelAllocatedValue = $object.expectedHSetDelAllocatedValue_linux
    $expectedMyDictSetGetAllocatedValue = $object.expectedMyDictSetGetAllocatedValue_linux
    
    $expectedLua1AllocatedValue = $object.expectedLua1AllocatedValue_linux
    $expectedLua2AllocatedValue = $object.expectedLua2AllocatedValue_linux
    $expectedLua3AllocatedValue = $object.expectedLua3AllocatedValue_linux
    $expectedLua4AllocatedValue = $object.expectedLua4AllocatedValue_linux
}
else {
    # Windows expected values
    $expectedZAddRemAllocatedValue = $object.expectedZAddRemAllocatedValue_win
    $expectedLPushPopAllocatedValue = $object.expectedLPushPopAllocatedValue_win
    $expectedSAddRemAllocatedValue = $object.expectedSAddRemAllocatedValue_win
    $expectedHSetDelAllocatedValue = $object.expectedHSetDelAllocatedValue_win
    $expectedMyDictSetGetAllocatedValue = $object.expectedMyDictSetGetAllocatedValue_win

    $expectedLua1AllocatedValue = $object.expectedLua1AllocatedValue_win
    $expectedLua2AllocatedValue = $object.expectedLua2AllocatedValue_win
    $expectedLua3AllocatedValue = $object.expectedLua3AllocatedValue_win
    $expectedLua4AllocatedValue = $object.expectedLua4AllocatedValue_win
}

# percent allowed variance when comparing expected vs actual found value - same for linux and windows. 
$acceptableAllocatedRange = $object.acceptableAllocatedRange 

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

Write-Output "** Start BDN Benchmark: $filter"
Write-Output " "
Write-Output "** Start:  dotnet run -c $configuration -f $framework --filter $filter --project $BDNbenchmarkPath --exporters json > $resultsFile 2> $BDNbenchmarkErrorFile"
dotnet run -c $configuration -f $framework --filter $filter --project $BDNbenchmarkPath --exporters json  > $resultsFile 2> $BDNbenchmarkErrorFile

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

Write-Output "************************"
Write-Output "**   RESULTS  "
Write-Output "**   "

# Set the test suite to pass and if any one fails, then mark the suite as fail - just one result failure will mark the whole test as failed
$testSuiteResult = $true

# Read the results file line by line and pull out the specific results if exists
Get-Content $resultsFile | ForEach-Object {
    $line = $_
    switch -Wildcard ($line) {
        "*| ZAddRem*" {
            Write-Host "** ZAddRem Allocated Value test"
            $foundZAddRemAllocatedValue = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundZAddRemAllocatedValue $expectedZAddRemAllocatedValue $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| LPushPop*" {
            Write-Host "** LPushPop Allocated Value test"
            $foundLPushPopAllocatedValue = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundLPushPopAllocatedValue $expectedLPushPopAllocatedValue $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| SAddRem*" {
            Write-Host "** SAddRem Allocated Value test"
            $foundSAddRemAllocatedValue = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundSAddRemAllocatedValue $expectedSAddRemAllocatedValue $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| HSetDel*" {
            Write-Host "** HSetDel Allocated Value test"
            $foundHSetDelAllocatedValue = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundHSetDelAllocatedValue $expectedHSetDelAllocatedValue $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| MyDictSetGet*" {
            Write-Host "** MyDictSetGet Allocated Value test"
            $foundMyDictSetGetAllocatedValue = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundMyDictSetGetAllocatedValue $expectedMyDictSetGetAllocatedValue $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| Lua1*" {
            Write-Host "** Lua1 Allocated Value test"
            $foundLua1AllocatedValue = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundLua1AllocatedValue $expectedLua1AllocatedValue $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| Lua2*" {
            Write-Host "** Lua2 Allocated Value test"
            $foundLua2AllocatedValue = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundLua2AllocatedValue $expectedLua2AllocatedValue $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| Lua3*" {
            Write-Host "** Lua3 Allocated Value test"
            $foundLua3AllocatedValue = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundLua3AllocatedValue $expectedLua3AllocatedValue $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| Lua4*" {
            Write-Host "** Lua4 Allocated Value test"
            $foundLua4AllocatedValue = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundLua4AllocatedValue $expectedLua4AllocatedValue $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
    }
}

Write-Output "**  "
Write-Output "************************"
Write-Output "**  Final summary:"
Write-Output "**  "
if ($testSuiteResult) {
    Write-Output "**   PASS!  All tests passed  "
} else {
    Write-Error -Message "**   BDN Benchmark PERFORMANCE REGRESSION FAIL!  At least one test had benchmark value outside of expected range. NOTE: Expected results are based on CI machine and may differ from the machine that this was ran on."
}
Write-Output "**  "
Write-Output "************************"
