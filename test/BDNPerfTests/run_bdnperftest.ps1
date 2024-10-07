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

$OFS = "`r`n"

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
        Write-Host "**   ** PASS! **  Test Value result ($dblfoundResultValue) is in the acceptable range +/-$acceptablePercentRange% ($LowerBound -> $UpperBound) of expected value: $expectedResultValue " 
        Write-Host "** "
        return $true # the values are close enough
    }
    else {
        if ($warnonly) {
            Write-Host "**   << PERF REGRESSION WARNING! >>  The BDN benchmark Value result ($dblfoundResultValue) is OUT OF RANGE +/-$acceptablePercentRange% ($LowerBound -> $UpperBound) of expected value: $expectedResultValue" 
            Write-Host "** "
        }
        else {
            Write-Host "**   << PERF REGRESSION FAIL! >>  The  BDN benchmark Value ($dblfoundResultValue) is OUT OF ACCEPTABLE RANGE +/-$acceptablePercentRange% ($LowerBound -> $UpperBound) of expected value: $expectedResultValue"
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

    $foundValue = $foundValue.Trim(' us')  
    $foundValue = $foundValue.Trim(' ns')  
    $foundValue = $foundValue.Trim(' B')  

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

# This is special case that allows passing test without specifying CI_BDN_Confi_ at the beginning - need for perf test
if ($configFile -notlike "CI_BDN_Config_*") {
    $configFile = "CI_BDN_Config_" + $configFile  
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

Write-Host "************** Start BDN.benchmark ********************" 
Write-Host " "

# Set all the config options (args to benchmark app) based on values from config json file
$configuration = $object.configuration
$framework = $object.framework
$filter = $object.filter
$meanColumn = "1"
$allocatedRespParseColumn = "5"  # Windows adds an additional column before the allocated column
$allocatedLuaColumn = "5"
if ($IsLinux) {
    $allocatedRespParseColumn = "4"
    $allocatedLuaColumn = "4"
}

# Set the expected values based on the OS
if ($IsLinux) {
    # Linux expected values
    $expectedInLinePingMeanValue = $object.expectedInLinePingMeanValue_linux
    $expectedSetMeanValue = $object.expectedSETMeanValue_linux
    $expectedSetEXMeanValue = $object.expectedSETEXMeanValue_linux
    $expectedGetMeanValue = $object.expectedGETMeanValue_linux

    $expectedZAddRemMeanValue = $object.expectedZAddRemMeanValue_linux
    $expectedLPushPopMeanValue = $object.expectedLPushPopMeanValue_linux
    $expectedSAddRemMeanValue = $object.expectedSAddRemMeanValue_linux
    $expectedHSetDelMeanValue = $object.expectedHSetDelMeanValue_linux
    $expectedMyDictSetGetMeanValue = $object.expectedMyDictSetGetMeanValue_linux
    $expectedZAddRemAllocatedValue = $object.expectedZAddRemAllocatedValue_linux
    $expectedLPushPopAllocatedValue = $object.expectedLPushPopAllocatedValue_linux
    $expectedSAddRemAllocatedValue = $object.expectedSAddRemAllocatedValue_linux
    $expectedHSetDelAllocatedValue = $object.expectedHSetDelAllocatedValue_linux
    $expectedMyDictSetGetAllocatedValue = $object.expectedMyDictSetGetAllocatedValue_linux
    
    $expectedMGetMeanValue = $object.expectedMGETMeanValue_linux
    $expectedMSetMeanValue = $object.expectedMSETMeanValue_linux
    $expectedIncrMeanValue = $object.expectedIncrMeanValue_linux

    $expectedBasicLua1MeanValue = $object.expectedBasicLua1MeanValue_linux
    $expectedBasicLua2MeanValue = $object.expectedBasicLua2MeanValue_linux
    $expectedBasicLua3MeanValue = $object.expectedBasicLua3MeanValue_linux
    $expectedBasicLua4MeanValue = $object.expectedBasicLua4MeanValue_linux
    $expectedBasicLua1AllocatedValue = $object.expectedBasicLua1AllocatedValue_linux
    $expectedBasicLua2AllocatedValue = $object.expectedBasicLua2AllocatedValue_linux
    $expectedBasicLua3AllocatedValue = $object.expectedBasicLua3AllocatedValue_linux
    $expectedBasicLua4AllocatedValue = $object.expectedBasicLua4AllocatedValue_linux

    $expectedBasicLuaRunner1MeanValue = $object.expectedBasicLuaRunner1MeanValue_linux
    $expectedBasicLuaRunner2MeanValue = $object.expectedBasicLuaRunner2MeanValue_linux
    $expectedBasicLuaRunner3MeanValue = $object.expectedBasicLuaRunner3MeanValue_linux
    $expectedBasicLuaRunner4MeanValue = $object.expectedBasicLuaRunner4MeanValue_linux
    $expectedBasicLuaRunner1AllocatedValue = $object.expectedBasicLuaRunner1AllocatedValue_linux
    $expectedBasicLuaRunner2AllocatedValue = $object.expectedBasicLuaRunner2AllocatedValue_linux
    $expectedBasicLuaRunner3AllocatedValue = $object.expectedBasicLuaRunner3AllocatedValue_linux
    $expectedBasicLuaRunner4AllocatedValue = $object.expectedBasicLuaRunner4AllocatedValue_linux

}
else {
    # Windows expected values
    $expectedInLinePingMeanValue = $object.expectedInLinePingMeanValue_win
    $expectedSetMeanValue = $object.expectedSETMeanValue_win
    $expectedSetEXMeanValue = $object.expectedSETEXMeanValue_win
    $expectedGetMeanValue = $object.expectedGETMeanValue_win

    $expectedZAddRemMeanValue = $object.expectedZAddRemMeanValue_win
    $expectedLPushPopMeanValue = $object.expectedLPushPopMeanValue_win
    $expectedSAddRemMeanValue = $object.expectedSAddRemMeanValue_win
    $expectedHSetDelMeanValue = $object.expectedHSetDelMeanValue_win
    $expectedMyDictSetGetMeanValue = $object.expectedMyDictSetGetMeanValue_win
    $expectedZAddRemAllocatedValue = $object.expectedZAddRemAllocatedValue_win
    $expectedLPushPopAllocatedValue = $object.expectedLPushPopAllocatedValue_win
    $expectedSAddRemAllocatedValue = $object.expectedSAddRemAllocatedValue_win
    $expectedHSetDelAllocatedValue = $object.expectedHSetDelAllocatedValue_win
    $expectedMyDictSetGetAllocatedValue = $object.expectedMyDictSetGetAllocatedValue_win

    $expectedMGetMeanValue = $object.expectedMGETMeanValue_win
    $expectedMSetMeanValue = $object.expectedMSETMeanValue_win
    $expectedIncrMeanValue = $object.expectedIncrMeanValue_win

    $expectedBasicLua1MeanValue = $object.expectedBasicLua1MeanValue_win
    $expectedBasicLua2MeanValue = $object.expectedBasicLua2MeanValue_win
    $expectedBasicLua3MeanValue = $object.expectedBasicLua3MeanValue_win
    $expectedBasicLua4MeanValue = $object.expectedBasicLua4MeanValue_win
    $expectedBasicLua1AllocatedValue = $object.expectedBasicLua1AllocatedValue_win
    $expectedBasicLua2AllocatedValue = $object.expectedBasicLua2AllocatedValue_win
    $expectedBasicLua3AllocatedValue = $object.expectedBasicLua3AllocatedValue_win
    $expectedBasicLua4AllocatedValue = $object.expectedBasicLua4AllocatedValue_win

    $expectedBasicLuaRunner1MeanValue = $object.expectedBasicLuaRunner1MeanValue_win
    $expectedBasicLuaRunner2MeanValue = $object.expectedBasicLuaRunner2MeanValue_win
    $expectedBasicLuaRunner3MeanValue = $object.expectedBasicLuaRunner3MeanValue_win
    $expectedBasicLuaRunner4MeanValue = $object.expectedBasicLuaRunner4MeanValue_win
    $expectedBasicLuaRunner1AllocatedValue = $object.expectedBasicLuaRunner1AllocatedValue_win
    $expectedBasicLuaRunner2AllocatedValue = $object.expectedBasicLuaRunner2AllocatedValue_win
    $expectedBasicLuaRunner3AllocatedValue = $object.expectedBasicLuaRunner3AllocatedValue_win
    $expectedBasicLuaRunner4AllocatedValue = $object.expectedBasicLuaRunner4AllocatedValue_win
}

# percent allowed variance when comparing expected vs actual found value - same for linux and windows. 
$acceptableMeanRange = $object.acceptableMeanRange 
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
        "*| InlinePing*" {
            Write-Output "** InlinePing Mean Value test"
            $foundInLinePingMeanValue = ParseValueFromResults $line $meanColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundInLinePingMeanValue $expectedInLinePingMeanValue $acceptableMeanRange $true)
        }
        # This one a bit different as need extra space in the check so doesn't pick up other Set* calls
        "*| Set *" {
            Write-Host "** Set Mean Value test"
            $foundSetMeanValue = ParseValueFromResults $line $meanColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundSetMeanValue $expectedSetMeanValue $acceptableMeanRange $true)
        }
        "*| SetEx*" {
            Write-Host "** SetEx Mean Value test"
            $foundSetExMeanValue = ParseValueFromResults $line $meanColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundSetExMeanValue $expectedSETEXMeanValue $acceptableMeanRange $true)
        }
        "*| Get*" {
            Write-Host "** Get Mean Value test"
            $foundGetMeanValue = ParseValueFromResults $line $meanColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundGetMeanValue $expectedGetMeanValue $acceptableMeanRange $true)
        }
        "*| ZAddRem*" {
            Write-Host "** ZAddRem Mean Value test"
            $foundZAddRemMeanValue = ParseValueFromResults $line $meanColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundZAddRemMeanValue $expectedZAddRemMeanValue $acceptableMeanRange $true)
        }
        "*| LPushPop*" {
            Write-Host "** LPushPop Mean Value test"
            $foundLPushPopMeanValue = ParseValueFromResults $line $meanColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundLPushPopMeanValue $expectedLPushPopMeanValue $acceptableMeanRange $true)
        }
        "*| SAddRem*" {
            Write-Host "** SAddRem Mean Value test"
            $foundSAddRemMeanValue = ParseValueFromResults $line $meanColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundSAddRemMeanValue $expectedSAddRemMeanValue $acceptableMeanRange $true)
        }
        "*| HSetDel*" {
            Write-Host "** HSetDel Mean Value test"
            $foundHSetDelMeanValue = ParseValueFromResults $line $meanColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundHSetDelMeanValue $expectedHSetDelMeanValue $acceptableMeanRange $true)
        }
        "*| MyDictSetGet*" {
            Write-Host "** MyDictSetGet Mean Value test"
            $foundMyDictSetGetMeanValue = ParseValueFromResults $line $meanColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundMyDictSetGetMeanValue $expectedMyDictSetGetMeanValue $acceptableMeanRange $true)
        }
        "*| ZAddRem*" {
            Write-Host "** ZAddRem Allocated Value test"
            $foundZAddRemAllocatedValue = ParseValueFromResults $line $allocatedRespParseColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundZAddRemAllocatedValue $expectedZAddRemAllocatedValue $acceptableAllocatedRange $true)
        }
        "*| LPushPop*" {
            Write-Host "** LPushPop Allocated Value test"
            $foundLPushPopAllocatedValue = ParseValueFromResults $line $allocatedRespParseColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundLPushPopAllocatedValue $expectedLPushPopAllocatedValue $acceptableAllocatedRange $true)
        }
        "*| SAddRem*" {
            Write-Host "** SAddRem Allocated Value test"
            $foundSAddRemAllocatedValue = ParseValueFromResults $line $allocatedRespParseColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundSAddRemAllocatedValue $expectedSAddRemAllocatedValue $acceptableAllocatedRange $true)
        }
        "*| HSetDel*" {
            Write-Host "** HSetDel Allocated Value test"
            $foundHSetDelAllocatedValue = ParseValueFromResults $line $allocatedRespParseColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundHSetDelAllocatedValue $expectedHSetDelAllocatedValue $acceptableAllocatedRange $true)
        }
        "*| MyDictSetGet*" {
            Write-Host "** MyDictSetGet Allocated Value test"
            $foundMyDictSetGetAllocatedValue = ParseValueFromResults $line $allocatedRespParseColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundMyDictSetGetAllocatedValue $expectedMyDictSetGetAllocatedValue $acceptableAllocatedRange $true)
        }
        "*| Incr*" {
            Write-Host "** Incr Mean Value test"
            $foundIncrMeanValue = ParseValueFromResults $line $meanColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundIncrMeanValue $expectedIncrMeanValue $acceptableMeanRange $true)
        }
        "*| MGet*" {
            Write-Host "** MGet Mean Value test"
            $foundMGetMeanValue = ParseValueFromResults $line $meanColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundMGetMeanValue $expectedMGetMeanValue $acceptableMeanRange $true)
        }
        "*| MSet*" {
            Write-Host "** MSet Mean Value test"
            $foundMSetMeanValue = ParseValueFromResults $line $meanColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundMSetMeanValue $expectedMSetMeanValue $acceptableMeanRange $true)
        }
        "*| BasicLua1*" {
            Write-Host "** BasicLua1 Mean Value test"
            $foundBasicLua1MeanValue = ParseValueFromResults $line $meanColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundBasicLua1MeanValue $expectedBasicLua1MeanValue $acceptableMeanRange $true)
        }
        "*| BasicLua2*" {
            Write-Host "** BasicLua2 Mean Value test"
            $foundBasicLua2MeanValue = ParseValueFromResults $line $meanColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundBasicLua2MeanValue $expectedBasicLua2MeanValue $acceptableMeanRange $true)
        }
        <# 
        # Have this disabled for now for the CI runs. These are too volatile to have a CI gated on them.
        "*| BasicLua3*" {
            Write-Host "** BasicLua3 Mean Value test"
            $foundBasicLua3MeanValue = ParseValueFromResults $line $meanColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundBasicLua3MeanValue $expectedBasicLua3MeanValue $acceptableMeanRange $true)
        }
        "*| BasicLua4*" {
            Write-Host "** BasicLua4 Mean Value test"
            $foundBasicLua4MeanValue = ParseValueFromResults $line $meanColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundBasicLua4MeanValue $expectedBasicLua4MeanValue $acceptableMeanRange $true)
        }
        #>                   
        "*| BasicLuaRunner1*" {
            Write-Host "** BasicLuaRunner1 Mean Value test"
            $foundBasicLuaRunner1MeanValue = ParseValueFromResults $line $meanColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundBasicLuaRunner1MeanValue $expectedBasicLuaRunner1MeanValue $acceptableMeanRange $true)
        }
        "*| BasicLuaRunner2*" {
            Write-Host "** BasicLuaRunner2 Mean Value test"
            $foundBasicLuaRunner2MeanValue = ParseValueFromResults $line $meanColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundBasicLuaRunner2MeanValue $expectedBasicLuaRunner2MeanValue $acceptableMeanRange $true)
        }
        <#
        # Have this disabled for now for the CI runs. These are too volatile to have a CI gated on them.
        "*| BasicLuaRunner3*" {
            Write-Host "** BasicLuaRunner3 Mean Value test"
            $foundBasicLuaRunner3MeanValue = ParseValueFromResults $line $meanColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundBasicLuaRunner3MeanValue $expectedBasicLuaRunner3MeanValue $acceptableMeanRange $true)
        }
        "*| BasicLuaRunner4*" {
            Write-Host "** BasicLuaRunner4 Mean Value test"
            $foundBasicLuaRunner4MeanValue = ParseValueFromResults $line $meanColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundBasicLuaRunner4MeanValue $expectedBasicLuaRunner4MeanValue $acceptableMeanRange $true)
        }
        #>            
        "*| BasicLua1*" {
            Write-Host "** BasicLua1 Allocated Value test"
            $foundBasicLua1AllocatedValue = ParseValueFromResults $line $allocatedLuaColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundBasicLua1AllocatedValue $expectedBasicLua1AllocatedValue $acceptableAllocatedRange $true)
        }
        "*| BasicLua2*" {
            Write-Host "** BasicLua2 Allocated Value test"
            $foundBasicLua2AllocatedValue = ParseValueFromResults $line $allocatedLuaColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundBasicLua2AllocatedValue $expectedBasicLua2AllocatedValue $acceptableAllocatedRange $true)
        }
        "*| BasicLua3*" {
            Write-Host "** BasicLua3 Allocated Value test"
            $foundBasicLua3AllocatedValue = ParseValueFromResults $line $allocatedLuaColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundBasicLua3AllocatedValue $expectedBasicLua3AllocatedValue $acceptableAllocatedRange $true)
        }
        "*| BasicLua4*" {
            Write-Host "** BasicLua4 Allocated Value test"
            $foundBasicLua4AllocatedValue = ParseValueFromResults $line $allocatedLuaColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundBasicLua4AllocatedValue $expectedBasicLua4AllocatedValue $acceptableAllocatedRange $true)
        }
        "*| BasicLuaRunner1*" {
            Write-Host "** BasicLuaRunner1 Allocated Value test"
            $foundBasicLuaRunner1AllocatedValue = ParseValueFromResults $line $allocatedLuaColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundBasicLuaRunner1AllocatedValue $expectedBasicLuaRunner1AllocatedValue $acceptableAllocatedRange $true)
        }
        "*| BasicLuaRunner2*" {
            Write-Host "** BasicLuaRunner2 Allocated Value test"
            $foundBasicLuaRunner2AllocatedValue = ParseValueFromResults $line $allocatedLuaColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundBasicLuaRunner2AllocatedValue $expectedBasicLuaRunner2AllocatedValue $acceptableAllocatedRange $true)
        }
        "*| BasicLuaRunner3*" {
            Write-Host "** BasicLuaRunner3 Allocated Value test"
            $foundBasicLuaRunner3AllocatedValue = ParseValueFromResults $line $allocatedLuaColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundBasicLuaRunner3AllocatedValue $expectedBasicLuaRunner3AllocatedValue $acceptableAllocatedRange $true)
        }
        "*| BasicLuaRunner4*" {
            Write-Host "** BasicLuaRunner4 Allocated Value test"
            $foundBasicLuaRunner4AllocatedValue = ParseValueFromResults $line $allocatedLuaColumn
            $testSuiteResult = $testSuiteResult -and (AnalyzeResult $foundBasicLuaRunner4AllocatedValue $expectedBasicLuaRunner4AllocatedValue $acceptableAllocatedRange $true)
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
