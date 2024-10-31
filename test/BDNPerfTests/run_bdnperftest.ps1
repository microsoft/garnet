<#$f
.SYNOPSIS
    This script is designed for to run BDN Benchmarking and use the results as a gate in the GitHub CIs to ensure performance isn't declining 

.DESCRIPTION

    Script to test for performance regressions in Allocated Memory using BDN Benchmark tool.  There are configuration files (in /ConfigFiles dir) associated with each test that contains name and expected values of the BDN benchmark. 
    Any of these can be sent as the parameter to the file.
    
        CI_CONFIG_BDN_Benchmark_Operations.BasicOperations.json

    NOTE: The expected values are specific for the CI Machine. If you run these on your machine, you will need to change the expected values.
    NOTE: The acceptablerange* parameters in the config file is how far +/- X% the found value can be from the expected value and still say it is pass. Defaulted to 10% 
    
.EXAMPLE
    ./run_bdnperftest.ps1 
    ./run_bdnperftest.ps1 CI_CONFIG_BDN_Benchmark_Operations.BasicOperations.json
#>


# Send the config file for the benchmark. Defaults to a simple one
param (
  [string]$configFile = "CI_CONFIG_BDN_Benchmark_Operations.BasicOperations.json"
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
    if ($foundValue -eq "-") {
        $foundValue = "0"
    }
   
    $foundValue = $foundValue.Trim(' B')  
    $foundValue = $foundValue.Trim(' K')  
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
if ($configFile -notlike "CI_CONFIG_BDN_Benchmark_*") {
    $configFile = "CI_CONFIG_BDN_Benchmark_" + $configFile  
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

# Set all the config options (args to benchmark app)
$configuration = "Release"
$framework = "net8.0"
$allocatedColumn = "-1"   # last one is allocated, just to ensure in case other column gets added
$paramNone = "None"
$paramDSV = "DSV"
$paramACL = "ACL"
$paramAOF = "AOF"
$acceptableAllocatedRange = "10"   # percent allowed variance when comparing expected vs actual found value - same for linux and windows. 
$filter = $object.filter

# Expected values for Allocated amount 
switch -Wildcard ($configFile) {
    "CI_CONFIG_BDN_Benchmark_Cluster.ClusterMigrate.json" {
        $expectedGet_None = $object.expectedGET_None
        $expectedSet_None = $object.expectedSET_None
        $expectedMGet_None = $object.expectedMGET_None
        $expectedMSet_None = $object.expectedMSET_None
    }
    "CI_CONFIG_BDN_Benchmark_Cluster.ClusterOperations.json" {
        $expectedGet_DSV = $object.expectedGET_DSV
        $expectedSet_DSV = $object.expectedSET_DSV
        $expectedMGet_DSV = $object.expectedMGET_DSV
        $expectedMSet_DSV = $object.expectedMSET_DSV
        $expectedCPBSET_DSV = $object.expectedCPBSET_DSV

        $expectedGet_None = $object.expectedGET_None
        $expectedSet_None = $object.expectedSET_None
        $expectedMGet_None = $object.expectedMGET_None
        $expectedMSet_None = $object.expectedMSET_None
        $expectedCPBSET_None = $object.expectedCPBSET_None
    }
    "CI_CONFIG_BDN_Benchmark_Lua.LuaScripts.json" {
        $expectedScript1_None = $object.expectedScript1_None
        $expectedScript2_None = $object.expectedScript2_None
        $expectedScript3_None = $object.expectedScript3_None
        $expectedScript4_None = $object.expectedScript4_None
    }
    "CI_CONFIG_BDN_Benchmark_Operations.BasicOperations.json" {
        $expectedInlinePing_ACL = $object.expectedInlinePing_ACL
        $expectedInlinePing_AOF = $object.expectedInlinePing_AOF
        $expectedInlinePing_None = $object.expectedInlinePing_None
    }
    "CI_CONFIG_BDN_Benchmark_Operations.ObjectOperations.json" {
        $expectedZAddRem_None = $object.expectedZAddRem_None
        $expectedLPushPop_None = $object.expectedLPushPop_None
        $expectedSAddRem_None = $object.expectedSAddRem_None
        $expectedHSetDel_None = $object.expectedHSetDel_None
        $expectedMyDictSetGet_None = $object.expectedMyDictSetGet_None
        $expectedCustomProcSet_None = $object.expectedCustomProcSet_None

        $expectedZAddRem_ACL = $object.expectedZAddRem_ACL
        $expectedLPushPop_ACL = $object.expectedLPushPop_ACL
        $expectedSAddRem_ACL = $object.expectedSAddRem_ACL
        $expectedHSetDel_ACL = $object.expectedHSetDel_ACL
        $expectedMyDictSetGet_ACL = $object.expectedMyDictSetGet_ACL
        $expectedCustomProcSet_ACL = $object.expectedCustomProcSet_ACL

        $expectedZAddRem_AOF = $object.expectedZAddRem_AOF
        $expectedLPushPop_AOF = $object.expectedLPushPop_AOF
        $expectedSAddRem_AOF = $object.expectedSAddRem_AOF
        $expectedHSetDel_AOF = $object.expectedHSetDel_AOF
        $expectedMyDictSetGet_AOF = $object.expectedMyDictSetGet_AOF
        $expectedCustomProcSet_AOF = $object.expectedCustomProcSet_AOF
    }
}

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
        "*| Set*$paramNone*" {
            Write-Host "** Set ($paramNone) Allocated Value test"
            $foundSet_None = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundSet_None $expectedSet_None $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| Get*$paramNone*" {
            Write-Host "** Get ($paramNone) Allocated Value test"
            $foundGet_None = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundGet_None $expectedGet_None $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| MSet*$paramNone*" {
            Write-Host "** MSet ($paramNone) Allocated Value test"
            $foundMSet_None = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundMSet_None $expectedMSet_None $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| MGet*$paramNone*" {
            Write-Host "** MGet ($paramNone) Allocated Value test"
            $foundMGet_None = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundMGet_None $expectedMGet_None $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| CPBSET*$paramNone*" {
            Write-Host "** CPBSET ($paramNone) Allocated Value test"
            $foundCPBSET_None = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundCPBSET_None $expectedCPBSET_None $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }

        "*| Set*$paramDSV*" {
            Write-Host "** Set ($paramDSV) Allocated Value test"
            $foundSet_DSV = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundSet_DSV $expectedSet_DSV $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| Get*$paramDSV*" {
            Write-Host "** Get ($paramDSV) Allocated Value test"
            $foundGet_DSV = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundGet_DSV $expectedGet_DSV $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| MSet*$paramDSV*" {
            Write-Host "** MSet ($paramDSV) Allocated Value test"
            $foundMSet_DSV = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundMSet_DSV $expectedMSet_DSV $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| MGet*$paramDSV*" {
            Write-Host "** MGet ($paramDSV) Allocated Value test"
            $foundMGet_DSV = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundMGet_DSV $expectedMGet_DSV $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| CPBSET*$paramDSV*" {
            Write-Host "** CPBSET ($paramDSV) Allocated Value test"
            $foundCPBSET_DSV = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundCPBSET_DSV $expectedCPBSET_DSV $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }

        "*| ZAddRem*$paramNone*" {
            Write-Host "** ZAddRem ($paramNone) Allocated Value test"
            $foundZAddRem_None = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundZAddRem_None $expectedZAddRem_None $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| LPushPop*$paramNone*" {
            Write-Host "** LPushPop ($paramNone) Allocated Value test"
            $foundLPushPop_None = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundLPushPop_None $expectedLPushPop_None $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| SAddRem*$paramNone*" {
            Write-Host "** SAddRem ($paramNone) Allocated Value test"
            $foundSAddRem_None = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundSAddRem_None $expectedSAddRem_None $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| HSetDel*$paramNone*" {
            Write-Host "** HSetDel ($paramNone) Allocated Value test"
            $foundHSetDel_None = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundHSetDel_None $expectedHSetDel_None $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| MyDictSetGet*$paramNone*" {
            Write-Host "** MyDictSetGet ($paramNone) Allocated Value test"
            $foundMyDictSetGet_None = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundMyDictSetGet_None $expectedMyDictSetGet_None $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| CustomProcSet*$paramNone*" {
            Write-Host "** MyDictSetGet ($paramNone) Allocated Value test"
            $foundCustomProcSet_None = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundCustomProcSet_None $expectedCustomProcSet_None $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| ZAddRem*$paramACL*" {
            Write-Host "** ZAddRem ($paramACL) Allocated Value test"
            $foundZAddRem_ACL = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundZAddRem_ACL $expectedZAddRem_ACL $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| LPushPop*$paramACL*" {
            Write-Host "** LPushPop ($paramACL) Allocated Value test"
            $foundLPushPop_ACL = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundLPushPop_ACL $expectedLPushPop_ACL $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| SAddRem*$paramACL*" {
            Write-Host "** SAddRem ($paramACL) Allocated Value test"
            $foundSAddRem_ACL = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundSAddRem_ACL $expectedSAddRem_ACL $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| HSetDel*$paramACL*" {
            Write-Host "** HSetDel ($paramACL) Allocated Value test"
            $foundHSetDel_ACL = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundHSetDel_ACL $expectedHSetDel_ACL $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| MyDictSetGet*$paramACL*" {
            Write-Host "** MyDictSetGet ($paramACL) Allocated Value test"
            $foundMyDictSetGet_ACL = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundMyDictSetGet_ACL $expectedMyDictSetGet_ACL $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| CustomProcSet*$paramACL*" {
            Write-Host "** MyDictSetGet ($paramACL) Allocated Value test"
            $foundCustomProcSet_ACL = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundCustomProcSet_ACL $expectedCustomProcSet_ACL $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }

        "*| ZAddRem*$paramAOF*" {
            Write-Host "** ZAddRem ($paramAOF) Allocated Value test"
            $foundZAddRem_AOF = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundZAddRem_AOF $expectedZAddRem_AOF $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| LPushPop*$paramAOF*" {
            Write-Host "** LPushPop ($paramAOF) Allocated Value test"
            $foundLPushPop_AOF = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundLPushPop_AOF $expectedLPushPop_AOF $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| SAddRem*$paramAOF*" {
            Write-Host "** SAddRem ($paramAOF) Allocated Value test"
            $foundSAddRem_AOF = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundSAddRem_AOF $expectedSAddRem_AOF $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| HSetDel*$paramAOF*" {
            Write-Host "** HSetDel ($paramAOF) Allocated Value test"
            $foundHSetDel_AOF = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundHSetDel_AOF $expectedHSetDel_AOF $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| MyDictSetGet*$paramAOF*" {
            Write-Host "** MyDictSetGet ($paramAOF) Allocated Value test"
            $foundMyDictSetGet_AOF = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundMyDictSetGet_AOF $expectedMyDictSetGet_AOF $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| CustomProcSet*$paramAOF*" {
            Write-Host "** CustomProcSet ($paramAOF) Allocated Value test"
            $foundMyCustomProcSet_AOF = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundMyCustomProcSet_AOF $expectedCustomProcSet_AOF $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }

        "*| Script1*$paramNone*" {
            Write-Host "** Lua Script1 ($paramNone) Allocated Value test"
            $foundScript1_None = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundScript1_None $expectedScript1_None $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| Script2*$paramNone*" {
            Write-Host "** Lua Script2 ($paramNone) Allocated Value test"
            $foundScript2_None = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundScript2_None $expectedScript2_None $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| Script3*$paramNone*" {
            Write-Host "** Lua Script3 ($paramNone) Allocated Value test"
            $foundScript3_None = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundScript3_None $expectedScript3_None $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| Script4*$paramNone*" {
            Write-Host "** LuaScript4 ($paramNone) Allocated Value test"
            $foundScript4_None = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundScript4_None $expectedScript4_None $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }


        "*| InlinePing*$paramNone*" {
            Write-Host "** InlinePing ($paramNone) Allocated Value test"
            $foundInlinePing_None = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundInlinePing_None $expectedInlinePing_None $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }

        "*| InlinePing*$paramACL*" {
            Write-Host "** InlinePing ($paramACL) Allocated Value test"
            $foundInlinePing_ACL = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundInlinePing_ACL $expectedInlinePing_ACL $acceptableAllocatedRange $true
            if ($currentResults -eq $false) {
                $testSuiteResult = $false
            }
        }
        "*| InlinePing*$paramAOF*" {
            Write-Host "** InlinePing ($paramAOF) Allocated Value test"
            $foundInlinePing_AOF = ParseValueFromResults $line $allocatedColumn
            $currentResults = AnalyzeResult $foundInlinePing_AOF $expectedInlinePing_AOF $acceptableAllocatedRange $true
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
