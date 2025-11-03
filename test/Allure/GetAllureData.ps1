#Requires -Version 7

<#$f
.SYNOPSIS
    This script copies the Allure results into the single location of arctifacts so that all can be merged into one big report.
#>

#param (
    #[string]$framework,
    #[string]$configuration,
    #[string]$testsuite
#)

$OFS = "`r`n"

$framework = "net9.0"
$configuration = "Release"
#$testsuite = "Garnet.test"
$testsuite = "Tsavorite.test"

Write-Host "Framework: $framework"
Write-Host "Configuration: $configuration"
Write-Host "Test Suite: $testsuite"

# Get base path since paths can differ from machine to machine
$pathstring = $pwd.Path
if ($pathstring.Contains("test")) {
    $position = $pathString.IndexOf("test")
    $basePath = $pathstring.Substring(0,$position-1)  # take off slash off end as well
} else {
    $basePath = $pathstring  # already in base and not in test
    Set-Location .\test\Allure\
}

# Run Tests
$resultsDir = "$basePath/test/$testsuite-Windows2022-$framework-$configuration"
Write-Host "Results Directory: $resultsDir"


#dotnet test -c Release --no-build --filter "FullyQualifiedName~CanUseHKEYSWithLeftOverBuffer" --logger "trx;LogFileName=results.trx" --results-directory "C:\GarnetGitHub\test\TestResults"
#dotnet test "$basePath/test/Garnet.test/Garnet.test.csproj" -c $configuration --filter "FullyQualifiedName~CanUseHKEYSWithLeftOverBuffer" --logger "trx;LogFileName=results.trx" --results-directory $resultsDir
# DEBUG dotnet test "$basePath/libs/storage/Tsavorite/cs/test/Tsavorite.test.csproj" -c $configuration --filter "FullyQualifiedName~TsavoriteLogTest1" --logger "trx;LogFileName=results.trx" --results-directory $resultsDir

# DEBUG DEBUG - all tests  dotnet test "$basePath/test/Garnet.test/Garnet.test.csproj" -c $configuration --logger "trx;LogFileName=results.trx" --results-directory $resultsDir

# might not be needed as will have combined results dir in artifacts
$allureResultsCombinedDir = "$basePath/test/Allure/CombinedResults"

# Create CombinedResults dir if it doesn't exist
if (-not (Test-Path -Path $allureResultsCombinedDir)) {
    Write-Host "Created CombinedResults directory at $allureResultsCombinedDir"
    New-Item -Path $allureResultsCombinedDir -ItemType Directory
}

# Copy all the results from the test directory to the CombinedResults directory
# GARNET.TEST DIRECTORY  $sourceAllureResultsDir = "$basePath/test/$testsuite/bin/$configuration/$framework/allure-results"
$sourceAllureResultsDir = "$basePath/libs/storage/Tsavorite/cs/test/bin/$configuration/$framework/allure-results"
if (Test-Path -Path $sourceAllureResultsDir) {
    Write-Host "Copying Allure results from $sourceAllureResultsDir to $allureResultsCombinedDir"
    Copy-Item -Path "$sourceAllureResultsDir\*" -Destination $allureResultsCombinedDir -Recurse -Force
} else {
    Write-Error -Message "The source Allure results directory $sourceAllureResultsDir does not exist. Make sure the tests have been run and generated Allure results." -Category ObjectNotFound
    exit
}       

# Verify got results files into CombinedResults directory
$fileCount = (Get-ChildItem -Path $allureResultsCombinedDir -Recurse | Measure-Object).Count
if ($fileCount -le 0) {
    Write-Error -Message "No result files were found in $allureResultsCombinedDir directory." -Category ObjectNotFound
    exit
}

Write-Output "************************"
Write-Output "**"
Write-Output "**  Done!"
Write-Output "**"
Write-Output "************************"