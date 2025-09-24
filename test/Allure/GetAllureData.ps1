#Requires -Version 7

<#$f
.SYNOPSIS
    This script copies the Allure results into the single location of arctifacts so that all can be merged into one big report.
#>

param (
    [string]$framework,
    [string]$configuration,
    [string]$testsuite
)

$OFS = "`r`n"

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

# C:\AllureTestProject\AllureTestProject\AllureTests\CombinedResults -- might not be needed as will have combined results dir in artifacts
$allureResultsCombinedDir = "$basePath/test/Allure/CombinedResults"

# Create CombinedResults dir if it doesn't exist
if (-not (Test-Path -Path $allureResultsCombinedDir)) {
    Write-Host "Created CombinedResults directory at $allureResultsCombinedDir"
    New-Item -Path $allureResultsCombinedDir -ItemType Directory
}

# Copy all the results from the test directory to the CombinedResults directory
# $sourceAllureResultsDir = "$basePath/test/$testsuite/bin/$configuration/$framework/allure-results"   # <-- will use this one when we have real data
$sourceAllureResultsDir = "$basePath/test/Allure/TestData/$framework/allure-results"   # <--- this is just using the test data that is checked in because real data not available yet
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