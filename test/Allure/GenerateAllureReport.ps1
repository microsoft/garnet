#Requires -Version 7

<#$f
.SYNOPSIS
    This script is called after all the Allure data is merged into one location and generates the Allure report.

    It is getting the data from the test/Allure/CombinedResults directory and generating the report into the test/Allure/allure-report directory.
#>

$OFS = "`r`n"

# Get base path since paths can differ from machine to machine
$pathstring = $pwd.Path
if ($pathstring.Contains("test")) {
    $position = $pathString.IndexOf("test")
    $basePath = $pathstring.Substring(0,$position-1)  # take off slash off end as well
} else {
    $basePath = $pathstring  # already in base and not in test
    Set-Location .\test\Allure\
}

# Location of all the allure results
$allureResultsCombinedDir = "$basePath/test/Allure/CombinedResults"

# Double check combined results dir exists
if (-not (Test-Path -Path $allureResultsCombinedDir)) {
    Write-Error -Message "The Combined results directory $allureResultsCombinedDir does not exist. " -Category ObjectNotFound
    exit
}

# Copy categories.json to the CombinedResults directory
Write-Host "Copying categories.json to $allureResultsCombinedDir"
Copy-Item -Path "$basePath/test/Allure/categories.json" -Destination "$allureResultsCombinedDir/categories.json"

# Copy the history folder to CombinedResults - this is where history of tests all store
Write-Host "Copying history to $allureResultsCombinedDir"
$historySourceDir = "$basePath/test/Allure/history"
$historyDestDir = "$allureResultsCombinedDir/history"
if (Test-Path -Path $historySourceDir) {
    Copy-Item -Path $historySourceDir -Destination $historyDestDir -Force
}   

# Generate the report
Write-Host "Generate the Allure report from $allureResultsCombinedDir"
allure generate CombinedResults -o allure-report --clean

# verify report generated
$reportDir = "$basePath/test/Allure/allure-report"
if (-not (Test-Path -Path $reportDir)) {
    Write-Error -Message "The Allure report directory $reportDir did not get created." -Category ObjectNotFound
# DEBUG    exit
}

# Open the report - only when ran locally ... in GitHub action, don't want to run this
# DEBUG Write-Host "Open the Allure report"
# DEBUG Start-Process -FilePath "allure" -ArgumentList "open allure-report" -WindowStyle Normal

# Deletes all .json except categories.json - do this after the report is ran just so we don't have a 1000s of result files in artifacts for each run
Write-Host "Deleting Allure result files from $allureResultsCombinedDir"
Get-ChildItem -Path $allureResultsCombinedDir -Filter *.json |
    Where-Object { $_.Name -ne 'categories.json' } |
    Remove-Item

# Copy the history folder from .\test\Allure\allure-report to .\test\Allure
# Need to go one file at a time so it overwrites properly. Could delete history and copy, but this is safer to only overwrite files that exist
Write-Host "Copy the history files to $basePath/test/Allure for next run"
$historySourceDir = "$basePath/test/Allure/allure-report/history"
$historyDestDir = "$basePath/test/Allure/history"

if (Test-Path -Path $historySourceDir) {
    Get-ChildItem -Path $historySourceDir -Recurse | ForEach-Object {
        $dest = Join-Path $historyDestDir $_.FullName.Substring($historySourceDir.Length)
        Copy-Item -Path $_.FullName -Destination $dest -Force
    }
}

Write-Output "************************"
Write-Output "**"
Write-Output "**  Done!"
Write-Output "**"
Write-Output "************************"

