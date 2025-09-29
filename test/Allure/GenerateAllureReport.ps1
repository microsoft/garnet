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
$historyDestDir = "$allureResultsCombinedDir"
if (Test-Path $historySourceDir) {
    Copy-Item -Path $historySourceDir -Destination $historyDestDir -Force -Recurse
    Write-Host "Copied history into CombinedResults"
}   
else {
    Write-Host "No history directory found at $historySourceDir, so not copying history."
}

# Generate the report
Write-Host "Generate the Allure report from $allureResultsCombinedDir"
allure generate CombinedResults -o allure-report --clean

# verify report generated
$reportDir = "$basePath/test/Allure/allure-report"
if (-not (Test-Path -Path $reportDir)) {
    Write-Error -Message "The Allure report directory $reportDir did not get created." -Category ObjectNotFound
    exit
}

# Deletes all .json except categories.json - do this after the report is ran just so we don't have a 1000s of result files in artifacts for each run
Write-Host "Deleting Allure result files from $allureResultsCombinedDir"
Get-ChildItem -Path $allureResultsCombinedDir -Filter *.json |
    Where-Object { $_.Name -ne 'categories.json' } |
    Remove-Item

# Copy the history folder from .\test\Allure\allure-report to .\test\Allure
$newHistoryDir = "$basePath/test/Allure/allure-report/history"
$persistedHistoryDir = "$basePath/test/Allure"
if (Test-Path $newHistoryDir) {
    Copy-Item -Path $newHistoryDir -Destination $persistedHistoryDir -Recurse -Force
    Write-Host "Saved updated history for next run from $newHistoryDir to $persistedHistoryDir"
}
else {
    Write-Host "No history directory found at $newHistoryDir, so not copying history."
}

# TO DO:  At some point, need to actually push history back to the current branch ... not sure how do this yet
# From CoPilot:
# Set Git identity
# DEBUG git config --global user.name "github-actions"
# DEBUG git config --global user.email "actions@github.com"

# Stage updated history files
# DEBUG $historyPath = "$basePath/test/Allure/history"
# DEBUG git add $historyPath/*.json

# Commit if there are changes
# DEBUG if (git diff --cached --quiet) {
# DEBUG     Write-Host "No changes to commit."
# DEBUG } else {
# DEBUG     git commit -m "Update Allure history [CI]"
    # Push using GitHub token
# DEBUG     $token = $env:GITHUB_TOKEN
# DEBUG     $repo = $env:GITHUB_REPOSITORY
# DEBUG     $url = "https://x-access-token:$token@github.com/$repo.git"
# DEBUG     git push $url HEAD:${env:GITHUB_REF}
# DEBUG }

Write-Output "************************"
Write-Output "**"
Write-Output "**  Done!"
Write-Output "**"
Write-Output "************************"

