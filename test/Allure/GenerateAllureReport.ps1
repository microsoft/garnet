#Requires -Version 7

<#$f
.SYNOPSIS
    This script is called after all the Allure data is merged into one location and generates the Allure report.

    It is getting the data from the test/Allure/CombinedResults directory and generating the report into the test/Allure/allure-report directory.

    NOTE: Preserving history between runs is handled in the GitHub Actions workflow by downloading and uploading the history folder as an artifact.
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

# Generate the report
Write-Host "Generate the Allure report from $allureResultsCombinedDir"
allure generate CombinedResults -o allure-report --clean

# verify report generated
$reportDir = "$basePath/test/Allure/allure-report"
if (-not (Test-Path -Path $reportDir)) {
    Write-Error -Message "The Allure report directory $reportDir did not get created." -Category ObjectNotFound
    exit
}
else {
    Write-Host "Allure report generated successfully at $reportDir.  Use 'allure open allure-report' to view it locally."
}

# DEBUG DEBUG - Removes comments
# Deletes all .json except categories.json - do this after the report is ran just so we don't have a 1000s of result files in artifacts for each run
# Write-Host "Deleting Allure result files from $allureResultsCombinedDir"
# Get-ChildItem -Path $allureResultsCombinedDir -Filter *.json |
#    Where-Object { $_.Name -ne 'categories.json' } |
#    Remove-Item

Write-Output "************************"
Write-Output "**"
Write-Output "**  Done!"
Write-Output "**"
Write-Output "************************"

