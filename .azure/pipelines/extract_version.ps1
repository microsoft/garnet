<#$f
.DESCRIPTION

    This script pulls the version number from "version.json" which is located in the root directory.
    It then assigns the value to PACKAGE_VERSION variable which can be used in pipeline yml files
#>
$rootPath = Resolve-Path -Path "$PSScriptRoot/../../version.json"
$json = Get-Content -Path $rootPath -Raw | ConvertFrom-Json
$version = $json.version
Write-Host "##vso[task.setvariable variable=PACKAGE_VERSION]$version"