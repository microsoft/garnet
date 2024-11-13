<#$f
.DESCRIPTION

    This script pulls the version number from Directory.Build.props which is located in the root directory.
    It then assigns the value to PACKAGE_VERSION variable which can be used in pipeline yml files
#>

# scripts/extract_version_prefix.ps1
$propsFile = Resolve-Path -Path "$PSScriptRoot/../../Directory.Build.props"
[xml]$xml = Get-Content -Path $propsFile
$versionPrefix = $xml.Project.PropertyGroup.VersionPrefix
Write-Host "##vso[task.setvariable variable=PACKAGE_VERSION]$versionPrefix"
