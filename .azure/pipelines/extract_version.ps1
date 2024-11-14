<#$f
.DESCRIPTION

    This script pulls the version number from Directory.Build.props which is located in the root directory.
    It then assigns the value to the BuildNumber) which can be used in pipeline yml files
#>

$propsFile = Resolve-Path -Path "$PSScriptRoot/../../Directory.Build.props"
[xml]$xml = Get-Content -Path $propsFile
$version = $xml.Project.PropertyGroup.VersionPrefix
Write-Host "##vso[build.updatebuildnumber]$version"