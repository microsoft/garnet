<#$f
.DESCRIPTION

    This script pulls the version number from Directory.Build.props which is located in the root directory.
    It then assigns the value to "version" variable (which is used as BuildName as well) which can be used in pipeline yml files
#>

$propsFile = Resolve-Path -Path "$PSScriptRoot/../../Directory.Build.props"
[xml]$xml = Get-Content -Path $propsFile
$version = $xml.Project.PropertyGroup.VersionPrefix
Write-Host "##vso[task.setvariable variable=version]$version"
Write-Host "##vso[build.updatebuildnumber]$version"