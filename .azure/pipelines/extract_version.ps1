# scripts/extract_version.ps1
$json = Get-Content -Path "version.json" | ConvertFrom-Json
$version = $json.version
Write-Host "##vso[task.setvariable variable=PACKAGE_VERSION]$version"