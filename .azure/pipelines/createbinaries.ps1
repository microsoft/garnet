<#$f
.SYNOPSIS
    This script is designed to publish GarnetServer into various platforms. 

.DESCRIPTION

    Script to publish the GarnetServer executable from various Profiles. It will clean up all the files not needed and by default it will zip them all up in the output directory. 
	
    Parameter: mode
		0 (default) = do both publish and zip
		1 = publish only
		2 = zip only

		The mode allows for special case when running from a pipeline that it only does one of the two parts (publish  or zip) at a time. 
			Doing this allows actions (code signing etc) on the files before it is zipped. Running the script after that can zip up everything (using mode 2).
   
.EXAMPLE
    ./createbinaries.ps1 
 	./createbinaries.ps1 0
	./createbinaries.ps1 -mode 1
#>

# Send the config file for the benchmark. Defaults to a simple one
param (
  [string]$mode = 0
)



# ******** FUNCTION DEFINITIONS  *********

################## CleanUpFiles ##################### 
#  
#  Publishes the files and clean it up so only the necessary files will be ready to be zipped
#  
######################################################
function CleanUpFiles {
    param ($publishFolder, $platform)

	$publishPath = "$basePath/main/GarnetServer/bin/Release/net8.0/publish/$publishFolder"
	$garnetServerEXE = "$publishPath/GarnetServer.exe"
	$excludeGarnetServerPDB = 'GarnetServer.pdb'

	# Native binary is different based on OS by default
	$nativeFile = "libnative_device.so"
	$garnetServerEXE = "$publishPath/GarnetServer"

	if ($platform -match "win-x64") {
		$nativeFile = "native_device.dll"
		$garnetServerEXE = "$publishPath/GarnetServer.exe"
	}

	$nativeRuntimePathFile = "$publishPath/runtimes/$platform/native/$nativeFile"
	
	if (Test-Path $garnetServerEXE) {
		Get-ChildItem -Path $publishPath -Filter '*.xml' | Remove-Item -Force
		Get-ChildItem -Path $publishPath -Filter '*.pfx' | Remove-Item -Force
		Get-ChildItem -Path $publishPath -Filter *.pdb | Where-Object { $_.Name -ne $excludeGarnetServerPDB } | Remove-Item

		# Copy proper native run time to publish directory
		Copy-Item -Path $nativeRuntimePathFile -Destination $publishPath

		# Confirm the files are there
		if (Test-Path "$publishPath/$nativeFile") {
	
			# Delete RunTimes folder
			Remove-Item -Path "$publishPath/runtimes" -Recurse -Force

		} else {
			Write-Error "$publishPath/$nativeFile does not exist."
		}
	} else {
		Write-Error "$garnetServerEXE was not found."
	}
}

$lastPwd = $pwd

# Get base path since paths can differ from machine to machine
$string = $pwd.Path
$position = $string.IndexOf(".azure")
$basePath = $string.Substring(0,$position-1)  # take off slash off end as well
Set-Location $basePath/main/GarnetServer

if ($mode -eq 0 -or $mode -eq 1) {
	Write-Host "** Publish ... **"
	dotnet publish GarnetServer.csproj -p:PublishProfile=linux-arm64-based -f:net8.0
	dotnet publish GarnetServer.csproj -p:PublishProfile=linux-x64-based -f:net8.0
	dotnet publish GarnetServer.csproj -p:PublishProfile=osx-arm64-based -f:net8.0
	dotnet publish GarnetServer.csproj -p:PublishProfile=osx-x64-based -f:net8.0
	dotnet publish GarnetServer.csproj -p:PublishProfile=portable -f:net8.0
	dotnet publish GarnetServer.csproj -p:PublishProfile=win-arm64-based-readytorun -f:net8.0
	dotnet publish GarnetServer.csproj -p:PublishProfile=win-x64-based-readytorun -f:net8.0

	# Clean up all the extra files
	CleanUpFiles "linux-arm64" "linux-x64"
	CleanUpFiles "linux-x64" "linux-x64"
	CleanUpFiles "osx-arm64" "linux-x64"
	CleanUpFiles "osx-x64" "linux-x64"
	#CleanUpFiles "portable" "win-x64"  # don't clean up all files for portable ... leave as is
	CleanUpFiles "win-x64" "win-x64"
	CleanUpFiles "win-arm64" "win-x64"
}

if ($mode -eq 0 -or $mode -eq 2) {

	# Make sure the publish folder exists as basic check files are actually published before trying to zip
	$publishedFilesFolder = "$basePath/main/GarnetServer/bin/Release/net8.0/publish"
	if (!(Test-Path $publishedFilesFolder)) {
		Write-Error "$publishedFilesFolder does not exist. Run .\CreateBinaries 1 to publish the binaries first."
		exit
	}
  
	# Create the directories
	if (!(Test-Path $basePath/main/GarnetServer/bin/Release/net8.0/publish/output)) {
		mkdir $basePath/main/GarnetServer/bin/Release/net8.0/publish/output
	}
	Set-Location $basePath/main/GarnetServer/bin/Release/net8.0/publish/output

	# Compress the files
	Write-Host "** Compressing the files ... **"
	7z a -mmt20 -mx5 -scsWIN win-x64-based-readytorun.zip ../win-x64/*
	7z a -mmt20 -mx5 -scsWIN win-arm64-based-readytorun.zip ../win-arm64/*
	7z a -scsUTF-8 linux-x64-based.tar ../linux-x64/*
	7z a -scsUTF-8 linux-arm64-based.tar ../linux-arm64/*
	7z a -scsUTF-8 osx-x64-based.tar ../osx-x64/*
	7z a -scsUTF-8 osx-arm64-based.tar ../osx-arm64/*
	7z a -mmt20 -mx5 -sdel linux-x64-based.tar.xz linux-x64-based.tar
	7z a -mmt20 -mx5 -sdel linux-arm64-based.tar.xz linux-arm64-based.tar
	7z a -mmt20 -mx5 -sdel osx-x64-based.tar.xz osx-x64-based.tar
	7z a -mmt20 -mx5 -sdel osx-arm64-based.tar.xz osx-arm64-based.tar
	7z a -mmt20 -mx5 -scsUTF-8 portable.7z ../portable/*
}

Write-Host "** DONE! **"
Set-Location $lastPwd
