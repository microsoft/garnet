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
#  After build is published, this cleans it up so only the necessary files will be ready to be zipped
#  
######################################################
function CleanUpFiles {
    param ($publishFolder, $platform, $framework, $deleteRunTimes = $true)

	$publishPath = "$basePath/main/GarnetServer/bin/Release/$framework/publish/$publishFolder"
	$excludeGarnetServerPDB = 'GarnetServer.pdb'

	# Native binary is different based on OS by default
	$nativeFile = "libnative_device.so"

	if ($platform -match "win-x64") {
		$nativeFile = "native_device.dll"
	}

	$nativeRuntimePathFile = "$publishPath/runtimes/$platform/native/$nativeFile"

	if (Test-Path -Path $publishPath) {
		Get-ChildItem -Path $publishPath -Filter '*.pfx' | Remove-Item -Force
		Get-ChildItem -Path $publishPath -Filter '*.xml' | Remove-Item -Force
		Get-ChildItem -Path $publishPath -Filter '*.pdb' | Where-Object { $_.Name -ne $excludeGarnetServerPDB } | Remove-Item

		# Copy proper native run time to publish directory
		Copy-Item -Path $nativeRuntimePathFile -Destination $publishPath
	} else {
		Write-Host "Publish Path not found: $publishPath"
	}

	# Delete the runtimes folder
	if ($deleteRunTimes -eq $true) {
		Remove-Item -Path "$publishPath/runtimes" -Recurse -Force
	}
}

################## CopyGarnetJSON ####################
#
#  Copies GarnetJSON.dll to the extensions folder in each publish directory
#
######################################################
function CopyGarnetJSON {
    param ($publishFolder, $framework)

    $publishPath = "$basePath/main/GarnetServer/bin/Release/$framework/publish/$publishFolder"
    $extensionsPath = "$publishPath/extensions"
    $jsonModulePath = "$basePath/modules/GarnetJSON/bin/Release/$framework/GarnetJSON.dll"

    if (Test-Path -Path $publishPath) {
        # Create extensions folder if it doesn't exist
        if (!(Test-Path $extensionsPath)) {
            New-Item -ItemType Directory -Path $extensionsPath -Force | Out-Null
        }

        # Copy GarnetJSON.dll
        if (Test-Path $jsonModulePath) {
            Copy-Item -Path $jsonModulePath -Destination $extensionsPath -Force
        } else {
            Write-Warning "GarnetJSON.dll not found at: $jsonModulePath"
        }
    } else {
        Write-Host "Publish Path not found: $publishPath"
    }
}

$lastPwd = $pwd

# Get base path since paths can differ from machine to machine
$string = $pwd.Path
$position = $string.IndexOf(".azure")
$basePath = $string.Substring(0,$position-1)  # take off slash off end as well
Set-Location $basePath/main/GarnetServer

if ($mode -eq 0 -or $mode -eq 1) {
	# Build GarnetJSON module for both frameworks
	Write-Host "** Building GarnetJSON module ...  **"
	Set-Location $basePath/modules/GarnetJSON
	dotnet build GarnetJSON.csproj -c Release -f net8.0
	dotnet build GarnetJSON.csproj -c Release -f net9.0
	Set-Location $basePath/main/GarnetServer

	Write-Host "** Publish ... **"
	dotnet publish GarnetServer.csproj -p:PublishProfile=linux-arm64-based -f:net8.0
	dotnet publish GarnetServer.csproj -p:PublishProfile=linux-x64-based -f:net8.0 
	dotnet publish GarnetServer.csproj -p:PublishProfile=osx-arm64-based -f:net8.0 
	dotnet publish GarnetServer.csproj -p:PublishProfile=osx-x64-based -f:net8.0 
	dotnet publish GarnetServer.csproj -p:PublishProfile=portable -f:net8.0 
	dotnet publish GarnetServer.csproj -p:PublishProfile=win-arm64-based-readytorun -f:net8.0 
	dotnet publish GarnetServer.csproj -p:PublishProfile=win-x64-based-readytorun -f:net8.0 

	dotnet publish GarnetServer.csproj -p:PublishProfile=linux-arm64-based -f:net9.0
	dotnet publish GarnetServer.csproj -p:PublishProfile=linux-x64-based -f:net9.0 
	dotnet publish GarnetServer.csproj -p:PublishProfile=osx-arm64-based -f:net9.0 
	dotnet publish GarnetServer.csproj -p:PublishProfile=osx-x64-based -f:net9.0 
	dotnet publish GarnetServer.csproj -p:PublishProfile=portable -f:net9.0 
	dotnet publish GarnetServer.csproj -p:PublishProfile=win-arm64-based-readytorun -f:net9.0 
	dotnet publish GarnetServer.csproj -p:PublishProfile=win-x64-based-readytorun -f:net9.0 

	# Clean up all the extra files
	CleanUpFiles "linux-arm64" "linux-x64" "net8.0"
	CleanUpFiles "linux-x64" "linux-x64" "net8.0"
	CleanUpFiles "osx-arm64" "linux-x64" "net8.0"
	CleanUpFiles "osx-x64" "linux-x64" "net8.0"
	#CleanUpFiles "portable" "win-x64" "net8.0" # don't clean up all files for portable ... leave as is
	CleanUpFiles "win-x64\Service" "win-x64" "net8.0" $false
	CleanUpFiles "win-x64" "win-x64" "net8.0"
	CleanUpFiles "win-arm64" "win-x64" "net8.0"

	CleanUpFiles "linux-arm64" "linux-x64" "net9.0"
	CleanUpFiles "linux-x64" "linux-x64" "net9.0"
	CleanUpFiles "osx-arm64" "linux-x64" "net9.0"
	CleanUpFiles "osx-x64" "linux-x64" "net9.0"
	#CleanUpFiles "portable" "win-x64" "net9.0" # don't clean up all files for portable ... leave as is
	CleanUpFiles "win-x64\Service" "win-x64" "net9.0" $false
	CleanUpFiles "win-x64" "win-x64" "net9.0"
	CleanUpFiles "win-arm64" "win-x64" "net9.0"

	# Copy GarnetJSON.dll to all platforms
	Write-Host "** Copying GarnetJSON.dll to extensions folders... **"
	CopyGarnetJSON "linux-arm64" "net8.0"
	CopyGarnetJSON "linux-x64" "net8.0"
	CopyGarnetJSON "osx-arm64" "net8.0"
	CopyGarnetJSON "osx-x64" "net8.0"
	CopyGarnetJSON "portable" "net8.0"
	CopyGarnetJSON "win-x64" "net8.0"
	CopyGarnetJSON "win-arm64" "net8.0"

	CopyGarnetJSON "linux-arm64" "net9.0"
	CopyGarnetJSON "linux-x64" "net9.0"
	CopyGarnetJSON "osx-arm64" "net9.0"
	CopyGarnetJSON "osx-x64" "net9.0"
	CopyGarnetJSON "portable" "net9.0"
	CopyGarnetJSON "win-x64" "net9.0"
	CopyGarnetJSON "win-arm64" "net9.0"
	Write-Host "** GarnetJSON.dll copied to all extensions folders **"
}

if ($mode -eq 0 -or $mode -eq 2) {

	# Make sure at publish folders are there as basic check files are actually published before trying to zip
	$publishedFilesFolderNet8 = "$basePath/main/GarnetServer/bin/Release/net8.0/publish"
	$publishedFilesFolderNet9 = "$basePath/main/GarnetServer/bin/Release/net9.0/publish"
	
	if (!(Test-Path $publishedFilesFolderNet8) -or !(Test-Path $publishedFilesFolderNet9)) {
		Write-Error "$publishedFilesFolderNet8 or $publishedFilesFolderNet9 does not exist. Run .\CreateBinaries 1 to publish the binaries first."
		Set-Location $lastPwd
		exit
	}
	
	# Create the directories - both net80 and net90 will be in the same zip file.
	$directories = @("linux-arm64", "linux-x64", "osx-arm64", "osx-x64", "portable", "win-arm64", "win-x64")
	$sourceFramework = @("net8.0", "net9.0")
	$baseSourcePath = "$basePath/main/GarnetServer/bin/Release"
	$destinationPath = "$basePath/main/GarnetServer/bin/Release/publish"
	$zipfiledestinationPath = "$destinationPath/output"

	# Make the destination path where the compressed files will be
	if (!(Test-Path $destinationPath)) {
		mkdir $destinationPath
	}
	if (!(Test-Path $zipfiledestinationPath)) {
		mkdir $zipfiledestinationPath
	}
	Set-Location $zipfiledestinationPath

	foreach ($dir in $directories) {
		if (!(Test-Path (Join-Path -Path $destinationPath -ChildPath $dir))) {
			mkdir (Join-Path -Path $destinationPath -ChildPath $dir)
		}
	}

	foreach ($dir in $directories) {
		foreach ($version in $sourceFramework) {
			$sourcePath = Join-Path -Path $baseSourcePath -ChildPath "$version\publish\$dir"
			$destDirPath = Join-Path -Path $destinationPath -ChildPath $dir
			$destVersionPath = Join-Path -Path $destDirPath -ChildPath $version
			
			if (!(Test-Path $destVersionPath)) {
				mkdir $destVersionPath
			}
			
			Copy-Item -Path "$sourcePath\*" -Destination $destVersionPath -Recurse -Force
		}
	}
 
	# Compress the files - both net80 and net90 in the same zip file
	Write-Host "** Compressing the files ... **"
	7z a -mmt20 -mx5 -scsWIN -r win-x64-based-readytorun.zip ../win-x64/*
	7z a -mmt20 -mx5 -scsWIN -r win-arm64-based-readytorun.zip ../win-arm64/*
	7z a -scsUTF-8 -r linux-x64-based.tar ../linux-x64/*
	7z a -scsUTF-8 -r linux-arm64-based.tar ../linux-arm64/*
	7z a -scsUTF-8 -r osx-x64-based.tar ../osx-x64/*
	7z a -scsUTF-8 -r osx-arm64-based.tar ../osx-arm64/*
	7z a -mmt20 -mx5 -sdel linux-x64-based.tar.xz linux-x64-based.tar
	7z a -mmt20 -mx5 -sdel linux-arm64-based.tar.xz linux-arm64-based.tar
	7z a -mmt20 -mx5 -sdel osx-x64-based.tar.xz osx-x64-based.tar
	7z a -mmt20 -mx5 -sdel osx-arm64-based.tar.xz osx-arm64-based.tar
	7z a -mmt20 -mx5 -scsUTF-8 portable.7z ../portable/*
}

Write-Host "** DONE! **"
Set-Location $lastPwd