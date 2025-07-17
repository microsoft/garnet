<#
.SYNOPSIS
	This script is designed to publish GarnetServer into various platforms.

.DESCRIPTION

	Script to publish the GarnetServer executable from various Profiles. It will clean up all the files not needed and by default it will zip them all up in the output directory.
	
.PARAMETER mode
		0 (default) = do both publish and zip
		1 = publish only
		2 = zip only

		The mode allows for special case when running from a pipeline that it only does one of the two parts (publish  or zip) at a time. 
			Doing this allows actions (code signing etc) on the files before it is zipped. Running the script after that can zip up everything (using mode 2).
   
.PARAMETER destDir
		Defaults to main\GarnetServer from base of solution

		This parameter allows choosing the destination directory for the build.

.PARAMETER buildDir
		Defaults to none, solution settings will be used

		This parameter allows choosing the artifacts directory for the build.

.EXAMPLE
	./createbinaries.ps1
	./createbinaries.ps1 0
	./createbinaries.ps1 -mode 1
#>

# Send the config file for the benchmark. Defaults to a simple one
param (
  [int]$mode = 0,
  [string]$destDir = "",
  [string]$buildDir = ""
)


# ******** FUNCTION DEFINITIONS  *********

################## BuildAndCleanUpFiles #####################
#  
#  Publishes build and afterwards cleans it up so only the necessary files will be ready to be zipped
#  
######################################################
function BuildAndCleanUpFiles {
	param ($publishFolder, $platform, $framework)

	$publishPath = "$destDir/bin/Release/$framework/publish/$publishFolder"
	$excludeGarnetServerPDB = 'GarnetServer.pdb'

	# Native binary is different based on OS by default
	$nativeRuntimePathFile = "$publishPath/runtimes/$platform/native/libnative_device.so"

	$GarnetServer = "$basePath/main/GarnetServer/GarnetServer.csproj"
	$GarnetWorker = "$basePath/hosting/Windows/Garnet.worker/Garnet.worker.csproj"

	if ($publishFolder -eq "portable") {
		dotnet publish $GarnetServer -p:PublishProfile="portable" -f:$framework @artifactArg -o "$publishPath"
		# don't clean up all files for portable ... leave as is
		return;
	} elseif ($platform -eq "win-x64") {
		$nativeRuntimePathFile = "$publishPath/runtimes/$platform/native/native_device.dll"
		dotnet publish $GarnetServer -p:PublishProfile="$publishFolder-based-readytorun" -f:$framework @artifactArg -o "$publishPath"
		dotnet publish $GarnetWorker -r $publishFolder -p:SelfContained=false -p:PublishSingleFile=true -f:$framework @artifactArg -o "$publishPath/Service"
	} else {
		dotnet publish $GarnetServer -p:PublishProfile="$publishFolder-based" -f:$framework @artifactArg -o "$publishPath"
	}

	if (Test-Path -Path $publishPath) {
		Get-ChildItem -Path $publishPath -Filter '*.pfx' -Recurse | Remove-Item -Force
		Get-ChildItem -Path $publishPath -Filter '*.xml' -Recurse | Remove-Item -Force
		Get-ChildItem -Path $publishPath -Filter '*.pdb' -Recurse | Where-Object { $_.Name -ne $excludeGarnetServerPDB } | Remove-Item -Force

		# Copy proper native run time to publish directory
		Copy-Item -Path $nativeRuntimePathFile -Destination $publishPath

		if (Test-Path -Path $publishPath/Service) {
			# Copy proper native run time to Service publish directory
			Copy-Item -Path $nativeRuntimePathFile -Destination "$publishPath/Service"
		} elseif ($platform -eq "win-x64") {
			Write-Host "Publish Path not found: $publishPath/Service"
		}

		# Delete the runtimes folder
		Get-ChildItem -Path $publishPath -Filter 'runtimes' -Recurse | Remove-Item -Recurse -Force
	} else {
		Write-Host "Publish Path not found: $publishPath"
	}
}

# Get base path since paths can differ from machine to machine
$string = $pwd.Path
$position = $string.IndexOf(".azure")
$basePath = $string.Substring(0,$position-1)  # take off slash off end as well

if ($buildDir -ne "") {
	New-Item -Type Directory -Force $buildDir | Out-Null
	$artifactArg = "--artifacts-path",(Resolve-Path $buildDir).Path
}
if ($destDir -ne "") {
	New-Item -Type Directory -Force $destDir | Out-Null
	$destDir = (Resolve-Path $destDir).Path
} else {
	$destDir = "$basePath/main/GarnetServer"
}

if ($mode -eq 0 -or $mode -eq 1) {
	Write-Host "** Publish ... **"

	# Build, then clean up extra files not needed for publishing
	BuildAndCleanUpFiles "linux-arm64" "linux-x64" "net8.0"
	BuildAndCleanUpFiles "linux-x64" "linux-x64" "net8.0"
	BuildAndCleanUpFiles "osx-arm64" "linux-x64" "net8.0"
	BuildAndCleanUpFiles "osx-x64" "linux-x64" "net8.0"
	BuildAndCleanUpFiles "portable" "win-x64" "net8.0"
	BuildAndCleanUpFiles "win-arm64" "win-x64" "net8.0"
	BuildAndCleanUpFiles "win-x64" "win-x64" "net8.0"

	BuildAndCleanUpFiles "linux-arm64" "linux-x64" "net9.0"
	BuildAndCleanUpFiles "linux-x64" "linux-x64" "net9.0"
	BuildAndCleanUpFiles "osx-arm64" "linux-x64" "net9.0"
	BuildAndCleanUpFiles "osx-x64" "linux-x64" "net9.0"
	BuildAndCleanUpFiles "portable" "win-x64" "net9.0"
	BuildAndCleanUpFiles "win-arm64" "win-x64" "net9.0"
	BuildAndCleanUpFiles "win-x64" "win-x64" "net9.0"
}

if ($mode -eq 0 -or $mode -eq 2) {

	# Make sure at publish folders are there as basic check files are actually published before trying to zip
	$publishedFilesFolderNet8 = "$destDir/bin/Release/net8.0/publish"
	$publishedFilesFolderNet9 = "$destDir/bin/Release/net9.0/publish"
	
	if (!(Test-Path $publishedFilesFolderNet8) -or !(Test-Path $publishedFilesFolderNet9)) {
		Write-Error "$publishedFilesFolderNet8 or $publishedFilesFolderNet9 does not exist. Run .\CreateBinaries 1 to publish the binaries first."
		exit
	}
	
	# Create the directories - both net80 and net90 will be in the same zip file.
	$directories = @("linux-arm64", "linux-x64", "osx-arm64", "osx-x64", "portable", "win-arm64", "win-x64")
	$sourceFramework = @("net8.0", "net9.0")
	$baseSourcePath = "$destDir/bin/Release"
	$destinationPath = "$destDir/bin/Release/publish"
	$zipfiledestinationPath = "$destinationPath/output"

	# Make the destination path where the compressed files will be
	New-Item -Type Directory -Force $destinationPath | Out-Null
	New-Item -Type Directory -Force $zipfiledestinationPath | Out-Null

	foreach ($dir in $directories) {
		New-Item -Type Directory -Force (Join-Path -Path $destinationPath -ChildPath $dir) | Out-Null
	}

	foreach ($dir in $directories) {
		foreach ($version in $sourceFramework) {
			$sourcePath = Join-Path -Path $baseSourcePath -ChildPath "$version/publish/$dir"
			$destDirPath = Join-Path -Path $destinationPath -ChildPath $dir
			$destVersionPath = Join-Path -Path $destDirPath -ChildPath $version
			
			New-Item -Type Directory -Force $destVersionPath | Out-Null
			Copy-Item -Path "$sourcePath/*" -Destination $destVersionPath -Recurse -Force
		}
	}
 
	# Compress the files - both net80 and net90 in the same zip file
	Write-Host "** Compressing the files ... **"
	Push-Location $zipfiledestinationPath -StackName createbinaries
	try {
		7z a -mmt20 -mx5 -mm=Deflate64 -scsWIN -r win-x64-based-readytorun.zip ../win-x64/*
		7z a -mmt20 -mx5 -mm=Deflate64 -scsWIN -r win-arm64-based-readytorun.zip ../win-arm64/*
		7z a -scsUTF-8 -r linux-x64-based.tar ../linux-x64/*
		7z a -scsUTF-8 -r linux-arm64-based.tar ../linux-arm64/*
		7z a -scsUTF-8 -r osx-x64-based.tar ../osx-x64/*
		7z a -scsUTF-8 -r osx-arm64-based.tar ../osx-arm64/*
		7z a -mmt20 -mx5 -sdel linux-x64-based.tar.xz linux-x64-based.tar
		7z a -mmt20 -mx5 -sdel linux-arm64-based.tar.xz linux-arm64-based.tar
		7z a -mmt20 -mx5 -sdel osx-x64-based.tar.xz osx-x64-based.tar
		7z a -mmt20 -mx5 -sdel osx-arm64-based.tar.xz osx-arm64-based.tar
		7z a -mmt20 -mx5 -scsUTF-8 portable.7z ../portable/*
	} finally {
		Pop-Location -StackName createbinaries
	}
}

Write-Host "** DONE! **"