function Get-SshKeyManifest {
    param([string]$ManifestPath)

    if (-not (Test-Path -LiteralPath $ManifestPath -PathType Leaf)) {
        # manifest.json is git-ignored (per-user). Seed it from the tracked
        # template on first use, then ask the user to fill in their key names.
        $templatePath = Join-Path (Split-Path -Parent $ManifestPath) 'manifest.template.json'
        if (Test-Path -LiteralPath $templatePath -PathType Leaf) {
            Copy-Item -LiteralPath $templatePath -Destination $ManifestPath
            throw "Created '$ManifestPath' from manifest.template.json. Edit its 'userKeys' and 'vmKeys' entries with your SSH key names, then re-run."
        }
        throw "SSH manifest not found: $ManifestPath"
    }

    $raw = Get-Content -LiteralPath $ManifestPath -Raw
    if ($raw -match '__SSH_USER_KEY__|__SSH_VM_KEY__') {
        throw "SSH manifest '$ManifestPath' still contains placeholder values. Edit its 'userKeys' and 'vmKeys' entries with your SSH key names."
    }

    $manifest = $raw | ConvertFrom-Json
    if (-not $manifest.basePath -or -not $manifest.userKeys -or -not $manifest.vmKeys) {
        throw "SSH manifest must define basePath, userKeys, and vmKeys."
    }

    $basePath = [Environment]::ExpandEnvironmentVariables([string]$manifest.basePath)
    if ($basePath.StartsWith('~')) {
        $basePath = Join-Path $HOME $basePath.Substring(1).TrimStart('\', '/')
    }

    return [pscustomobject]@{
        BasePath = $basePath
        UserKeys = @($manifest.userKeys)
        VmKeys   = $manifest.vmKeys
    }
}

function Resolve-SshUserPrivateKey {
    param(
        [object]$Manifest,
        [switch]$AllowMissing
    )

    $candidates = @($Manifest.UserKeys | ForEach-Object {
        $name = [string]$_
        if ($name.EndsWith('.pub', [System.StringComparison]::OrdinalIgnoreCase)) {
            $name = $name.Substring(0, $name.Length - 4)
        }
        Join-Path $Manifest.BasePath $name
    })
    $resolved = $candidates | Where-Object {
        Test-Path -LiteralPath $_ -PathType Leaf
    } | Select-Object -First 1

    if (-not $resolved -and -not $AllowMissing) {
        throw "None of the SSH private keys from the manifest exist: $($candidates -join ', ')"
    }
    return $resolved
}

function Sync-SshPublicKeys {
    param(
        [object]$Manifest,
        [string]$SecurityDir
    )

    $resolvedNames = @()
    $resolvedPaths = @()
    $missingNames = @()
    $keyNames = @(@($Manifest.UserKeys) + @($Manifest.VmKeys) |
        Where-Object { $_ } | Select-Object -Unique)

    foreach ($name in $keyNames) {
        $fileName = if ([string]$name -match '\.pub$') { [string]$name } else { "$name.pub" }
        $sourcePath = Join-Path $Manifest.BasePath $fileName
        $cachedPath = Join-Path $SecurityDir $fileName

        if (Test-Path -LiteralPath $sourcePath -PathType Leaf) {
            Copy-Item -LiteralPath $sourcePath -Destination $cachedPath -Force
            $resolvedPath = $cachedPath
        } elseif (Test-Path -LiteralPath $cachedPath -PathType Leaf) {
            Write-Warning "Source key not found: $sourcePath. Using cached public key."
            $resolvedPath = $cachedPath
        } else {
            Write-Warning "SSH public key not found: $sourcePath"
            $missingNames += $name
            continue
        }

        $resolvedNames += $name
        $resolvedPaths += $resolvedPath
    }

    return [pscustomobject]@{
        ResolvedNames = $resolvedNames
        ResolvedPaths = $resolvedPaths
        MissingNames  = $missingNames
    }
}
