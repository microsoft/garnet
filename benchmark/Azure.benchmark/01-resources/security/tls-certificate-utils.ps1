function New-TlsRandomPassword {
    param([int]$ByteCount = 32)

    $bytes = [byte[]]::new($ByteCount)
    [System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
    return [Convert]::ToBase64String($bytes)
}

function New-TlsCertificateSerialNumber {
    $serial = [byte[]]::new(16)
    [System.Security.Cryptography.RandomNumberGenerator]::Fill($serial)
    $serial[0] = $serial[0] -band 0x7f
    if (($serial | Where-Object { $_ -ne 0 }).Count -eq 0) {
        $serial[15] = 1
    }
    return $serial
}

function New-TlsCertificateBundle {
    param(
        [Parameter(Mandatory)][string]$OutputDirectory,
        [Parameter(Mandatory)][string]$TargetHost,
        [int]$ValidityDays = 365
    )

    if ([string]::IsNullOrWhiteSpace($TargetHost)) {
        throw "TLS target host is required."
    }
    if ($TargetHost -notmatch '^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$') {
        throw "TLS target host '$TargetHost' is not a valid DNS name."
    }
    if ($ValidityDays -lt 30) {
        throw "TLS certificate validity must be at least 30 days."
    }

    New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null

    $notBefore = [DateTimeOffset]::UtcNow.AddMinutes(-5)
    $notAfter = [DateTimeOffset]::UtcNow.AddDays($ValidityDays)
    $generationId = [guid]::NewGuid().ToString('N')
    $caPassword = New-TlsRandomPassword
    $serverPassword = New-TlsRandomPassword
    $clientPassword = New-TlsRandomPassword

    $caKey = [System.Security.Cryptography.RSA]::Create(4096)
    $serverKey = [System.Security.Cryptography.RSA]::Create(3072)
    $clientKey = [System.Security.Cryptography.RSA]::Create(3072)
    $caCertificate = $null
    $serverCertificate = $null
    $clientCertificate = $null

    try {
        $hash = [System.Security.Cryptography.HashAlgorithmName]::SHA256
        $padding = [System.Security.Cryptography.RSASignaturePadding]::Pkcs1

        $caRequest = [System.Security.Cryptography.X509Certificates.CertificateRequest]::new(
            'CN=AzureBench Benchmark CA', $caKey, $hash, $padding)
        $caRequest.CertificateExtensions.Add(
            [System.Security.Cryptography.X509Certificates.X509BasicConstraintsExtension]::new($true, $false, 0, $true))
        $caRequest.CertificateExtensions.Add(
            [System.Security.Cryptography.X509Certificates.X509KeyUsageExtension]::new(
                [System.Security.Cryptography.X509Certificates.X509KeyUsageFlags]::KeyCertSign -bor
                [System.Security.Cryptography.X509Certificates.X509KeyUsageFlags]::CrlSign -bor
                [System.Security.Cryptography.X509Certificates.X509KeyUsageFlags]::DigitalSignature, $true))
        $caRequest.CertificateExtensions.Add(
            [System.Security.Cryptography.X509Certificates.X509SubjectKeyIdentifierExtension]::new($caRequest.PublicKey, $false))
        $caCertificate = $caRequest.CreateSelfSigned($notBefore, $notAfter)

        $serverRequest = [System.Security.Cryptography.X509Certificates.CertificateRequest]::new(
            "CN=$TargetHost", $serverKey, $hash, $padding)
        $serverRequest.CertificateExtensions.Add(
            [System.Security.Cryptography.X509Certificates.X509BasicConstraintsExtension]::new($false, $false, 0, $true))
        $serverRequest.CertificateExtensions.Add(
            [System.Security.Cryptography.X509Certificates.X509KeyUsageExtension]::new(
                [System.Security.Cryptography.X509Certificates.X509KeyUsageFlags]::DigitalSignature -bor
                [System.Security.Cryptography.X509Certificates.X509KeyUsageFlags]::KeyEncipherment, $true))
        $serverEku = [System.Security.Cryptography.OidCollection]::new()
        $null = $serverEku.Add([System.Security.Cryptography.Oid]::new('1.3.6.1.5.5.7.3.1', 'Server Authentication'))
        $serverRequest.CertificateExtensions.Add(
            [System.Security.Cryptography.X509Certificates.X509EnhancedKeyUsageExtension]::new($serverEku, $true))
        $serverSan = [System.Security.Cryptography.X509Certificates.SubjectAlternativeNameBuilder]::new()
        $serverSan.AddDnsName($TargetHost)
        $serverRequest.CertificateExtensions.Add($serverSan.Build())
        $serverRequest.CertificateExtensions.Add(
            [System.Security.Cryptography.X509Certificates.X509SubjectKeyIdentifierExtension]::new($serverRequest.PublicKey, $false))
        $issuedServer = $serverRequest.Create(
            $caCertificate, $notBefore, $notAfter, (New-TlsCertificateSerialNumber))
        try {
            $serverCertificate = [System.Security.Cryptography.X509Certificates.RSACertificateExtensions]::CopyWithPrivateKey(
                $issuedServer, $serverKey)
        } finally {
            $issuedServer.Dispose()
        }

        $clientRequest = [System.Security.Cryptography.X509Certificates.CertificateRequest]::new(
            'CN=azurebench-client', $clientKey, $hash, $padding)
        $clientRequest.CertificateExtensions.Add(
            [System.Security.Cryptography.X509Certificates.X509BasicConstraintsExtension]::new($false, $false, 0, $true))
        $clientRequest.CertificateExtensions.Add(
            [System.Security.Cryptography.X509Certificates.X509KeyUsageExtension]::new(
                [System.Security.Cryptography.X509Certificates.X509KeyUsageFlags]::DigitalSignature -bor
                [System.Security.Cryptography.X509Certificates.X509KeyUsageFlags]::KeyEncipherment, $true))
        $clientEku = [System.Security.Cryptography.OidCollection]::new()
        $null = $clientEku.Add([System.Security.Cryptography.Oid]::new('1.3.6.1.5.5.7.3.2', 'Client Authentication'))
        $clientRequest.CertificateExtensions.Add(
            [System.Security.Cryptography.X509Certificates.X509EnhancedKeyUsageExtension]::new($clientEku, $true))
        $clientRequest.CertificateExtensions.Add(
            [System.Security.Cryptography.X509Certificates.X509SubjectKeyIdentifierExtension]::new($clientRequest.PublicKey, $false))
        $issuedClient = $clientRequest.Create(
            $caCertificate, $notBefore, $notAfter, (New-TlsCertificateSerialNumber))
        try {
            $clientCertificate = [System.Security.Cryptography.X509Certificates.RSACertificateExtensions]::CopyWithPrivateKey(
                $issuedClient, $clientKey)
        } finally {
            $issuedClient.Dispose()
        }

        $paths = [ordered]@{
            CaPfx      = Join-Path $OutputDirectory 'azurebench-ca.pfx'
            CaCert     = Join-Path $OutputDirectory 'azurebench-ca.crt'
            ServerPfx  = Join-Path $OutputDirectory 'azurebench-server.pfx'
            ServerCert = Join-Path $OutputDirectory 'azurebench-server.crt'
            ClientPfx  = Join-Path $OutputDirectory 'azurebench-client.pfx'
            ClientCert = Join-Path $OutputDirectory 'azurebench-client.crt'
            Metadata   = Join-Path $OutputDirectory 'metadata.json'
        }

        [System.IO.File]::WriteAllBytes(
            $paths.CaPfx,
            $caCertificate.Export([System.Security.Cryptography.X509Certificates.X509ContentType]::Pfx, $caPassword))
        [System.IO.File]::WriteAllText($paths.CaCert, $caCertificate.ExportCertificatePem())
        [System.IO.File]::WriteAllBytes(
            $paths.ServerPfx,
            $serverCertificate.Export([System.Security.Cryptography.X509Certificates.X509ContentType]::Pfx, $serverPassword))
        [System.IO.File]::WriteAllText($paths.ServerCert, $serverCertificate.ExportCertificatePem())
        [System.IO.File]::WriteAllBytes(
            $paths.ClientPfx,
            $clientCertificate.Export([System.Security.Cryptography.X509Certificates.X509ContentType]::Pfx, $clientPassword))
        [System.IO.File]::WriteAllText($paths.ClientCert, $clientCertificate.ExportCertificatePem())

        $metadata = [ordered]@{
            schemaVersion = 1
            status        = 'complete'
            generationId  = $generationId
            targetHost    = $TargetHost
            createdAt     = [DateTimeOffset]::UtcNow.ToString('o')
            notAfter      = $notAfter.ToString('o')
            caThumbprint  = $caCertificate.Thumbprint
            serverThumbprint = $serverCertificate.Thumbprint
            clientThumbprint = $clientCertificate.Thumbprint
        }
        [System.IO.File]::WriteAllText(
            $paths.Metadata,
            ($metadata | ConvertTo-Json -Depth 4) + [Environment]::NewLine)

        return [pscustomobject]@{
            Paths          = [pscustomobject]$paths
            Passwords      = [pscustomobject]@{
                Ca     = $caPassword
                Server = $serverPassword
                Client = $clientPassword
            }
            Metadata       = [pscustomobject]$metadata
            MetadataJson   = $metadata | ConvertTo-Json -Depth 4 -Compress
        }
    } finally {
        if ($clientCertificate) { $clientCertificate.Dispose() }
        if ($serverCertificate) { $serverCertificate.Dispose() }
        if ($caCertificate) { $caCertificate.Dispose() }
        $clientKey.Dispose()
        $serverKey.Dispose()
        $caKey.Dispose()
    }
}

function Set-KeyVaultTextSecret {
    param(
        [Parameter(Mandatory)][string]$VaultName,
        [Parameter(Mandatory)][string]$SecretName,
        [Parameter(Mandatory)][AllowEmptyString()][string]$Value
    )

    $tempFile = Join-Path ([System.IO.Path]::GetTempPath()) "azurebench-secret-$([guid]::NewGuid().ToString('N')).txt"
    try {
        [System.IO.File]::WriteAllText($tempFile, $Value)
        az keyvault secret set --vault-name $VaultName --name $SecretName --file $tempFile --output none 2>$null
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to upload Key Vault secret '$SecretName'."
        }
    } finally {
        if (Test-Path -LiteralPath $tempFile) {
            Remove-Item -LiteralPath $tempFile -Force
        }
    }
}

function Ensure-TlsCertificates {
    param(
        [Parameter(Mandatory)][string]$VaultName,
        [Parameter(Mandatory)][string]$OutputDirectory,
        [string]$TargetHost = 'azurebench-server',
        [int]$ValidityDays = 365,
        [int]$RenewBeforeDays = 30,
        [switch]$Rotate
    )

    if ($RenewBeforeDays -lt 0 -or $RenewBeforeDays -ge $ValidityDays) {
        throw "TLS renewal threshold must be at least 0 and less than the certificate validity period."
    }

    $secretNames = [ordered]@{
        CaPfx          = 'azurebench-tls-ca-pfx'
        CaPassword     = 'azurebench-tls-ca-password'
        CaCertificate  = 'azurebench-tls-ca-certificate'
        ServerPfx      = 'azurebench-tls-server-pfx'
        ServerPassword = 'azurebench-tls-server-password'
        ServerCert     = 'azurebench-tls-server-certificate'
        ClientPfx      = 'azurebench-tls-client-pfx'
        ClientPassword = 'azurebench-tls-client-password'
        ClientCert     = 'azurebench-tls-client-certificate'
        Metadata       = 'azurebench-tls-metadata'
    }

    Write-Host "`n=== Ensuring TLS certificates ===" -ForegroundColor Cyan
    Write-Host "  Vault       : $VaultName"
    Write-Host "  Target host : $TargetHost"

    $existingJson = az keyvault secret list --vault-name $VaultName `
        --query "[].{name:name,enabled:attributes.enabled}" -o json 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $existingJson) {
        throw "Could not list secrets in Key Vault '$VaultName'."
    }
    $existing = @($existingJson | ConvertFrom-Json)
    $enabledNames = @($existing | Where-Object { $_.enabled -ne $false } | ForEach-Object { [string]$_.name })
    $missingNames = @($secretNames.Values | Where-Object { $_ -notin $enabledNames })

    $reason = $null
    if ($Rotate) {
        $reason = 'rotation was requested'
    } elseif ($missingNames.Count -gt 0) {
        $reason = "missing or disabled secrets: $($missingNames -join ', ')"
    } else {
        $metadataJson = az keyvault secret show --vault-name $VaultName --name $secretNames.Metadata `
            --query value -o tsv 2>$null
        if ($LASTEXITCODE -ne 0 -or -not $metadataJson) {
            $reason = 'TLS metadata could not be read'
        } else {
            try {
                $metadata = $metadataJson | ConvertFrom-Json
                $expiry = [DateTimeOffset]::Parse([string]$metadata.notAfter)
                if ($metadata.schemaVersion -ne 1) {
                    $reason = "unsupported TLS metadata schema '$($metadata.schemaVersion)'"
                } elseif ($metadata.status -ne 'complete') {
                    $reason = "the previous TLS certificate upload did not complete"
                } elseif ($metadata.targetHost -ne $TargetHost) {
                    $reason = "stored target host '$($metadata.targetHost)' does not match '$TargetHost'"
                } elseif ($expiry -le [DateTimeOffset]::UtcNow.AddDays($RenewBeforeDays)) {
                    $reason = "certificates expire on $($expiry.ToString('u'))"
                }
            } catch {
                $reason = "invalid TLS metadata: $($_.Exception.Message)"
            }
        }
    }

    if (-not $reason) {
        Write-Host "  Complete TLS certificate set already exists; skipping generation." -ForegroundColor Yellow
        return [pscustomobject]@{ Changed = $false; SecretNames = [pscustomobject]$secretNames }
    }

    Write-Host "  Generating a new certificate set because $reason." -ForegroundColor Yellow
    $bundle = New-TlsCertificateBundle -OutputDirectory $OutputDirectory `
        -TargetHost $TargetHost -ValidityDays $ValidityDays

    # Mark the set incomplete before replacing any artifact versions. If an upload
    # is interrupted, the next run will regenerate instead of trusting mixed versions.
    $uploadingMetadata = [ordered]@{
        schemaVersion = 1
        status        = 'uploading'
        generationId  = $bundle.Metadata.generationId
        targetHost    = $bundle.Metadata.targetHost
        createdAt     = $bundle.Metadata.createdAt
        notAfter      = $bundle.Metadata.notAfter
    } | ConvertTo-Json -Compress
    Write-Host "  Marking TLS certificate upload in progress..."
    Set-KeyVaultTextSecret -VaultName $VaultName -SecretName $secretNames.Metadata -Value $uploadingMetadata

    $values = [ordered]@{
        $secretNames.CaPfx          = [Convert]::ToBase64String([System.IO.File]::ReadAllBytes($bundle.Paths.CaPfx))
        $secretNames.CaPassword     = $bundle.Passwords.Ca
        $secretNames.CaCertificate  = [System.IO.File]::ReadAllText($bundle.Paths.CaCert)
        $secretNames.ServerPfx      = [Convert]::ToBase64String([System.IO.File]::ReadAllBytes($bundle.Paths.ServerPfx))
        $secretNames.ServerPassword = $bundle.Passwords.Server
        $secretNames.ServerCert     = [System.IO.File]::ReadAllText($bundle.Paths.ServerCert)
        $secretNames.ClientPfx      = [Convert]::ToBase64String([System.IO.File]::ReadAllBytes($bundle.Paths.ClientPfx))
        $secretNames.ClientPassword = $bundle.Passwords.Client
        $secretNames.ClientCert     = [System.IO.File]::ReadAllText($bundle.Paths.ClientCert)
    }

    foreach ($entry in $values.GetEnumerator()) {
        Write-Host "  Uploading secret '$($entry.Key)'..."
        Set-KeyVaultTextSecret -VaultName $VaultName -SecretName $entry.Key -Value $entry.Value
    }

    # Replace the in-progress marker last. Only status=complete is reusable.
    Write-Host "  Uploading secret '$($secretNames.Metadata)'..."
    Set-KeyVaultTextSecret -VaultName $VaultName -SecretName $secretNames.Metadata -Value $bundle.MetadataJson

    Write-Host "  TLS certificates generated locally in '$OutputDirectory' and uploaded." -ForegroundColor Green
    return [pscustomobject]@{
        Changed     = $true
        SecretNames = [pscustomobject]$secretNames
        Metadata    = $bundle.Metadata
        Paths       = $bundle.Paths
    }
}
