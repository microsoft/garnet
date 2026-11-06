$benchmarkDir = Split-Path -Parent $PSScriptRoot
. (Join-Path $benchmarkDir '03-workload\bench\utils.ps1')
function ssh { throw 'SSH must be mocked' }

Describe 'Native benchmark configuration forwarding' {
    It 'forwards exact bytes and argument boundaries, propagates exit status and removes the upload (<ExitCode>)' -TestCases @(
        @{ ExitCode = 0 }
        @{ ExitCode = 27 }
    ) {
        param($ExitCode)
        $configPath = Join-Path $TestDrive 'native.conf'
        $content = "{`r`n  `"UseFixedWorkerPool`": false, `"FutureOption`": [1, 2]`r`n}"
        [IO.File]::WriteAllText($configPath, $content)
        $overrides = @('--host', '10.0.0.4', '--port', '7000', '--tlshost', 'literal $value; not a command')
        $command = New-RespBenchmarkRemoteCommand -ConfigFile $configPath -Arguments $overrides
        $remoteScript = [Text.Encoding]::Unicode.GetString([Convert]::FromBase64String(($command -split ' ')[-1]))
        $env:GARNET_CONFIG_TEST_CAPTURE = Join-Path $TestDrive 'capture.json'
        $env:GARNET_CONFIG_TEST_EXIT = [string]$ExitCode
        $fake = @'
function Resp.benchmark {
    [pscustomobject]@{
        Bytes = [Convert]::ToBase64String([IO.File]::ReadAllBytes($args[1]))
        Path = $args[1]
        Arguments = @($args | Select-Object -Skip 2)
    } | ConvertTo-Json | Set-Content -LiteralPath $env:GARNET_CONFIG_TEST_CAPTURE
    $global:LASTEXITCODE = [int]$env:GARNET_CONFIG_TEST_EXIT
}
'@
        try {
            $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($fake + "`n" + $remoteScript))
            & pwsh -NoProfile -NonInteractive -EncodedCommand $encoded
            $LASTEXITCODE | Should Be $ExitCode
            $captured = Get-Content -LiteralPath $env:GARNET_CONFIG_TEST_CAPTURE -Raw | ConvertFrom-Json
            $captured.Bytes | Should Be ([Convert]::ToBase64String([IO.File]::ReadAllBytes($configPath)))
            ($captured.Arguments -join '|') | Should Be ($overrides -join '|')
            Test-Path -LiteralPath $captured.Path | Should Be $false
        } finally {
            Remove-Item Env:\GARNET_CONFIG_TEST_CAPTURE, Env:\GARNET_CONFIG_TEST_EXIT
        }
    }

    It 'rejects failed native validation before any workload or server probe' {
        $source = Get-Content (Join-Path $benchmarkDir '03-workload\bench\resp-bench.ps1') -Raw
        $end = $source.IndexOf('if ($tlsEnabled) {', $source.IndexOf('foreach ($clientHost'))
        if ($end -lt 0) { throw 'Native validation boundary not found' }
        $path = Join-Path $TestDrive 'validate-driver.ps1'
        Set-Content -LiteralPath $path -Value $source.Substring(0, $end)
        Copy-Item (Join-Path $benchmarkDir '03-workload\bench\utils.ps1') $TestDrive
        $config = Join-Path $TestDrive 'invalid.conf'
        '{"UnknownOption":true}' | Set-Content $config
        $key = Join-Path $TestDrive 'key'
        'not a private key' | Set-Content $key
        Mock ssh {
            $global:LASTEXITCODE = 1
            'Unknown benchmark configuration option: UnknownOption'
        }
        {
            & $path -ConfigFile $config -ClientHosts client-a.example.com,client-b.example.com `
                -SshKey $key -SshUser operator -Server 10.0.0.4 -Port 7000
        } | Should Throw 'Native benchmark configuration validation failed'
        Assert-MockCalled ssh -Times 1 -Exactly -Scope It
    }
}
