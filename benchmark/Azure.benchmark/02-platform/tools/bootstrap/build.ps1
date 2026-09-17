#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Build and install a benchmark system or a selected vLLM backend.

.DESCRIPTION
    PowerShell port of build.sh. Targets Linux VMSS instances (invokes native
    git/make/dotnet/autoconf toolchains). Usage: build.ps1 <system> [branch] [tls]

.EXAMPLE
    build.ps1 valkey
    build.ps1 valkey 9.0
    build.ps1 valkey 9.0 tls
    build.ps1 redis unstable tls
    build.ps1 garnet main
    build.ps1 resp-bench
    build.ps1 memtier
    build.ps1 vllm -Backend cpu
    build.ps1 vllm -Backend cuda
#>
param(
    [Parameter(Position = 0, Mandatory = $true)][string]$System,
    [Parameter(Position = 1)][string]$Branch = '',
    [Parameter(Position = 2)][string]$Tls = '',
    [ValidateSet('cpu', 'cuda')][string]$Backend = 'cpu'
)

$ErrorActionPreference = 'Stop'

# --- Load shared config.env ---
$configFile = '/opt/deploy-actions/config.env'
if (Test-Path $configFile) {
    Get-Content $configFile | ForEach-Object {
        if ($_ -match '^\s*([A-Z_]+)="?([^"]*)"?\s*$' -and $_ -notmatch '^\s*#') {
            Set-Variable -Name $Matches[1] -Value $Matches[2] -Scope Script
        }
    }
}

$manifest = "$USER_HOME/tools/manifest.json"
$cores = (& nproc).Trim()

function Get-RepoPath([string]$Name) {
    $data = Get-Content $manifest -Raw | ConvertFrom-Json
    ($data.repos | Where-Object { $_.name -eq $Name } | Select-Object -First 1).path
}

function Invoke-Git([string[]]$GitArgs) {
    & sudo -u $DEPLOY_USER git @GitArgs
    if ($LASTEXITCODE -ne 0) { throw "git $($GitArgs -join ' ') failed" }
}

$garnetDir = Get-RepoPath 'garnet'
$valkeyDir = Get-RepoPath 'valkey'
$redisDir = Get-RepoPath 'redis'
$memtierDir = Get-RepoPath 'memtier'
$vllmDir = Get-RepoPath 'vllm'

function Get-Rid {
    $arch = (& uname -m).Trim()
    if ($arch -eq 'aarch64') { 'linux-arm64' } else { 'linux-x64' }
}

function Build-ValkeyRedis([string]$dir) {
    if (-not (Test-Path $dir)) { Write-Host "ERROR: $dir not found. Clone the repo first."; exit 1 }
    Set-Location $dir

    if ($Branch) {
        Write-Host "==== Checking out $System $Branch ===="
        Invoke-Git @('fetch', '--tags')
        Invoke-Git @('checkout', $Branch, '--')
        Invoke-Git @('reset', '--hard', $Branch, '--')
    }

    & make distclean 2>$null
    $global:LASTEXITCODE = 0

    if ($Tls -eq 'tls') {
        Write-Host "==== Building $System with TLS ===="
        & sudo -u $DEPLOY_USER make "-j$cores" BUILD_TLS=yes
    }
    else {
        Write-Host "==== Building $System ===="
        & sudo -u $DEPLOY_USER make "-j$cores"
    }
    if ($LASTEXITCODE -ne 0) { throw "make failed" }

    if ($Tls -eq 'tls') {
        $serverSource = @('./src/valkey-server', './src/redis-server') |
            Where-Object { Test-Path $_ } |
            Select-Object -First 1
        $cliSource = @('./src/valkey-cli', './src/redis-cli') |
            Where-Object { Test-Path $_ } |
            Select-Object -First 1
        if (-not $serverSource -or -not $cliSource) {
            throw "TLS build did not produce the expected server and CLI binaries."
        }

        $serverTarget = "$INSTALL_DIR/$System-server-tls"
        $cliTarget = "$INSTALL_DIR/$System-cli-tls"
        Write-Host "==== Installing TLS binaries as $serverTarget and $cliTarget ===="
        & sudo install -m 0755 $serverSource $serverTarget
        if ($LASTEXITCODE -ne 0) { throw "Failed to install $serverTarget" }
        & sudo install -m 0755 $cliSource $cliTarget
        if ($LASTEXITCODE -ne 0) { throw "Failed to install $cliTarget" }
    }
    else {
        Write-Host "==== Installing $System ===="
        & sudo make install
        if ($LASTEXITCODE -ne 0) { throw "make install failed" }
    }

    Write-Host "==== Build complete ===="
    # Print the built server version. Valkey produces valkey-server (older
    # forks redis-server); guard with Test-Path so a missing binary does not
    # raise a terminating "not recognized" error and fail an otherwise-good build.
    foreach ($bin in @('./src/valkey-server', './src/redis-server')) {
        if (Test-Path $bin) { & $bin --version; break }
    }
}

function Build-Garnet {
    if (-not (Test-Path $garnetDir)) { Write-Host "ERROR: $garnetDir not found. Clone the garnet repo first."; exit 1 }
    Set-Location $garnetDir

    if ($Branch) {
        Write-Host "==== Checking out $Branch ===="
        Invoke-Git @('fetch', '--all', '--tags')
        Invoke-Git @('checkout', $Branch, '--')
        Invoke-Git @('reset', '--hard', $Branch, '--')
    }

    $rid = Get-Rid
    Write-Host "==== Building GarnetServer (Release, $rid) ===="
    & sudo -u $DEPLOY_USER dotnet publish $GARNET_PROJECT -c Release -r $rid -f net10.0 -o "$garnetDir/publish"
    if ($LASTEXITCODE -ne 0) { throw "dotnet publish failed" }

    & mkdir -p "$INSTALL_DIR/garnet"
    & cp -r "$garnetDir/publish/." "$INSTALL_DIR/garnet/"
    & ln -sf "$INSTALL_DIR/garnet/GarnetServer" "$INSTALL_DIR/GarnetServer"
    & chmod +x "$INSTALL_DIR/garnet/GarnetServer"

    Write-Host "==== Build complete ===="
    & GarnetServer --version 2>$null
    if ($LASTEXITCODE -ne 0) { Write-Host "GarnetServer installed at $INSTALL_DIR/GarnetServer" }
}

function Build-Memtier {
    if (-not (Test-Path $memtierDir)) { Write-Host "ERROR: $memtierDir not found. Clone the repo first."; exit 1 }
    Set-Location $memtierDir

    if ($Branch) {
        Write-Host "==== Checking out memtier $Branch ===="
        Invoke-Git @('fetch', '--tags')
        Invoke-Git @('checkout', $Branch, '--')
        Invoke-Git @('reset', '--hard', $Branch, '--')
    }

    Write-Host "==== Building memtier_benchmark ===="
    & autoreconf -ivf
    if ($LASTEXITCODE -ne 0) { throw "autoreconf failed" }
    & ./configure
    if ($LASTEXITCODE -ne 0) { throw "configure failed" }
    & make "-j$cores"
    if ($LASTEXITCODE -ne 0) { throw "make failed" }
    & sudo make install
    if ($LASTEXITCODE -ne 0) { throw "make install failed" }

    Write-Host "==== Build complete ===="
    & memtier_benchmark --version
}

function Build-RespBench {
    if (-not (Test-Path $garnetDir)) { Write-Host "ERROR: $garnetDir not found. Clone the garnet repo first."; exit 1 }
    Set-Location $garnetDir

    if ($Branch) {
        Write-Host "==== Checking out $Branch ===="
        Invoke-Git @('fetch', '--all')
        Invoke-Git @('checkout', $Branch, '--')
        Invoke-Git @('reset', '--hard', $Branch, '--')
    }

    $rid = Get-Rid
    Write-Host "==== Building Resp.benchmark (Release, $rid) ===="
    & sudo -u $DEPLOY_USER dotnet publish $RESP_BENCH_PROJECT -c Release -r $rid -f net10.0 -o "$garnetDir/resp-bench-publish"
    if ($LASTEXITCODE -ne 0) { throw "dotnet publish failed" }

    & mkdir -p "$INSTALL_DIR/resp-bench"
    & cp -r "$garnetDir/resp-bench-publish/." "$INSTALL_DIR/resp-bench/"
    & ln -sf "$INSTALL_DIR/resp-bench/Resp.benchmark" "$INSTALL_DIR/Resp.benchmark"
    & chmod +x "$INSTALL_DIR/resp-bench/Resp.benchmark"

    Write-Host "==== Resp.benchmark build complete ===="
    Write-Host "Installed at $INSTALL_DIR/resp-bench/Resp.benchmark"
}

function Build-Vllm {
    if (-not (Test-Path "$vllmDir/pyproject.toml")) {
        throw "vLLM checkout not found at $vllmDir. Deploy the vllm-dev workload profile first."
    }

    if ($Branch) {
        Write-Host "==== Checking out vLLM $Branch ===="
        Invoke-Git @('-C', $vllmDir, 'fetch', '--all', '--tags')
        Invoke-Git @('-C', $vllmDir, 'checkout', $Branch, '--')
        Invoke-Git @('-C', $vllmDir, 'reset', '--hard', $Branch, '--')
    }

    $venvDir = "$vllmDir/.venv-$Backend"
    $python = "$venvDir/bin/python"
    $uv = "$venvDir/bin/uv"
    if (-not (Test-Path $python) -or -not (Test-Path $uv)) {
        throw "The $Backend dependency environment is missing at $venvDir. Re-run setup-vllm-dependencies.ps1 with the appropriate hardware profile."
    }

    if ($Backend -eq 'cuda') {
        $nvcc = '/usr/local/cuda-13.0/bin/nvcc'
        if (-not (Get-Command nvidia-smi -ErrorAction SilentlyContinue)) {
            throw 'The NVIDIA driver is unavailable. CUDA builds require a GPU hardware profile.'
        }
        & nvidia-smi 2>$null | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw 'nvidia-smi failed. Verify that the Azure NVIDIA driver extension completed.'
        }
        if (-not (Test-Path $nvcc)) {
            throw "CUDA compiler nvcc is unavailable at $nvcc."
        }
    }

    $distDir = "$vllmDir/dist/$Backend"
    foreach ($generatedPath in @("$vllmDir/build", $distDir)) {
        if (Test-Path $generatedPath) {
            Remove-Item $generatedPath -Recurse -Force
        }
    }
    New-Item -ItemType Directory -Path $distDir -Force | Out-Null
    & chown -R "${DEPLOY_USER}:${DEPLOY_USER}" "$vllmDir/dist"

    $buildEnv = @(
        "HOME=$USER_HOME",
        "PATH=$venvDir/bin:$USER_HOME/.cargo/bin:/usr/local/cuda-13.0/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "VLLM_TARGET_DEVICE=$Backend",
        "MAX_JOBS=$cores"
    )
    if ($Backend -eq 'cuda') {
        $buildEnv += 'CUDA_HOME=/usr/local/cuda-13.0'
    }

    Write-Host "==== Building vLLM backend: $Backend ===="
    Push-Location $vllmDir
    try {
        & sudo -u $DEPLOY_USER env @buildEnv $python setup.py bdist_wheel --dist-dir $distDir
        if ($LASTEXITCODE -ne 0) {
            throw "vLLM $Backend wheel build failed."
        }
    }
    finally {
        Pop-Location
    }

    $wheel = Get-ChildItem -Path $distDir -Filter '*.whl' |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    if (-not $wheel) {
        throw "vLLM $Backend build did not produce a wheel."
    }

    & sudo -u $DEPLOY_USER env @buildEnv $uv pip install --python $python `
        --force-reinstall --no-deps $wheel.FullName
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to install the vLLM $Backend wheel."
    }

    $validation = if ($Backend -eq 'cuda') {
        "import torch, vllm; assert torch.cuda.is_available(); print(vllm.__version__, torch.cuda.get_device_name(0))"
    }
    else {
        "import torch, vllm; print(vllm.__version__, torch.__version__)"
    }
    Push-Location $USER_HOME
    try {
        & sudo -u $DEPLOY_USER env @buildEnv $python -c $validation
        if ($LASTEXITCODE -ne 0) {
            throw "vLLM $Backend validation failed."
        }
    }
    finally {
        Pop-Location
    }

    Write-Host "==== vLLM $Backend build complete: $($wheel.FullName) ===="
}

switch ($System) {
    'redis' { Build-ValkeyRedis $redisDir }
    'valkey' { Build-ValkeyRedis $valkeyDir }
    'garnet' { Build-Garnet }
    'resp-bench' { Build-RespBench }
    'memtier' { Build-Memtier }
    'vllm' { Build-Vllm }
    default {
        Write-Host "Unknown system: $System (use redis, valkey, garnet, resp-bench, memtier, or vllm)"
        exit 1
    }
}
