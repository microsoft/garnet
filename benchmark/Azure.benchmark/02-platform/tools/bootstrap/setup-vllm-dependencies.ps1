#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Installs vLLM development dependencies without building vLLM.
#>

$ErrorActionPreference = 'Stop'

$configEnv = '/opt/deploy-actions/config.env'
$deploymentEnv = '/opt/deploy-actions/deployment.env'
$deployUser = 'guser'
$hardwareProfile = 'cpu'

if (Test-Path $configEnv) {
    Get-Content $configEnv | ForEach-Object {
        if ($_ -match '^DEPLOY_USER="?([^"]+)"?$') { $deployUser = $Matches[1] }
    }
}
if (Test-Path $deploymentEnv) {
    Get-Content $deploymentEnv | ForEach-Object {
        if ($_ -match '^HARDWARE_PROFILE="?([^"]+)"?$') { $hardwareProfile = $Matches[1] }
    }
}
if ($hardwareProfile -notin @('cpu', 'gpu')) {
    throw "Unsupported hardware profile '$hardwareProfile'."
}

$userHome = "/home/$deployUser"
$repoDir = "$userHome/vllm"
if (-not (Test-Path "$repoDir/pyproject.toml")) {
    throw "vLLM checkout not found at $repoDir."
}

$architecture = (& uname -m).Trim()
if ($architecture -ne 'x86_64') {
    throw "The vLLM development profile requires x86-64; detected '$architecture'."
}

$isAzureLinux = $null -ne (Get-Command tdnf -ErrorAction SilentlyContinue)
if ($isAzureLinux) {
    Write-Host 'Installing vLLM dependencies for Azure Linux...'
    & tdnf install -y `
        binutils ca-certificates ccache cmake curl gcc gcc-c++ git `
        glibc-devel libnuma-devel make numactl pkgconf-pkg-config python3 `
        python3-devel python3-pip python3-virtualenv tar wget xz zlib-devel
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to install vLLM dependencies with tdnf.'
    }
}
elseif (Get-Command apt-get -ErrorAction SilentlyContinue) {
    Write-Host 'Installing vLLM dependencies for Ubuntu...'
    $env:DEBIAN_FRONTEND = 'noninteractive'
    $aptOptions = @('-o', 'DPkg::Lock::Timeout=600', '-o', 'Acquire::Retries=5')
    & apt-get @aptOptions update
    & apt-get @aptOptions install -y --no-install-recommends `
        build-essential ca-certificates ccache clangd-14 curl ffmpeg git `
        libgl1 libnuma-dev libsm6 libtcmalloc-minimal4 libxext6 lsof `
        make ninja-build numactl pkg-config python3 python3-dev `
        python3-pip python3-venv software-properties-common wget xz-utils zlib1g-dev
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to install vLLM dependencies with apt-get.'
    }

    & add-apt-repository -y ppa:ubuntu-toolchain-r/test
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to configure the Ubuntu toolchain repository.'
    }
    & apt-get @aptOptions update
    & apt-get @aptOptions install -y --no-install-recommends gcc-15 g++-15
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to install GCC 15.'
    }
    & update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-15 15 `
        --slave /usr/bin/g++ g++ /usr/bin/g++-15
}
else {
    throw 'A supported package manager was not found (expected tdnf or apt-get).'
}

$systemPython = @('python3.12', 'python3') |
    ForEach-Object { Get-Command $_ -ErrorAction SilentlyContinue } |
    Select-Object -First 1
if (-not $systemPython) {
    throw 'Python 3 was not installed by the system package manager.'
}
$pythonVersion = [version](& $systemPython.Source -c `
    "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
if ($pythonVersion -lt [version]'3.10' -or $pythonVersion -ge [version]'3.15') {
    throw "vLLM requires Python >=3.10,<3.15; detected $pythonVersion."
}

$rustup = '/tmp/rustup-init'
if (-not (Test-Path "$userHome/.cargo/bin/cargo")) {
    Invoke-WebRequest -Uri 'https://sh.rustup.rs' -OutFile $rustup
    & chmod 755 $rustup
    & sudo -u $deployUser env "HOME=$userHome" $rustup -y --profile minimal
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to install the Rust toolchain.'
    }
    Remove-Item $rustup -Force
}

function Install-PythonDependencies {
    param(
        [Parameter(Mandatory)]
        [ValidateSet('cpu', 'cuda')]
        [string]$Backend
    )

    $venvDir = "$repoDir/.venv-$Backend"
    $python = "$venvDir/bin/python"
    $uv = "$venvDir/bin/uv"
    if (-not (Test-Path $python)) {
        & sudo -u $deployUser $systemPython.Source -m venv $venvDir
        if ($LASTEXITCODE -ne 0) {
            & sudo -u $deployUser $systemPython.Source -m virtualenv $venvDir
            if ($LASTEXITCODE -ne 0) {
                throw "Failed to create the $Backend virtual environment."
            }
        }
    }

    & sudo -u $deployUser $python -m pip install --upgrade pip uv
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to install uv in the $Backend virtual environment."
    }

    $installEnv = @(
        "HOME=$userHome",
        "PATH=$userHome/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        'UV_HTTP_TIMEOUT=500',
        'UV_INDEX_STRATEGY=unsafe-best-match'
    )
    $torchBackend = if ($Backend -eq 'cpu') { 'cpu' } else { 'cu130' }
    foreach ($requirements in @(
        "$repoDir/requirements/$Backend.txt",
        "$repoDir/requirements/build/$Backend.txt",
        "$repoDir/requirements/lint.txt",
        "$repoDir/requirements/test/$Backend.txt"
    )) {
        & sudo -u $deployUser env @installEnv $uv pip install --python $python `
            -r $requirements --torch-backend $torchBackend
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to install $Backend dependencies from $requirements."
        }
    }

    & sudo -u $deployUser env @installEnv $python -c "import torch; print(torch.__version__)"
    if ($LASTEXITCODE -ne 0) {
        throw "PyTorch validation failed for the $Backend environment."
    }
}

Install-PythonDependencies -Backend cpu

if ($hardwareProfile -eq 'gpu') {
    if ($isAzureLinux) {
        $cudaRepo = '/etc/yum.repos.d/cuda-azl3.repo'
        Invoke-WebRequest `
            -Uri 'https://developer.download.nvidia.com/compute/cuda/repos/azl3/x86_64/cuda-azl3.repo' `
            -OutFile $cudaRepo
        & tdnf clean expire-cache
        & tdnf install -y cuda-toolkit-13-0
    }
    else {
        $osRelease = @{}
        Get-Content /etc/os-release | ForEach-Object {
            if ($_ -match '^([A-Z_]+)="?([^"]*)"?$') {
                $osRelease[$Matches[1]] = $Matches[2]
            }
        }
        $ubuntuRepo = switch ($osRelease.VERSION_ID) {
            '22.04' { 'ubuntu2204' }
            '24.04' { 'ubuntu2404' }
            default { throw "CUDA installation does not support Ubuntu $($osRelease.VERSION_ID)." }
        }
        $cudaKeyring = '/tmp/cuda-keyring_1.1-1_all.deb'
        Invoke-WebRequest `
            -Uri "https://developer.download.nvidia.com/compute/cuda/repos/$ubuntuRepo/x86_64/cuda-keyring_1.1-1_all.deb" `
            -OutFile $cudaKeyring
        & apt-get @aptOptions install -y --no-install-recommends $cudaKeyring
        if ($LASTEXITCODE -eq 0) {
            Remove-Item $cudaKeyring -Force
            & apt-get @aptOptions update
            & apt-get @aptOptions install -y --no-install-recommends cuda-toolkit-13-0
        }
    }
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to install the CUDA 13.0 toolkit.'
    }
    Install-PythonDependencies -Backend cuda
}

@"
export PATH="$userHome/.cargo/bin:`$PATH"
export HF_HOME=$userHome/.cache/huggingface
function use-vllm-cpu() {
    source "$repoDir/.venv-cpu/bin/activate"
    export VLLM_TARGET_DEVICE=cpu
}
function use-vllm-cuda() {
    if [ ! -f "$repoDir/.venv-cuda/bin/activate" ]; then
        echo "CUDA dependencies are not installed on this VM." >&2
        return 1
    fi
    source "$repoDir/.venv-cuda/bin/activate"
    export VLLM_TARGET_DEVICE=cuda
    export CUDA_HOME=/usr/local/cuda-13.0
    export PATH="`$CUDA_HOME/bin:`$PATH"
}
"@ | Set-Content -Path '/etc/profile.d/vllm-dev.sh' -Encoding utf8
& chmod 644 /etc/profile.d/vllm-dev.sh
& sudo -u $deployUser mkdir -p "$userHome/.cache/huggingface"
& chown -R "${deployUser}:${deployUser}" $repoDir "$userHome/.cache"

Write-Host "vLLM dependencies are ready for hardware profile '$hardwareProfile'." -ForegroundColor Green
