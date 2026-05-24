# Tsavorite Native Device (C++)

We use CMake to build the native device (optionally used on Linux and Windows).
To build, create one or more build directories and use CMake to set up build
scripts for your target OS. Once CMake has generated the build scripts, it will
try to update them, as needed, during ordinary build.

The resulting library is named `native_device.dll` on Windows and
`libnative_device.so` on Linux. Both expose the same C ABI; selection of the
underlying IO backend at runtime is done via the C# `IoBackend` enum, which
Garnet surfaces as `--device-io-backend default|libaio|uring`.

> **Note on the prebuilt artifacts.** Prebuilt binaries are checked in at
> `libs/storage/Tsavorite/cs/src/core/Device/runtimes/{linux-x64,win-x64}/native/`
> and copied into `bin/` at build time. If you change C++ sources you must
> rebuild *and* re-check in the matching prebuilt for the platform you changed,
> otherwise consumers will not see your changes. See "Updating the shipped
> prebuilt binaries" below.

### Building on Windows

The native device builds with MSVC (Visual Studio 2022, Visual Studio 2026
Insiders, or the standalone Build Tools). You need the **Desktop development
with C++** workload installed, plus the **Spectre-mitigated CRT libraries**
(see prerequisites below).

#### 1. Confirm CMake and Visual Studio are installed

CMake **3.21 or newer** is required for the Visual Studio 2022 generator;
**3.31 or newer** is recommended for Visual Studio 2026 Insiders. Check with:

```powershell
cmake --version
```

To list what Visual Studio installs CMake will discover:

```powershell
& "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe" -prerelease -products * -format json |
    Select-String -Pattern '"displayName"|"installationPath"|"installationVersion"'
```

Pre-release / Insiders editions only show up with the `-prerelease` flag —
omitting it is the most common reason for a missing-VS error.

#### 2. Install the Spectre-mitigated CRT libraries

Our `CMakeLists.txt` builds with `/Qspectre /guard:cf /sdl` (source-level
Spectre v1 mitigation, Control Flow Guard, and Security Development
Lifecycle checks) and the project requires the matching Spectre-mitigated
CRT runtime libraries at link time. They are an *optional* component in
the VS Installer:

1. Open the **Visual Studio Installer**, click **Modify** on your VS
   2022 or VS 2026 Insiders install.
2. Switch to the **Individual components** tab.
3. Search for `Spectre` and check the entry that matches your toolset and
   architecture — e.g. **MSVC v143 - VS 2022 C++ x64/x86 Spectre-mitigated
   libs (Latest)** for VS 2022, or the equivalent **v144** / `v18` entry
   for VS 2026 Insiders.
4. Click **Modify** to install (about 200 MB).

Skipping this step will surface as `error MSB8040: Spectre-mitigated
libraries are required for this project` during `cmake --build`.

#### 3. Pick a generator

| Visual Studio                       | CMake generator string             |
| ----------------------------------- | ---------------------------------- |
| Visual Studio 2026 Insiders         | `"Visual Studio 18 2026"`          |
| Visual Studio 2022 (any edition)    | `"Visual Studio 17 2022"`          |
| Build Tools 2022 (no IDE)           | `"Visual Studio 17 2022"`          |
| Anything else / unsure              | `Ninja` (see below)                |

If `cmake --help` does not list your generator, your CMake is too old —
upgrade with `winget install Kitware.CMake` or download a fresh build from
<https://cmake.org/download/>.

#### 4. Configure and build

Create a `build` directory under `Tsavorite\cc` and configure from there:

```powershell
cd libs\storage\Tsavorite\cc
mkdir build
cd build

# Pick the line matching your VS edition:
cmake -G "Visual Studio 18 2026" -A x64 ..    # VS 2026 Insiders (current dev tooling)
cmake -G "Visual Studio 17 2022" -A x64 ..    # VS 2022 (any edition) / Build Tools 2022

cmake --build . --config Release
```

That produces `build\src\Release\native_device.dll` (and a matching
`native_device.pdb`). The CMake configuration also creates a `Tsavorite.sln`
that you can open in Visual Studio for interactive development; both `Debug`
and `Release` profiles are added for `x64`.

`USE_URING` is a no-op on Windows; the Windows DLL only exposes the
Default (IOCP) backend.

##### Generator-agnostic fallback (Ninja)

If you have Build Tools without an IDE, an unsupported / preview VS
version, or you just want a faster command-line build, use Ninja. Open
the **x64 Native Tools Command Prompt for VS \<version\>** (the start-menu
shortcut that ships with every VS install) and run:

```cmd
cd libs\storage\Tsavorite\cc
mkdir build && cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ..
cmake --build .
```

Ninja picks up `cl.exe` from the developer prompt automatically, so it
bypasses the generator-string lookup entirely.

### Building on Linux

#### 1. Install the required packages

The native device links against `aio` (libaio) and, when `USE_URING=ON`
(the default), `uring` (liburing). Plus the usual build toolchain.

On **Debian / Ubuntu** (including Ubuntu 24.04 with the t64 ABI transition):

```sh
sudo apt-get install build-essential cmake libaio-dev liburing-dev
```

On **Fedora / RHEL / Azure Linux**:

```sh
sudo dnf install gcc-c++ cmake libaio-devel liburing-devel
```

On **Alpine** (musl): the prebuilt won't load — Alpine images use the
managed `LocalStorageDevice` instead.

CMake **3.21 or newer** is required.

#### 2. Configure and build

CMake on Linux generates scripts for **either** Debug or Release per build
directory — run CMake twice, in two different directories, if you want
both.

```sh
cd libs/storage/Tsavorite/cc
mkdir -p build/Release && cd build/Release
cmake -DCMAKE_BUILD_TYPE=Release ../..
make -j"$(nproc)"
```

Or, for a Debug build:

```sh
mkdir -p build/Debug && cd build/Debug
cmake -DCMAKE_BUILD_TYPE=Debug ../..
make -j"$(nproc)"
```

The output is `build/<config>/libnative_device.so`. It exposes both
backends; select one at runtime via the C# `IoBackend` enum, which
Garnet surfaces as `--device-io-backend libaio|uring`. The Linux default
is libaio.

> **Runtime dependencies of the shipped prebuilt.** The Linux prebuilt
> checked into this repo is built with `USE_URING=ON`, so it has BOTH
> `libaio.so.1` *and* `liburing.so.2` recorded as `NEEDED` ELF entries.
> Even if you only ever request the `Libaio` (or `Default`) backend, the
> dynamic linker still has to resolve `liburing.so.2` at load time. The
> Garnet Dockerfiles in the repo install `liburing2` / `liburing`
> alongside `libaio` for this reason. If your deployment cannot install
> liburing, build the native lib yourself with `-DUSE_URING=OFF` (see
> "Disabling io_uring" below) and ship that artifact instead.

> **Ubuntu 24.04 (t64 ABI) note.** Since the t64 transition the system
> ships `libaio.so.1t64` instead of `libaio.so.1`. CMake's `-laio` will
> find `/usr/lib/x86_64-linux-gnu/libaio.so` which is a symlink to the
> `.1t64.0.2` file, so the build itself succeeds — but the resulting
> binary records `libaio.so.1t64` in its `NEEDED` list. To restore the
> portable name, run `patchelf --replace-needed libaio.so.1t64
> libaio.so.1 libnative_device.so` after the build (see the prebuilt
> update section below).

#### Disabling io_uring (optional)

To produce a smaller `libnative_device.so` without a liburing runtime
dependency (for environments where you cannot install liburing2), pass
`-DUSE_URING=OFF` to cmake:

```sh
cmake -DCMAKE_BUILD_TYPE=Release -DUSE_URING=OFF ../..
```

The resulting binary links only libaio. Requesting the uring backend at
runtime against such a build returns null from
`NativeDevice_CreateWithBackend`, which the C# layer surfaces as a
clear `TsavoriteException`; the `Default` and `Libaio` backends remain
available.

### Updating the shipped prebuilt binaries

The C# project copies the prebuilt native libraries from
`libs/storage/Tsavorite/cs/src/core/Device/runtimes/<rid>/native/` into the
output directory at build time. After rebuilding, copy the new artifact over
the corresponding file in that directory and check it in.

**Linux (`linux-x64`):**

```sh
# from libs/storage/Tsavorite/cc, after a Release build with USE_URING=ON:
cp build/Release/libnative_device.so \
   ../cs/src/core/Device/runtimes/linux-x64/native/libnative_device.so
```

The shipped Linux binary records `libaio.so.1` and `liburing.so.2` as its
NEEDED entries. On Ubuntu 24.04 and later (post-t64 ABI transition) the linker
records `libaio.so.1t64` instead; the Dockerfiles in this repo install
`libaio1t64` and create a `libaio.so.1 -> libaio.so.1t64` compat symlink, but
we keep the checked-in NEEDED name portable by patching it:

```sh
patchelf --replace-needed libaio.so.1t64 libaio.so.1 \
   ../cs/src/core/Device/runtimes/linux-x64/native/libnative_device.so
```

**Windows (`win-x64`):**

A VS generator places the binary under `src\Release\` (or `src\Debug\`); a
Ninja build places it under `src\` directly. Adjust the source path
accordingly:

```cmd
:: from libs\storage\Tsavorite\cc, after a Release build with the VS generator:
copy /Y build\src\Release\native_device.dll ^
   ..\cs\src\core\Device\runtimes\win-x64\native\
copy /Y build\src\Release\native_device.pdb ^
   ..\cs\src\core\Device\runtimes\win-x64\native\
```

To verify that the new entrypoints are present in the rebuilt binary:

```sh
# Linux
nm -D --defined-only libnative_device.so | grep NativeDevice_
```

```cmd
:: Windows
dumpbin /exports native_device.dll | findstr NativeDevice_
```

You should see `NativeDevice_CreateWithBackend` and
`NativeDevice_AvailableBackends` in addition to the legacy entrypoints.

### Other options

You can try other generators (compilers) supported by CMake. The main CMake
build script is the CMakeLists.txt located in the root directory (Tsavorite/cc).

