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

Create new directory "build" off the root directory (Tsavorite\cc). From the new
"build" directory, execute:

```sh
cmake .. -G "<MSVC compiler>"
```

To see a list of supported MSVC compiler versions, just run "cmake -G". As of
this writing, we're using Visual Studio 2022, so you would execute:

```sh
cmake .. -G "Visual Studio 17 2022" -A x64
```

That will create build scripts inside your new "build" directory, including
a `Tsavorite.sln` file that you can use inside Visual Studio. CMake will add several
build profiles to `Tsavorite.sln`, including Debug/x64 and Release/x64.

To build from the command line:

```sh
cmake --build . --config Release
```

The outputs land in `build\src\Release\native_device.dll` and
`build\src\Release\native_device.pdb`. `USE_URING` is a no-op on Windows; the
Windows DLL only exposes the Default (IOCP) backend.

### Building on Linux

The Linux build requires several packages (both libraries and header files);
see "CMakeFiles.txt" in the root directory (Tsavorite/cc) for the list of libraries
being linked to, on Linux.

As of this writing, the required libraries are:

- stdc++fs : for <experimental/filesytem>, used for cross-platform directory
             creation.
- uuid : support for GUIDs.
- tbb : Intel's Thread Building Blocks library, used for concurrent_queue.
- gcc
- aio : Kernel Async I/O, used by QueueFile / QueueIoHandler.
- uring : io_uring async-IO interface (enabled by default; see "Disabling
          io_uring" below to opt out).
- stdc++
- pthread : thread library.

On Debian/Ubuntu, install with:

```sh
sudo apt-get install build-essential cmake uuid-dev libaio-dev liburing-dev
```

Also, CMake on Linux, for the gcc compiler, generates build scripts for either
Debug or Release build, but not both; so you'll have to run CMake twice, in two
different directories, to get both Debug and Release build scripts.

Create new directories "build/Debug" and "build/Release" off the root directory
(Tsavorite/cc). From "build/Debug", run:

```sh
cmake -DCMAKE_BUILD_TYPE=Debug ../..
```

--and from "build/Release", run:

```sh
cmake -DCMAKE_BUILD_TYPE=Release ../..
```

Then you can build Debug or Release binaries by running "make" inside the
relevant build directory.

The resulting `libnative_device.so` exposes both backends. Select one at
runtime via the C# `IoBackend` enum, which Garnet surfaces as
`--device-io-backend libaio|uring`. The Linux default is libaio.

#### Disabling io_uring (optional)

To produce a smaller `libnative_device.so` without a liburing runtime
dependency (for environments where you cannot install liburing2), pass
`-DUSE_URING=OFF` to cmake:

```sh
cmake -DCMAKE_BUILD_TYPE=Release -DUSE_URING=OFF ../..
```

The resulting binary links only libaio. Requesting the uring backend at
runtime against such a build throws a clear error and Default/Libaio remain
available.

### Updating the shipped prebuilt binaries

The C# project copies the prebuilt native libraries from
`libs/storage/Tsavorite/cs/src/core/Device/runtimes/<rid>/native/` into the
output directory at build time. After rebuilding, copy the new artifact over
the corresponding file in that directory and check it in.

**Linux (`linux-x64`):**

```sh
# from libs/storage/Tsavorite/cc, after a Release build with USE_URING=ON:
cp build/Release/src/libnative_device.so \
   ../cs/src/core/Device/runtimes/linux-x64/native/libnative_device.so
```

The shipped Linux binary records `libaio.so.1` and `liburing.so.2` as its
NEEDED entries. On distributions that have only `libaio.so.1t64` (Ubuntu 24.04
and later), the Dockerfiles in the repo set up a `libaio.so.1 -> libaio.so.1t64`
compat symlink. If you build on such a host the linker will record
`libaio.so.1t64` instead; correct it with:

```sh
patchelf --replace-needed libaio.so.1t64 libaio.so.1 \
   ../cs/src/core/Device/runtimes/linux-x64/native/libnative_device.so
```

**Windows (`win-x64`):**

```cmd
:: from libs\storage\Tsavorite\cc, after a Release build:
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

