// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    struct NativeResult
    {
        public DeviceIOCompletionCallback callback;
        public object context;
    }

    /// <summary>
    /// Native version of local storage device
    /// </summary>
    public unsafe class NativeStorageDevice : StorageDeviceBase
    {
        /// <summary>
        /// Hard ceiling on the effective in-flight throttle (max concurrent I/Os the device will drive),
        /// and the upper bound the native submission ring is sized to. A configured
        /// <see cref="StorageDeviceBase.ThrottleLimit"/> above this is clamped (and logged).
        ///
        /// This is a kernel-aware ceiling, not an arbitrary number:
        /// <list type="bullet">
        /// <item><b>libaio</b>: each io_context's <c>io_setup(maxevents)</c> draws from the system-wide
        /// <c>fs.aio-max-nr</c> budget (default 65536, shared across every io_context in every process); one
        /// device at this depth already takes ~1/16 of it, and deeper rings risk <c>io_setup</c> EAGAIN failures
        /// elsewhere.</item>
        /// <item><b>io_uring</b>: <c>IORING_MAX_ENTRIES</c> caps a ring at 32768 entries.</item>
        /// </list>
        /// 4096 is already 8-32x a single NVMe's useful queue depth, so it is not a practical limit. If an exotic
        /// configuration ever needs more, raise this constant deliberately and re-validate against the kernel
        /// limits above — do not allow an unbounded throttle that would silently degrade into the submit-ring spin.
        /// </summary>
        const int MaxThrottle = 1 << 12;

        /// <summary>
        /// Size of the in-flight completion-tracking pool (the <see cref="results"/> array). Deliberately larger
        /// than <see cref="MaxThrottle"/> so the pool is never the binding backpressure: with the effective
        /// throttle capped at <see cref="MaxThrottle"/>, in-flight settles around that value, and the extra
        /// headroom absorbs the brief race where several engine threads clear the <see cref="Throttle"/> gate at
        /// once — so those overshoot reads never hit the userspace slot spin-wait in <see cref="ReadAsync"/>.
        /// The pool is pure managed memory (no kernel cost), so the headroom is cheap.
        /// </summary>
        const int MaxResults = MaxThrottle * 2;

        /// <summary>
        /// Default per-context native submission-ring depth (libaio io_setup maxevents / io_uring
        /// SQ entries) when the throttle limit does not call for a deeper ring. Matches the native
        /// backend floor (file_linux.h QueueIoHandler/UringIoHandler kMaxEvents).
        /// </summary>
        const int DefaultNativeRingDepth = 128;

        /// <summary>
        /// Sentinel returned by an int-valued native entry point (currently NativeDevice_QueueRunFor)
        /// when its body threw a C++ exception that the C ABI firewall caught. Distinct from the
        /// normal failure code (-1) so the completion worker can surface NativeDevice_GetLastError
        /// rather than silently treating it as a benign drain result. Kept in sync with the native
        /// kCABIExceptionSentinel (native_device_wrapper.cc).
        /// </summary>
        const int NativeCABIExceptionSentinel = int.MinValue;

        /// <summary>
        /// Floor sector size used when the alignment probe fails (parent directory missing,
        /// or kernel/filesystem combinations that do not populate statx STATX_DIOALIGN).
        /// </summary>
        const uint MinSectorSize = IDevice.MinDeviceSectorSize;

        readonly ConcurrentQueue<int> freeResults = new();
        readonly ILogger logger;
        NativeResult[] results;

        /// <summary>
        /// Number of pending reads on device
        /// </summary>
        int numPending = 0;

        /// <summary>
        /// Effective in-flight throttle used by <see cref="Throttle"/>, i.e. <c>min(ThrottleLimit, MaxThrottle)</c>
        /// captured once when the native device is created (the same point the ring depth is fixed), so the hot
        /// throttle-spin loop does not re-clamp on every call. Defaults to <see cref="MaxThrottle"/>; only consulted
        /// once reads are in flight, by which point the native device — and this value — have been established.
        /// </summary>
        int effectiveThrottleLimit = MaxThrottle;

        int resultOffset;

        /// <summary>
        /// Configuration captured at construction time; the underlying native device is created
        /// lazily on the first IO call via <see cref="EnsureNativeDeviceCreated"/> using
        /// <c>segmentSize</c> as the requested segment size (defaults to -1 = unbounded
        /// single segment unless <see cref="Initialize"/> was called to override). All four
        /// fields are immutable after the constructor returns.
        /// </summary>
        readonly string filename;
        readonly bool deleteOnClose;
        readonly bool disableFileBuffering;
        readonly int numCompletionThreadsConfig;
        readonly int numIoContextsConfig;
        readonly IoBackend ioBackendConfig;

        /// <summary>
        /// Runtime segment size in bytes that the native shim was asked to use. Populated by
        /// <see cref="EnsureNativeDeviceCreated"/> the first time the native handle is created.
        /// When the upper-layer requested <c>segmentSize = -1</c> (unbounded single segment)
        /// this is <see cref="UnboundedNativeSegmentSizeBytes"/>, large enough that any
        /// non-negative <c>long</c> upper-layer address routes to segment 0 under the native
        /// shim's <c>shift = log2(segment_size_bytes)</c> math. Used only for diagnostics /
        /// assertions on the C# side; the authoritative value lives inside the native device.
        /// </summary>
        ulong nativeSegmentSizeBytes;

        /// <summary>
        /// Native-side segment size used to represent unbounded single-segment mode (the default
        /// when neither the ctor nor <see cref="Initialize"/> overrides it, equivalent to
        /// <c>Initialize(segmentSize: -1)</c>). 1&lt;&lt;63 = 9.2 EiB; any non-negative
        /// <c>long</c> address is below this and so shifts to segment 0 inside the native
        /// <c>FileSystemSegmentedFile</c>. The C# managed side uses <c>segmentSizeBits = 64</c> /
        /// <c>segmentSizeMask = ~0</c> for its own address math, so segment IDs are always 0 in
        /// this mode on both sides.
        /// </summary>
        const ulong UnboundedNativeSegmentSizeBytes = 1UL << 63;

        /// <summary>
        /// Atomic flag (0 = alive, 1 = disposed) set once <see cref="Dispose"/> has freed
        /// <see cref="nativeDevice"/>. All native dispatch points check this flag before crossing
        /// the P/Invoke boundary, so a late call from a Tsavorite epoch-drain path (e.g.
        /// TryComplete fired after Dispose returned) is a silent no-op instead of a use-after-free.
        /// Using an int + <see cref="Interlocked.Exchange(ref int, int)"/> makes <see cref="Dispose"/>
        /// idempotent: a second call short-circuits before re-running the (non-idempotent) shutdown
        /// sequence.
        /// </summary>
        int disposedFlag;

        #region Native storage interface

        const string NativeLibraryName = "native_device";
        static readonly string NativeLibraryPath = null;
        static readonly string LibaioFallbackLibraryPath = null;

        static NativeStorageDevice()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                NativeLibraryPath = "runtimes/win-x64/native/native_device.dll";
                LibaioFallbackLibraryPath = null;
            }
            else
            {
                // We ship two Linux native libraries:
                //   * libnative_device.so         — built with USE_URING=ON, links libaio AND
                //     liburing. Used on hosts that have liburing2 installed; exposes both the
                //     Libaio and Uring backends.
                //   * libnative_device_libaio.so  — built with USE_URING=OFF, links libaio only.
                //     Used as a fallback on hosts without liburing2 installed; exposes the
                //     Libaio backend only (selecting Uring at construction time produces a
                //     clear TsavoriteException pointing the user at the install command).
                // The two-binary scheme keeps the Uring hot path zero-overhead (direct calls,
                // no function-pointer indirection) while still giving end-users on stock
                // distributions a libnative_device that loads cleanly without installing
                // liburing manually.
                NativeLibraryPath = "runtimes/linux-x64/native/libnative_device.so";
                LibaioFallbackLibraryPath = "runtimes/linux-x64/native/libnative_device_libaio.so";
            }
            NativeLibrary.SetDllImportResolver(typeof(NativeStorageDevice).Assembly, ImportResolver);
        }

        static IntPtr ImportResolver(string libraryName, Assembly assembly, DllImportSearchPath? searchPath)
        {
            if (libraryName != NativeLibraryName || NativeLibraryPath == null)
                return IntPtr.Zero;

            var resolvedPath = ResolveNativeLibraryPath(assembly, NativeLibraryPath);

            try
            {
                return NativeLibrary.Load(resolvedPath);
            }
            catch (DllNotFoundException ex) when (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)
                                                  && LibaioFallbackLibraryPath != null
                                                  && ex.Message.Contains("liburing.so.2", StringComparison.Ordinal))
            {
                // Host has no liburing2 installed. Fall back to the libaio-only build so that
                // the Libaio backend (the default) keeps working. Selecting IoBackend.Uring at
                // construction time on the fallback binary throws TsavoriteException with an
                // install-liburing2 instruction; we never silently downgrade Uring to Libaio.
                var fallbackPath = ResolveNativeLibraryPath(assembly, LibaioFallbackLibraryPath);
                try
                {
                    return NativeLibrary.Load(fallbackPath);
                }
                catch (DllNotFoundException fallbackEx)
                {
                    throw new DllNotFoundException(
                        $"Failed to load either '{Path.GetFileName(resolvedPath)}' (needs liburing.so.2) " +
                        $"or fallback '{Path.GetFileName(fallbackPath)}' (libaio-only). " +
                        $"Primary error: {ex.Message}. Fallback error: {fallbackEx.Message}. " +
                        $"On Debian/Ubuntu install with: sudo apt-get install -y libaio1t64 liburing2",
                        ex);
                }
            }
            catch (DllNotFoundException ex) when (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)
                                                  && ex.Message.Contains("libaio.so.1", StringComparison.Ordinal))
            {
                // SONAME-mismatch shim for libaio. Our shipped binaries link libaio using the
                // build box's SONAME: on Debian 13 / Ubuntu 24.04+ the libaio1 package was renamed
                // to libaio1t64 (64-bit time_t ABI transition) and its SONAME became
                // "libaio.so.1t64", so binaries built there carry a DT_NEEDED of libaio.so.1t64.
                // Other glibc distros (Azure Linux, RHEL, Fedora, ...) ship the historical
                // "libaio.so.1" instead. Whichever SONAME the loader could not resolve, drop a
                // symlink of that name -> the libaio the host actually provides, next to
                // libnative_device.so; the native library is built with RPATH=$ORIGIN so it picks
                // the symlink up.
                var missingSoname = ex.Message.Contains("libaio.so.1t64", StringComparison.Ordinal)
                    ? "libaio.so.1t64"
                    : "libaio.so.1";
                if (TryCreateLibaioCompatSymlink(resolvedPath, missingSoname, out var symlinkedPath))
                {
                    try
                    {
                        return NativeLibrary.Load(resolvedPath);
                    }
                    catch (DllNotFoundException)
                    {
                        // Fall through to the detailed error below.
                    }
                }

                throw new DllNotFoundException(BuildLibaioDiagnostic(symlinkedPath, missingSoname, ex), ex);
            }
        }

        /// <summary>
        /// Resolve <paramref name="relativePath"/> (a NuGet-style "runtimes/&lt;rid&gt;/native/&lt;lib&gt;"
        /// relative path) to an absolute filesystem path. We probe (in order) the assembly's own
        /// directory, the application's base directory, and finally the current working directory
        /// when it is available. Falls back to the raw relative path if none of these exist, so
        /// dlopen's error message surfaces as before.
        /// </summary>
        static string ResolveNativeLibraryPath(Assembly assembly, string relativePath)
        {
            string[] searchRoots =
            [
                Path.GetDirectoryName(assembly?.Location),
                AppContext.BaseDirectory,
                TryGetCurrentDirectory(),
            ];

            foreach (var root in searchRoots)
            {
                if (string.IsNullOrEmpty(root))
                    continue;
                var candidate = Path.Combine(root, relativePath);
                if (File.Exists(candidate))
                    return Path.GetFullPath(candidate);
            }

            return relativePath;
        }

        /// <summary>
        /// Returns Directory.GetCurrentDirectory() if it can be obtained, otherwise null. The current
        /// directory can be unavailable (e.g., deleted or inaccessible to the process), which should
        /// not block native library resolution.
        /// </summary>
        static string TryGetCurrentDirectory()
        {
            try
            {
                return Directory.GetCurrentDirectory();
            }
            catch (Exception)
            {
                return null;
            }
        }

        /// <summary>
        /// True on musl-based distros (e.g., Alpine), detected via the musl dynamic loader
        /// (/lib/ld-musl-*.so*). The shipped native device libraries are glibc builds, so on musl
        /// they are unsupported: fabricating a libaio compat symlink and retrying the load would bind
        /// the glibc library against musl's libaio and crash. On musl we skip the compat shim and let
        /// the load fail cleanly so the caller can fall back to a managed device.
        /// </summary>
        static readonly bool IsMuslRuntime = DetectMuslRuntime();

        static bool DetectMuslRuntime()
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                return false;
            try
            {
                using var e = Directory.EnumerateFiles("/lib", "ld-musl-*.so*").GetEnumerator();
                return e.MoveNext();
            }
            catch
            {
                // /lib may be absent or inaccessible; treat as non-musl.
                return false;
            }
        }

        /// <summary>
        /// Candidate absolute paths for a host-provided libaio shared object, across the multiarch
        /// and lib64 layouts of the distros we support. The 64-bit time_t SONAME "libaio.so.1t64"
        /// (shipped by Debian 13 / Ubuntu 24.04+ libaio1t64) is listed before the historical
        /// "libaio.so.1" (shipped by Azure Linux, RHEL, Fedora, and pre-t64 Debian/Ubuntu) so that,
        /// when both are present, we prefer linking against the real t64 file. Add more here if
        /// additional architectures or layouts appear.
        /// </summary>
        static readonly string[] LibaioHostCandidatePaths =
        [
            "/usr/lib/x86_64-linux-gnu/libaio.so.1t64",
            "/usr/lib/aarch64-linux-gnu/libaio.so.1t64",
            "/lib/x86_64-linux-gnu/libaio.so.1t64",
            "/lib/aarch64-linux-gnu/libaio.so.1t64",
            "/usr/lib64/libaio.so.1t64",
            "/usr/lib/libaio.so.1t64",
            "/usr/lib/x86_64-linux-gnu/libaio.so.1",
            "/usr/lib/aarch64-linux-gnu/libaio.so.1",
            "/lib/x86_64-linux-gnu/libaio.so.1",
            "/lib/aarch64-linux-gnu/libaio.so.1",
            "/usr/lib64/libaio.so.1",
            "/usr/lib/libaio.so.1",
            "/lib/libaio.so.1",
        ];

        /// <summary>
        /// Finds the first host-provided libaio from <see cref="LibaioHostCandidatePaths"/>, or
        /// returns false if none is installed. The returned path reflects the distro's actual layout
        /// (e.g. /usr/lib64 on RHEL/Fedora/Azure Linux, /usr/lib/&lt;triplet&gt; on Debian/Ubuntu).
        /// </summary>
        static bool TryFindHostLibaio(out string hostLibaioPath)
        {
            hostLibaioPath = null;
            foreach (var candidate in LibaioHostCandidatePaths)
            {
                if (File.Exists(candidate))
                {
                    hostLibaioPath = candidate;
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Locate a host-provided libaio (see <see cref="LibaioHostCandidatePaths"/>) and create a
        /// <paramref name="missingSoname"/> symlink next to libnative_device.so so that the dynamic
        /// linker (searching RPATH=$ORIGIN) can satisfy the DT_NEEDED entry the load failed on. This
        /// bridges the SONAME mismatch in either direction: a t64 binary on a non-t64 distro
        /// (libaio.so.1t64 -> host libaio.so.1) or a pre-t64 binary on a t64 distro
        /// (libaio.so.1 -> host libaio.so.1t64). Returns true when a usable symlink exists at the
        /// expected path after the call - whether we created it or a concurrently-starting process
        /// did. Sets <paramref name="createdSymlink"/> to the link path in that case.
        /// </summary>
        static bool TryCreateLibaioCompatSymlink(string resolvedNativeLibraryPath, string missingSoname, out string createdSymlink)
        {
            createdSymlink = null;

            // On musl (Alpine) the glibc-built native device cannot load; do not fabricate a libaio
            // symlink that would bind it against musl's libaio and segfault - fail cleanly instead so
            // the caller falls back to a managed device (matches pre-shim behavior on musl).
            if (IsMuslRuntime)
                return false;

            if (!TryFindHostLibaio(out var hostLibaioPath))
                return false;

            string shimPath;
            try
            {
                var nativeDir = Path.GetDirectoryName(Path.GetFullPath(resolvedNativeLibraryPath));
                if (string.IsNullOrEmpty(nativeDir) || !Directory.Exists(nativeDir))
                    return false;

                shimPath = Path.Combine(nativeDir, missingSoname);
            }
            catch (Exception)
            {
                return false;
            }

            try
            {
                File.CreateSymbolicLink(shimPath, hostLibaioPath);
                createdSymlink = shimPath;
                return true;
            }
            catch (IOException)
            {
                // Either a concurrently-starting process already created the symlink (common in
                // container fleets where multiple Garnet instances share an image), or a stale file
                // of the same name is present. If it already resolves to a real libaio, treat that
                // as success; otherwise fall through to the diagnostic error.
                if (IsUsableLibaioShim(shimPath))
                {
                    createdSymlink = shimPath;
                    return true;
                }
                return false;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Returns true if <paramref name="shimPath"/> is an existing symlink whose target is a
        /// supported libaio: the "libaio.so.1t64" or historical "libaio.so.1" SONAME, or a versioned
        /// "libaio.so.1.*" real file (possibly via a relative or absolute target).
        /// </summary>
        static bool IsUsableLibaioShim(string shimPath)
        {
            try
            {
                var info = new FileInfo(shimPath);
                if (!info.Exists) return false;
                var target = info.LinkTarget;
                if (string.IsNullOrEmpty(target)) return false;
                // LinkTarget may be relative (e.g. "libaio.so.1t64") or absolute (e.g.
                // "/usr/lib64/libaio.so.1.0.2"); compare on the file name and accept only the two
                // supported SONAMEs or a versioned "libaio.so.1.*" real file - without matching an
                // unrelated future SONAME such as "libaio.so.10".
                var name = Path.GetFileName(target);
                return name == "libaio.so.1t64"
                    || name == "libaio.so.1"
                    || name.StartsWith("libaio.so.1.", StringComparison.Ordinal);
            }
            catch
            {
                return false;
            }
        }

        static string BuildLibaioDiagnostic(string attemptedSymlinkPath, string missingSoname, Exception inner)
        {
            var attempted = attemptedSymlinkPath == null
                ? "Could not find a host libaio in standard multiarch/lib64 paths; auto-repair skipped."
                : $"Attempted to create '{attemptedSymlinkPath}' -> a host libaio but the load still failed.";

            // Build the compat-symlink hint from the libaio the host actually ships so the suggested
            // path is correct on every distro (e.g. /usr/lib64 on RHEL/Fedora/Azure Linux,
            // /usr/lib/<triplet> on Debian/Ubuntu). Only offer it when a real libaio is present;
            // otherwise the right fix is to install the package.
            string symlinkFix;
            if (TryFindHostLibaio(out var hostLibaioPath))
            {
                var linkPath = Path.Combine(Path.GetDirectoryName(hostLibaioPath) ?? "/usr/lib", missingSoname);
                symlinkFix = $"(b) as root, create the compat symlink 'ln -sf {hostLibaioPath} {linkPath}', ";
            }
            else
            {
                symlinkFix = $"(b) if libaio is installed under a different SONAME, as root create a '{missingSoname}' compat symlink next to it, ";
            }

            return
                $"Failed to load native storage device library '{NativeLibraryPath}' because its dependency '{missingSoname}' " +
                "is not resolvable by the dynamic linker. This is a libaio SONAME mismatch between the build box and the host: " +
                "binaries built on Debian 13 (trixie) / Ubuntu 24.04 (noble) require 'libaio.so.1t64' (the libaio1t64 package, " +
                "64-bit time_t ABI transition), whereas Azure Linux, RHEL, Fedora and pre-t64 Debian/Ubuntu ship 'libaio.so.1'. " +
                attempted + " " +
                "To fix, either (a) install the host's libaio package (for example 'apt-get install -y libaio1t64', " +
                "'tdnf install -y libaio', or 'dnf install -y libaio'), " +
                symlinkFix +
                "or (c) switch to a non-native device by setting '--device-type RandomAccess' (or removing '--use-native-device-linux'). " +
                "Original loader error: " + inner.Message;
        }

        /// <summary>
        /// Async callback delegate
        /// </summary>
        public delegate void AsyncIOCallback(IntPtr context, int result, ulong bytesTransferred);
        IntPtr nativeDevice;

        /// <summary>
        /// Selects the IO backend used by the underlying native device. On Linux,
        /// <see cref="Libaio"/> uses the historical libaio path (the default). <see cref="Uring"/>
        /// uses io_uring. On Windows, only <see cref="Default"/> is supported (Windows ThreadPool).
        /// </summary>
        /// <remarks>
        /// Whether a given backend is actually available at runtime depends on how the loaded
        /// <c>libnative_device.so</c> / <c>native_device.dll</c> was built. Call
        /// <see cref="GetAvailableBackends"/> to probe at runtime.
        /// <para>
        /// The Linux prebuilt shipped under <c>runtimes/linux-x64/native/</c> is built with
        /// <c>USE_URING=ON</c>, so <c>liburing.so.2</c> is a NEEDED ELF entry that the dynamic
        /// linker must resolve at load time even when only <see cref="Libaio"/> is selected.
        /// </para>
        /// Must stay in sync with <c>NativeDeviceBackend</c> in <c>native_device.h</c>.
        /// </remarks>
        public enum IoBackend : int
        {
            /// <summary>Platform default (libaio on Linux, ThreadPool on Windows).</summary>
            Default = 0,
            /// <summary>Linux libaio. Same as Default on Linux.</summary>
            Libaio = 1,
            /// <summary>Linux io_uring. Requires native lib built with FASTER_URING.</summary>
            Uring = 2,
        }

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_CreateWithBackend", CallingConvention = CallingConvention.Cdecl)]
        static extern IntPtr NativeDevice_CreateWithBackend(string file, bool enablePrivileges, bool unbuffered, bool delete_on_close, int backend, ulong segmentSizeBytes, bool omitSegmentIdFromFilename, int numIoContexts, int maxEvents);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_GetSegmentSize", CallingConvention = CallingConvention.Cdecl)]
        static extern ulong NativeDevice_GetSegmentSize(IntPtr device);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_AvailableBackends", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_AvailableBackends();

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_Destroy", CallingConvention = CallingConvention.Cdecl)]
        static extern void NativeDevice_Destroy(IntPtr device);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_sector_size", CallingConvention = CallingConvention.Cdecl)]
        static extern uint NativeDevice_sector_size(IntPtr device);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_ProbeAlignment", CallingConvention = CallingConvention.Cdecl)]
        static extern uint NativeDevice_ProbeAlignment(string filename);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_ReadAsync", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_ReadAsync(IntPtr device, ulong source, IntPtr dest, uint length, AsyncIOCallback callback, IntPtr context);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_WriteAsync", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_WriteAsync(IntPtr device, IntPtr source, ulong dest, uint length, AsyncIOCallback callback, IntPtr context);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_CreateDir", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_CreateDir(IntPtr device, string dir, int deleteExisting);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_TryComplete", CallingConvention = CallingConvention.Cdecl)]
        static extern bool NativeDevice_TryComplete(IntPtr device);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_QueueRun", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_QueueRun(IntPtr device, int timeout_secs);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_QueueRunFor", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_QueueRunFor(IntPtr device, int ctxIdx, int timeout_secs);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_WakeCompletionWorker", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_WakeCompletionWorker(IntPtr device, int ctxIdx);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_NumIoContexts", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_NumIoContexts(IntPtr device);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_GetFileSize", CallingConvention = CallingConvention.Cdecl)]
        static extern ulong NativeDevice_GetFileSize(IntPtr device, ulong segment);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_Reset", CallingConvention = CallingConvention.Cdecl)]
        static extern void NativeDevice_Reset(IntPtr device);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_RemoveSegment", CallingConvention = CallingConvention.Cdecl)]
        static extern void NativeDevice_RemoveSegment(IntPtr device, ulong segment);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_GetLastError", CallingConvention = CallingConvention.Cdecl)]
        static extern IntPtr NativeDevice_GetLastError();
        #endregion

        /// <summary>
        /// Reads the thread-local last-error message produced by the native device. The native
        /// side guarantees that, for synchronous P/Invoke calls, the returned pointer references
        /// storage on the calling managed thread, so it is safe to read here without holding
        /// onto the pointer. Returns an empty string when there is no error.
        /// </summary>
        static string GetNativeLastError()
        {
            try
            {
                var ptr = NativeDevice_GetLastError();
                if (ptr == IntPtr.Zero) return string.Empty;
                return Marshal.PtrToStringUTF8(ptr) ?? string.Empty;
            }
            catch (EntryPointNotFoundException)
            {
                // Older builds of the native library without NativeDevice_GetLastError exported.
                return string.Empty;
            }
        }

        /// <summary>
        /// Returns the native thread-local last-error formatted for appending to an exception
        /// message (": &lt;message&gt;"), or an empty string when the native side reported none.
        /// Must be called on the same managed thread that made the failing P/Invoke call, since the
        /// native last-error storage is thread-local.
        /// </summary>
        static string FormatNativeError()
        {
            var msg = GetNativeLastError();
            return string.IsNullOrEmpty(msg) ? string.Empty : $": {msg}";
        }

        readonly AsyncIOCallback _callbackDelegate;
        CancellationTokenSource completionThreadToken;
        Thread[] completionThreads;

        // Instrumentation: peak concurrent in-flight writes seen, and submit/complete counters.
        // Set TSAVORITE_DEVICE_INSTRUMENT=1 in the environment to enable.
        static readonly bool s_instrument = Environment.GetEnvironmentVariable("TSAVORITE_DEVICE_INSTRUMENT") == "1";
        int peakNumPending;
        long submitCount;
        long completeCount;
        long submitNanos;

        void _callback(IntPtr context, int errorCode, ulong numBytes)
        {
            if (s_instrument) Interlocked.Increment(ref completeCount);
            int offset = (int)context;
            var result = results[offset];
            // CRITICAL: this method is invoked via a function pointer from native code (libaio /
            // io_uring completion drainer thread) across the C ABI boundary. ANY managed
            // exception that escapes this method propagates back into the native dispatch
            // loop and, when it crosses the ABI boundary, causes the .NET runtime to
            // terminate the drainer thread (silently, since it's a background thread). That
            // leaves the device with no completion processor: all subsequent IOs are
            // submitted but never completed, numPending grows unbounded, device.Throttle()
            // stays true forever, and the next worker thread to call ReadAsync deadlocks
            // spinning in the throttle-wait loop.
            //
            // Tsavorite's user callback (AsyncGetFromDiskCallback) re-throws in several
            // error paths (no completionEvent set, exception during validation, etc.). So we
            // MUST catch absolutely everything here and never let it escape — even fatal
            // exceptions like OOM should be swallowed and the slot/bookkeeping cleaned up.
            // Errors that need surfacing should go through the result.callback's own error
            // channel (numBytes=0 + errorCode), not via a throw.
            //
            // try/finally also ensures that on a throwing user callback the result slot is
            // returned AND numPending is decremented. Dispose() spins until numPending == 0,
            // so decrementing here (after the callback returns) guarantees Dispose waits for
            // all in-flight user callbacks to finish before destroying the native device
            // underneath them.
            try
            {
                result.callback((uint)errorCode, (uint)numBytes, result.context);
            }
            catch (Exception ex)
            {
                logger?.LogCritical(ex, "Unhandled exception in user IO completion callback (suppressed to keep drainer alive)");
            }
            finally
            {
                freeResults.Enqueue(offset);
                Interlocked.Decrement(ref numPending);
            }
        }

        /// <summary>Diagnostic: snapshot and reset per-second submit/complete counters and peak in-flight.
        /// Set environment variable <c>TSAVORITE_DEVICE_INSTRUMENT=1</c> to enable population.</summary>
        public (int curPending, int peakPending, long submits, long completes, long submitNs) GetAndResetStats()
        {
            var stats = (numPending, peakNumPending, submitCount, completeCount, submitNanos);
            peakNumPending = numPending;
            submitCount = 0;
            completeCount = 0;
            submitNanos = 0;
            return stats;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Gates on <see cref="effectiveThrottleLimit"/>, the configured <see cref="StorageDeviceBase.ThrottleLimit"/>
        /// clamped to <see cref="MaxThrottle"/> and captured once at device creation. A configured throttle above
        /// that ceiling cannot be honored (the kernel submission ring is capped there — see <see cref="MaxThrottle"/>),
        /// so gating on the raw value would let the engine drive more in-flight reads than the ring can hold and push
        /// them into the submit-ring spin. The clamp is precomputed (not recomputed per call) because this runs in the
        /// engine's throttle-spin loop.
        /// <para>
        /// Before the native device is lazily created (cold start), <see cref="effectiveThrottleLimit"/> still holds
        /// its <see cref="MaxThrottle"/> seed, which would let a startup burst of concurrent submitters bypass the
        /// configured throttle and flood the just-sized ring. So until the handle exists we gate on the live clamped
        /// limit; the predictable branch is paid only on the cold path.
        /// </para>
        /// </remarks>
        public override bool Throttle()
            => numPending > (Volatile.Read(ref nativeDevice) == IntPtr.Zero
                ? Math.Min(ThrottleLimit, MaxThrottle)
                : effectiveThrottleLimit);

        /// <summary>
        /// Computes the per-context native kernel submission-ring depth from <see cref="StorageDeviceBase.ThrottleLimit"/>.
        /// The rings (one per io_context) must, in aggregate, hold the in-flight burst <see cref="Throttle"/> permits:
        /// if a ring is smaller than the load it sees, io_submit / io_uring_get_sqe spin in their ring-full backoff while
        /// holding a native epoch slot, and under enough concurrent submitters that starves the native epoch-slot table
        /// (the libaio hang this addresses). Submitters spread across the contexts by thread affinity, so each context
        /// handles ~<c>ThrottleLimit / numIoContexts</c> in steady state; the per-context depth is sized to that share
        /// (rounded up to a power of two) rather than the full throttle, so the total kernel capacity
        /// (<c>numIoContexts × depth</c>) stays bounded — libaio's io_setup draws from the shared <c>fs.aio-max-nr</c>
        /// budget, so applying the full throttle to every context could exhaust it with many completion threads. The
        /// <see cref="DefaultNativeRingDepth"/> floor adds headroom against uneven distribution (a brief ring-full is a
        /// non-fatal unwind-and-retry, not a hang); the cap is the kernel-safe maximum (see <see cref="MaxThrottle"/>).
        /// For the default single context this is <c>max(DefaultNativeRingDepth, NextPowerOf2(ThrottleLimit))</c>, clamped to
        /// <see cref="MaxThrottle"/>. Read at native-device creation time (first IO), by which point the factory has applied
        /// the configured throttle. Ignored by the Windows (IOCP) backend.
        /// </summary>
        int ComputeNativeRingDepth()
        {
            int throttle = ThrottleLimit;
            if (throttle <= 0)
                return DefaultNativeRingDepth;
            if (throttle > MaxThrottle)
            {
                // Don't silently ignore the configured throttle: surface that it exceeds the device's
                // maximum supported in-flight depth and is being clamped. The ceiling is the kernel-safe
                // submission-ring depth (io_uring max SQ entries / shared libaio fs.aio-max-nr budget), and
                // already exceeds practical NVMe queue depths, so raising it is rarely useful.
                logger?.LogWarning(
                    "NativeStorageDevice: ThrottleLimit ({throttle}) exceeds the device's maximum in-flight I/O depth ({max}); effective in-flight is capped at that maximum.",
                    throttle, MaxThrottle);
                throttle = MaxThrottle;
            }
            int contexts = numIoContextsConfig < 1 ? 1 : numIoContextsConfig;
            int perContext = (throttle + contexts - 1) / contexts;   // ceil(throttle / contexts)
            int depth = (int)Utility.NextPowerOf2(perContext);
            if (depth < DefaultNativeRingDepth)
                depth = DefaultNativeRingDepth;
            if (depth > MaxThrottle)
                depth = MaxThrottle;
            return depth;
        }

        /// <summary>
        /// Returns the set of IO backends that the currently-loaded native library was built
        /// with. Always includes <see cref="IoBackend.Default"/>; on Linux may also include
        /// <see cref="IoBackend.Uring"/> if the native lib was compiled with FASTER_URING.
        /// </summary>
        public static (bool defaultAvailable, bool uringAvailable) GetAvailableBackends()
        {
            int mask = NativeDevice_AvailableBackends();
            return ((mask & 1) != 0, (mask & 2) != 0);
        }

        /// <summary>
        /// Constructor with more options for derived classes.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The constructor only captures configuration; the underlying native device is created
        /// lazily on the first IO call via <see cref="EnsureNativeDeviceCreated"/>. This lets
        /// callers configure the segment size (if non-default) by calling <see cref="Initialize"/>
        /// in between construction and the first IO, without paying any cost for the native
        /// device creation up-front. Callers that do not call <see cref="Initialize"/> get the
        /// ctor defaults (unbounded single segment, equivalent to <c>Initialize(-1)</c>).
        /// </para>
        /// </remarks>
        /// <param name="filename">File name (or prefix) with path</param>
        /// <param name="deleteOnClose"></param>
        /// <param name="disableFileBuffering"></param>
        /// <param name="capacity">The maximum number of bytes this storage device can accommodate, or CAPACITY_UNSPECIFIED if there is no such limit </param>
        /// <param name="numCompletionThreads">Number of IO completion threads (drainers).
        /// Each drainer is bound 1:1 to its own io_context (libaio) or io_uring ring; the
        /// number of rings equals numCompletionThreads on both backends. Submitters distribute
        /// across rings via per-thread affinity. Ignored on Windows (IOCP). When &lt; 1,
        /// treated as 1.</param>
        /// <param name="ioBackend">IO backend to use (default platform backend, or explicit libaio / io_uring on Linux).</param>
        /// <param name="logger"></param>
        public NativeStorageDevice(string filename,
                                      bool deleteOnClose = false,
                                      bool disableFileBuffering = true,
                                      long capacity = Devices.CAPACITY_UNSPECIFIED,
                                      int numCompletionThreads = 1,
                                      IoBackend ioBackend = IoBackend.Default,
                                      ILogger logger = null)
                : base(filename, EnsureParentDirectoryAndProbeSectorSize(filename), capacity)
        {
            Debug.Assert(numCompletionThreads >= 1);

            // The 260-char MAX_PATH limit does not apply to extended-length paths (those prefixed with
            // "\\?\"); only enforce it for normal paths.
            if (!Native32.IsExtendedLengthPath(filename) && filename.Length > Native32.WIN32_MAX_PATH - 11)     // -11 to allow for ".<segment>"
                throw new TsavoriteException($"Path {filename} is too long");

            // Configuration is captured here; the native device handle (and its completion-drainer
            // thread, libaio / io_uring rings, etc.) is created lazily on the first IO call.
            this.filename = filename;
            this.deleteOnClose = deleteOnClose;
            this.disableFileBuffering = disableFileBuffering;
            this.numCompletionThreadsConfig = numCompletionThreads < 1 ? 1 : numCompletionThreads;
            // rings always track numCompletionThreads (1:1 drainer-to-ring binding). Each
            // drainer blocks on its own ring inside QueueRunFor with a timeout, so a single
            // drainer cannot cover multiple rings without starving any ring whose submitters
            // produce completions while the drainer is parked on another ring. With per-thread
            // submit affinity (pick_ring's thread_local index), every ring eventually receives
            // submissions, so each ring must have its own drainer. For throughput scaling,
            // callers should set numCompletionThreads >= expected submitter concurrency.
            this.numIoContextsConfig = this.numCompletionThreadsConfig;
            this.ioBackendConfig = ioBackend;
            this.logger = logger;

            ThrottleLimit = 120;
            _callbackDelegate = _callback;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Validates that segmentSize is a positive power of two and at least the device sector
        /// size — the native side enforces the same invariant when it later creates the device,
        /// but failing fast in managed code keeps the error message close to the caller. Like
        /// the base implementation this is purely a configuration call (the ctor already
        /// establishes valid defaults); the underlying native handle is created lazily on the
        /// first IO via <see cref="EnsureNativeDeviceCreated"/> using the final
        /// <c>base.segmentSizeBits</c>, so subsequent calls that change the segment size are
        /// honoured as long as no IO has flowed yet.
        /// <para>
        /// Passing <c>segmentSize = -1</c> selects unbounded single-segment mode: the native
        /// shim is asked to use <see cref="UnboundedNativeSegmentSizeBytes"/> (1&lt;&lt;63) so
        /// every non-negative upper-layer address routes to segment 0 in both the C++ and
        /// managed bit-shift math, and the on-disk layout is a single segment file
        /// (<c>&lt;basename&gt;.0</c>) that grows on demand. When combined with
        /// <paramref name="omitSegmentIdFromFilename"/> = true, the file is named
        /// just <c>&lt;basename&gt;</c> (no segment suffix) — only allowed with
        /// <paramref name="segmentSize"/> = -1, matching the managed devices' behaviour.
        /// </para>
        /// </remarks>
        public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            // Metadata only — matches LocalStorageDevice / RandomAccessLocalStorageDevice. The
            // native handle is created lazily on first IO via EnsureNativeDeviceCreated() using
            // the current base.segmentSizeBits, so repeated calls before the first IO end up
            // creating a native device with the most-recently-requested segment size.
            if (omitSegmentIdFromFilename && segmentSize != -1)
                throw new TsavoriteException("omitSegmentIdFromFilename requires segmentSize = -1 (single unbounded segment); multiple segments would all map to the same on-disk path and clobber each other.");
            if (segmentSize != -1)
            {
                if (segmentSize <= 0 || (segmentSize & (segmentSize - 1)) != 0)
                    throw new TsavoriteException($"Native device segment size must be a positive power of two (or -1 for unbounded); got {segmentSize}.");
                if (segmentSize < SectorSize)
                    throw new TsavoriteException($"Segment size {segmentSize} must be at least the device sector size {SectorSize}.");
            }
            base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
        }

        readonly object nativeCreateLock = new();

        /// <summary>
        /// Lazily creates the native device, spawns completion-drainer threads, and runs the
        /// startup ABI / segment-size / sector-size cross-checks. Uses
        /// <c>segmentSize</c> as the requested segment size — callers may override
        /// the default (-1, unbounded single segment) by calling <see cref="Initialize"/>
        /// before the first IO. Idempotent: subsequent calls are a single non-locking read once
        /// the native handle exists. Thread-safe via double-checked locking. Throws if the
        /// device has been disposed or if the native shim rejects the configuration.
        /// </summary>
        void EnsureNativeDeviceCreated()
        {
            // Pair Volatile.Read here with Volatile.Write below (line ~764). The fast path
            // skips the lock; without an acquire barrier on ARM the reader can observe a
            // non-null nativeDevice while still seeing a stale `results` array reference or
            // partially-initialised completion-thread state. Volatile.Read on weak-memory
            // hosts costs a single ldar instruction, no cost on x86 (plain mov).
            if (Volatile.Read(ref nativeDevice) != IntPtr.Zero) return;
            if (Volatile.Read(ref disposedFlag) != 0)
                throw new ObjectDisposedException(nameof(NativeStorageDevice));
            lock (nativeCreateLock)
            {
                // Inside the lock the acquire fence guarantees we see writes from the prior
                // owner, so a plain field read is safe here.
                if (nativeDevice != IntPtr.Zero) return;
                if (Volatile.Read(ref disposedFlag) != 0)
                    throw new ObjectDisposedException(nameof(NativeStorageDevice));

                ulong sizeForNative = segmentSize == -1
                    ? UnboundedNativeSegmentSizeBytes
                    : (ulong)segmentSize;

                nativeSegmentSizeBytes = sizeForNative;

                // Capture the effective in-flight throttle once, here, where the ring depth is also fixed —
                // ThrottleLimit has been applied by the factory before the first IO. Throttle() then reads this
                // field directly instead of re-clamping on every spin-loop iteration.
                effectiveThrottleLimit = Math.Min(ThrottleLimit, MaxThrottle);

                var newDevice = NativeDevice_CreateWithBackend(filename, false, disableFileBuffering, deleteOnClose, (int)ioBackendConfig, sizeForNative, OmitSegmentIdFromFileName, numIoContextsConfig, ComputeNativeRingDepth());
                if (newDevice == IntPtr.Zero)
                {
                    var nativeMessage = GetNativeLastError();
                    var available = GetAvailableBackends();
                    var detail = string.IsNullOrEmpty(nativeMessage)
                        ? $"Requested IO backend '{ioBackendConfig}' is not available in the loaded native_device library."
                        : $"Native device initialization failed: {nativeMessage}";
                    throw new TsavoriteException(
                        $"{detail} " +
                        $"Available backends: default={available.defaultAvailable}, io_uring={available.uringAvailable}. " +
                        (ioBackendConfig == IoBackend.Uring
                            ? "The io_uring backend requires liburing.so.2 to be present at process start. " +
                              "Install it (Debian/Ubuntu: 'sudo apt-get install -y liburing2'; Fedora/RHEL: 'sudo dnf install -y liburing') and restart the process. " +
                              "Alpine (musl) is not supported by the prebuilt native library — use a glibc-based image or fall back to a managed device. " +
                              "The libaio backend (selected with IoBackend.Default / IoBackend.Libaio) is always available and does not require liburing."
                            : "Verify the native library matches the requested backend."));
                }

                ulong actualSegmentSize = NativeDevice_GetSegmentSize(newDevice);
                if (actualSegmentSize != sizeForNative)
                {
                    NativeDevice_Destroy(newDevice);
                    throw new TsavoriteException(
                        $"Native device segment size mismatch: requested {sizeForNative}, native returned {actualSegmentSize}. " +
                        "This indicates an ABI mismatch between the loaded native_device library and the managed wrapper. " +
                        "Ensure libnative_device.so matches the current build.");
                }

                uint nativeSectorSize = NativeDevice_sector_size(newDevice);
                if (nativeSectorSize != SectorSize)
                {
                    // Both sides (managed probe in EnsureParentDirectoryAndProbeSectorSize,
                    // native probe in NativeDeviceImpl's field initializer) go through the
                    // same ProbeDioAlignment routine on the same filename with the parent
                    // directory pre-materialised, so the two values are guaranteed to agree
                    // on every well-formed host. A drift here is a real ABI / loaded-library
                    // mismatch — e.g. the shipped libnative_device.so was rebuilt from a
                    // different branch than the managed wrapper, or the host kernel changed
                    // STATX_DIOALIGN semantics between the two calls. Hard-fail so it is
                    // caught at the first I/O rather than silently mis-aligning every write.
                    NativeDevice_Destroy(newDevice);
                    throw new TsavoriteException(
                        $"Native device sector-size mismatch on '{filename}': managed wrapper probed {SectorSize} bytes but the kernel reports {nativeSectorSize} bytes for the actual file. " +
                        "The most likely cause is a stale libnative_device.so or a managed/native version skew. " +
                        "Rebuild the native library from this branch (libs/storage/Tsavorite/cc) and reinstall the resulting binary into libs/storage/Tsavorite/cs/src/core/Device/runtimes/<rid>/native/.");
                }

                if (results == null) results = new NativeResult[MaxResults];

                if (NativeDevice_QueueRun(newDevice, 0) >= 0)
                {
                    try
                    {
                        _ = NativeDevice_NumIoContexts(newDevice);
                        _ = NativeDevice_QueueRunFor(newDevice, 0, 0);
                    }
                    catch (EntryPointNotFoundException ex)
                    {
                        NativeDevice_Destroy(newDevice);
                        throw new TsavoriteException(
                            "Loaded libnative_device.so/dll is missing the sharded-ABI exports " +
                            "NativeDevice_NumIoContexts / NativeDevice_QueueRunFor. The shared library " +
                            "predates the multi-io-context change and must be rebuilt from this branch " +
                            "(libs/storage/Tsavorite/cc) and the resulting binary installed to " +
                            "libs/storage/Tsavorite/cs/src/core/Device/runtimes/<rid>/native/.", ex);
                    }

                    completionThreadToken = new();
                    int actualIoContexts = NativeDevice_NumIoContexts(newDevice);
                    if (actualIoContexts < 1) actualIoContexts = 1;
                    // We pass numCompletionThreadsConfig to the native ctor as num_io_contexts;
                    // the native side may clamp at 1 if it received 0 or negative, but otherwise
                    // honors it. So actualIoContexts should equal numCompletionThreadsConfig. Each
                    // drainer is bound 1:1 to its own ring via QueueRunFor(ctxIdx, ...).
                    completionThreads = new Thread[actualIoContexts];
                    for (int i = 0; i < actualIoContexts; i++)
                    {
                        int ctxIdx = i;
                        completionThreads[i] = new Thread(() => CompletionWorker(ctxIdx))
                        {
                            IsBackground = true
                        };
                        completionThreads[i].Start();
                    }
                }

                // Publish last: a reader observing nativeDevice != IntPtr.Zero is guaranteed to
                // see a fully-initialised handle with completion threads already running.
                Volatile.Write(ref nativeDevice, newDevice);
            }
        }

        /// <inheritdoc />
        /// <remarks>
        /// Mirrors the contract used by <see cref="LocalStorageDevice"/> and
        /// <see cref="ManagedLocalStorageDevice"/>: closes all open segment handles and forgets
        /// them. Subsequent writes lazily reopen segments via the native
        /// <c>FileSystemSegmentedFile::OpenSegment</c> path, so the device remains usable.
        /// </remarks>
        public override void Reset()
        {
            if (Volatile.Read(ref disposedFlag) != 0) return;
            // Lease the native handle (same protocol as ReadAsync/WriteAsync) so a concurrent
            // Dispose() cannot drain numPending to its int.MinValue poison and free the handle
            // while we are inside the native call. A <= 0 result means Dispose already poisoned;
            // restore it and no-op.
            if (Interlocked.Increment(ref numPending) <= 0)
            {
                Interlocked.Decrement(ref numPending);
                return;
            }
            try
            {
                // No-op if the native device has not been created yet (no handles to reset).
                var dev = Volatile.Read(ref nativeDevice);
                if (dev != IntPtr.Zero)
                    NativeDevice_Reset(dev);
            }
            finally
            {
                Interlocked.Decrement(ref numPending);
            }
        }

        /// <summary>
        /// Asserts that an I/O request is properly aligned for the underlying O_DIRECT / aligned
        /// path. The libaio and io_uring submission paths require that the file offset, the
        /// transfer length, and the user buffer pointer all be multiples of the device's sector
        /// size; misaligned requests fail with EINVAL at completion time.
        /// </summary>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private void ThrowIfMisaligned(ulong offset, uint length, IntPtr buffer, string op)
        {
            uint mask = SectorSize - 1;
            if ((offset & mask) != 0 || (length & mask) != 0 || ((ulong)buffer.ToInt64() & mask) != 0)
                ThrowMisaligned(offset, length, buffer, op);
        }

        // Cold path; NoInlining keeps the AggressiveInlining guard small.
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
        private void ThrowMisaligned(ulong offset, uint length, IntPtr buffer, string op)
        {
            throw new IOException(
                $"NativeStorageDevice.{op}: misaligned I/O — sector size is {SectorSize}, but " +
                $"offset=0x{offset:X16}, length={length}, buffer=0x{buffer.ToInt64():X16}. " +
                "All three values must be a multiple of the device sector size for the " +
                "O_DIRECT / libaio / io_uring path. This usually indicates an upper-layer " +
                "staging buffer was allocated with the wrong alignment or a flush boundary " +
                "is not on a sector multiple.");
        }

        /// <inheritdoc />
        public override void ReadAsync(int segmentId, ulong sourceAddress,
                                     IntPtr destinationAddress,
                                     uint readLength,
                                     DeviceIOCompletionCallback callback,
                                     object context)
        {
            if (Volatile.Read(ref disposedFlag) != 0)
                throw new ObjectDisposedException(nameof(NativeStorageDevice));
            EnsureNativeDeviceCreated();

            // The libaio/io_uring path requires O_DIRECT-aligned offset, length, AND buffer.
            // Misalignment in release builds would otherwise produce a cryptic EINVAL from the
            // kernel (read returns -EINVAL via the completion callback), or in debug builds
            // hit an assert in DCHECK_ALIGNMENT inside file_linux.cc. Three predicated AND
            // operations is negligible vs the syscall itself.
            ThrowIfMisaligned(sourceAddress, readLength, destinationAddress, nameof(ReadAsync));

            int offset;
            while (!freeResults.TryDequeue(out offset))
            {
                if (resultOffset < MaxResults)
                {
                    offset = Interlocked.Increment(ref resultOffset) - 1;
                    if (offset < MaxResults) break;
                }
                Thread.Yield();
            }
            ref var result = ref results[offset];
            result.context = context;
            result.callback = callback;

            try
            {
                if (Interlocked.Increment(ref numPending) <= 0)
                    throw new Exception("Cannot operate on disposed device");
                int _result = NativeDevice_ReadAsync(nativeDevice, ((ulong)segmentId << segmentSizeBits) | sourceAddress, destinationAddress, readLength, _callbackDelegate, (IntPtr)offset);

                if (_result != 0)
                    throw new IOException($"Error reading from log file (status {_result}){FormatNativeError()}", _result);
            }
            catch (IOException e)
            {
                logger?.LogCritical(e, $"{nameof(ReadAsync)}");
                try
                {
                    callback((uint)(e.HResult & 0x0000FFFF), 0, context);
                }
                finally
                {
                    freeResults.Enqueue(offset);
                    Interlocked.Decrement(ref numPending);
                }
            }
            catch (Exception e)
            {
                logger?.LogCritical(e, $"{nameof(ReadAsync)}");
                try
                {
                    callback(uint.MaxValue, 0, context);
                }
                finally
                {
                    freeResults.Enqueue(offset);
                    Interlocked.Decrement(ref numPending);
                }
            }
        }

        /// <inheritdoc />
        public override unsafe void WriteAsync(IntPtr sourceAddress,
                                      int segmentId,
                                      ulong destinationAddress,
                                      uint numBytesToWrite,
                                      DeviceIOCompletionCallback callback,
                                      object context)
        {
            if (Volatile.Read(ref disposedFlag) != 0)
                throw new ObjectDisposedException(nameof(NativeStorageDevice));
            EnsureNativeDeviceCreated();

            // Same rationale as ReadAsync — see the comment there. Kernel rejects misaligned
            // O_DIRECT writes with EINVAL; we want to surface this in managed code with the
            // actual offsets/lengths/buffer pointers visible so the caller can diagnose
            // whichever upper-layer staging buffer is misaligned.
            ThrowIfMisaligned(destinationAddress, numBytesToWrite, sourceAddress, nameof(WriteAsync));

            int offset;
            while (!freeResults.TryDequeue(out offset))
            {
                if (resultOffset < MaxResults)
                {
                    offset = Interlocked.Increment(ref resultOffset) - 1;
                    if (offset < MaxResults) break;
                }
                Thread.Yield();
            }
            ref var result = ref results[offset];
            result.context = context;
            result.callback = callback;

            try
            {
                var newPending = Interlocked.Increment(ref numPending);
                if (newPending <= 0)
                    throw new Exception("Cannot operate on disposed device");
                if (s_instrument)
                {
                    Interlocked.Increment(ref submitCount);
                    var prevPeak = peakNumPending;
                    while (newPending > prevPeak)
                    {
                        var actual = Interlocked.CompareExchange(ref peakNumPending, newPending, prevPeak);
                        if (actual == prevPeak) break;
                        prevPeak = actual;
                    }
                }
                long ts0 = s_instrument ? Stopwatch.GetTimestamp() : 0;
                int _result = NativeDevice_WriteAsync(nativeDevice, sourceAddress, ((ulong)segmentId << segmentSizeBits) | destinationAddress, numBytesToWrite, _callbackDelegate, (IntPtr)offset);
                if (s_instrument)
                {
                    var elapsed = Stopwatch.GetTimestamp() - ts0;
                    Interlocked.Add(ref submitNanos, (long)(elapsed * 1_000_000_000.0 / Stopwatch.Frequency));
                }

                if (_result != 0)
                {
                    throw new IOException($"Error writing to log file (status {_result}){FormatNativeError()}", _result);
                }
            }
            catch (IOException e)
            {
                logger?.LogCritical(e, $"{nameof(WriteAsync)}");
                try
                {
                    callback((uint)(e.HResult & 0x0000FFFF), 0, context);
                }
                finally
                {
                    freeResults.Enqueue(offset);
                    Interlocked.Decrement(ref numPending);
                }
            }
            catch (Exception e)
            {
                logger?.LogCritical(e, $"{nameof(WriteAsync)}");
                try
                {
                    callback(uint.MaxValue, 0, context);
                }
                finally
                {
                    freeResults.Enqueue(offset);
                    Interlocked.Decrement(ref numPending);
                }
            }
        }

        /// <summary>
        /// <see cref="IDevice.RemoveSegment(int)"/>
        /// </summary>
        /// <param name="segment"></param>
        public override void RemoveSegment(int segment)
        {
            if (Volatile.Read(ref disposedFlag) != 0) return;
            // Lease the native handle so a concurrent Dispose() can't free it mid-call.
            if (Interlocked.Increment(ref numPending) <= 0)
            {
                Interlocked.Decrement(ref numPending);
                return;
            }
            try
            {
                var dev = Volatile.Read(ref nativeDevice);
                if (dev != IntPtr.Zero)
                {
                    // Native owns the open handle; let it close+unlink.
                    NativeDevice_RemoveSegment(dev, (ulong)segment);
                    return;
                }
            }
            finally
            {
                Interlocked.Decrement(ref numPending);
            }
            // No native handle yet — delete the on-disk segment file directly so callers
            // observe the same semantics as LocalStorageDevice / RandomAccessLocalStorageDevice
            // (best-effort unlink that ignores ENOENT).
            try { File.Delete(GetSegmentName(segment)); }
            catch { }
        }

        /// <summary>
        /// <see cref="IDevice.RemoveSegmentAsync(int, AsyncCallback, IAsyncResult)"/>
        /// </summary>
        /// <param name="segment"></param>
        /// <param name="callback"></param>
        /// <param name="result"></param>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            RemoveSegment(segment);
            callback(result);
        }

        /// <summary>
        /// Close device. Shutdown ordering matters: any in-flight IOs must complete first so the
        /// numPending CAS terminates; the completion threads must exit BEFORE we destroy the native
        /// device, otherwise they can dereference a freed io_uring/libaio ring inside
        /// <see cref="NativeDevice_QueueRun"/>.
        /// </summary>
        /// <remarks>
        /// <para>Idempotent — multiple calls are safe; only the first does work.</para>
        /// <para>
        /// User IO callbacks fire on completion-worker threads. Dispose() cannot run on one of
        /// those threads, because joining the caller would deadlock — we detect and throw
        /// <see cref="InvalidOperationException"/> in that case.
        /// </para>
        /// <para>
        /// Worst-case shutdown stall is bounded by the duration of the longest in-flight user
        /// callback: blocked completion drainers are woken immediately by
        /// <see cref="NativeDevice_WakeCompletionWorker"/> rather than waiting on
        /// <see cref="CompletionWorkerTimeoutSecs"/> to fire. If callbacks are slow,
        /// Dispose() waits for them.
        /// </para>
        /// </remarks>
        public override void Dispose()
        {
            // Self-join deadlock guard MUST run before we touch disposedFlag: if a user IO
            // callback (running on a completion thread) calls Dispose(), joining the caller would
            // deadlock. We surface this as InvalidOperationException so disposedFlag stays 0 and
            // a subsequent Dispose() from a different thread can still proceed.
            if (completionThreads != null)
            {
                var self = Thread.CurrentThread;
                foreach (var t in completionThreads)
                {
                    if (ReferenceEquals(t, self))
                    {
                        throw new InvalidOperationException(
                            "NativeStorageDevice.Dispose() called from an IO completion thread. "
                            + "User callbacks must not dispose the device synchronously; "
                            + "post the disposal to a separate thread.");
                    }
                }
            }

            // Idempotent: second and subsequent calls short-circuit. Setting the flag here gates
            // the late P/Invoke entry points (TryComplete, GetFileSize, Reset, RemoveSegment) on
            // their first line; the numPending lease they then take closes the race where this
            // drain poisons and frees the handle between that check and the native call.
            if (Interlocked.Exchange(ref disposedFlag, 1) != 0)
                return;

            // Drain in-flight ops by poisoning numPending to int.MinValue once it hits 0. Submit
            // paths fail their Interlocked.Increment(numPending) <= 0 check and route through the
            // error callback; the _callback decrement in the success path runs in `finally` after
            // the user callback, so by the time we observe numPending == 0 all completions are done.
            while (numPending >= 0)
            {
                Interlocked.CompareExchange(ref numPending, int.MinValue, 0);
                Thread.Yield();
            }

            // Cancel and Join every completion thread, then destroy the native device.
            // Take nativeCreateLock so a concurrent EnsureNativeDeviceCreated cannot publish a
            // brand-new native handle after we have already torn down (which would leak it).
            lock (nativeCreateLock)
            {
                if (completionThreads != null)
                {
                    completionThreadToken.Cancel();
                    // Wake every blocked completion drainer by submitting a no-op IO to each
                    // io_context. The drainer is otherwise sleeping in NativeDevice_QueueRunFor
                    // waiting for completion events; the wake-up causes the syscall to return
                    // promptly so the cancellation token can be observed on the next loop
                    // iteration. Best-effort: on submit failure the drainer still wakes when
                    // its QueueRunFor timeout fires.
                    for (int i = 0; i < completionThreads.Length; i++)
                        _ = NativeDevice_WakeCompletionWorker(nativeDevice, i);
                    foreach (var t in completionThreads) t.Join();
                    completionThreadToken.Dispose();
                    completionThreads = null;
                }

                var dev = Interlocked.Exchange(ref nativeDevice, IntPtr.Zero);
                if (dev != IntPtr.Zero)
                {
                    NativeDevice_Destroy(dev);
                    // NativeDevice_Destroy runs log_.Close() under the C ABI firewall; if that threw
                    // it was caught and recorded rather than crashing teardown. Surface it so a
                    // teardown failure is reported instead of looking like "the server hung".
                    var err = GetNativeLastError();
                    if (!string.IsNullOrEmpty(err))
                        logger?.LogError("NativeStorageDevice native teardown reported an error: {error}", err);
                }
            }
        }

        /// <inheritdoc/>
        public override bool TryComplete()
        {
            if (Volatile.Read(ref disposedFlag) != 0) return false;
            // Lease the native handle so a concurrent Dispose() can't free it mid-call. The
            // disposedFlag check above rejects the common post-dispose case without the interlocked
            // cost; the lease closes the remaining race where Dispose poisons numPending between
            // that check and the native call.
            if (Interlocked.Increment(ref numPending) <= 0)
            {
                Interlocked.Decrement(ref numPending);
                return false;
            }
            try
            {
                var dev = Volatile.Read(ref nativeDevice);
                return dev != IntPtr.Zero && NativeDevice_TryComplete(dev);
            }
            finally
            {
                Interlocked.Decrement(ref numPending);
            }
        }

        /// <inheritdoc/>
        public override long GetFileSize(int segment)
        {
            if (Volatile.Read(ref disposedFlag) != 0) return 0;
            // Lease the native handle so a concurrent Dispose() can't free it mid-call.
            if (Interlocked.Increment(ref numPending) > 0)
            {
                try
                {
                    var dev = Volatile.Read(ref nativeDevice);
                    if (dev != IntPtr.Zero)
                        return (long)NativeDevice_GetFileSize(dev, (ulong)segment);
                }
                finally
                {
                    Interlocked.Decrement(ref numPending);
                }
            }
            else
            {
                Interlocked.Decrement(ref numPending);
            }
            // No native handle yet (or disposed) — stat the on-disk segment file directly. Matches
            // LocalStorageDevice / RandomAccessLocalStorageDevice semantics where size is
            // observable before any IO has flowed through the device. Returns 0 for missing
            // files (the cluster manager and checkpoint-recovery code rely on this to decide
            // whether to recover persisted config without first opening the device).
            try
            {
                var fi = new FileInfo(GetSegmentName(segment));
                return fi.Exists ? fi.Length : 0;
            }
            catch
            {
                return 0;
            }
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="segmentId"></param>
        /// <returns></returns>
        protected string GetSegmentName(int segmentId) => GetSegmentFilename(FileName, segmentId);

        /// <summary>
        /// Cold-path probe of the kernel's required direct-I/O alignment for the target file.
        /// Called from the ctor (via base.ctor) before the native device is created;
        /// StorageDeviceBase.SectorSize is set in the base ctor and immutable thereafter.
        /// </summary>
        /// <remarks>
        /// Never throws: on any failure returns <see cref="MinSectorSize"/> (512). A stale or
        /// wrong probe is caught when <see cref="EnsureNativeDeviceCreated"/> later cross-checks
        /// the managed SectorSize against the value the native shim reports for the actual file.
        /// </remarks>
        private static uint GetSectorSize(string filename)
        {
            // The probe is a no-op on Windows (returns MinSectorSize) because the
            // libnative_device.so / .dll on Windows uses the ThreadPool backend and the
            // file_windows.cc path already queries the actual sector size via
            // GetDiskFreeSpace. Linux is where the 4K-native distinction matters.
            try
            {
                // The probe walks up to the nearest existing ancestor, so it's safe to call
                // even when `filename` itself doesn't exist yet (which is the common case at
                // startup before any segment has been written).
                uint probed = NativeDevice_ProbeAlignment(filename);
                if (probed >= MinSectorSize && (probed & (probed - 1)) == 0)
                    return probed;
            }
            catch (DllNotFoundException) { }
            catch (EntryPointNotFoundException) { }
            return MinSectorSize;
        }

        /// <summary>
        /// Materializes the parent directory of <paramref name="filename"/> (if missing) and
        /// then probes the kernel's required direct-I/O alignment for the eventual data
        /// file. Returns the value once and for all — the result is passed to the
        /// <see cref="StorageDeviceBase"/> ctor argument and the resulting
        /// <see cref="IDevice.SectorSize"/> is immutable for the lifetime of the device.
        /// </summary>
        /// <remarks>
        /// The probe is deterministic by construction:
        ///   1. <see cref="Directory.CreateDirectory(string)"/> ensures the parent exists on
        ///      the target filesystem before the probe runs.
        ///   2. The probe call is given the parent directory's path (not the not-yet-existing
        ///      data file), so <c>stat()</c> succeeds on the first try and never walks up to
        ///      a grandparent on a different filesystem.
        /// Together these collapse both the managed-side probe and the later native-side
        /// probe (run from the <c>NativeDeviceImpl</c> field initializer with the same
        /// parent dir present) onto the same kernel STATX_DIOALIGN / sysfs queue-block-size
        /// value, so the cross-check in <see cref="EnsureNativeDeviceCreated"/> is a real
        /// ABI / loaded-library drift detector with no path-resolution false positives.
        /// </remarks>
        private static uint EnsureParentDirectoryAndProbeSectorSize(string filename)
            => GetSectorSize(MaterializeParentDirForProbe(filename));

        /// <summary>
        /// Creates <paramref name="filename"/>'s parent directory (if missing) and returns it as
        /// the probe path — probing the existing parent (not the lazily-created data file) keeps
        /// the probe on the target filesystem.
        /// </summary>
        private static string MaterializeParentDirForProbe(string filename)
        {
            try
            {
                var parent = new FileInfo(filename).Directory?.FullName;
                if (!string.IsNullOrEmpty(parent))
                {
                    Directory.CreateDirectory(parent);
                    return parent;
                }
            }
            catch
            {
                // Not fatal — fall back to the filename; the probe walks up to an ancestor.
            }
            return filename;
        }

        /// <summary>
        /// Required O_DIRECT alignment shared by all local-disk devices (native, RandomAccess,
        /// Managed, Local) so they agree per host. Power of two &gt;= <see cref="IDevice.MinDeviceSectorSize"/>.
        /// </summary>
        /// <remarks>
        /// Linux: the native probe (<c>NativeDevice_ProbeAlignment</c>), falling back to
        /// <see cref="Native32.GetDeviceSectorSize"/> (512) when the native library can't load
        /// (e.g. musl). Windows: <see cref="Native32.GetDeviceSectorSize"/> (GetDiskFreeSpace
        /// logical), taking no native-library dependency.
        /// </remarks>
        internal static uint ProbeSectorSize(string filename)
        {
            var probePath = MaterializeParentDirForProbe(filename);

            uint result = 0;
            if (OperatingSystem.IsLinux())
            {
                try
                {
                    uint probed = NativeDevice_ProbeAlignment(probePath);
                    if (probed >= MinSectorSize && (probed & (probed - 1)) == 0)
                        result = probed;
                }
                catch (DllNotFoundException) { }
                catch (EntryPointNotFoundException) { }
                // A present-but-broken native lib (wrong-arch/corrupt) throws these; managed
                // devices don't require it, so fall back to the managed probe instead of failing.
                catch (BadImageFormatException) { }
                catch (FileLoadException) { }
            }

            if (result == 0)
                result = Native32.GetDeviceSectorSize(probePath);

            // Final guard: power-of-two floor at MinDeviceSectorSize.
            if (result < MinSectorSize || (result & (result - 1)) != 0)
                result = MinSectorSize;
            return result;
        }

        /// <summary>
        /// Drain loop for one completion thread, bound 1:1 to ring shard <paramref name="ctxIdx"/>.
        /// Blocks in <c>NativeDevice_QueueRunFor</c> with a long timeout. Dispose() wakes blocked
        /// workers via <c>NativeDevice_WakeCompletionWorker</c> rather than relying on the
        /// timeout to fire.
        /// </summary>
        void CompletionWorker(int ctxIdx)
        {
            // Defense-in-depth: catch around the whole drain loop. _callback already swallows
            // all exceptions from the user callback (see its big comment), but if anything
            // else managed-side throws here (e.g. nativeDevice goes IntPtr.Zero mid-call
            // during a race with Dispose, or a P/Invoke marshalling exception), losing the
            // drainer thread silently is catastrophic: no completions ever fire, numPending
            // grows unbounded, the next submitter spins forever in device.Throttle() and the
            // whole engine deadlocks. So if anything escapes, log it loudly and exit cleanly.
            try
            {
                while (true)
                {
                    if (completionThreadToken.IsCancellationRequested) break;
                    int rc = NativeDevice_QueueRunFor(nativeDevice, ctxIdx, CompletionWorkerTimeoutSecs);
                    if (rc == NativeCABIExceptionSentinel)
                    {
                        // The native drain threw and was firewalled by the C ABI guard (instead of
                        // unwinding across P/Invoke and terminating the process). Surface the message
                        // so it can be reported, then pause briefly to avoid a hot error loop if the
                        // fault is persistent. The drainer keeps running so Dispose can still proceed.
                        logger?.LogError("NativeStorageDevice completion drainer (ctxIdx={ctxIdx}) hit a native exception: {error}", ctxIdx, GetNativeLastError());
                        Thread.Sleep(10);
                    }
                    Thread.Yield();
                }
            }
            catch (Exception ex)
            {
                logger?.LogCritical(ex, "NativeStorageDevice completion drainer (ctxIdx={ctxIdx}) terminated by unhandled exception", ctxIdx);
            }
        }

        // Per-iteration timeout for completion workers. Long enough that the idle syscall rate is
        // negligible; Dispose() does not rely on this firing because it submits a synthetic wake-up
        // event via NativeDevice_WakeCompletionWorker to unblock the worker immediately.
        const int CompletionWorkerTimeoutSecs = 1;
    }
}