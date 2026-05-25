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
        const int MaxResults = 1 << 12;

        /// <summary>
        /// Floor sector size used when the alignment probe fails (e.g., parent directory
        /// doesn't exist yet, or running on a kernel/filesystem combination where neither
        /// statx STATX_DIOALIGN nor statvfs.f_frsize reports anything useful). Matches the
        /// historical Garnet assumption that every other Linux device hardcodes; in the
        /// 4K-native disk case the probe overrides this with 4096.
        /// </summary>
        const uint MinSectorSize = 512;

        readonly ConcurrentQueue<int> freeResults = new();
        readonly ILogger logger;
        NativeResult[] results;

        /// <summary>
        /// Number of pending reads on device
        /// </summary>
        int numPending = 0;

        int resultOffset;

        /// <summary>
        /// Configuration captured at construction time; native device creation is DEFERRED until
        /// <see cref="Initialize"/> because the segment size only becomes available at that
        /// point. All four fields are immutable after the constructor returns.
        /// </summary>
        readonly string filename;
        readonly bool deleteOnClose;
        readonly bool disableFileBuffering;
        readonly int numCompletionThreadsConfig;
        readonly IoBackend ioBackendConfig;

        /// <summary>
        /// Runtime segment size in bytes that we asked the native shim to use. Set by
        /// <see cref="Initialize"/>. When the user requested <c>segmentSize = -1</c> (unbounded
        /// single segment) this is <see cref="UnboundedNativeSegmentSizeBytes"/>, large enough
        /// that any non-negative <c>long</c> upper-layer address routes to segment 0 under the
        /// native shim's <c>shift = log2(segment_size_bytes)</c> math. Used only for
        /// diagnostics/assertions on the C# side; the authoritative value lives inside the
        /// native device.
        /// </summary>
        ulong nativeSegmentSizeBytes;

        /// <summary>
        /// Native-side segment size used to represent unbounded single-segment mode
        /// (corresponds to <c>Initialize(segmentSize: -1)</c>). 1&lt;&lt;63 = 9.2 EiB; any
        /// non-negative <c>long</c> address is below this and so shifts to segment 0 inside the
        /// native <c>FileSystemSegmentedFile</c>. The C# managed side still uses
        /// <c>segmentSizeBits = 64</c> / <c>segmentSizeMask = ~0</c> for its own address math,
        /// so segment IDs are always 0 in this mode on both sides.
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

        static NativeStorageDevice()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                NativeLibraryPath = "runtimes/win-x64/native/native_device.dll";
            else
                NativeLibraryPath = "runtimes/linux-x64/native/libnative_device.so";
            NativeLibrary.SetDllImportResolver(typeof(NativeStorageDevice).Assembly, ImportResolver);
        }

        static IntPtr ImportResolver(string libraryName, Assembly assembly, DllImportSearchPath? searchPath)
        {
            if (libraryName != NativeLibraryName || NativeLibraryPath == null)
                return IntPtr.Zero;

            var resolvedPath = ResolveNativeLibraryPath(assembly);

            try
            {
                return NativeLibrary.Load(resolvedPath);
            }
            catch (DllNotFoundException ex) when (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)
                                                  && ex.Message.Contains("libaio.so.1", StringComparison.Ordinal))
            {
                // Debian 13 (trixie) / Ubuntu 24.04 (noble) renamed libaio1 to libaio1t64 as part of the
                // 64-bit time_t ABI transition. The package now ships libaio.so.1t64 (SONAME "libaio.so.1t64"),
                // which does NOT satisfy our DT_NEEDED of "libaio.so.1". Try to repair by dropping a
                // libaio.so.1 -> libaio.so.1t64 symlink next to libnative_device.so; this works because the
                // native library is built with RPATH=$ORIGIN.
                if (TryCreateLibaioCompatSymlink(resolvedPath, out var symlinkedPath))
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

                throw new DllNotFoundException(BuildLibaioDiagnostic(symlinkedPath, ex), ex);
            }
        }

        /// <summary>
        /// Resolve NativeLibraryPath (which is a NuGet-style "runtimes/&lt;rid&gt;/native/&lt;lib&gt;" relative
        /// path) to an absolute filesystem path. We probe (in order) the assembly's own directory, the
        /// application's base directory, and finally the current working directory when it is available.
        /// Falls back to the raw relative path if none of these exist, so dlopen's error message
        /// surfaces as before.
        /// </summary>
        static string ResolveNativeLibraryPath(Assembly assembly)
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
                var candidate = Path.Combine(root, NativeLibraryPath);
                if (File.Exists(candidate))
                    return Path.GetFullPath(candidate);
            }

            return NativeLibraryPath;
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
        /// Candidate paths for libaio.so.1t64 on Debian/Ubuntu multiarch layouts. These match what
        /// libaio1t64 installs on amd64 and arm64; add more here if additional architectures appear.
        /// </summary>
        static readonly string[] LibaioT64CandidatePaths =
        [
            "/usr/lib/x86_64-linux-gnu/libaio.so.1t64",
            "/usr/lib/aarch64-linux-gnu/libaio.so.1t64",
            "/lib/x86_64-linux-gnu/libaio.so.1t64",
            "/lib/aarch64-linux-gnu/libaio.so.1t64",
            "/usr/lib64/libaio.so.1t64",
            "/usr/lib/libaio.so.1t64",
        ];

        /// <summary>
        /// Locate libaio.so.1t64 and create a libaio.so.1 symlink next to libnative_device.so so that
        /// the dynamic linker (searching RPATH=$ORIGIN) can satisfy the DT_NEEDED entry. Returns true
        /// when after the call a usable symlink exists at the expected path - whether we created it or
        /// a concurrently-starting process did. Sets <paramref name="createdSymlink"/> to the link path
        /// in that case.
        /// </summary>
        static bool TryCreateLibaioCompatSymlink(string resolvedNativeLibraryPath, out string createdSymlink)
        {
            createdSymlink = null;

            string t64Path = null;
            foreach (var candidate in LibaioT64CandidatePaths)
            {
                if (File.Exists(candidate))
                {
                    t64Path = candidate;
                    break;
                }
            }
            if (t64Path == null)
                return false;

            string shimPath;
            try
            {
                var nativeDir = Path.GetDirectoryName(Path.GetFullPath(resolvedNativeLibraryPath));
                if (string.IsNullOrEmpty(nativeDir) || !Directory.Exists(nativeDir))
                    return false;

                shimPath = Path.Combine(nativeDir, "libaio.so.1");
            }
            catch (Exception)
            {
                return false;
            }

            try
            {
                File.CreateSymbolicLink(shimPath, t64Path);
                createdSymlink = shimPath;
                return true;
            }
            catch (IOException)
            {
                // Either a concurrently-starting process already created the symlink (common in
                // container fleets where multiple Garnet instances share an image), or a stale file
                // of the same name is present. If it's a symlink resolving to libaio.so.1t64, treat
                // that as success; otherwise fall through to the diagnostic error.
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
        /// Returns true if <paramref name="shimPath"/> is an existing symlink that points to a
        /// libaio.so.1t64 file (possibly via relative or absolute target).
        /// </summary>
        static bool IsUsableLibaioShim(string shimPath)
        {
            try
            {
                var info = new FileInfo(shimPath);
                if (!info.Exists) return false;
                var target = info.LinkTarget;
                if (string.IsNullOrEmpty(target)) return false;
                // LinkTarget can be a relative path (e.g., just "libaio.so.1t64"); accept either.
                return target.EndsWith("libaio.so.1t64", StringComparison.Ordinal);
            }
            catch
            {
                return false;
            }
        }

        static string TryGetLinuxMultiarchTriplet(Architecture architecture) => architecture switch
        {
            Architecture.X64 => "x86_64-linux-gnu",
            Architecture.Arm64 => "aarch64-linux-gnu",
            Architecture.Arm => "arm-linux-gnueabihf",
            _ => null
        };

        static string BuildLibaioDiagnostic(string attemptedSymlinkPath, Exception inner)
        {
            var arch = TryGetLinuxMultiarchTriplet(RuntimeInformation.ProcessArchitecture);
            var attempted = attemptedSymlinkPath == null
                ? "Could not find libaio.so.1t64 in standard multiarch paths; auto-repair skipped."
                : $"Attempted to create '{attemptedSymlinkPath}' -> libaio.so.1t64 but the load still failed.";
            var compatSymlinkFix = arch == null
                ? "(b) as root, create a 'libaio.so.1' -> 'libaio.so.1t64' compat symlink in the appropriate multiarch library directory for your distro, "
                : $"(b) as root, create the compat symlink: sudo ln -s /usr/lib/{arch}/libaio.so.1t64 /usr/lib/{arch}/libaio.so.1, ";
            return
                $"Failed to load native storage device library '{NativeLibraryPath}' because its dependency 'libaio.so.1' " +
                "is not resolvable by the dynamic linker. This typically happens on Debian 13 (trixie) or " +
                "Ubuntu 24.04 (noble) where the libaio1 package was renamed to libaio1t64 (64-bit time_t ABI " +
                "transition) and only ships 'libaio.so.1t64'. " + attempted + " " +
                "To fix, either (a) install the legacy-named package if available for your distro, " +
                compatSymlinkFix +
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
        /// Note: the Linux prebuilt shipped in <c>runtimes/linux-x64/native/</c> is built with
        /// <c>USE_URING=ON</c> and therefore records <c>liburing.so.2</c> as a NEEDED ELF entry.
        /// The dynamic linker must resolve it at load time even when only the <see cref="Libaio"/>
        /// backend is selected. Deployments without liburing must build the native library
        /// themselves with <c>-DUSE_URING=OFF</c>; in that case the <see cref="Uring"/> backend is
        /// rejected at <see cref="NativeStorageDevice"/> construction and <see cref="Libaio"/> /
        /// <see cref="Default"/> remain available.
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
        static extern IntPtr NativeDevice_CreateWithBackend(string file, bool enablePrivileges, bool unbuffered, bool delete_on_close, int backend, ulong segmentSizeBytes, bool omitSegmentIdFromFilename);

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
            // try/finally so a throwing user callback still returns the result slot AND decrements
            // numPending. The Dispose() drain loop spins until numPending == 0, so decrementing
            // here (after the callback returns) guarantees Dispose waits for all in-flight user
            // callbacks to finish before destroying the native device underneath them.
            try
            {
                result.callback((uint)errorCode, (uint)numBytes, result.context);
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
        public override bool Throttle() => numPending > ThrottleLimit;

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
        /// Native device creation is DEFERRED until <see cref="Initialize"/> is called with the
        /// segment size — the constructor only stores configuration. This is the only way to
        /// thread the segment size from the upstream log layer (which knows it) down to the
        /// native <c>FileSystemSegmentedFile</c> (which needs it for shift/mask geometry). Until
        /// <see cref="Initialize"/> runs, <see cref="nativeDevice"/> is <c>IntPtr.Zero</c> and
        /// every IO entry point (<see cref="ReadAsync"/>, <see cref="WriteAsync"/>, etc.) throws
        /// <see cref="InvalidOperationException"/>.
        /// </para>
        /// </remarks>
        /// <param name="filename">File name (or prefix) with path</param>
        /// <param name="deleteOnClose"></param>
        /// <param name="disableFileBuffering"></param>
        /// <param name="capacity">The maximum number of bytes this storage device can accommodate, or CAPACITY_UNSPECIFIED if there is no such limit </param>
        /// <param name="numCompletionThreads">Number of IO completion threads</param>
        /// <param name="ioBackend">IO backend to use (default platform backend, or explicit libaio / io_uring on Linux).</param>
        /// <param name="logger"></param>
        public NativeStorageDevice(string filename,
                                      bool deleteOnClose = false,
                                      bool disableFileBuffering = true,
                                      long capacity = Devices.CAPACITY_UNSPECIFIED,
                                      int numCompletionThreads = 1,
                                      IoBackend ioBackend = IoBackend.Default,
                                      ILogger logger = null)
                : base(filename, GetSectorSize(filename), capacity)
        {
            Debug.Assert(numCompletionThreads >= 1);

            if (filename.Length > Native32.WIN32_MAX_PATH - 11)     // -11 to allow for ".<segment>"
                throw new TsavoriteException($"Path {filename} is too long");

            // Capture configuration; native device creation defers to Initialize().
            this.filename = filename;
            this.deleteOnClose = deleteOnClose;
            this.disableFileBuffering = disableFileBuffering;
            this.numCompletionThreadsConfig = numCompletionThreads;
            this.ioBackendConfig = ioBackend;
            this.logger = logger;

            ThrottleLimit = 120;
            _callbackDelegate = _callback;

            string path = new FileInfo(filename).Directory.FullName;
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);
        }

        /// <inheritdoc />
        /// <remarks>
        /// Creates the underlying native device with the requested segment size. Validates that
        /// segmentSize is a positive power of two and at least the device sector size — the
        /// native side enforces the same invariant, but failing fast in managed code keeps the
        /// error message close to the caller. The native device's actual segment size is read
        /// back via <see cref="NativeDevice_GetSegmentSize"/> as a defense against ABI mismatches
        /// between the .so and the C# wrapper.
        /// <para>
        /// Passing <c>segmentSize = -1</c> selects unbounded single-segment mode: native is
        /// asked to use <see cref="UnboundedNativeSegmentSizeBytes"/> (1&lt;&lt;63) so every
        /// non-negative upper-layer address routes to segment 0 in both the C++ and managed
        /// bit-shift math, and the on-disk layout is a single segment file
        /// (<c>&lt;basename&gt;.0</c>) that grows on demand. When combined with
        /// <paramref name="omitSegmentIdFromFilename"/> = true, the file is named
        /// just <c>&lt;basename&gt;</c> (no segment suffix) — only allowed with
        /// <paramref name="segmentSize"/> = -1, matching the managed devices' behaviour.
        /// </para>
        /// </remarks>
        public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            if (omitSegmentIdFromFilename && segmentSize != -1)
                throw new TsavoriteException("omitSegmentIdFromFilename requires segmentSize = -1 (single unbounded segment); multiple segments would all map to the same on-disk path and clobber each other.");

            ulong sizeForNative;
            if (segmentSize == -1)
            {
                // Unbounded single-segment mode: ask native for 1<<63 so the C++ shift math
                // collapses every non-negative upper-layer address into segment 0 (the same
                // address space the managed side covers with segmentSizeBits=64/mask=~0).
                sizeForNative = UnboundedNativeSegmentSizeBytes;
            }
            else
            {
                if (segmentSize <= 0 || (segmentSize & (segmentSize - 1)) != 0)
                    throw new TsavoriteException($"Native device segment size must be a positive power of two (or -1 for unbounded); got {segmentSize}.");
                if (segmentSize < SectorSize)
                    throw new TsavoriteException($"Segment size {segmentSize} must be at least the device sector size {SectorSize}.");
                sizeForNative = (ulong)segmentSize;
            }
            if (nativeDevice != IntPtr.Zero)
                throw new TsavoriteException("NativeStorageDevice.Initialize called more than once.");

            nativeSegmentSizeBytes = sizeForNative;

            // Create the native device with the requested segment size.
            nativeDevice = NativeDevice_CreateWithBackend(filename, false, disableFileBuffering, deleteOnClose, (int)ioBackendConfig, sizeForNative, omitSegmentIdFromFilename);
            if (nativeDevice == IntPtr.Zero)
            {
                // Pull the actionable error message from the native thread-local before doing any
                // other native_device API call on this thread (which would clobber it).
                var nativeMessage = GetNativeLastError();
                var available = GetAvailableBackends();
                var detail = string.IsNullOrEmpty(nativeMessage)
                    ? $"Requested IO backend '{ioBackendConfig}' is not available in the loaded native_device library."
                    : $"Native device initialization failed: {nativeMessage}";
                throw new TsavoriteException(
                    $"{detail} " +
                    $"Available backends: default={available.defaultAvailable}, io_uring={available.uringAvailable}. " +
                    (ioBackendConfig == IoBackend.Uring
                        ? "Rebuild the native library with -DUSE_URING=ON and install liburing-dev to enable io_uring."
                        : "Verify the native library matches the requested backend."));
            }

            // Defense in depth: read back the segment size the native side actually used and
            // assert it matches what we asked for. Catches stale .so binaries that don't include
            // Phase 6's ABI change.
            ulong actualSegmentSize = NativeDevice_GetSegmentSize(nativeDevice);
            if (actualSegmentSize != sizeForNative)
            {
                NativeDevice_Destroy(nativeDevice);
                nativeDevice = IntPtr.Zero;
                throw new TsavoriteException(
                    $"Native device segment size mismatch: requested {sizeForNative}, native returned {actualSegmentSize}. " +
                    "This indicates an ABI mismatch between the loaded native_device library and the managed wrapper. " +
                    "Ensure libnative_device.so matches the current build.");
            }

            // Cross-check the sector alignment: NativeStorageDevice.GetSectorSize probed the
            // filesystem via NativeDevice_ProbeAlignment at construction time and used the
            // result to seed base.SectorSize. Now that the file is actually open, the C++ side
            // has its own authoritative answer from File::GetDeviceAlignment(). They MUST
            // agree — if they don't, every I/O the upper layer issues will be misaligned for
            // the kernel and either rejected with EINVAL or (worse) silently coerced. Throw
            // at startup with a clear message rather than corrupt the log.
            //
            // The mismatch can happen in practice when:
            //   (1) the file is on a different mount than what GetSectorSize probed (e.g.,
            //       parent dir was probed before the file was created on a remote/bind mount);
            //   (2) a stale .so is loaded whose ProbeAlignment is older than sector_size();
            //   (3) probe returned 512 (fallback) but the kernel sees 4096 via statx after open.
            uint nativeSectorSize = NativeDevice_sector_size(nativeDevice);
            if (nativeSectorSize != SectorSize)
            {
                NativeDevice_Destroy(nativeDevice);
                nativeDevice = IntPtr.Zero;
                throw new TsavoriteException(
                    $"Native device sector-size mismatch on '{filename}': managed wrapper probed " +
                    $"{SectorSize} bytes but the kernel reports {nativeSectorSize} bytes for the " +
                    "actual file. The most likely cause is a 4K-native disk where the probe ran " +
                    "against a directory on a different filesystem than the eventual log file, " +
                    "or a stale libnative_device.so. Place the log file on a filesystem whose " +
                    "DIO alignment matches the probe result, or rebuild the native library to " +
                    "match the managed wrapper.");
            }

            results = new NativeResult[MaxResults];

            // If Queue IO is enabled, we spin up completion threads.
            //
            // Each Thread handle is kept so Dispose() can Thread.Join() every worker — this is the
            // ONLY way to guarantee the worker has fully returned from its last
            // NativeDevice_QueueRun (io_getevents / io_uring_wait_cqe_timeout) syscall before we
            // call NativeDevice_Destroy. The previous semaphore-based pattern, where the last
            // worker decremented a counter and released a single semaphore in its `finally` block,
            // could fire BEFORE other workers had exited the kernel — leading to a use-after-free
            // on the libaio/uring ring inside the still-blocked thread.
            if (NativeDevice_QueueRun(nativeDevice, 0) >= 0)
            {
                completionThreadToken = new();
                completionThreads = new Thread[numCompletionThreadsConfig];
                for (int i = 0; i < numCompletionThreadsConfig; i++)
                {
                    completionThreads[i] = new Thread(CompletionWorker)
                    {
                        IsBackground = true
                    };
                    completionThreads[i].Start();
                }
            }

            base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
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
            if (!EnsureReadyOrSilent()) return;
            NativeDevice_Reset(nativeDevice);
        }

        /// <summary>
        /// Returns false (silent no-op) if the device has been disposed — late fires from epoch
        /// drain paths are expected after Dispose() returns. Throws
        /// <see cref="InvalidOperationException"/> if the device has not been initialized yet,
        /// because that indicates a real ordering bug in the caller.
        /// </summary>
        bool EnsureReadyOrSilent()
        {
            if (nativeDevice != IntPtr.Zero) return true;
            if (Volatile.Read(ref disposedFlag) != 0) return false;
            throw new InvalidOperationException(
                "NativeStorageDevice must be Initialize()d (which creates the underlying native device) before any IO is issued.");
        }

        /// <summary>
        /// Asserts that an I/O request is properly aligned for the underlying O_DIRECT / aligned
        /// path. The libaio and io_uring submission paths require that the file offset, the
        /// transfer length, and the user buffer pointer all be multiples of the device's sector
        /// size; misaligned requests fail with EINVAL at completion time. Catching them here
        /// produces a clean, diagnosable error message instead of a cryptic kernel error code
        /// arriving through the IO callback.
        /// </summary>
        /// <remarks>
        /// Cost on the hot path: three predicated AND-comparisons and a never-taken branch.
        /// In well-behaved callers the branch predictor sees a steady-state "no throw" and the
        /// guard amortizes to a couple of cycles — negligible against the cost of an actual
        /// kernel I/O submission.
        /// </remarks>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private void ThrowIfMisaligned(ulong offset, uint length, IntPtr buffer, string op)
        {
            uint mask = SectorSize - 1;
            if ((offset & mask) != 0 || (length & mask) != 0 || ((ulong)buffer.ToInt64() & mask) != 0)
                ThrowMisaligned(offset, length, buffer, op);
        }

        // Cold-path throw is in its own NoInlining method so the AggressiveInlining guard stays
        // small and inlinable. The IOException carries every input the caller needs to track
        // down which upper-layer staging buffer is producing the misaligned request.
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
            // Fail fast if Initialize() hasn't run yet — calling into the native side with a
            // null device handle would dereference a null pointer in C++.
            if (nativeDevice == IntPtr.Zero)
                throw new InvalidOperationException("NativeStorageDevice.ReadAsync called before Initialize().");

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
                    throw new IOException("Error reading from log file", _result);
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
            if (nativeDevice == IntPtr.Zero)
                throw new InvalidOperationException("NativeStorageDevice.WriteAsync called before Initialize().");

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
                    throw new IOException("Error writing to log file", _result);
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
            if (!EnsureReadyOrSilent()) return;
            NativeDevice_RemoveSegment(nativeDevice, (ulong)segment);
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
        /// Worst-case shutdown stall = (<see cref="CompletionWorkerTimeoutSecs"/> + duration of
        /// the longest in-flight user callback). If callbacks are slow, Dispose() waits for them.
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

            // Idempotent: a second call short-circuits without re-running the (non-idempotent)
            // shutdown sequence. Setting the flag here also serves as the guard for all the
            // late-fire P/Invoke entry points (TryComplete, GetFileSize, Reset, RemoveSegment).
            if (Interlocked.Exchange(ref disposedFlag, 1) != 0)
                return;

            // 1. Stop accepting new requests, then drain everything in flight.
            //    The CAS races against WriteAsync / ReadAsync incrementing numPending; we spin until
            //    a moment is observed where numPending == 0 and we successfully swap it to int.MinValue,
            //    at which point no new submissions can succeed and all completions have fired
            //    (since _callback decrements numPending in `finally` AFTER the user callback returns).
            while (numPending >= 0)
            {
                Interlocked.CompareExchange(ref numPending, int.MinValue, 0);
                Thread.Yield();
            }

            // 2. Tell the completion threads to stop, then Thread.Join EVERY worker.
            //    Thread.Join returns strictly after the worker method returns, which is strictly
            //    after its last NativeDevice_QueueRun syscall has returned. Unlike the previous
            //    semaphore + Interlocked.Decrement(numCompletionThreads) pattern, there is no
            //    race where one worker signals completion while another is still blocked inside
            //    io_getevents / io_uring_wait_cqe_timeout.
            if (completionThreads != null)
            {
                completionThreadToken.Cancel();
                foreach (var t in completionThreads) t.Join();
                completionThreadToken.Dispose();
            }

            // 3. No thread is inside native code anymore; destroy then poison the handle so
            //    any guard-bypassed call fails cleanly instead of dereferencing freed memory.
            //    Skip Destroy() entirely if Dispose was called pre-Initialize (no native device
            //    was ever created). delete on a null pointer is well-defined in C++, but skipping
            //    the P/Invoke avoids transitioning into native code unnecessarily.
            if (nativeDevice != IntPtr.Zero)
            {
                NativeDevice_Destroy(nativeDevice);
                nativeDevice = IntPtr.Zero;
            }
        }

        /// <inheritdoc/>
        public override bool TryComplete()
            => !EnsureReadyOrSilent() ? false : NativeDevice_TryComplete(nativeDevice);

        /// <inheritdoc/>
        public override long GetFileSize(int segment)
            => !EnsureReadyOrSilent() ? 0 : (long)NativeDevice_GetFileSize(nativeDevice, (ulong)segment);

        /// <summary>
        ///
        /// </summary>
        /// <param name="segmentId"></param>
        /// <returns></returns>
        protected string GetSegmentName(int segmentId) => GetSegmentFilename(FileName, segmentId);

        /// <summary>
        /// Cold-path probe of the kernel's required direct-I/O alignment for the target file.
        /// Called from the ctor (via base.ctor) BEFORE the native device is created, because
        /// StorageDeviceBase.SectorSize is set in the base ctor and is immutable thereafter.
        /// </summary>
        /// <remarks>
        /// The probe never throws: on any failure it falls back to <see cref="MinSectorSize"/>
        /// (512). If the probe reports a stale or wrong value, the cross-check inside
        /// <see cref="Initialize"/> (comparing the upper-layer <see cref="StorageDeviceBase.SectorSize"/>
        /// against <see cref="NativeDevice_sector_size"/>) catches the mismatch at startup
        /// rather than silently emitting misaligned I/Os.
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
        /// Long-running drain loop for one completion thread. Each iteration blocks up to
        /// <see cref="CompletionWorkerTimeoutSecs"/> seconds inside the native handler (waiting on
        /// io_getevents / io_uring_wait_cqe_timeout) and then returns control here so we can observe
        /// a cancellation request promptly. The short timeout ensures Dispose() does not stall: the
        /// worst-case shutdown stall is one timeout window per blocked thread.
        /// </summary>
        void CompletionWorker()
        {
            // No `finally` shutdown signal needed: Dispose() now uses Thread.Join() on the worker
            // handle, which fires strictly after this method returns.
            while (true)
            {
                if (completionThreadToken.IsCancellationRequested) break;
                NativeDevice_QueueRun(nativeDevice, CompletionWorkerTimeoutSecs);
                Thread.Yield();
            }
        }

        // Per-iteration timeout passed to NativeDevice_QueueRun by completion workers. Kept short so
        // a cancelled worker exits the native call within roughly this many seconds; the cost of a
        // shorter value is one extra syscall round-trip per idle second (negligible at server scale).
        const int CompletionWorkerTimeoutSecs = 1;
    }
}