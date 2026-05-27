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
        /// Floor sector size used when the alignment probe fails (parent directory missing,
        /// or kernel/filesystem combinations that do not populate statx STATX_DIOALIGN).
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
        /// Default ring count for the uring backend. Sized so that on a typical multi-submitter
        /// workload, threads distributed across 4 rings rarely contend on sq_lock; with a single
        /// drainer covering all 4 rings via QueueRun, this matches libaio ct=1 throughput on
        /// the test hardware (~700K IOPS on Dell P5600). Smaller values leave throughput on the
        /// table at moderate thread counts; larger values cost the single drainer extra empty-
        /// ring polls at low thread counts without measurable gain at high thread counts.
        const int kDefaultUringRings = 4;
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
                // Compatibility shim for Debian 13 / Ubuntu 24.04+, where libaio1 was renamed to
                // libaio1t64 and the library now exports SONAME "libaio.so.1t64" (the 64-bit
                // time_t ABI transition). Drop a libaio.so.1 -> libaio.so.1t64 symlink next to
                // libnative_device.so; the native library is built with RPATH=$ORIGIN so it picks
                // the symlink up.
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
        static extern IntPtr NativeDevice_CreateWithBackend(string file, bool enablePrivileges, bool unbuffered, bool delete_on_close, int backend, ulong segmentSizeBytes, bool omitSegmentIdFromFilename, int numIoContexts);

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
        /// Each completion thread drains CQEs from one or more io_context shards. The number
        /// of underlying io_context shards (rings) is derived automatically from the backend
        /// and numCompletionThreads — uring uses <c>max(4, numCompletionThreads)</c> to escape
        /// per-ring sq_lock contention even with a single drainer (the single drainer covers
        /// all rings via the legacy <c>QueueRun</c> compat scanner); libaio uses
        /// numCompletionThreads directly (the kernel-side per-context mutex is already
        /// efficient and extra rings don't help). Ignored on Windows (IOCP). When &lt; 1,
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
                : base(filename, GetSectorSize(filename), capacity)
        {
            Debug.Assert(numCompletionThreads >= 1);

            if (filename.Length > Native32.WIN32_MAX_PATH - 11)     // -11 to allow for ".<segment>"
                throw new TsavoriteException($"Path {filename} is too long");

            // Configuration is captured here; the native device handle (and its completion-drainer
            // thread, libaio / io_uring rings, etc.) is created lazily on the first IO call.
            this.filename = filename;
            this.deleteOnClose = deleteOnClose;
            this.disableFileBuffering = disableFileBuffering;
            this.numCompletionThreadsConfig = numCompletionThreads < 1 ? 1 : numCompletionThreads;
            // For uring, force at least kDefaultUringRings shards even if the caller asked for a
            // single drainer. This eliminates per-ring sq_lock contention without requiring the
            // caller to know how many submitter threads they have or to pay for N drainer threads.
            // Per-thread ring affinity (file_linux.h pick_ring) distributes submitters across the
            // rings; the single drainer scans all rings via QueueRun. With ct = 1 and 16 submitter
            // threads on Dell P5600 NVMe this lifts uring throughput from ~340K to ~700K IOPS,
            // matching libaio at ct=1.
            //
            // When numCompletionThreads > kDefaultUringRings, rings tracks numCompletionThreads
            // (1:1 binding, existing sharded behavior).
            //
            // libaio doesn't benefit from extra rings (the kernel io_context mutex is already
            // efficient and io_submit batches well), so it uses numCompletionThreads directly.
            int rings = (ioBackend == IoBackend.Uring)
                ? Math.Max(kDefaultUringRings, this.numCompletionThreadsConfig)
                : this.numCompletionThreadsConfig;
            this.numIoContextsConfig = rings;
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
            if (nativeDevice != IntPtr.Zero) return;
            if (Volatile.Read(ref disposedFlag) != 0)
                throw new ObjectDisposedException(nameof(NativeStorageDevice));
            lock (nativeCreateLock)
            {
                if (nativeDevice != IntPtr.Zero) return;
                if (Volatile.Read(ref disposedFlag) != 0)
                    throw new ObjectDisposedException(nameof(NativeStorageDevice));

                ulong sizeForNative = segmentSize == -1
                    ? UnboundedNativeSegmentSizeBytes
                    : (ulong)segmentSize;

                nativeSegmentSizeBytes = sizeForNative;

                var newDevice = NativeDevice_CreateWithBackend(filename, false, disableFileBuffering, deleteOnClose, (int)ioBackendConfig, sizeForNative, OmitSegmentIdFromFileName, numIoContextsConfig);
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
                    NativeDevice_Destroy(newDevice);
                    throw new TsavoriteException(
                        $"Native device sector-size mismatch on '{filename}': managed wrapper probed " +
                        $"{SectorSize} bytes but the kernel reports {nativeSectorSize} bytes for the " +
                        "actual file. The most likely cause is a 4K-native disk where the probe ran " +
                        "against a directory on a different filesystem than the eventual log file, " +
                        "or a stale libnative_device.so. Place the log file on a filesystem whose " +
                        "DIO alignment matches the probe result, or rebuild the native library to " +
                        "match the managed wrapper.");
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
                    // Drainers: numCompletionThreadsConfig threads cover actualIoContexts rings.
                    // Three cases:
                    //   D == R: 1:1 binding, each drainer calls QueueRunFor(i).
                    //   D == 1, R > 1: single drainer calls QueueRun (all rings).
                    //   D > 1, R != D: not currently supported; clamp drainers to actualIoContexts.
                    int numDrainers = numCompletionThreadsConfig;
                    if (numDrainers > actualIoContexts) numDrainers = actualIoContexts;
                    bool singleDrainerAllRings = numDrainers == 1 && actualIoContexts > 1;
                    completionThreads = new Thread[numDrainers];
                    for (int i = 0; i < numDrainers; i++)
                    {
                        int ctxIdx = singleDrainerAllRings ? -1 : i;
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
            // No-op if the native device has not been created yet (no handles to reset).
            var dev = nativeDevice;
            if (dev == IntPtr.Zero) return;
            NativeDevice_Reset(dev);
        }

        /// <summary>
        /// Returns false (silent no-op) if the device has been disposed. Otherwise drives
        /// <see cref="EnsureNativeDeviceCreated"/> to materialise the native handle on first
        /// use; subsequent calls are a single read once the handle exists.
        /// </summary>
        bool EnsureReadyOrSilent()
        {
            if (Volatile.Read(ref disposedFlag) != 0) return false;
            EnsureNativeDeviceCreated();
            return true;
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
            if (Volatile.Read(ref disposedFlag) != 0) return;
            var dev = nativeDevice;
            if (dev != IntPtr.Zero)
            {
                // Native owns the open handle; let it close+unlink.
                NativeDevice_RemoveSegment(dev, (ulong)segment);
                return;
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

            // Idempotent: second and subsequent calls short-circuit. Setting the flag here also
            // gates late P/Invoke entry points (TryComplete, GetFileSize, Reset, RemoveSegment)
            // via EnsureReadyOrSilent.
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
                    NativeDevice_Destroy(dev);
            }
        }

        /// <inheritdoc/>
        public override bool TryComplete()
        {
            if (Volatile.Read(ref disposedFlag) != 0) return false;
            var dev = nativeDevice;
            return dev == IntPtr.Zero ? false : NativeDevice_TryComplete(dev);
        }

        /// <inheritdoc/>
        public override long GetFileSize(int segment)
        {
            if (Volatile.Read(ref disposedFlag) != 0) return 0;
            var dev = nativeDevice;
            if (dev != IntPtr.Zero)
                return (long)NativeDevice_GetFileSize(dev, (ulong)segment);
            // No native handle yet — stat the on-disk segment file directly. Matches
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
        /// Drain loop for one completion thread.
        ///
        /// When <paramref name="ctxIdx"/> is &gt;= 0, the thread is bound 1:1 to that ring shard
        /// and blocks in <c>NativeDevice_QueueRunFor</c> with a long timeout. When ctxIdx == -1
        /// (only set when numDrainers == 1 &amp;&amp; numRings &gt; 1) the thread calls
        /// <c>NativeDevice_QueueRun</c> which scans all rings (waits on ring 0 with timeout,
        /// then polls the rest). Dispose() wakes blocked workers via
        /// <c>NativeDevice_WakeCompletionWorker</c> rather than relying on the timeout to fire.
        /// </summary>
        void CompletionWorker(int ctxIdx)
        {
            while (true)
            {
                if (completionThreadToken.IsCancellationRequested) break;
                if (ctxIdx < 0)
                    NativeDevice_QueueRun(nativeDevice, CompletionWorkerTimeoutSecs);
                else
                    NativeDevice_QueueRunFor(nativeDevice, ctxIdx, CompletionWorkerTimeoutSecs);
                Thread.Yield();
            }
        }

        // Per-iteration timeout for completion workers. Long enough that the idle syscall rate is
        // negligible; Dispose() does not rely on this firing because it submits a synthetic wake-up
        // event via NativeDevice_WakeCompletionWorker to unblock the worker immediately.
        const int CompletionWorkerTimeoutSecs = 1;
    }
}