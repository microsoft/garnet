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
        const uint sectorSize = 512;

        readonly ConcurrentQueue<int> freeResults = new();
        readonly ILogger logger;
        NativeResult[] results;

        /// <summary>
        /// Number of pending reads on device
        /// </summary>
        int numPending = 0;

        readonly int nativeSegmentSizeBits;
        int resultOffset;

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
        readonly IntPtr nativeDevice;

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_Create", CallingConvention = CallingConvention.Cdecl)]
        static extern IntPtr NativeDevice_Create(string file, bool enablePrivileges, bool unbuffered, bool delete_on_close);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_Destroy", CallingConvention = CallingConvention.Cdecl)]
        static extern void NativeDevice_Destroy(IntPtr device);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_sector_size", CallingConvention = CallingConvention.Cdecl)]
        static extern uint NativeDevice_sector_size(IntPtr device);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_ReadAsync", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_ReadAsync(IntPtr device, ulong source, IntPtr dest, uint length, AsyncIOCallback callback, IntPtr context);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_WriteAsync", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_WriteAsync(IntPtr device, IntPtr source, ulong dest, uint length, AsyncIOCallback callback, IntPtr context);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_CreateDir", CallingConvention = CallingConvention.Cdecl)]
        static extern void NativeDevice_CreateDir(IntPtr device, string dir);

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
        #endregion

        readonly AsyncIOCallback _callbackDelegate;
        readonly CancellationTokenSource completionThreadToken;
        readonly SemaphoreSlim completionThreadSemaphore;
        int numCompletionThreads;

        void _callback(IntPtr context, int errorCode, ulong numBytes)
        {
            Interlocked.Decrement(ref numPending);
            var result = results[(int)context];
            result.callback((uint)errorCode, (uint)numBytes, result.context);
            freeResults.Enqueue((int)context);
        }

        /// <inheritdoc />
        public override bool Throttle() => numPending > ThrottleLimit;

        /// <summary>
        /// Constructor with more options for derived classes
        /// </summary>
        /// <param name="filename">File name (or prefix) with path</param>
        /// <param name="deleteOnClose"></param>
        /// <param name="disableFileBuffering"></param>
        /// <param name="capacity">The maximum number of bytes this storage device can accommodate, or CAPACITY_UNSPECIFIED if there is no such limit </param>
        /// <param name="numCompletionThreads">Number of IO completion threads</param>
        /// <param name="logger"></param>
        public NativeStorageDevice(string filename,
                                      bool deleteOnClose = false,
                                      bool disableFileBuffering = true,
                                      long capacity = Devices.CAPACITY_UNSPECIFIED, int numCompletionThreads = 1, ILogger logger = null)
                : base(filename, GetSectorSize(filename), capacity)
        {
            Debug.Assert(numCompletionThreads >= 1);

            // Native device uses a fixed segment size
            nativeSegmentSizeBits = 30;

            _callbackDelegate = _callback;

            if (filename.Length > Native32.WIN32_MAX_PATH - 11)     // -11 to allow for ".<segment>"
                throw new TsavoriteException($"Path {filename} is too long");

            ThrottleLimit = 120;

            string path = new FileInfo(filename).Directory.FullName;
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);
            this.logger = logger;

            nativeDevice = NativeDevice_Create(filename, false, disableFileBuffering, deleteOnClose);
            results = new NativeResult[MaxResults];

            // If Queue IO is enabled, we spin up completion threads
            if (NativeDevice_QueueRun(nativeDevice, 0) >= 0)
            {
                this.numCompletionThreads = numCompletionThreads;
                completionThreadToken = new();
                completionThreadSemaphore = new(0);
                for (int i = 0; i < numCompletionThreads; i++)
                {
                    var thread = new Thread(CompletionWorker)
                    {
                        IsBackground = true
                    };
                    thread.Start();
                }
            }
        }

        /// <inheritdoc />
        public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            // We can simulate any segment size less than or equal to native segment size
            if (segmentSize > (1L << nativeSegmentSizeBits))
                throw new TsavoriteException("Native storage device does not support segment sizes greater than 1GB");
            base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
        }

        /// <inheritdoc />
        public override void Reset()
            => NativeDevice_Reset(nativeDevice);

        /// <inheritdoc />
        public override void ReadAsync(int segmentId, ulong sourceAddress,
                                     IntPtr destinationAddress,
                                     uint readLength,
                                     DeviceIOCompletionCallback callback,
                                     object context)
        {
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
                int _result = NativeDevice_ReadAsync(nativeDevice, ((ulong)segmentId << nativeSegmentSizeBits) | sourceAddress, destinationAddress, readLength, _callbackDelegate, (IntPtr)offset);

                if (_result != 0)
                {
                    throw new IOException("Error reading from log file", _result);
                }
            }
            catch (IOException e)
            {
                logger?.LogCritical(e, $"{nameof(ReadAsync)}");
                Interlocked.Decrement(ref numPending);
                callback((uint)(e.HResult & 0x0000FFFF), 0, context);
                freeResults.Enqueue(offset);
            }
            catch (Exception e)
            {
                logger?.LogCritical(e, $"{nameof(ReadAsync)}");
                Interlocked.Decrement(ref numPending);
                callback(uint.MaxValue, 0, context);
                freeResults.Enqueue(offset);
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
                int _result = NativeDevice_WriteAsync(nativeDevice, sourceAddress, ((ulong)segmentId << nativeSegmentSizeBits) | destinationAddress, numBytesToWrite, _callbackDelegate, (IntPtr)offset);

                if (_result != 0)
                {
                    throw new IOException("Error writing to log file", _result);
                }
            }
            catch (IOException e)
            {
                logger?.LogCritical(e, $"{nameof(WriteAsync)}");
                Interlocked.Decrement(ref numPending);
                callback((uint)(e.HResult & 0x0000FFFF), 0, context);
            }
            catch (Exception e)
            {
                logger?.LogCritical(e, $"{nameof(WriteAsync)}");
                Interlocked.Decrement(ref numPending);
                callback(uint.MaxValue, 0, context);
            }
        }

        /// <summary>
        /// <see cref="IDevice.RemoveSegment(int)"/>
        /// </summary>
        /// <param name="segment"></param>
        public override void RemoveSegment(int segment)
            => NativeDevice_RemoveSegment(nativeDevice, (ulong)segment);

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
        /// Close device
        /// </summary>
        public override void Dispose()
        {
            // Stop accepting new requests, drain all pending
            while (numPending >= 0)
            {
                Interlocked.CompareExchange(ref numPending, int.MinValue, 0);
                Thread.Yield();
            }

            if (numCompletionThreads > 0)
            {
                // Stop completion threads
                completionThreadToken.Cancel();
            }

            // Destroy - should end ongoing waits
            // Potential rare race here, where Linux io_destroy may cause ongoing io_getevents to crash
            NativeDevice_Destroy(nativeDevice);

            if (numCompletionThreads > 0)
            {
                // Wait for completion thread to finish
                completionThreadSemaphore.Wait();

                // Dispose completion objects
                completionThreadToken.Dispose();
                completionThreadSemaphore.Dispose();
            }
        }

        /// <inheritdoc/>
        public override bool TryComplete()
            => NativeDevice_TryComplete(nativeDevice);

        /// <inheritdoc/>
        public override long GetFileSize(int segment)
            => (long)NativeDevice_GetFileSize(nativeDevice, (ulong)segment);

        /// <summary>
        ///
        /// </summary>
        /// <param name="segmentId"></param>
        /// <returns></returns>
        protected string GetSegmentName(int segmentId) => GetSegmentFilename(FileName, segmentId);

        private static uint GetSectorSize(string filename)
        {
            return sectorSize;
        }

        void CompletionWorker()
        {
            try
            {
                while (true)
                {
                    if (completionThreadToken.IsCancellationRequested) break;
                    NativeDevice_QueueRun(nativeDevice, 5);
                    Thread.Yield();
                }
            }
            finally
            {
                if (Interlocked.Decrement(ref numCompletionThreads) == 0)
                {
                    completionThreadSemaphore.Release();
                }
            }
        }
    }
}