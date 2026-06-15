// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Win32.SafeHandles;

namespace Tsavorite.core
{
    /// <summary>
    /// Per-segment read/write file-handle holder. Each direction's <see cref="SafeFileHandle"/>
    /// is created lazily on first access so that callers exercising only one direction
    /// (e.g. read-only recovery, or write-only flush) never open the other one. The single
    /// handle per direction is shared across all concurrent IOs for that segment because
    /// <c>pread</c>/<c>pwrite</c> (and their .NET wrappers <c>RandomAccess.ReadAsync</c>/
    /// <c>WriteAsync</c>) are thread-safe positional calls that do not touch the file
    /// descriptor's cursor. Sharing a single handle eliminates the per-IO Get/Return against
    /// an internal <c>SemaphoreSlim</c>/<c>ConcurrentQueue</c> pool, which under high request
    /// rates produces significant Monitor.Enter_Slowpath contention.
    /// </summary>
    sealed class SegmentHandles : IDisposable
    {
        private readonly Func<SafeFileHandle> readFactory;
        private readonly Func<SafeFileHandle> writeFactory;
        private SafeFileHandle readHandle;
        private SafeFileHandle writeHandle;
        private int disposed;

        public SegmentHandles(Func<SafeFileHandle> readFactory, Func<SafeFileHandle> writeFactory)
        {
            this.readFactory = readFactory;
            this.writeFactory = writeFactory;
        }

        public SafeFileHandle ReadHandle => GetOrCreate(ref readHandle, readFactory);
        public SafeFileHandle WriteHandle => GetOrCreate(ref writeHandle, writeFactory);

        private SafeFileHandle GetOrCreate(ref SafeFileHandle field, Func<SafeFileHandle> factory)
        {
            var existing = Volatile.Read(ref field);
            if (existing != null) return existing;
            if (Volatile.Read(ref disposed) != 0)
                throw new ObjectDisposedException(nameof(SegmentHandles));

            var fresh = factory();
            var prev = Interlocked.CompareExchange(ref field, fresh, null);
            if (prev != null)
            {
                // Lost the race; another caller installed first. Drop ours and reuse theirs.
                fresh.Dispose();
                return prev;
            }
            // If Dispose ran between our CAS and now, clean up so the handle is not leaked.
            if (Volatile.Read(ref disposed) != 0)
            {
                if (Interlocked.CompareExchange(ref field, null, fresh) == fresh)
                    fresh.Dispose();
                throw new ObjectDisposedException(nameof(SegmentHandles));
            }
            return fresh;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0) return;
            Interlocked.Exchange(ref readHandle, null)?.Dispose();
            Interlocked.Exchange(ref writeHandle, null)?.Dispose();
        }
    }

    /// <summary>
    /// Managed device using .NET streams
    /// </summary>
    public sealed class RandomAccessLocalStorageDevice : StorageDeviceBase
    {
        private readonly bool preallocateFile;
        private readonly bool deleteOnClose;
        private readonly bool disableFileBuffering;
        private readonly bool osReadBuffering;
        private readonly bool readOnly;
        private readonly ILogger logger;
        private readonly SafeConcurrentDictionary<int, SegmentHandles> logHandles;
        private readonly SectorAlignedBufferPool pool;
        private static uint sectorSize = 0;

        // Lock-free pool of UnmanagedMemoryManager<byte> wrappers reused across IOs.
        // Each in-flight RandomAccess.ReadAsync/WriteAsync needs a Memory<byte> over its
        // raw destination/source pointer; the wrapper carries the pointer + length and
        // can be rebound via SetDestination. Rent on submission, return in the finally
        // of the IO continuation. Bounded in steady state by ThrottleLimit (typically
        // ~120 entries = ~4 KB). ConcurrentQueue is allocation-free in steady state
        // (one CAS per enqueue/dequeue) and has no Monitor/SemaphoreSlim — the exact
        // anti-pattern we eliminated when removing the per-segment AsyncPool.
        private readonly ConcurrentQueue<UnmanagedMemoryManager<byte>> ummPool = new();

        /// <summary>
        /// Number of pending reads on device
        /// </summary>
        private int numPending = 0;

        private bool _disposed;

        /// <inheritdoc/>
        public override string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            return $"secSize {sectorSize}, numPend {numPending}, RO {bstr(readOnly)}, preAll {bstr(preallocateFile)}, delClose {bstr(deleteOnClose)}, noFileBuf {bstr(disableFileBuffering)}";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="filename">File name (or prefix) with path</param>
        /// <param name="preallocateFile"></param>
        /// <param name="deleteOnClose"></param>
        /// <param name="disableFileBuffering">Whether file buffering (during write) is disabled (default of true requires aligned writes)</param>
        /// <param name="capacity">The maximal number of bytes this storage device can accommodate, or CAPACITY_UNSPECIFIED if there is no such limit</param>
        /// <param name="recoverDevice">Whether to recover device metadata from existing files</param>
        /// <param name="osReadBuffering">Enable OS read buffering</param>
        /// <param name="readOnly">Open file in readOnly mode</param>
        /// <param name="logger"></param>
        public RandomAccessLocalStorageDevice(string filename, bool preallocateFile = false, bool deleteOnClose = false, bool disableFileBuffering = true, long capacity = Devices.CAPACITY_UNSPECIFIED, bool recoverDevice = false, bool osReadBuffering = false, bool readOnly = false, ILogger logger = null)
            : base(filename, GetSectorSize(filename), capacity)
        {
            pool = new(1, 1);
            ThrottleLimit = 120;

            string path = new FileInfo(filename).Directory.FullName;
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            _disposed = false;
            this.preallocateFile = preallocateFile;
            this.deleteOnClose = deleteOnClose;
            this.disableFileBuffering = disableFileBuffering;
            this.osReadBuffering = osReadBuffering;
            this.readOnly = readOnly;
            this.logger = logger;
            logHandles = new();
            if (recoverDevice)
                RecoverFiles();
        }

        /// <inheritdoc />
        public override void Reset()
        {
            DrainPendingIO();
            while (logHandles.Count > 0)
            {
                foreach (var entry in logHandles)
                {
                    if (logHandles.TryRemove(entry.Key, out _))
                    {
                        entry.Value.Dispose();
                        if (deleteOnClose)
                            File.Delete(GetSegmentName(entry.Key));
                    }
                }
            }
        }

        /// <inheritdoc />
        // RandomAccess.ReadAsync/WriteAsync are thread-safe on a single SafeFileHandle, so we
        // do not pool handles per segment. To bound in-flight IOs we throttle based on the
        // numPending counter (same scheme as LocalStorageDevice and NativeStorageDevice). This
        // gives the caller backpressure without the per-IO Monitor.Enter cost that a
        // SemaphoreSlim-based pool incurs on every Get/Return.
        public override bool Throttle() => numPending > ThrottleLimit;

        private void RecoverFiles()
        {
            FileInfo fi = new(FileName); // may not exist
            DirectoryInfo di = fi.Directory;
            if (!di.Exists) return;

            string bareName = fi.Name;

            List<int> segids = new();
            foreach (FileInfo item in di.GetFiles(bareName + "*"))
            {
                if (item.Name == bareName)
                {
                    continue;
                }
                segids.Add(Int32.Parse(item.Name.Replace(bareName, "").Replace(".", "")));
            }
            segids.Sort();

            int prevSegmentId = -1;
            foreach (int segmentId in segids)
            {
                if (segmentId != prevSegmentId + 1)
                {
                    startSegment = segmentId;
                }
                else
                {
                    endSegment = segmentId;
                }
                prevSegmentId = segmentId;
            }
            // No need to populate map because logHandles use Open or create on files.
        }

        /// <summary>
        /// Read async
        /// </summary>
        /// <param name="segmentId"></param>
        /// <param name="sourceAddress"></param>
        /// <param name="destinationAddress"></param>
        /// <param name="readLength"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, DeviceIOCompletionCallback callback, object context)
        {
            using (ExecutionContext.SuppressFlow())
            {
                _ = ReadWorkerAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);
            }
        }

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        async ValueTask ReadWorkerAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, DeviceIOCompletionCallback callback, object context)
        {
            uint errorCode = 0;
            uint numBytes = 0;

            _ = Interlocked.Increment(ref numPending);

            UnmanagedMemoryManager<byte> memoryManager = null;
            try
            {
                var readHandle = GetOrAddHandle(segmentId).ReadHandle;

                unsafe
                {
                    memoryManager = RentUmm((byte*)destinationAddress, (int)readLength);
                }
                numBytes = (uint)await RandomAccess.ReadAsync(readHandle, memoryManager.Memory, (long)sourceAddress).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger?.LogCritical(ex, $"{nameof(ReadAsync)}");
                var ioex = ex as IOException ?? ex.InnerException as IOException;
                if (ioex is not null)
                    errorCode = (uint)(ioex.HResult & 0x0000FFFF);
                else
                    errorCode = uint.MaxValue;
            }
            finally
            {
                if (memoryManager is not null)
                    ummPool.Enqueue(memoryManager);
                _ = Interlocked.Decrement(ref numPending);
                // Issue user callback
                callback(errorCode, numBytes, context);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sourceAddress"></param>
        /// <param name="segmentId"></param>
        /// <param name="destinationAddress"></param>
        /// <param name="numBytesToWrite"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            using (ExecutionContext.SuppressFlow())
            {
                _ = WriteWorkerAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);
            }
        }

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        async ValueTask WriteWorkerAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            uint errorCode = 0;

            HandleCapacity(segmentId);

            _ = Interlocked.Increment(ref numPending);

            UnmanagedMemoryManager<byte> memoryManager = null;
            try
            {
                var writeHandle = GetOrAddHandle(segmentId).WriteHandle;

                unsafe
                {
                    memoryManager = RentUmm((byte*)sourceAddress, (int)numBytesToWrite);
                }
                await RandomAccess.WriteAsync(writeHandle, memoryManager.Memory, (long)destinationAddress).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger?.LogCritical(ex, $"{nameof(WriteAsync)}");
                var ioex = ex as IOException ?? ex.InnerException as IOException;
                if (ioex is not null)
                    errorCode = (uint)(ioex.HResult & 0x0000FFFF);
                else
                    errorCode = uint.MaxValue;
                numBytesToWrite = 0;
            }
            finally
            {
                if (memoryManager is not null)
                    ummPool.Enqueue(memoryManager);
                _ = Interlocked.Decrement(ref numPending);
                // Issue user callback
                callback(errorCode, numBytesToWrite, context);
            }
        }

        /// <summary>
        /// Rent a wrapper from <see cref="ummPool"/> (or allocate one if the pool is empty)
        /// and bind it to the given unmanaged buffer. Caller must <c>Enqueue</c> back to
        /// <see cref="ummPool"/> in the finally block of the IO continuation. Wrappers are
        /// only handed out one-IO-at-a-time, so concurrent <see cref="UnmanagedMemoryManager{T}.SetDestination"/>
        /// on the same wrapper cannot occur.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe UnmanagedMemoryManager<byte> RentUmm(byte* pointer, int length)
        {
            if (!ummPool.TryDequeue(out var memoryManager))
                memoryManager = new UnmanagedMemoryManager<byte>();
            memoryManager.SetDestination(pointer, length);
            return memoryManager;
        }

        /// <summary>
        /// <see cref="IDevice.RemoveSegment(int)"/>
        /// </summary>
        /// <param name="segment"></param>
        public override void RemoveSegment(int segment)
        {
            if (logHandles.TryRemove(segment, out var logHandle))
            {
                logHandle.Dispose();
            }
            try
            {
                File.Delete(GetSegmentName(segment));
            }
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

        /// <inheritdoc/>
        public override long GetFileSize(int segment)
        {
            if (segmentSize > 0) return segmentSize;
            return RandomAccess.GetLength(GetOrAddHandle(segment).ReadHandle);
        }

        /// <summary>
        /// Close device
        /// </summary>
        public override void Dispose()
        {
            _disposed = true;
            DrainPendingIO();
            foreach (var entry in logHandles)
            {
                entry.Value.Dispose();
                if (deleteOnClose)
                    File.Delete(GetSegmentName(entry.Key));
            }
            pool.Free();
        }

        /// <summary>
        /// Wait briefly for in-flight IOs (tracked via <see cref="numPending"/>) to drain
        /// before disposing handles. Callers (the Tsavorite allocator) are expected to
        /// ensure no new IOs are issued before tear-down; this drain provides defence in
        /// depth against the small race between a thread reading a <see cref="SafeFileHandle"/>
        /// out of <see cref="logHandles"/> and that handle being disposed by us. The bounded
        /// wait avoids hanging Dispose forever if the caller's contract is violated.
        /// </summary>
        private void DrainPendingIO()
        {
            var deadline = Environment.TickCount64 + 5_000;
            var sw = new SpinWait();
            while (Volatile.Read(ref numPending) > 0 && Environment.TickCount64 < deadline)
                sw.SpinOnce();
        }

        private string GetSegmentName(int segmentId) => GetSegmentFilename(FileName, segmentId);

        private static uint GetSectorSize(string filename)
        {
            if (sectorSize <= 0)
                sectorSize = Native32.GetDeviceSectorSize(filename);
            return sectorSize;
        }

        private SafeFileHandle CreateReadHandle(int segmentId)
        {
            const int FILE_FLAG_NO_BUFFERING = 0x20000000;
            FileOptions fo =
                FileOptions.WriteThrough |
                FileOptions.Asynchronous |
                FileOptions.None;
            if (!osReadBuffering)
                fo |= (FileOptions)FILE_FLAG_NO_BUFFERING;

            // On Linux, FileOptions cannot express O_DIRECT, so File.OpenHandle would open the file through
            // the page cache even when FILE_FLAG_NO_BUFFERING is asked for (it is a Windows-only bit and is
            // silently dropped). Use libc open() to obtain a true O_DIRECT FD wrapped in a SafeFileHandle.
            // Falls back to File.OpenHandle on non-Linux or if O_DIRECT is unsupported by the underlying
            // filesystem. Both paths return a SafeFileHandle consumed by RandomAccess.{Read,Write}Async.
            SafeFileHandle logReadHandle;
            if (!TryOpenDirectHandle(segmentId, FileAccess.Read, createIfMissing: true, out logReadHandle))
            {
                logReadHandle = File.OpenHandle(
                    GetSegmentName(segmentId), FileMode.OpenOrCreate,
                    FileAccess.Read, readOnly ? FileShare.Read : FileShare.ReadWrite, fo);
            }

            return logReadHandle;
        }

        private SafeFileHandle CreateWriteHandle(int segmentId)
        {
            const int FILE_FLAG_NO_BUFFERING = 0x20000000;
            FileOptions fo =
                FileOptions.WriteThrough |
                FileOptions.Asynchronous |
                FileOptions.None;

            if (disableFileBuffering)
                fo |= (FileOptions)FILE_FLAG_NO_BUFFERING;

            SafeFileHandle logWriteHandle;
            if (!TryOpenDirectHandle(segmentId, FileAccess.Write, createIfMissing: true, out logWriteHandle))
            {
                logWriteHandle = File.OpenHandle(
                    GetSegmentName(segmentId), FileMode.OpenOrCreate,
                    FileAccess.Write, FileShare.ReadWrite, fo);
            }

            if (preallocateFile && segmentSize != -1)
                SetFileSize(logWriteHandle, segmentSize);

            return logWriteHandle;
        }

        // Cached probe result so we only test O_DIRECT capability once per device instance.
        private int directIOSupportedCached = -1; // -1 unknown, 0 no, 1 yes

        private bool TryOpenDirectHandle(int segmentId, FileAccess access, bool createIfMissing, out SafeFileHandle handle)
        {
            handle = null;
            if (disableFileBuffering == false && access == FileAccess.Write)
                return false;
            if (access == FileAccess.Read && osReadBuffering)
                return false;
            if (!OperatingSystem.IsLinux())
                return false;

            var directory = Path.GetDirectoryName(FileName);
            if (Volatile.Read(ref directIOSupportedCached) == -1)
            {
                var supported = LinuxFileExtensions.IsDirectIOSupported(directory) ? 1 : 0;
                _ = Interlocked.CompareExchange(ref directIOSupportedCached, supported, -1);
            }
            if (Volatile.Read(ref directIOSupportedCached) == 0)
                return false;

            try
            {
                handle = LinuxFileExtensions.OpenDirect(GetSegmentName(segmentId), access == FileAccess.Read ? FileAccess.Read : FileAccess.ReadWrite, createIfMissing);
                return true;
            }
            catch (IOException ex)
            {
                logger?.LogInformation(ex, "O_DIRECT open failed for segment {segmentId}; falling back to page-cached File.OpenHandle", segmentId);
                _ = Interlocked.Exchange(ref directIOSupportedCached, 0);
                return false;
            }
        }

        private SegmentHandles AddHandle(int _segmentId)
            => new SegmentHandles(() => CreateReadHandle(_segmentId), () => CreateWriteHandle(_segmentId));

        private SegmentHandles GetOrAddHandle(int _segmentId)
        {
            if (logHandles.TryGetValue(_segmentId, out var h))
            {
                return h;
            }
            var result = logHandles.GetOrAdd(_segmentId, AddHandle);

            if (_disposed)
            {
                // If disposed, dispose the segment handles and return the (disposed) result
                foreach (var entry in logHandles)
                {
                    entry.Value.Dispose();
                    if (deleteOnClose)
                        File.Delete(GetSegmentName(entry.Key));
                }
            }
            return result;
        }

        /// <summary>
        /// Sets file size on a SafeFileHandle. Uses RandomAccess.SetLength which works without
        /// wrapping the handle in a Stream.
        /// </summary>
        private static bool SetFileSize(SafeFileHandle logHandle, long size)
        {
            RandomAccess.SetLength(logHandle, size);
            return true;
        }
    }
}