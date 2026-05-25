// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Win32.SafeHandles;

namespace Tsavorite.core
{
    struct StorageAccessContext : IDisposable
    {
        public SafeFileHandle handle;
        public UnmanagedMemoryManager<byte> memoryManager;

        public void Dispose()
        {
            handle?.Dispose();
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
        private readonly SafeConcurrentDictionary<int, (AsyncPool<StorageAccessContext>, AsyncPool<StorageAccessContext>)> logHandles;
        private readonly SectorAlignedBufferPool pool;

        /// <summary>
        /// Number of pending reads on device
        /// </summary>
        private int numPending = 0;

        private bool _disposed;

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
            while (logHandles.Count > 0)
            {
                foreach (var entry in logHandles)
                {
                    if (logHandles.TryRemove(entry.Key, out _))
                    {
                        entry.Value.Item1.Dispose();
                        entry.Value.Item2.Dispose();
                        if (deleteOnClose)
                            File.Delete(GetSegmentName(entry.Key));
                    }
                }
            }
        }

        /// <inheritdoc />
        // We do not throttle RandomAccessLocalStorageDevice because our AsyncPool of handles takes care of this
        public override bool Throttle() => false;

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
            EnsureInitialized();
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
            StorageAccessContext storageAccessContext = default;
            AsyncPool<StorageAccessContext> storageAccessContextPool = default;

            try
            {
                _ = Interlocked.Increment(ref numPending);
                storageAccessContextPool = GetOrAddHandle(segmentId).Item1;

                storageAccessContext = await storageAccessContextPool.GetAsync().ConfigureAwait(false);

                unsafe
                {
                    storageAccessContext.memoryManager.SetDestination((byte*)destinationAddress, (int)readLength);
                }
                numBytes = (uint)await RandomAccess.ReadAsync(storageAccessContext.handle, storageAccessContext.memoryManager.Memory, (long)sourceAddress).ConfigureAwait(false);
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
                _ = Interlocked.Decrement(ref numPending);
                // Sequentialize all reads from same handle
                if (storageAccessContextPool != null && storageAccessContext.handle != default)
                    storageAccessContextPool.Return(storageAccessContext);
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
            EnsureInitialized();
            using (ExecutionContext.SuppressFlow())
            {
                _ = WriteWorkerAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);
            }
        }

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        async ValueTask WriteWorkerAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            uint errorCode = 0;
            StorageAccessContext storageAccessContext = default;
            AsyncPool<StorageAccessContext> storageAccessContextPool = default;

            HandleCapacity(segmentId);

            try
            {
                _ = Interlocked.Increment(ref numPending);
                storageAccessContextPool = GetOrAddHandle(segmentId).Item2;

                storageAccessContext = await storageAccessContextPool.GetAsync().ConfigureAwait(false);

                unsafe
                {
                    storageAccessContext.memoryManager.SetDestination((byte*)sourceAddress, (int)numBytesToWrite);
                }
                await RandomAccess.WriteAsync(storageAccessContext.handle, storageAccessContext.memoryManager.Memory, (long)destinationAddress).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger?.LogCritical(ex, $"{nameof(ReadAsync)}");
                var ioex = ex as IOException ?? ex.InnerException as IOException;
                if (ioex is not null)
                    errorCode = (uint)(ioex.HResult & 0x0000FFFF);
                else
                    errorCode = uint.MaxValue;
                numBytesToWrite = 0;
            }
            finally
            {
                _ = Interlocked.Decrement(ref numPending);
                // Sequentialize all writes to same handle
                if (storageAccessContextPool != null && storageAccessContext.handle != default)
                    storageAccessContextPool.Return(storageAccessContext);
                // Issue user callback
                callback(errorCode, numBytesToWrite, context);
            }
        }

        /// <summary>
        /// <see cref="IDevice.RemoveSegment(int)"/>
        /// </summary>
        /// <param name="segment"></param>
        public override void RemoveSegment(int segment)
        {
            EnsureInitialized();
            if (logHandles.TryRemove(segment, out (AsyncPool<StorageAccessContext>, AsyncPool<StorageAccessContext>) logHandle))
            {
                logHandle.Item1.Dispose();
                logHandle.Item2.Dispose();
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
            EnsureInitialized();
            RemoveSegment(segment);
            callback(result);
        }

        /// <inheritdoc/>
        public override long GetFileSize(int segment)
        {
            if (segmentSize > 0) return segmentSize;
            var pool = GetOrAddHandle(segment);
            if (!pool.Item1.TryGet(out var stream))
                stream = pool.Item1.Get();

            long size = RandomAccess.GetLength(stream.handle);
            pool.Item1.Return(stream);
            return size;
        }

        /// <summary>
        /// Close device
        /// </summary>
        public override void Dispose()
        {
            _disposed = true;
            foreach (var entry in logHandles)
            {
                entry.Value.Item1.Dispose();
                entry.Value.Item2.Dispose();
                if (deleteOnClose)
                    File.Delete(GetSegmentName(entry.Key));
            }
            pool.Free();
        }

        private string GetSegmentName(int segmentId) => GetSegmentFilename(FileName, segmentId);

        private static uint GetSectorSize(string filename)
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Debug.WriteLine("Assuming 512 byte sector alignment for disk with file " + filename);
                return 512;
            }

            if (!Native32.GetDiskFreeSpace(filename.Substring(0, 3), out _, out uint _sectorSize, out _, out _))
            {
                Debug.WriteLine("Unable to retrieve information for disk " + filename.Substring(0, 3) + " - check if the disk is available and you have specified the full path with drive name. Assuming sector size of 512 bytes.");
                _sectorSize = 512;
            }
            return _sectorSize;
        }

        private StorageAccessContext CreateReadHandle(int segmentId)
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

            return new StorageAccessContext { handle = logReadHandle, memoryManager = new UnmanagedMemoryManager<byte>() };
        }

        private StorageAccessContext CreateWriteHandle(int segmentId)
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

            return new StorageAccessContext { handle = logWriteHandle, memoryManager = new UnmanagedMemoryManager<byte>() };
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

        private (AsyncPool<StorageAccessContext>, AsyncPool<StorageAccessContext>) AddHandle(int _segmentId)
        {
            return (new AsyncPool<StorageAccessContext>(ThrottleLimit, () => CreateReadHandle(_segmentId)), new AsyncPool<StorageAccessContext>(ThrottleLimit, () => CreateWriteHandle(_segmentId)));
        }

        private (AsyncPool<StorageAccessContext>, AsyncPool<StorageAccessContext>) GetOrAddHandle(int _segmentId)
        {
            if (logHandles.TryGetValue(_segmentId, out var h))
            {
                return h;
            }
            var result = logHandles.GetOrAdd(_segmentId, AddHandle);

            if (_disposed)
            {
                // If disposed, dispose the fixed pools and return the (disposed) result
                foreach (var entry in logHandles)
                {
                    entry.Value.Item1.Dispose();
                    entry.Value.Item2.Dispose();
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