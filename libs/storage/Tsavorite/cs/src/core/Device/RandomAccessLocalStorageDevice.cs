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

namespace Tsavorite.core
{
    struct StorageAccessContext : IDisposable
    {
        public FileStream handle;
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
        public RandomAccessLocalStorageDevice(string filename, bool preallocateFile = false, bool deleteOnClose = false, bool disableFileBuffering = true, long capacity = Devices.CAPACITY_UNSPECIFIED, bool recoverDevice = false, bool osReadBuffering = false, bool readOnly = false)
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

                var task = storageAccessContextPool.GetAsync();
                if (task.IsCompletedSuccessfully)
                    storageAccessContext = task.Result;
                else
                    storageAccessContext = await task.ConfigureAwait(false);

                unsafe
                {
                    storageAccessContext.memoryManager.SetDestination((byte*)destinationAddress, (int)readLength);
                }
                numBytes = (uint)await RandomAccess.ReadAsync(storageAccessContext.handle.SafeFileHandle, storageAccessContext.memoryManager.Memory, (long)sourceAddress).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
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

                var task = storageAccessContextPool.GetAsync();
                if (task.IsCompletedSuccessfully)
                    storageAccessContext = task.Result;
                else
                    storageAccessContext = await task.ConfigureAwait(false);

                unsafe
                {
                    storageAccessContext.memoryManager.SetDestination((byte*)sourceAddress, (int)numBytesToWrite);
                }
                await RandomAccess.WriteAsync(storageAccessContext.handle.SafeFileHandle, storageAccessContext.memoryManager.Memory, (long)destinationAddress);
            }
            catch (Exception ex)
            {
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

            long size = stream.handle.Length;
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

            var logReadHandle = new FileStream(
                GetSegmentName(segmentId), FileMode.OpenOrCreate,
                FileAccess.Read, readOnly ? FileShare.Read : FileShare.ReadWrite, 512, fo);

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

            var logWriteHandle = new FileStream(
                GetSegmentName(segmentId), FileMode.OpenOrCreate,
                FileAccess.Write, FileShare.ReadWrite, 512, fo);

            if (preallocateFile && segmentSize != -1)
                SetFileSize(logWriteHandle, segmentSize);

            return new StorageAccessContext { handle = logWriteHandle, memoryManager = new UnmanagedMemoryManager<byte>() };
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
        /// Sets file size to the specified value.
        /// Does not reset file seek pointer to original location.
        /// </summary>
        /// <param name="logHandle"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        private static bool SetFileSize(Stream logHandle, long size)
        {
            logHandle.SetLength(size);
            return true;
        }
    }
}