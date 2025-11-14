// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace ReadFileServer
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
        private readonly bool deleteOnClose;
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
        /// <param name="capacity">The maximal number of bytes this storage device can accommondate, or CAPACITY_UNSPECIFIED if there is no such limit</param>
        /// <param name="recoverDevice">Whether to recover device metadata from existing files</param>
        /// <param name="osReadBuffering">Enable OS read buffering</param>
        /// <param name="readOnly">Open file in readOnly mode</param>
        public RandomAccessLocalStorageDevice(string filename, bool deleteOnClose = false, bool readOnly = false)
            : base(filename, GetSectorSize(filename))
        {
            pool = new(1, 1);

            string path = new FileInfo(filename).Directory.FullName;
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            _disposed = false;
            this.deleteOnClose = deleteOnClose;
            this.readOnly = readOnly;
            logHandles = new();
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

                // NOTE: If we change the above to this, then it will NOT hang:
                // _ = Task.Run(async () => await ReadWorkerAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context));
            }
        }

        //[AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
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

        private string GetSegmentName(int segmentId) => GetSegmentFilename(FileName, segmentId, false);

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

            fo |= (FileOptions)FILE_FLAG_NO_BUFFERING;

            var logWriteHandle = new FileStream(
                GetSegmentName(segmentId), FileMode.OpenOrCreate,
                FileAccess.Write, FileShare.ReadWrite, 512, fo);

            return new StorageAccessContext { handle = logWriteHandle, memoryManager = new UnmanagedMemoryManager<byte>() };
        }

        private (AsyncPool<StorageAccessContext>, AsyncPool<StorageAccessContext>) AddHandle(int _segmentId)
        {
            return (new AsyncPool<StorageAccessContext>(120, () => CreateReadHandle(_segmentId)), new AsyncPool<StorageAccessContext>(120, () => CreateWriteHandle(_segmentId)));
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