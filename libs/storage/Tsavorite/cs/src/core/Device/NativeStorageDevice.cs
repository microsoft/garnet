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
            IntPtr libHandle = IntPtr.Zero;
            if (libraryName == NativeLibraryName && NativeLibraryPath != null)
                libHandle = NativeLibrary.Load(NativeLibraryPath);
            return libHandle;
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
                Interlocked.Decrement(ref numPending);
                callback((uint)(e.HResult & 0x0000FFFF), 0, context);
                freeResults.Enqueue(offset);
            }
            catch
            {
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
                Interlocked.Decrement(ref numPending);
                callback((uint)(e.HResult & 0x0000FFFF), 0, context);
            }
            catch
            {
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