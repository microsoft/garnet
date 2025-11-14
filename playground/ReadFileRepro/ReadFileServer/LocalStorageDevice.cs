// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace ReadFileServer
{
    /// <summary>
    /// Local storage device
    /// </summary>
    [System.Runtime.Versioning.SupportedOSPlatform("windows")]
    public unsafe class LocalStorageDevice : StorageDeviceBase
    {
        private readonly bool deleteOnClose;
        private readonly SafeConcurrentDictionary<int, SafeFileHandle> logHandles;
        private readonly ConcurrentQueue<SimpleAsyncResult> results;
        private static uint sectorSize = 0;
        private readonly bool useIoCompletionPort;
        private bool _disposed;
        readonly bool readOnly;
        private IntPtr ioCompletionPort;
        private const int NumCompletionThreads = 4;

        /// <summary>
        /// Number of pending reads on device
        /// </summary>
        private int numPending = 0;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="filename">File name (or prefix) with path</param>
        /// <param name="preallocateFile"></param>
        /// <param name="deleteOnClose"></param>
        /// <param name="disableFileBuffering">Whether file buffering (during write) is disabled (default of true requires aligned writes)</param>
        /// <param name="recoverDevice">Whether to recover device metadata from existing files</param>
        /// <param name="useIoCompletionPort">Whether we use IO completion port with polling</param>
        public LocalStorageDevice(string filename,
                                  bool preallocateFile = false,
                                  bool deleteOnClose = false,
                                  bool disableFileBuffering = true,
                                  bool useIoCompletionPort = false,
                                  bool readOnly = false)
            : this(filename, deleteOnClose, useIoCompletionPort, readOnly)
        {
        }

        void _callback(uint errorCode, uint numBytes, NativeOverlapped* pOVERLAP)
        {
            Interlocked.Decrement(ref numPending);
            var result = (SimpleAsyncResult)Overlapped.Unpack(pOVERLAP).AsyncResult;
            result.callback(errorCode, numBytes, result.context);
            results.Enqueue(result);
        }

        /// <summary>
        /// Constructor with more options for derived classes
        /// </summary>
        /// <param name="filename">File name (or prefix) with path</param>
        /// <param name="preallocateFile"></param>
        /// <param name="deleteOnClose"></param>
        /// <param name="disableFileBuffering"></param>
        /// <param name="capacity">The maximum number of bytes this storage device can accommodate, or CAPACITY_UNSPECIFIED if there is no such limit </param>
        /// <param name="useIoCompletionPort">Whether we use IO completion port with polling</param>
        protected internal LocalStorageDevice(string filename,
                                      bool deleteOnClose = false,
                                      bool useIoCompletionPort = false,
                                      bool readOnly = false)
                : base(filename, GetSectorSize(filename))
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                throw new Exception("Cannot use LocalStorageDevice from non-Windows OS platform, use ManagedLocalStorageDevice instead.");
            }

            if (filename.Length > Native32.WIN32_MAX_PATH - 11)     // -11 to allow for ".<segment>"
                throw new Exception($"Path {filename} is too long");

            _disposed = false;

            string path = new FileInfo(filename).Directory.FullName;
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);
            this.deleteOnClose = deleteOnClose;
            this.useIoCompletionPort = useIoCompletionPort;
            this.readOnly = readOnly;
            results = new ConcurrentQueue<SimpleAsyncResult>();

            logHandles = new SafeConcurrentDictionary<int, SafeFileHandle>();

            if (useIoCompletionPort)
            {
                ThreadPool.GetMaxThreads(out int workerThreads, out _);
                ioCompletionPort = Native32.CreateIoCompletionPort(new SafeFileHandle(new IntPtr(-1), false), IntPtr.Zero, UIntPtr.Zero, (uint)(workerThreads + NumCompletionThreads));
                for (int i = 0; i < NumCompletionThreads; i++)
                {
                    var thread = new Thread(() => LocalStorageDeviceCompletionWorker.Start(ioCompletionPort, _callback))
                    {
                        IsBackground = true
                    };
                    thread.Start();
                }
            }
        }

        /// <summary>
        /// Async read
        /// </summary>
        /// <param name="segmentId"></param>
        /// <param name="sourceAddress"></param>
        /// <param name="destinationAddress"></param>
        /// <param name="readLength"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public override void ReadAsync(int segmentId, ulong sourceAddress,
                                     IntPtr destinationAddress,
                                     uint readLength,
                                     DeviceIOCompletionCallback callback,
                                     object context)
        {
            SimpleAsyncResult result = default;
            try
            {
                if (!results.TryDequeue(out result))
                {
                    result = new SimpleAsyncResult();
                    result.overlapped = new Overlapped(0, 0, IntPtr.Zero, result);
                    result.nativeOverlapped = result.overlapped.UnsafePack(_callback, IntPtr.Zero);
                }

                result.context = context;
                result.callback = callback;
                var ovNative = result.nativeOverlapped;

                ovNative->OffsetLow = unchecked((int)((ulong)sourceAddress & 0xFFFFFFFF));
                ovNative->OffsetHigh = unchecked((int)(((ulong)sourceAddress >> 32) & 0xFFFFFFFF));

                var logHandle = GetOrAddHandle(segmentId);

                Interlocked.Increment(ref numPending);

                bool _result = Native32.ReadFile(logHandle,
                                                destinationAddress,
                                                readLength,
                                                out _,
                                                ovNative);

                if (!_result)
                {
                    int error = Marshal.GetLastWin32Error();
                    if (error != Native32.ERROR_IO_PENDING)
                    {
                        throw new IOException("Error reading from log file", error);
                    }
                }
            }
            catch (IOException e)
            {
                Interlocked.Decrement(ref numPending);
                callback((uint)(e.HResult & 0x0000FFFF), 0, context);
                results.Enqueue(result);
            }
            catch
            {
                Interlocked.Decrement(ref numPending);
                callback(uint.MaxValue, 0, context);
                results.Enqueue(result);
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
        public override unsafe void WriteAsync(IntPtr sourceAddress,
                                      int segmentId,
                                      ulong destinationAddress,
                                      uint numBytesToWrite,
                                      DeviceIOCompletionCallback callback,
                                      object context)
        {
            if (!results.TryDequeue(out SimpleAsyncResult result))
            {
                result = new SimpleAsyncResult();
                result.overlapped = new Overlapped(0, 0, IntPtr.Zero, result);
                result.nativeOverlapped = result.overlapped.UnsafePack(_callback, IntPtr.Zero);
            }

            result.context = context;
            result.callback = callback;
            var ovNative = result.nativeOverlapped;

            ovNative->OffsetLow = unchecked((int)(destinationAddress & 0xFFFFFFFF));
            ovNative->OffsetHigh = unchecked((int)((destinationAddress >> 32) & 0xFFFFFFFF));

            try
            {
                Interlocked.Increment(ref numPending);

                var logHandle = GetOrAddHandle(segmentId);

                bool _result = Native32.WriteFile(logHandle,
                                        sourceAddress,
                                        numBytesToWrite,
                                        out _,
                                        ovNative);

                if (!_result)
                {
                    int error = Marshal.GetLastWin32Error();
                    if (error != Native32.ERROR_IO_PENDING)
                    {
                        throw new IOException("Error writing to log file", error);
                    }
                }
            }
            catch (IOException e)
            {
                Interlocked.Decrement(ref numPending);
                callback((uint)(e.HResult & 0x0000FFFF), 0, context);
                results.Enqueue(result);
            }
            catch
            {
                Interlocked.Decrement(ref numPending);
                callback(uint.MaxValue, 0, context);
                results.Enqueue(result);
            }
        }

        /// <summary>
        /// Close device
        /// </summary>
        public override void Dispose()
        {
            _disposed = true;
            foreach (var logHandle in logHandles.Values)
                logHandle.Dispose();

            if (useIoCompletionPort)
                new SafeFileHandle(ioCompletionPort, true).Dispose();

            while (results.TryDequeue(out var entry))
            {
                Overlapped.Free(entry.nativeOverlapped);
            }
        }

        private SafeFileHandle CreateHandle(int segmentId, bool disableFileBuffering, bool deleteOnClose, string fileName)
            => CreateHandle(segmentId, disableFileBuffering, deleteOnClose, fileName, ioCompletionPort, omitSegmentId: OmitSegmentIdFromFileName, readOnly: readOnly);

        /// <summary>
        /// Creates a SafeFileHandle for the specified segment. This can be used by derived classes to prepopulate logHandles in the constructor.
        /// </summary>
        protected internal static SafeFileHandle CreateHandle(int segmentId, bool disableFileBuffering, bool deleteOnClose, string fileName, IntPtr ioCompletionPort, bool omitSegmentId = false, bool readOnly = false)
        {
            uint fileAccess = readOnly ? Native32.GENERIC_READ : Native32.GENERIC_READ | Native32.GENERIC_WRITE;
            uint fileShare = unchecked(((uint)FileShare.ReadWrite & ~(uint)FileShare.Inheritable));
            uint fileCreation = unchecked((uint)FileMode.OpenOrCreate);
            uint fileFlags = Native32.FILE_FLAG_OVERLAPPED;

            if (disableFileBuffering)
            {
                fileFlags |= Native32.FILE_FLAG_NO_BUFFERING;
            }

            if (deleteOnClose)
            {
                fileFlags |= Native32.FILE_FLAG_DELETE_ON_CLOSE;

                // FILE_SHARE_DELETE allows multiple Tsavorite instances to share a single log directory and each can specify deleteOnClose.
                // This will allow the files to persist until all handles across all instances have been closed.
                fileShare |= Native32.FILE_SHARE_DELETE;
            }

            string segmentFileName = GetSegmentFilename(fileName, segmentId, omitSegmentId);
            var logHandle = Native32.CreateFileW(
                segmentFileName,
                fileAccess, fileShare,
                IntPtr.Zero, fileCreation,
                fileFlags, IntPtr.Zero);

            if (logHandle.IsInvalid)
            {
                var error = Marshal.GetLastWin32Error();
                var message = $"Error creating log file for {segmentFileName}, error: {error} 0x({Native32.MakeHRFromErrorCode(error)})";
                if (error == Native32.ERROR_PATH_NOT_FOUND)
                    message += $" (Path not found; name length = {segmentFileName.Length}, MAX_PATH = {Native32.WIN32_MAX_PATH}";
                throw new IOException(message);
            }

            if (ioCompletionPort != IntPtr.Zero)
            {
                ThreadPool.GetMaxThreads(out int workerThreads, out _);
                Native32.CreateIoCompletionPort(logHandle, ioCompletionPort, (UIntPtr)(long)logHandle.DangerousGetHandle(), (uint)(workerThreads + NumCompletionThreads));
            }
            else
            {
                try
                {
                    ThreadPool.BindHandle(logHandle);
                }
                catch (Exception e)
                {
                    throw new Exception("Error binding log handle for " + segmentFileName + ": " + e.ToString());
                }
            }
            return logHandle;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="_segmentId"></param>
        /// <returns></returns>
        // Can be used to pre-load handles, e.g., after a checkpoint
        protected SafeFileHandle GetOrAddHandle(int _segmentId)
        {
            if (logHandles.TryGetValue(_segmentId, out SafeFileHandle h))
            {
                return h;
            }
            if (_disposed) return null;
            var result = logHandles.GetOrAdd(_segmentId, CreateHandle);
            if (_disposed)
            {
                foreach (var logHandle in logHandles.Values)
                    logHandle.Dispose();
                return null;
            }
            return result;
        }

        private SafeFileHandle CreateHandle(int segmentId)
            => CreateHandle(segmentId, true, deleteOnClose, FileName);

        private static uint GetSectorSize(string filename)
        {
            if (sectorSize > 0) return sectorSize;
            if (!Native32.GetDiskFreeSpace(filename.Substring(0, 3), out _, out sectorSize, out _, out _))
            {
                Debug.WriteLine("Unable to retrieve information for disk " + filename.Substring(0, 3) + " - check if the disk is available and you have specified the full path with drive name. Assuming sector size of 512 bytes.");
                sectorSize = 512;
            }
            return sectorSize;
        }
    }

    [System.Runtime.Versioning.SupportedOSPlatform("windows")]
    sealed unsafe class LocalStorageDeviceCompletionWorker
    {
        public static void Start(IntPtr ioCompletionPort, IOCompletionCallback _callback)
        {
            while (true)
            {
                Thread.Yield();
                bool succeeded = Native32.GetQueuedCompletionStatus(ioCompletionPort, out uint num_bytes, out _, out NativeOverlapped* nativeOverlapped, uint.MaxValue);

                if (nativeOverlapped != null)
                {
                    int errorCode = succeeded ? 0 : Marshal.GetLastWin32Error();
                    _callback((uint)errorCode, num_bytes, nativeOverlapped);
                }
                else
                    break;
            }
        }
    }
}