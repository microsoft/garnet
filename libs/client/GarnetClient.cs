// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using HdrHistogram;
using Microsoft.Extensions.Logging;

namespace Garnet.client
{
    struct OK_MEM : IMemoryOwner<byte>
    {
        static readonly Memory<byte> RESP_OK = "OK"u8.ToArray();
        public Memory<byte> Memory => RESP_OK;
        public void Dispose() { }
    }

    /// <summary>
    /// Garnet client (makes a single network connection to server)
    /// </summary>
    public sealed partial class GarnetClient : IServerHook, IMessageConsumer, IDisposable
    {
        static readonly Memory<byte> GET = "$3\r\nGET\r\n"u8.ToArray();
        static readonly Memory<byte> MGET = "$4\r\nMGET\r\n"u8.ToArray();
        static readonly Memory<byte> SET = "$3\r\nSET\r\n"u8.ToArray();
        static readonly Memory<byte> DEL = "$3\r\nDEL\r\n"u8.ToArray();
        static readonly Memory<byte> PING = "$4\r\nPING\r\n"u8.ToArray();
        static readonly Memory<byte> INCR = "$4\r\nINCR\r\n"u8.ToArray();
        static readonly Memory<byte> INCRBY = "$6\r\nINCRBY\r\n"u8.ToArray();
        static readonly Memory<byte> DECR = "$4\r\nDECR\r\n"u8.ToArray();
        static readonly Memory<byte> DECRBY = "$6\r\nDECRBY\r\n"u8.ToArray();
        static readonly Memory<byte> QUIT = "$4\r\nQUIT\r\n"u8.ToArray();
        static readonly Memory<byte> AUTH = "$4\r\nAUTH\r\n"u8.ToArray();
        static readonly MemoryResult<byte> RESP_OK = new(default(OK_MEM));

        readonly string address;
        readonly int port;
        readonly int sendPageSize;
        readonly int maxOutstandingTasks;
        NetworkWriter networkWriter;
        INetworkSender networkSender;

        readonly TcsWrapper[] tcsArray;
        readonly SslClientAuthenticationOptions sslOptions;
        readonly MemoryPool<byte> memoryPool;
        GarnetClientTcpNetworkHandler networkHandler;
        int tcsOffset;

        Socket socket;
        int disposed;

        /// <inheritdoc />
        public bool Disposed => disposed > 0;

        readonly ILogger logger;

        /// <summary>
        /// CTS to allow cancellation of the timeout checker background task, called during Dispose
        /// </summary>
        readonly CancellationTokenSource timeoutCheckerCts;

        /// <summary>
        /// Timeout in milliseconds (0 for no timeout)
        /// </summary>
        readonly int timeoutMilliseconds;

        /// <summary>
        /// Max outstanding network sends allowed
        /// </summary>
        readonly int networkSendThrottleMax;

        /// <summary>
        /// Username to authenticate client on server.
        /// </summary>
        readonly string authUsername = null;

        /// <summary>
        /// Password to authenticate client on server.
        /// </summary>
        readonly string authPassword = null;

        /// <summary>
        /// Exception to throw to ongoing tasks when disposed
        /// </summary>
        static readonly Exception disposeException = new GarnetClientDisposedException();

        /// <summary>
        /// Whether we are connected to the server
        /// </summary>
        public bool IsConnected => socket != null && socket.Connected && !Disposed;

        /// <summary>
        /// Get the max number of allowed outstanding tasks.
        /// </summary>
        public int GetOutstandingTasksLimit => maxOutstandingTasks;

        /// <summary>
        /// Get the send page size.
        /// </summary>
        public int SendPageSize => sendPageSize;

        /// <summary>
        /// Create client instance
        /// </summary>
        /// <param name="address">IP address of server</param>
        /// <param name="port">Port of server</param>
        /// <param name="tlsOptions">TLS options</param>
        /// <param name="authUsername">Username to authenticate with</param>
        /// <param name="authPassword">Password to authenticate with</param>
        /// <param name="sendPageSize">Size of pages where requests are written to be sent, determines max request size (rounds down to previous power of 2)</param>
        /// <param name="maxOutstandingTasks">Maximum outstanding tasks before client throttles new requests (rounds down to previous power of 2), default 32K</param>
        /// <param name="timeoutMilliseconds">Timeout (in milliseconds) after which client disposes itself and throws exception on all active tasks</param>
        /// <param name="memoryPool">Pool for Memory based response buffers</param>
        /// <param name="recordLatency">Record latency using client internal histogram</param>
        /// <param name="useTimeoutChecker"></param>
        /// <param name="networkSendThrottleMax">Max outstanding network sends allowed</param>
        /// <param name="logger">Logger instance</param>
        public GarnetClient(
            string address,
            int port,
            SslClientAuthenticationOptions tlsOptions = null,
            string authUsername = null,
            string authPassword = null,
            int sendPageSize = 1 << 21,
            int maxOutstandingTasks = 1 << 19,
            int timeoutMilliseconds = 0,
            MemoryPool<byte> memoryPool = null,
            bool recordLatency = false,
            bool useTimeoutChecker = true,
            int networkSendThrottleMax = 8,
            ILogger logger = null)
        {
            this.address = address;
            this.port = port;
            this.sendPageSize = (int)Utility.PreviousPowerOf2(sendPageSize);
            this.authUsername = authUsername;
            this.authPassword = authPassword;

            if (maxOutstandingTasks > PageOffset.kTaskMask + 1)
            {
                ThrowException(new Exception($"Maximum outstanding tasks supported is {PageOffset.kTaskMask + 1}"));
            }

            if (maxOutstandingTasks != (int)Utility.PreviousPowerOf2(maxOutstandingTasks))
            {
                ThrowException(new Exception($"Maximum outstanding tasks should be a power of two, up to {PageOffset.kTaskMask + 1}"));
            }

            this.maxOutstandingTasks = maxOutstandingTasks;
            this.sslOptions = tlsOptions;
            this.disposed = 0;
            this.tcsArray = new TcsWrapper[maxOutstandingTasks];
            this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
            this.logger = logger;
            this.latency = recordLatency ? new LongHistogram(1, TimeStamp.Seconds(100), 2) : null;
            this.timeoutMilliseconds = timeoutMilliseconds;
            if (timeoutMilliseconds > 0 && useTimeoutChecker)
                timeoutCheckerCts = new();
            this.networkSendThrottleMax = networkSendThrottleMax;
            for (int i = 0; i < maxOutstandingTasks; i++)
                tcsArray[i].nextTaskId = i;
        }

        /// <summary>
        /// Finalizer
        /// </summary>
        ~GarnetClient()
        {
            Dispose(false);
        }

        /// <summary>
        /// Connect to server
        /// </summary>
        public void Connect(CancellationToken token = default)
        {
            socket = GetSendSocket(timeoutMilliseconds);
            networkWriter = new NetworkWriter(this, socket, 1 << 17, sslOptions, out networkHandler, sendPageSize, networkSendThrottleMax, logger);
            networkHandler.StartAsync(sslOptions, $"{address}:{port}", token).GetAwaiter().GetResult();
            networkSender = networkHandler.GetNetworkSender();

            if (timeoutMilliseconds > 0)
            {
                Task.Run(TimeoutChecker);
            }

            try
            {
                if (authUsername != null)
                {
                    ExecuteForStringResultAsync(AUTH, authUsername, authPassword == null ? "" : authPassword).GetAwaiter().GetResult();
                }
                else if (authPassword != null)
                {
                    ExecuteForStringResultAsync(AUTH, authPassword).GetAwaiter().GetResult();
                }
            }
            catch (Exception e)
            {
                logger?.LogError(e, "AUTH returned error");
                throw;
            }
        }

        /// <summary>
        /// Connect to server
        /// </summary>
        public async Task ConnectAsync(CancellationToken token = default)
        {
            socket = GetSendSocket(timeoutMilliseconds);
            networkWriter = new NetworkWriter(this, socket, 1 << 17, sslOptions, out networkHandler, sendPageSize, networkSendThrottleMax, logger);
            await networkHandler.StartAsync(sslOptions, $"{address}:{port}", token).ConfigureAwait(false);
            networkSender = networkHandler.GetNetworkSender();

            if (timeoutMilliseconds > 0)
            {
                _ = Task.Run(TimeoutChecker);
            }

            try
            {
                if (authUsername != null)
                {
                    await ExecuteForStringResultAsync(AUTH, authUsername, authPassword == null ? "" : authPassword);
                }
                else if (authPassword != null)
                {
                    await ExecuteForStringResultAsync(AUTH, authPassword);
                }
            }
            catch (Exception e)
            {
                logger?.LogError(e, "AUTH returned error");
                throw;
            }
        }

        Socket GetSendSocket(int millisecondsTimeout = 0)
        {
            _ = Format.TryParseEndPoint($"{address}:{port}", out var endPoint);

            var socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true
            };

            if (millisecondsTimeout > 0)
            {
                IAsyncResult result = socket.BeginConnect(endPoint, null, null);
                result.AsyncWaitHandle.WaitOne(millisecondsTimeout, true);

                if (socket.Connected)
                {
                    socket.EndConnect(result);
                }
                else
                {
                    socket.Close();
                    throw new Exception($"Failed to connect server {address}:{port}.");
                }
            }
            else
            {
                socket.Connect(endPoint);
            }

            return socket;
        }

        async Task TimeoutChecker()
        {
            try
            {
                var token = timeoutCheckerCts.Token;
                while (!token.IsCancellationRequested)
                {
                    var _tcsOffset = tcsOffset;
                    var _tailAddress = networkWriter.GetTailAddress();

                    await Task.Delay(timeoutMilliseconds, token);
                    // Check if no new tasks added + no new results processed
                    var _newTcsOffset = tcsOffset;
                    var _newNextTaskId = networkWriter.GetNextTaskId();
                    var _newTailAddress = networkWriter.GetTailAddress();

                    // If new responses were processed, continue
                    if (_newTcsOffset != _tcsOffset)
                        continue;

                    // If we sent more data, continue
                    if (_newTailAddress != _tailAddress)
                        continue;

                    // If we are all caught up, continue
                    if (_newTcsOffset == _newNextTaskId)
                        continue;

                    // Timeout all ongoing tasks, dispose socket and client
                    Dispose();
                    break;
                }
            }
            catch { }
        }

        /// <summary>
        /// Reconnect to server
        /// </summary>
        public void Reconnect(CancellationToken token = default)
        {
            if (Disposed) throw disposeException;
            try
            {
                socket?.Dispose();
                networkWriter?.Dispose();
            }
            catch { }
            Connect(token);
        }

        /// <summary>
        /// Reconnect to server
        /// </summary>
        public async Task ReconnectAsync(CancellationToken token = default)
        {
            if (Disposed) throw disposeException;
            try
            {
                socket?.Dispose();
                networkWriter?.Dispose();
            }
            catch { }
            await ConnectAsync(token);
        }

        /// <summary>
        /// Dispose instance
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

#pragma warning disable IDE0060 // Remove unused parameter
        void Dispose(bool disposing)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            if (Interlocked.Increment(ref disposed) > 1) return;

            timeoutCheckerCts?.Cancel();
            socket?.Dispose();
            networkWriter?.Dispose();
        }

        void CheckLength(int totalLen, TcsWrapper tcs)
        {
            if (totalLen > networkWriter.PageSize)
            {
                var e = new Exception($"Entry of size {totalLen} does not fit on page of size {networkWriter.PageSize}. Try increasing sendPageSize parameter to GarnetClient constructor.");
                switch (tcs.taskType)
                {
                    case TaskType.StringAsync:
                        tcs.stringTcs.TrySetException(e);
                        break;
                    case TaskType.LongAsync:
                        tcs.longTcs.TrySetException(e);
                        break;
                    case TaskType.StringArrayAsync:
                        tcs.stringArrayTcs.TrySetException(e);
                        break;
                    case TaskType.MemoryByteAsync:
                        tcs.memoryByteTcs.TrySetException(e);
                        break;
                    case TaskType.MemoryByteArrayAsync:
                        tcs.memoryByteArrayTcs.TrySetException(e);
                        break;
                }
                ThrowException(e);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long GetTimestamp() => latency != null ? Stopwatch.GetTimestamp() : 0;

        /// <summary>
        /// Get estimated number of outstanding tasks.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int PipelineLength()
        {
            var _tcsOffset = tcsOffset & (int)PageOffset.kTaskMask;
            var _nextTaskId = networkWriter.GetNextTaskId() & (int)PageOffset.kTaskMask;

            return _nextTaskId >= _tcsOffset ?
                _nextTaskId - _tcsOffset :
                _nextTaskId + ((int)PageOffset.kTaskMask - _tcsOffset);
        }

        async ValueTask InputGateAsync(CancellationToken token = default)
        {
            int delayMs = 0;
            while (true)
            {
                if (PipelineLength() < maxOutstandingTasks)
                    break;
                await Task.Delay(delayMs, token);
                if (delayMs == 0) delayMs = 1;
                else delayMs *= 2;
                if (delayMs > 4096) delayMs = 4096;
            }
        }

        async ValueTask AwaitPreviousTaskAsync(int taskId)
        {
            int shortTaskId = taskId & (maxOutstandingTasks - 1);
            var oldTcs = tcsArray[shortTaskId];
            while (oldTcs.taskType != TaskType.None || !oldTcs.IsNext(taskId))
            {
                logger?.LogDebug("Task {taskId} waiting for slot of task {oldTaskId}", taskId, oldTcs.nextTaskId);
                try
                {
                    switch (oldTcs.taskType)
                    {
                        case TaskType.None:
                            await Task.Yield();
                            break;
                        case TaskType.StringCallback:
                        case TaskType.MemoryByteCallback:
                            while (tcsArray[shortTaskId].taskType != TaskType.None) await Task.Yield();
                            break;
                        case TaskType.StringAsync:
                            if (oldTcs.stringTcs != null) await oldTcs.stringTcs.Task.ConfigureAwait(false);
                            break;
                        case TaskType.MemoryByteAsync:
                            if (oldTcs.memoryByteTcs != null) await oldTcs.memoryByteTcs.Task.ConfigureAwait(false);
                            break;
                        case TaskType.StringArrayAsync:
                            if (oldTcs.stringArrayTcs != null) await oldTcs.stringArrayTcs.Task.ConfigureAwait(false);
                            break;
                        case TaskType.MemoryByteArrayAsync:
                            if (oldTcs.memoryByteArrayTcs != null) await oldTcs.memoryByteArrayTcs.Task.ConfigureAwait(false);
                            break;
                    }
                }
                catch
                {
                    if (Disposed) ThrowException(disposeException);
                }
                await Task.Yield();
                oldTcs = tcsArray[shortTaskId];
            }
        }

        async ValueTask InternalExecuteAsync(TcsWrapper tcs, Memory<byte> op, string param1 = null, string param2 = null, CancellationToken token = default)
        {
            tcs.timestamp = GetTimestamp();
            int totalLen = 0;
            int arraySize = 1;

            totalLen += op.Length;

            if (param1 != null)
            {
                int len = Encoding.UTF8.GetByteCount(param1);
                totalLen += 1 + NumUtils.NumDigits(len) + 2 + len + 2;
                arraySize++;
            }
            if (param2 != null)
            {
                int len = Encoding.UTF8.GetByteCount(param2);
                totalLen += 1 + NumUtils.NumDigits(len) + 2 + len + 2;
                arraySize++;
            }

            totalLen += 1 + NumUtils.NumDigits(arraySize) + 2;
            CheckLength(totalLen, tcs);
            await InputGateAsync(token);

            try
            {
                networkWriter.epoch.Resume();

                #region reserveSpaceAndWriteIntoNetworkBuffer
                int taskId;
                long address;
                while (true)
                {
                    token.ThrowIfCancellationRequested();
                    if (!IsConnected)
                    {
                        Dispose();
                        ThrowException(disposeException);
                    }
                    (taskId, address) = networkWriter.TryAllocate(totalLen, out var flushEvent);
                    if (address >= 0) break;
                    try
                    {
                        networkWriter.epoch.Suspend();
                        await flushEvent.WaitAsync(token).ConfigureAwait(false);
                    }
                    finally
                    {
                        networkWriter.epoch.Resume();
                    }
                }

                // Console.WriteLine($"Allocated {taskId} @ {address}");
                tcs.nextTaskId = taskId;

                unsafe
                {
                    byte* curr = (byte*)networkWriter.GetPhysicalAddress(address);
                    byte* end = curr + totalLen;
                    RespWriteUtils.WriteArrayLength(arraySize, ref curr, end);

                    RespWriteUtils.WriteDirect(op.Span, ref curr, end);
                    if (param1 != null)
                        RespWriteUtils.WriteBulkString(param1, ref curr, end);
                    if (param2 != null)
                        RespWriteUtils.WriteBulkString(param2, ref curr, end);

                    Debug.Assert(curr == end);
                }
                #endregion

                #region waitForEmptySlot
                int shortTaskId = taskId & (maxOutstandingTasks - 1);
                var oldTcs = tcsArray[shortTaskId];
                //1. if taskType != None, we are waiting for previous task to finish
                //2. if taskType == None and my taskId is not the next in line wait for previous task to acquire slot
                if (oldTcs.taskType != TaskType.None || !oldTcs.IsNext(taskId))
                {
                    // Console.WriteLine($"Before filling slot {taskId & (maxOutstandingTasks - 1)} for task {taskId} @ {address} : {tcs.taskType}");
                    networkWriter.epoch.ProtectAndDrain();
                    networkWriter.DoAggressiveShiftReadOnly();
                    try
                    {
                        networkWriter.epoch.Suspend();
                        await AwaitPreviousTaskAsync(taskId); // does not take token, as task is not cancelable at this point
                    }
                    finally
                    {
                        networkWriter.epoch.Resume();
                    }
                }
                #endregion

                #region scheduleAwaitForResponse
                // Console.WriteLine($"Filled slot {taskId & (maxOutstandingTasks - 1)} for task {taskId} @ {address} : {tcs.taskType}");
                tcsArray[shortTaskId].LoadFrom(tcs);
                if (Disposed)
                {
                    DisposeOffset(shortTaskId);
                    ThrowException(disposeException);
                }
                // Console.WriteLine($"Filled {address}-{address + totalLen}");
                networkWriter.epoch.ProtectAndDrain();
                networkWriter.DoAggressiveShiftReadOnly();
                #endregion
            }
            finally
            {
                networkWriter.epoch.Suspend();
            }
        }

        async ValueTask InternalExecuteAsync(Memory<byte> op, Memory<byte> clusterOp, string nodeId, long currentAddress, long nextAddress, long payloadPtr, int payloadLength, CancellationToken token = default)
        {
            Debug.Assert(nodeId != null);

            int totalLen = 0;
            int arraySize = 1;

            totalLen += op.Length;

            int len = clusterOp.Length;
            totalLen += 1 + NumUtils.NumDigits(len) + 2 + len + 2;
            arraySize++;

            len = Encoding.UTF8.GetByteCount(nodeId);
            totalLen += 1 + NumUtils.NumDigits(len) + 2 + len + 2;
            arraySize++;

            len = NumUtils.NumDigitsInLong(currentAddress);
            totalLen += 1 + NumUtils.NumDigits(len) + 2 + len + 2;
            arraySize++;

            len = NumUtils.NumDigitsInLong(nextAddress);
            totalLen += 1 + NumUtils.NumDigits(len) + 2 + len + 2;
            arraySize++;

            len = payloadLength;
            totalLen += 1 + NumUtils.NumDigits(len) + 2 + len + 2;
            arraySize++;

            totalLen += 1 + NumUtils.NumDigits(arraySize) + 2;

            if (totalLen > networkWriter.PageSize)
            {
                ThrowException(new Exception($"Entry of size {totalLen} does not fit on page of size {networkWriter.PageSize}. Try increasing sendPageSize parameter to GarnetClient constructor."));
            }

            // No need for gate as this is a void return
            // await InputGateAsync(token);

            try
            {
                networkWriter.epoch.Resume();

                #region reserveSpaceAndWriteIntoNetworkBuffer
                int taskId;
                long address;
                while (true)
                {
                    token.ThrowIfCancellationRequested();
                    if (!IsConnected)
                    {
                        Dispose();
                        ThrowException(disposeException);
                    }
                    (taskId, address) = networkWriter.TryAllocate(totalLen, out var flushEvent);
                    if (address >= 0) break;
                    try
                    {
                        networkWriter.epoch.Suspend();
                        await flushEvent.WaitAsync(token).ConfigureAwait(false);
                    }
                    finally
                    {
                        networkWriter.epoch.Resume();
                    }
                }

                unsafe
                {
                    byte* curr = (byte*)networkWriter.GetPhysicalAddress(address);
                    byte* end = curr + totalLen;
                    RespWriteUtils.WriteArrayLength(arraySize, ref curr, end);

                    RespWriteUtils.WriteDirect(op.Span, ref curr, end);
                    RespWriteUtils.WriteBulkString(clusterOp.Span, ref curr, end);
                    RespWriteUtils.WriteBulkString(nodeId, ref curr, end);
                    RespWriteUtils.WriteArrayItem(currentAddress, ref curr, end);
                    RespWriteUtils.WriteArrayItem(nextAddress, ref curr, end);
                    RespWriteUtils.WriteBulkString(new Span<byte>((void*)payloadPtr, payloadLength), ref curr, end);

                    Debug.Assert(curr == end);
                }
                #endregion

                if (!IsConnected)
                {
                    Dispose();
                    ThrowException(disposeException);
                }
                // Console.WriteLine($"Filled {address}-{address + totalLen}");
                networkWriter.epoch.ProtectAndDrain();
                networkWriter.DoAggressiveShiftReadOnly();
            }
            finally
            {
                networkWriter.epoch.Suspend();
            }
            return;
        }

        async ValueTask InternalExecuteAsync(TcsWrapper tcs, Memory<byte> op, Memory<byte> param1, Memory<byte> param2, CancellationToken token = default)
        {
            tcs.timestamp = GetTimestamp();
            int totalLen = 0;
            int arraySize = 1;

            totalLen += op.Length;

            if (!param1.IsEmpty)
            {
                int len = param1.Length;
                totalLen += 1 + NumUtils.NumDigits(len) + 2 + len + 2;
                arraySize++;
            }
            if (!param2.IsEmpty)
            {
                int len = param2.Length;
                totalLen += 1 + NumUtils.NumDigits(len) + 2 + len + 2;
                arraySize++;
            }

            totalLen += 1 + NumUtils.NumDigits(arraySize) + 2;
            CheckLength(totalLen, tcs);
            await InputGateAsync(token);

            try
            {
                networkWriter.epoch.Resume();

                #region reserveSpaceAndWriteIntoNetworkBuffer
                int taskId;
                long address;
                while (true)
                {
                    token.ThrowIfCancellationRequested();
                    if (!IsConnected)
                    {
                        Dispose();
                        ThrowException(disposeException);
                    }
                    (taskId, address) = networkWriter.TryAllocate(totalLen, out var flushEvent);
                    if (address >= 0) break;
                    try
                    {
                        networkWriter.epoch.Suspend();
                        await flushEvent.WaitAsync(token).ConfigureAwait(false);
                    }
                    finally
                    {
                        networkWriter.epoch.Resume();
                    }
                }

                // Console.WriteLine($"Allocated {taskId} @ {address}");
                tcs.nextTaskId = taskId;

                unsafe
                {
                    byte* curr = (byte*)networkWriter.GetPhysicalAddress(address);
                    byte* end = curr + totalLen;
                    RespWriteUtils.WriteArrayLength(arraySize, ref curr, end);

                    RespWriteUtils.WriteDirect(op.Span, ref curr, end);
                    if (!param1.IsEmpty)
                        RespWriteUtils.WriteBulkString(param1.Span, ref curr, end);
                    if (!param2.IsEmpty)
                        RespWriteUtils.WriteBulkString(param2.Span, ref curr, end);

                    Debug.Assert(curr == end);
                }
                #endregion

                #region waitForEmptySlot
                int shortTaskId = taskId & (maxOutstandingTasks - 1);
                var oldTcs = tcsArray[shortTaskId];
                //1. if taskType != None, we are waiting for previous task to finish
                //2. if taskType == None and my taskId is not the next in line wait for previous task to acquire slot
                if (oldTcs.taskType != TaskType.None || !oldTcs.IsNext(taskId))
                {
                    // Console.WriteLine($"Before filling slot {taskId & (maxOutstandingTasks - 1)} for task {taskId} @ {address} : {tcs.taskType}");
                    networkWriter.epoch.ProtectAndDrain();
                    networkWriter.DoAggressiveShiftReadOnly();
                    try
                    {
                        networkWriter.epoch.Suspend();
                        await AwaitPreviousTaskAsync(taskId); // does not take token, as task is not cancelable at this point
                    }
                    finally
                    {
                        networkWriter.epoch.Resume();
                    }
                }
                #endregion

                #region scheduleAwaitForResponse
                // Console.WriteLine($"Filled slot {taskId & (maxOutstandingTasks - 1)} for task {taskId} @ {address} : {tcs.taskType}");
                tcsArray[shortTaskId].LoadFrom(tcs);
                if (Disposed)
                {
                    DisposeOffset(shortTaskId);
                    ThrowException(disposeException);
                }
                // Console.WriteLine($"Filled {address}-{address + totalLen}");
                networkWriter.epoch.ProtectAndDrain();
                networkWriter.DoAggressiveShiftReadOnly();
                #endregion
            }
            finally
            {
                networkWriter.epoch.Suspend();
            }
            return;
        }

        /// <summary>
        /// Issue command for execution
        /// </summary>
        /// <param name="op"></param>
        /// <param name="args"></param>
        /// <param name="token"></param>
        /// <param name="tcs"></param>
        async ValueTask InternalExecuteAsync(TcsWrapper tcs, string op, ICollection<string> args = null, CancellationToken token = default)
        {
            tcs.timestamp = GetTimestamp();
            bool isArray = args != null;
            int arraySize = 1 + (isArray ? args.Count : 0);
            int totalLen = 1 + NumUtils.NumDigits(arraySize) + 2 + //array header
                1 + NumUtils.NumDigits(op.Length) + 2 + op.Length + 2;//op header + op data

            if (isArray)
            {
                foreach (var arg in args)
                {
                    int len = Encoding.UTF8.GetByteCount(arg);
                    totalLen += 1 + NumUtils.NumDigits(len) + 2 + len + 2;
                }
            }

            CheckLength(totalLen, tcs);
            await InputGateAsync(token);

            try
            {
                networkWriter.epoch.Resume();

                #region reserveSpaceAndWriteIntoNetworkBuffer
                int taskId;
                long address;
                while (true)
                {
                    token.ThrowIfCancellationRequested();
                    if (!IsConnected)
                    {
                        Dispose();
                        ThrowException(disposeException);
                    }
                    (taskId, address) = networkWriter.TryAllocate(totalLen, out var flushEvent);
                    if (address >= 0) break;
                    try
                    {
                        networkWriter.epoch.Suspend();
                        await flushEvent.WaitAsync(token).ConfigureAwait(false);
                    }
                    finally
                    {
                        networkWriter.epoch.Resume();
                    }
                }

                // Console.WriteLine($"Allocated {taskId} @ {address}");
                tcs.nextTaskId = taskId;

                unsafe
                {
                    byte* curr = (byte*)networkWriter.GetPhysicalAddress(address);
                    byte* end = curr + totalLen;
                    RespWriteUtils.WriteArrayLength(arraySize, ref curr, end);

                    RespWriteUtils.WriteBulkString(op, ref curr, end);//Write op data
                    if (isArray)//Write arg data
                    {
                        foreach (var arg in args)
                            RespWriteUtils.WriteBulkString(arg, ref curr, end);
                    }

                    Debug.Assert(curr == end);
                }
                #endregion

                #region waitForEmptySlot
                int shortTaskId = taskId & (maxOutstandingTasks - 1);
                var oldTcs = tcsArray[shortTaskId];
                //1. if taskType != None, we are waiting for previous task to finish
                //2. if taskType == None and my taskId is not the next in line wait for previous task to acquire slot
                if (oldTcs.taskType != TaskType.None || !oldTcs.IsNext(taskId))
                {
                    // Console.WriteLine($"Before filling slot {taskId & (maxOutstandingTasks - 1)} for task {taskId} @ {address} : {tcs.taskType}");
                    networkWriter.epoch.ProtectAndDrain();
                    networkWriter.DoAggressiveShiftReadOnly();
                    try
                    {
                        networkWriter.epoch.Suspend();
                        await AwaitPreviousTaskAsync(taskId); // does not take token, as task is not cancelable at this point
                    }
                    finally
                    {
                        networkWriter.epoch.Resume();
                    }
                }
                #endregion

                #region scheduleAwaitForResponse
                // Console.WriteLine($"Filled slot {taskId & (maxOutstandingTasks - 1)} for task {taskId} @ {address} : {tcs.taskType}");
                tcsArray[shortTaskId].LoadFrom(tcs);
                if (Disposed)
                {
                    DisposeOffset(shortTaskId);
                    ThrowException(disposeException);
                }
                // Console.WriteLine($"Filled {address}-{address + totalLen}");
                networkWriter.epoch.ProtectAndDrain();
                networkWriter.DoAggressiveShiftReadOnly();
                #endregion
            }
            finally
            {
                networkWriter.epoch.Suspend();
            }
            return;
        }

        /// <summary>
        /// Issue command for execution with parameter array
        /// </summary>
        /// <param name="tcs"></param>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        async ValueTask InternalExecuteAsync(TcsWrapper tcs, Memory<byte> respOp, ICollection<Memory<byte>> args = null, CancellationToken token = default)
        {
            tcs.timestamp = GetTimestamp();
            bool isArray = args != null;
            int arraySize = 1 + (isArray ? args.Count : 0);
            int totalLen = 1 + NumUtils.NumDigits(arraySize) + 2 + respOp.Length;

            if (isArray)
            {
                foreach (var arg in args)
                {
                    int len = arg.Length;
                    totalLen += 1 + NumUtils.NumDigits(len) + 2 + len + 2;
                }
            }

            CheckLength(totalLen, tcs);
            await InputGateAsync(token);

            try
            {
                networkWriter.epoch.Resume();

                #region reserveSpaceAndWriteIntoNetworkBuffer
                int taskId;
                long address;
                while (true)
                {
                    token.ThrowIfCancellationRequested();
                    if (!IsConnected)
                    {
                        Dispose();
                        ThrowException(disposeException);
                    }
                    (taskId, address) = networkWriter.TryAllocate(totalLen, out var flushEvent);
                    if (address >= 0) break;
                    try
                    {
                        networkWriter.epoch.Suspend();
                        await flushEvent.WaitAsync(token).ConfigureAwait(false);
                    }
                    finally
                    {
                        networkWriter.epoch.Resume();
                    }
                }

                // Console.WriteLine($"Allocated {taskId} @ {address}");
                tcs.nextTaskId = taskId;

                unsafe
                {
                    byte* curr = (byte*)networkWriter.GetPhysicalAddress(address);
                    byte* end = curr + totalLen;
                    RespWriteUtils.WriteArrayLength(arraySize, ref curr, end);
                    RespWriteUtils.WriteDirect(respOp.Span, ref curr, end);
                    if (isArray)//Write arg data
                    {
                        foreach (var arg in args)
                            RespWriteUtils.WriteBulkString(arg.Span, ref curr, end);
                    }
                    Debug.Assert(curr == end);
                }
                #endregion

                #region waitForEmptySlot
                int shortTaskId = taskId & (maxOutstandingTasks - 1);
                var oldTcs = tcsArray[shortTaskId];
                //1. if taskType != None, we are waiting for previous task to finish
                //2. if taskType == None and my taskId is not the next in line wait for previous task to acquire slot
                if (oldTcs.taskType != TaskType.None || !oldTcs.IsNext(taskId))
                {
                    // Console.WriteLine($"Before filling slot {taskId & (maxOutstandingTasks - 1)} for task {taskId} @ {address} : {tcs.taskType}");
                    networkWriter.epoch.ProtectAndDrain();
                    networkWriter.DoAggressiveShiftReadOnly();
                    try
                    {
                        networkWriter.epoch.Suspend();
                        await AwaitPreviousTaskAsync(taskId); // does not take token, as task is not cancelable at this point
                    }
                    finally
                    {
                        networkWriter.epoch.Resume();
                    }
                }
                #endregion

                #region scheduleAwaitForResponse
                // Console.WriteLine($"Filled slot {taskId & (maxOutstandingTasks - 1)} for task {taskId} @ {address} : {tcs.taskType}");
                tcsArray[shortTaskId].LoadFrom(tcs);
                if (Disposed)
                {
                    DisposeOffset(shortTaskId);
                    ThrowException(disposeException);
                }
                // Console.WriteLine($"Filled {address}-{address + totalLen}");
                networkWriter.epoch.ProtectAndDrain();
                networkWriter.DoAggressiveShiftReadOnly();
                #endregion
            }
            finally
            {
                networkWriter.epoch.Suspend();
            }
            return;
        }

        static void ThrowException(Exception e) => throw e;

        /// <inheritdoc />
        public bool TryCreateMessageConsumer(Span<byte> bytesReceived, INetworkSender networkSender, out IMessageConsumer session)
            => throw new NotSupportedException();

        /// <inheritdoc />
        public void DisposeMessageConsumer(INetworkHandler session)
        {
            int c = tcsOffset;
            while (networkWriter != null && c != networkWriter.GetNextTaskId())
            {
                DisposeOffset(c & (maxOutstandingTasks - 1));
                c = (c + 1) & (int)PageOffset.kTaskMask;
            }
        }

        private void DisposeOffset(int shortTaskId)
        {
            var tcs = tcsArray[shortTaskId];
            switch (tcs.taskType)
            {
                case TaskType.StringCallback:
                    tcs.stringCallback?.Invoke(-1, null);
                    break;
                case TaskType.MemoryByteCallback:
                    tcs.memoryByteCallback?.Invoke(-1, default);
                    break;
                case TaskType.StringAsync:
                    tcs.stringTcs?.TrySetException(disposeException);
                    break;
                case TaskType.StringArrayAsync:
                    tcs.stringArrayTcs?.TrySetException(disposeException);
                    break;
                case TaskType.MemoryByteAsync:
                    tcs.memoryByteTcs?.TrySetException(disposeException);
                    break;
                case TaskType.MemoryByteArrayAsync:
                    tcs.memoryByteArrayTcs?.TrySetException(disposeException);
                    break;
                case TaskType.StringArrayCallback:
                    tcs.stringArrayCallback?.Invoke(-1, default, default);
                    break;
                case TaskType.MemoryByteArrayCallback:
                    tcs.memoryByteArrayCallback?.Invoke(-1, default, default);
                    break;
                case TaskType.None:
                    break;
            }
            ConsumeTcsOffset(shortTaskId);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ConsumeTcsOffset(int shortTaskId)
        {
            TcsWrapper reset = default;
            reset.nextTaskId = (tcsArray[shortTaskId].nextTaskId + maxOutstandingTasks) & (int)PageOffset.kTaskMask;
            tcsArray[shortTaskId].LoadFrom(reset);
            tcsOffset = (tcsOffset + 1) & (int)PageOffset.kTaskMask;
        }
    }
}