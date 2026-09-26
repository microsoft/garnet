// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;

namespace Garnet.client
{
    /// <summary>
    /// A lightweight Garnet client that makes a single network connection to a server and supports
    /// only out-of-line (chunked) payloads. It is intended for producers that fan out from multiple
    /// threads (for example cluster gossip and pub/sub forwarding).
    /// <para>
    /// Unlike <see cref="GarnetClient"/>, this client does not maintain a separate task-id space or a
    /// producer-side serialization gate. The response completion travels with the payload
    /// (<see cref="LightPayload"/>); the single-threaded flusher registers each completion in address
    /// order right before the payload is sent, and replies are matched in that same order.
    /// </para>
    /// </summary>
    public sealed partial class GarnetLightClient : IServerHook, IMessageConsumer, IDisposable
    {
        const int PayloadDescriptorSize = sizeof(long);

        // Number of circular pages backing the request lane's descriptor buffer.
        const int PageBufferCount = 2;

        static readonly Memory<byte> AUTH = "$4\r\nAUTH\r\n"u8.ToArray();
        static readonly Memory<byte> CLIENT = "$6\r\nCLIENT\r\n"u8.ToArray();
        static readonly Memory<byte>[] SETINFO = ["SETINFO"u8.ToArray(), "LIB-NAME"u8.ToArray(), "GarnetLightClient"u8.ToArray()];

        readonly int sendPageSize;
        readonly int bufferSize;
        readonly int maxOutstandingTasks;
        readonly LightEpoch epoch;
        readonly bool isEpochOwned;
        LightNetworkWriter networkWriter;

        readonly SslClientAuthenticationOptions sslOptions;
        readonly MemoryPool<byte> memoryPool;
        GarnetLightClientTcpNetworkHandler networkHandler;
        int tcsOffset;

        Socket socket;
        int disposed;

        /// <inheritdoc />
        public bool Disposed => disposed > 0;

        readonly ILogger logger;

        readonly CancellationTokenSource timeoutCheckerCts;
        readonly int timeoutMilliseconds;
        readonly int networkSendThrottleMax;

        readonly string authUsername = null;
        readonly string authPassword = null;
        readonly Memory<byte>[] clientName = null;

        static readonly Exception disposeException = new GarnetClientDisposedException();

        /// <summary>
        /// The host endpoint
        /// </summary>
        public EndPoint EndPoint { get; }

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
        /// <param name="endpoint">Endpoint of the server</param>
        /// <param name="tlsOptions">TLS options</param>
        /// <param name="authUsername">Username to authenticate with</param>
        /// <param name="authPassword">Password to authenticate with</param>
        /// <param name="clientName">Client name to be used with CLIENT SETNAME command</param>
        /// <param name="sendPageSize">Size of pages where descriptors are written, determines how many requests can be queued before page reuse waits for an earlier flush (rounds down to previous power of 2)</param>
        /// <param name="bufferSize">Network writer buffer size</param>
        /// <param name="maxOutstandingTasks">Maximum outstanding tasks before client throttles new requests (rounds down to previous power of 2)</param>
        /// <param name="timeoutMilliseconds">Timeout (in milliseconds) after which client disposes itself and throws exception on all active tasks</param>
        /// <param name="memoryPool">Pool for Memory based response buffers</param>
        /// <param name="useTimeoutChecker"></param>
        /// <param name="networkSendThrottleMax">Max outstanding network sends allowed</param>
        /// <param name="epoch">Shared epoch instance for thread protection; if null, a new instance is created and owned by this client</param>
        /// <param name="logger">Logger instance</param>
        public GarnetLightClient(
            EndPoint endpoint,
            SslClientAuthenticationOptions tlsOptions = null,
            string authUsername = null,
            string authPassword = null,
            string clientName = null,
            int sendPageSize = 1 << 21,
            int bufferSize = 1 << 17,
            int maxOutstandingTasks = 1 << 19,
            int timeoutMilliseconds = 0,
            MemoryPool<byte> memoryPool = null,
            bool useTimeoutChecker = true,
            int networkSendThrottleMax = 8,
            LightEpoch epoch = null,
            ILogger logger = null)
        {
            EndPoint = endpoint;
            this.sendPageSize = (int)Utility.PreviousPowerOf2(sendPageSize);
            this.bufferSize = bufferSize;
            this.authUsername = authUsername;
            this.authPassword = authPassword;
            this.clientName = clientName != null ? ["SETNAME"u8.ToArray(), Encoding.ASCII.GetBytes(clientName)] : null;

            if (maxOutstandingTasks > PageOffset.kTaskMask + 1)
                ThrowException(new Exception($"Maximum outstanding tasks supported is {PageOffset.kTaskMask + 1}"));

            if (maxOutstandingTasks != (int)Utility.PreviousPowerOf2(maxOutstandingTasks))
                ThrowException(new Exception($"Maximum outstanding tasks should be a power of two, up to {PageOffset.kTaskMask + 1}"));

            this.maxOutstandingTasks = maxOutstandingTasks;
            this.sslOptions = tlsOptions;
            this.disposed = 0;
            this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
            this.logger = logger;
            this.timeoutMilliseconds = timeoutMilliseconds;
            if (timeoutMilliseconds > 0 && useTimeoutChecker)
                timeoutCheckerCts = new();
            this.networkSendThrottleMax = networkSendThrottleMax;
            if (epoch == null)
            {
                this.epoch = new LightEpoch();
                isEpochOwned = true;
            }
            else
                this.epoch = epoch;
        }

        /// <summary>
        /// Finalizer
        /// </summary>
        ~GarnetLightClient()
        {
            Dispose(false);
        }

        /// <summary>
        /// Connect to server
        /// </summary>
        public void Connect(CancellationToken token = default)
        {
            socket = ConnectSendSocket();
            networkWriter = new LightNetworkWriter(this, socket, bufferSize, sslOptions, out networkHandler, sendPageSize, PageBufferCount, maxOutstandingTasks, networkSendThrottleMax, epoch, PoolOwnerType.GarnetClient, logger);
            networkHandler.Start(sslOptions, EndPoint.ToString(), token);

            if (timeoutMilliseconds > 0)
                Task.Run(TimeoutChecker);

            RunConnectHandshake();
        }

        /// <summary>
        /// Connect to server
        /// </summary>
        public async Task ConnectAsync(CancellationToken token = default)
        {
            socket = await ConnectSendSocketAsync(timeoutMilliseconds, token).ConfigureAwait(false);
            networkWriter = new LightNetworkWriter(this, socket, bufferSize, sslOptions, out networkHandler, sendPageSize, PageBufferCount, maxOutstandingTasks, networkSendThrottleMax, epoch, PoolOwnerType.GarnetClient, logger);
            await networkHandler.StartAsync(sslOptions, EndPoint.ToString(), token).ConfigureAwait(false);

            if (timeoutMilliseconds > 0)
                _ = Task.Run(TimeoutChecker);

            try
            {
                if (authUsername != null)
                    await ExecuteForStringResultWithCancellationAsync(AUTH, [authUsername, authPassword ?? ""], token).ConfigureAwait(false);
                else if (authPassword != null)
                    await ExecuteForStringResultWithCancellationAsync(AUTH, [authPassword], token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                logger?.LogError(e, "AUTH returned error!");
                throw;
            }

            try
            {
                if (clientName != null)
                {
                    _ = await ExecuteForStringResultWithCancellationAsync(CLIENT, SETINFO, token).ConfigureAwait(false);
                    _ = await ExecuteForStringResultWithCancellationAsync(CLIENT, clientName, token).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                logger?.LogError(e, "Client set info returned error!");
                throw;
            }
        }

        void RunConnectHandshake()
        {
            try
            {
                Task authTask =
                    (authUsername, authPassword) switch
                    {
                        (string u, string p) => ExecuteForStringResultWithCancellationAsync(AUTH, [u, p]),
                        (string u, null) => ExecuteForStringResultWithCancellationAsync(AUTH, [u, ""]),
                        (null, string p) => ExecuteForStringResultWithCancellationAsync(AUTH, [p]),
                        _ => null,
                    };

                if (authTask != null)
                    AsyncUtils.BlockingWait(authTask);
            }
            catch (Exception e)
            {
                logger?.LogError(e, "AUTH returned error");
                throw;
            }

            try
            {
                if (clientName != null)
                {
                    _ = ExecuteForStringResultWithCancellationAsync(CLIENT, SETINFO).ConfigureAwait(false).GetAwaiter().GetResult();
                    _ = ExecuteForStringResultWithCancellationAsync(CLIENT, clientName).ConfigureAwait(false).GetAwaiter().GetResult();
                }
            }
            catch (Exception e)
            {
                logger?.LogError(e, "Client set info returned error!");
                throw;
            }
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
            await ConnectAsync(token).ConfigureAwait(false);
        }

        /// <summary>
        /// Establish a connected send socket to <see cref="EndPoint"/>, trying every DNS entry when a
        /// host name is supplied.
        /// </summary>
        Socket ConnectSendSocket()
        {
            if (EndPoint is DnsEndPoint dnsEndpoint)
            {
                var hostEntries = Dns.GetHostEntry(dnsEndpoint.Host);
                // Try all available DNS entries if a hostName is provided
                foreach (var addressEntry in hostEntries.AddressList)
                {
                    var endpoint = new IPEndPoint(addressEntry, dnsEndpoint.Port);
                    var socket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
                    {
                        NoDelay = true
                    };

                    if (TryConnectSocket(socket, endpoint))
                        return socket;
                }
            }
            else
            {
                var socket = new Socket(EndPoint.AddressFamily, SocketType.Stream, ProtocolType.Unspecified);
                if (EndPoint is not UnixDomainSocketEndPoint)
                    socket.NoDelay = true;

                if (TryConnectSocket(socket, EndPoint))
                    return socket;
            }

            logger?.LogWarning("Failed to connect at {endpoint}", EndPoint);
            throw new Exception($"Failed to connect at {EndPoint}");
        }

        /// <inheritdoc cref="ConnectSendSocket"/>
        async Task<Socket> ConnectSendSocketAsync(int millisecondsTimeout = 0, CancellationToken cancellationToken = default)
        {
            if (EndPoint is DnsEndPoint dnsEndpoint)
            {
                var hostEntries = await Dns.GetHostEntryAsync(dnsEndpoint.Host, cancellationToken).ConfigureAwait(false);
                // Try all available DNS entries if a hostName is provided
                foreach (var addressEntry in hostEntries.AddressList)
                {
                    var endpoint = new IPEndPoint(addressEntry, dnsEndpoint.Port);
                    var socket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
                    {
                        NoDelay = true
                    };

                    if (await TryConnectSocketAsync(socket, endpoint, millisecondsTimeout, cancellationToken).ConfigureAwait(false))
                        return socket;
                }
            }
            else
            {
                var socket = new Socket(EndPoint.AddressFamily, SocketType.Stream, ProtocolType.Unspecified);
                if (EndPoint is not UnixDomainSocketEndPoint)
                    socket.NoDelay = true;

                if (await TryConnectSocketAsync(socket, EndPoint, millisecondsTimeout, cancellationToken).ConfigureAwait(false))
                    return socket;
            }

            logger?.LogWarning("Failed to connect at {endpoint}", EndPoint);
            throw new Exception($"Failed to connect at {EndPoint}");
        }

        /// <summary>
        /// Try to establish a connection for <paramref name="socket"/> using <paramref name="endpoint"/>.
        /// </summary>
        bool TryConnectSocket(Socket socket, EndPoint endpoint)
        {
            try
            {
                socket.Connect(endpoint);

                if (!socket.Connected)
                {
                    socket.Close();
                    throw new Exception($"Failed to connect server {endpoint}.");
                }
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "Failed at GarnetLightClient.TryConnectSocket");
                socket.Dispose();
                return false;
            }

            return true;
        }

        /// <inheritdoc cref="TryConnectSocket"/>
        async Task<bool> TryConnectSocketAsync(Socket socket, EndPoint endpoint, int millisecondsTimeout, CancellationToken cancellationToken = default)
        {
            try
            {
                if (millisecondsTimeout > 0)
                {
                    using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

                    var connectTask = socket.ConnectAsync(endpoint, timeoutCts.Token).AsTask();
                    if (await Task.WhenAny(connectTask, Task.Delay(millisecondsTimeout, timeoutCts.Token)).ConfigureAwait(false) == connectTask)
                    {
                        // Task completed within timeout.
                        // Consider that the task may have faulted or been canceled.
                        // We re-await the task so that any exceptions/cancellation is rethrown.
                        await connectTask.ConfigureAwait(false);
                    }
                    else
                    {
                        timeoutCts.Cancel();
                    }

                    if (!socket.Connected)
                    {
                        socket.Close();
                        throw new Exception($"Failed to connect server {endpoint}.");
                    }
                }
                else
                {
                    await socket.ConnectAsync(endpoint, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "Failed at GarnetLightClient.TryConnectSocketAsync");
                socket.Dispose();
                return false;
            }

            return true;
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
            if (isEpochOwned)
                epoch.Dispose();
        }

        /// <summary>
        /// Get estimated number of outstanding tasks.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int PipelineLength()
        {
            var _tcsOffset = tcsOffset & (int)PageOffset.kTaskMask;
            var _nextTaskId = networkWriter.CompletionTail & (int)PageOffset.kTaskMask;

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
                await Task.Delay(delayMs, token).ConfigureAwait(false);
                if (delayMs == 0) delayMs = 1;
                else delayMs *= 2;
                if (delayMs > 4096) delayMs = 4096;
            }
        }

        /// <summary>
        /// Issue an out-of-line command whose response completes the provided <paramref name="tcs"/>.
        /// </summary>
        async ValueTask InternalExecuteChunkedAsync(TcsWrapper tcs, Memory<byte> respOp, ICollection<Memory<byte>> args = null, CancellationToken token = default)
        {
            var isArray = args != null;
            var arraySize = checked(1 + (isArray ? args.Count : 0));
            var totalLength = checked(1 + NumUtils.CountDigits(arraySize) + 2 + respOp.Length);

            if (isArray)
            {
                foreach (var arg in args)
                {
                    var length = arg.Length;
                    totalLength = checked(totalLength + 1 + NumUtils.CountDigits(length) + 2 + length + 2);
                }
            }

            await InputGateAsync(token).ConfigureAwait(false);
            var payload = networkWriter.RentPayloadBuffer(totalLength);
            var payloadRegistered = false;

            try
            {
                unsafe
                {
                    fixed (byte* payloadPtr = payload.Buffer)
                    {
                        var curr = payloadPtr;
                        var end = payloadPtr + payload.Length;

                        if (!RespWriteUtils.TryWriteArrayLength(arraySize, ref curr, end) ||
                            !RespWriteUtils.TryWriteDirect(respOp.Span, ref curr, end))
                        {
                            throw new InvalidOperationException("Unable to serialize the out-of-line command into its reserved buffer.");
                        }

                        if (isArray)
                        {
                            foreach (var arg in args)
                            {
                                if (!RespWriteUtils.TryWriteBulkString(arg.Span, ref curr, end))
                                    throw new InvalidOperationException("Unable to serialize the out-of-line command into its reserved buffer.");
                            }
                        }

                        if (curr != end)
                            throw new InvalidOperationException("The serialized out-of-line command did not fill its reserved buffer.");
                    }
                }

                try
                {
                    networkWriter.epoch.Resume();

                    int taskId;
                    long address;
                    while (true)
                    {
                        token.ThrowIfCancellationRequested();
                        if (!IsConnected)
                        {
                            payload.Dispose();
                            payload = default;
                            Dispose();
                            ThrowException(disposeException);
                        }

                        (taskId, address) = networkWriter.TryAllocate(PayloadDescriptorSize, expectsResponse: true, out var flushEvent);
                        if (address >= 0)
                            break;

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

                    // Register the completion in its own reply-gated lane, keyed by the ticket the combined
                    // allocator handed out alongside the request address, before the request so the completion
                    // is visible to the reply reader by the time any reply can arrive.
                    networkWriter.RegisterCompletion(taskId, tcs);

                    networkWriter.RegisterRequest(address, payload);
                    payloadRegistered = true;

                    if (Disposed)
                        ThrowException(disposeException);

                    networkWriter.epoch.ProtectAndDrain();
                    networkWriter.DoAggressiveShiftReadOnly();
                }
                finally
                {
                    networkWriter.epoch.Suspend();
                }
            }
            finally
            {
                if (!payloadRegistered)
                    payload.Dispose();
            }
        }

        /// <summary>
        /// Issue an out-of-line command for execution without expecting a response.
        /// </summary>
        void InternalExecuteChunkedNoResponse(Memory<byte> respOp, ReadOnlySpan<byte> subop, Span<byte> param1, Span<byte> param2, CancellationToken token = default)
        {
            const int arraySize = 4;
            int totalLength = checked(1 + NumUtils.CountDigits(arraySize) + 2 + respOp.Length);

            int length = subop.Length;
            totalLength = checked(totalLength + 1 + NumUtils.CountDigits(length) + 2 + length + 2);
            length = param1.Length;
            totalLength = checked(totalLength + 1 + NumUtils.CountDigits(length) + 2 + length + 2);
            length = param2.Length;
            totalLength = checked(totalLength + 1 + NumUtils.CountDigits(length) + 2 + length + 2);

            var payload = networkWriter.RentPayloadBuffer(totalLength);
            var payloadRegistered = false;

            try
            {
                unsafe
                {
                    fixed (byte* payloadPtr = payload.Buffer)
                    {
                        byte* curr = payloadPtr;
                        byte* end = payloadPtr + payload.Length;

                        if (!RespWriteUtils.TryWriteArrayLength(arraySize, ref curr, end) ||
                            !RespWriteUtils.TryWriteDirect(respOp.Span, ref curr, end) ||
                            !RespWriteUtils.TryWriteBulkString(subop, ref curr, end) ||
                            !RespWriteUtils.TryWriteBulkString(param1, ref curr, end) ||
                            !RespWriteUtils.TryWriteBulkString(param2, ref curr, end))
                        {
                            throw new InvalidOperationException("Unable to serialize the out-of-line command into its reserved buffer.");
                        }

                        if (curr != end)
                            throw new InvalidOperationException("The serialized out-of-line command did not fill its reserved buffer.");
                    }
                }

                try
                {
                    networkWriter.epoch.Resume();

                    long address;
                    while (true)
                    {
                        token.ThrowIfCancellationRequested();
                        if (!IsConnected)
                        {
                            payload.Dispose();
                            payload = default;
                            Dispose();
                            ThrowException(disposeException);
                        }

                        (_, address) = networkWriter.TryAllocate(PayloadDescriptorSize, expectsResponse: false, out var flushEvent);
                        if (address >= 0)
                            break;

                        try
                        {
                            networkWriter.epoch.Suspend();
                            flushEvent.Wait(token);
                        }
                        finally
                        {
                            networkWriter.epoch.Resume();
                        }
                    }

                    // Fire-and-forget: no completion ticket is consumed and nothing is registered in the
                    // completion lane. A reply arriving for one of these is a protocol violation the reply
                    // reader is expected to assert on.
                    networkWriter.RegisterRequest(address, payload);
                    payloadRegistered = true;

                    if (Disposed)
                        ThrowException(disposeException);

                    networkWriter.epoch.ProtectAndDrain();
                    networkWriter.DoAggressiveShiftReadOnly();
                }
                finally
                {
                    networkWriter.epoch.Suspend();
                }
            }
            finally
            {
                if (!payloadRegistered)
                    payload.Dispose();
            }
        }

        static void ThrowException(Exception e) => throw e;

        /// <summary>
        /// Reply reader entry point. Parses each RESP reply and matches it to the outstanding completion in
        /// monotonic ticket order (via the ring's reply-gated completion lane), then enforces the
        /// fire-and-forget no-response invariant. See <see cref="ProcessReplies"/>.
        /// </summary>
        public unsafe int TryConsumeMessages(byte* reqBuffer, int bytesReceived)
            => ProcessReplies(reqBuffer, bytesReceived);

        /// <inheritdoc />
        public bool TryCreateMessageConsumer(Span<byte> bytesReceived, INetworkSender networkSender, out IMessageConsumer session)
            => throw new NotSupportedException();

        /// <inheritdoc />
        public void DisposeMessageConsumer(INetworkHandler session)
        {
            int c = tcsOffset;
            while (networkWriter != null && c != networkWriter.CompletionTail)
            {
                DisposeOffset(c);
                c = (c + 1) & (int)PageOffset.kTaskMask;
            }
        }

        private void DisposeOffset(int taskId)
        {
            if (!networkWriter.TryReadCompletion(taskId, out var tcs))
            {
                ConsumeTcsOffset();
                return;
            }

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
                case TaskType.LongAsync:
                    tcs.longTcs?.TrySetException(disposeException);
                    break;
                case TaskType.LongCallback:
                    tcs.longCallback?.Invoke(-1, default, null);
                    break;
                case TaskType.None:
                    break;
            }
            ConsumeTcsOffset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ConsumeTcsOffset()
        {
            tcsOffset = (tcsOffset + 1) & (int)PageOffset.kTaskMask;
            networkWriter.AdvanceReplied(1);
        }

        async Task TimeoutChecker()
        {
            try
            {
                var token = timeoutCheckerCts.Token;
                while (true)
                {
                    await Task.Delay(timeoutMilliseconds, token).ConfigureAwait(false);
                    if (!IsConnected)
                        break;
                }
            }
            catch { }
        }
    }
}
