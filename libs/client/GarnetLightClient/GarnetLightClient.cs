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
    /// inline and out-of-line payloads. It is intended for producers that fan out from multiple threads
    /// (for example cluster gossip and pub/sub forwarding).
    /// <para>
    /// Unlike <see cref="GarnetClient"/>, this client does not maintain a separate task-id space or a
    /// producer-side serialization gate. Request allocation assigns response completions monotonically
    /// ordered tickets in a separate reply-gated lane, and replies are matched in that same order.
    /// </para>
    /// </summary>
    public sealed partial class GarnetLightClient : IServerHook, IMessageConsumer, IDisposable
    {
        static readonly ReadOnlyMemory<byte> AUTH = "$4\r\nAUTH\r\n"u8.ToArray();
        static readonly ReadOnlyMemory<byte> CLIENT = "$6\r\nCLIENT\r\n"u8.ToArray();
        static readonly Memory<byte>[] SETINFO = ["SETINFO"u8.ToArray(), "LIB-NAME"u8.ToArray(), "GarnetLightClient"u8.ToArray()];

        readonly LightNetworkWriterOptions networkWriterOptions;
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

        sealed class Authentication
        {
            internal readonly string Username;
            internal readonly string Password;

            internal Authentication(string username, string password)
            {
                Username = username;
                Password = password;
            }
        }

        Authentication authentication;
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
        /// Accounted memory currently used by this client's request/completion ring and admitted out-of-line
        /// request buffers.
        /// </summary>
        /// <remarks>
        /// Uses the ring's minimum allocated footprint plus current out-of-line byte reservations. As with
        /// <see cref="LightNetworkWriterOptions.MinMemoryFootprint"/>, this estimate excludes array headers and
        /// network buffers.
        /// </remarks>
        public long ActiveMemoryUsageBytes
            => Volatile.Read(ref networkWriter)?.ActiveMemoryUsageBytes ?? 0;

        /// <summary>
        /// Maximum accounted memory allowed by this client's fully warmed request/completion ring and configured
        /// out-of-line request-byte capacity.
        /// </summary>
        /// <remarks>
        /// Uses the same exclusions as <see cref="LightNetworkWriterOptions.MaxMemoryFootprint"/>: array headers
        /// and network buffers are not included.
        /// <para>
        /// Returns <see cref="long.MaxValue"/> when out-of-line memory admission is disabled (configured capacity
        /// is zero), because that portion of the footprint is unbounded.
        /// </para>
        /// </remarks>
        public long MaxMemoryUsageBytes
        {
            get
            {
                var maxOutOfLineRentedBytes = networkWriterOptions.MaxOutOfLineRentedBytes;
                return maxOutOfLineRentedBytes == 0
                    ? long.MaxValue
                    : SaturatingAdd(networkWriterOptions.MaxMemoryFootprint(), maxOutOfLineRentedBytes);
            }
        }

        static long SaturatingAdd(long left, long right)
            => left > long.MaxValue - right ? long.MaxValue : left + right;

        /// <summary>
        /// Create client instance
        /// </summary>
        /// <param name="endpoint">Endpoint of the server</param>
        /// <param name="tlsOptions">TLS options</param>
        /// <param name="authUsername">Username to authenticate with</param>
        /// <param name="authPassword">Password to authenticate with</param>
        /// <param name="clientName">Client name to be used with CLIENT SETNAME command</param>
        /// <param name="networkWriterOptions">Request-ring, completion, network-buffer, and send-throttling options. Uses <see cref="LightNetworkWriterOptions.Default"/> when omitted.</param>
        /// <param name="timeoutMilliseconds">Timeout (in milliseconds) after which client disposes itself and throws exception on all active tasks</param>
        /// <param name="memoryPool">Pool for Memory based response buffers</param>
        /// <param name="useTimeoutChecker"></param>
        /// <param name="epoch">Shared epoch instance for thread protection; if null, a new instance is created and owned by this client</param>
        /// <param name="logger">Logger instance</param>
        public GarnetLightClient(
            EndPoint endpoint,
            SslClientAuthenticationOptions tlsOptions = null,
            string authUsername = null,
            string authPassword = null,
            string clientName = null,
            LightNetworkWriterOptions? networkWriterOptions = null,
            int timeoutMilliseconds = 0,
            MemoryPool<byte> memoryPool = null,
            bool useTimeoutChecker = true,
            LightEpoch epoch = null,
            ILogger logger = null)
        {
            EndPoint = endpoint;
            var options = networkWriterOptions ?? LightNetworkWriterOptions.Default;
            this.authentication = new(authUsername, authPassword);
            this.clientName = clientName != null ? ["SETNAME"u8.ToArray(), Encoding.ASCII.GetBytes(clientName)] : null;

            if (options.MaxOutstandingCompletions > PageOffset.kTaskMask + 1)
                ThrowException(new Exception($"Maximum outstanding tasks supported is {PageOffset.kTaskMask + 1}"));

            if (options.MaxOutstandingCompletions != (int)Utility.PreviousPowerOf2(options.MaxOutstandingCompletions))
                ThrowException(new Exception($"Maximum outstanding tasks should be a power of two, up to {PageOffset.kTaskMask + 1}"));

            this.networkWriterOptions = options;
            this.sslOptions = tlsOptions;
            this.disposed = 0;
            this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
            this.logger = logger;
            this.timeoutMilliseconds = timeoutMilliseconds;
            if (timeoutMilliseconds > 0 && useTimeoutChecker)
                timeoutCheckerCts = new();
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
            networkWriter = new LightNetworkWriter(this, socket, networkWriterOptions, sslOptions, out networkHandler, epoch, PoolOwnerType.GarnetClient, logger);
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
            networkWriter = new LightNetworkWriter(this, socket, networkWriterOptions, sslOptions, out networkHandler, epoch, PoolOwnerType.GarnetClient, logger);
            await networkHandler.StartAsync(sslOptions, EndPoint.ToString(), token).ConfigureAwait(false);

            if (timeoutMilliseconds > 0)
                _ = Task.Run(TimeoutChecker);

            var authentication = Volatile.Read(ref this.authentication);
            try
            {
                if (authentication.Username != null)
                    await ExecuteForStringResultWithCancellationAsync(AUTH, [authentication.Username, authentication.Password ?? ""], token).ConfigureAwait(false);
                else if (authentication.Password != null)
                    await ExecuteForStringResultWithCancellationAsync(AUTH, [authentication.Password], token).ConfigureAwait(false);
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
            var authentication = Volatile.Read(ref this.authentication);
            try
            {
                Task authTask =
                    (authentication.Username, authentication.Password) switch
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
                tcsOffset = 0;
            }
            catch { }
            await ConnectAsync(token).ConfigureAwait(false);
        }

        /// <summary>
        /// Updates the credentials used to authenticate on each subsequent connection.
        /// </summary>
        /// <param name="username">Username to authenticate with, or null for password-only or no authentication.</param>
        /// <param name="password">Password to authenticate with, or null for no authentication.</param>
        /// <remarks>
        /// The new credentials take effect on the next connection or reconnection. The current connection is not
        /// re-authenticated.
        /// </remarks>
        public void UpdateAuth(string username, string password)
            => Volatile.Write(ref authentication, new(username, password));

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
        /// Issue a command whose response completes the provided <paramref name="tcs"/>.
        /// </summary>
        async ValueTask InternalExecuteAsync(TcsWrapper tcs, ReadOnlyMemory<byte> respOp, ICollection<Memory<byte>> args = null, CancellationToken token = default)
        {
            var writer = networkWriter;
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

            // The choice between an inline record (serialized straight into the ring page, no pooled buffer) and
            // an out-of-line record (an 8-byte descriptor in the page plus a separately rented payload buffer) is
            // automatic and driven solely by whether the whole command fits in a single ring page. The decision
            // depends only on totalLength versus fixed page geometry, not on the current page fill, so it is
            // loop-invariant and made once here rather than re-evaluated per allocation attempt.
            var recordSize = writer.GetRecordSize(totalLength, out var inline);

            // Out-of-line rents its payload buffer and serializes into it up front, outside the epoch. Inline
            // rents nothing and defers serialization until it owns a page slot (written under the epoch below).

            LightRequestContext payload = default;
            var payloadRegistered = false;

            // Serialize the command's RESP bytes into the span [curr, end). Shared by both paths so the wire
            // format lives in one place; the caller supplies either page memory (inline) or the rented buffer.
            unsafe void SerializeCommand(byte* curr, byte* end)
            {
                if (!RespWriteUtils.TryWriteArrayLength(arraySize, ref curr, end) ||
                    !RespWriteUtils.TryWriteDirect(respOp.Span, ref curr, end))
                {
                    throw new InvalidOperationException("Unable to serialize the command into its reserved slot.");
                }

                if (isArray)
                {
                    foreach (var arg in args)
                    {
                        if (!RespWriteUtils.TryWriteBulkString(arg.Span, ref curr, end))
                            throw new InvalidOperationException("Unable to serialize the command into its reserved slot.");
                    }
                }

                if (curr != end)
                    throw new InvalidOperationException("The serialized command did not fill its reserved slot.");
            }

            try
            {
                // Allocate side-buffer for payload because it cannot be inlined.
                if (!inline)
                {
                    var allocationSize = 0;
                    var admissionReserved = false;
                    try
                    {
                        allocationSize = writer.GetRequestBufferAllocationSize(totalLength);
                        if (!await writer.RentMemoryThrottle(allocationSize, token).ConfigureAwait(false))
                            throw new InvalidOperationException("The out-of-line rental admission queue is full.");
                        admissionReserved = true;

                        payload = writer.RentAdmittedOutOfLineBuffer(totalLength, allocationSize);
                        admissionReserved = false;

                        unsafe
                        {
                            fixed (byte* payloadPtr = payload.Buffer)
                                SerializeCommand(payloadPtr, payloadPtr + payload.Length);
                        }
                    }
                    catch (Exception ex)
                    {
                        if (admissionReserved)
                            writer.ReleaseOutOfLineRental(allocationSize);
                        CompleteOnFailure(tcs, ex);
                        return;
                    }
                }

                try
                {
                    writer.epoch.Resume();

                    int taskId;
                    long address;
                    while (true)
                    {
                        token.ThrowIfCancellationRequested();
                        if (Disposed || writer.Closed || !ReferenceEquals(writer, Volatile.Read(ref networkWriter)) || !IsConnected)
                        {
                            if (!inline)
                            {
                                payload.Dispose();
                                payload = default;
                            }
                            CompleteOnFailure(tcs, disposeException);
                            return;
                        }

                        if (writer.TryScheduleSend(
                            recordSize,
                            expectsResponse: true,
                            out var reservation,
                            out var flushEvent))
                        {
                            taskId = reservation.completionTicket;
                            address = reservation.requestAddress;
                            break;
                        }

                        try
                        {
                            writer.epoch.Suspend();
                            await flushEvent.WaitAsync(token).ConfigureAwait(false);
                        }
                        finally
                        {
                            writer.epoch.Resume();
                        }
                    }

                    // Register the completion in its own reply-gated lane, keyed by the ticket the combined
                    // allocator handed out alongside the request address, before the request becomes flushable so
                    // the completion is visible to the reply reader by the time any reply can arrive.
                    writer.RegisterCompletion(taskId, tcs);

                    try
                    {
                        if (inline)
                        {
                            // Serialize straight into page memory. The epoch acquired above is held continuously
                            // across the reserve and this write (no Suspend/await between the successful allocate
                            // and here), which is what keeps the early-published inline header safe: a concurrent
                            // flush cannot read this record until this producer drains at ProtectAndDrain below.
                            // Do not introduce an await between the reserve and the completed payload write.
                            unsafe
                            {
                                var curr = writer.RegisterInlineRecord(address, totalLength);
                                SerializeCommand(curr, curr + totalLength);
                            }
                        }
                        else
                        {
                            writer.RegisterOfflineRecord(address, payload);
                        }
                    }
                    catch (ObjectDisposedException)
                    {
                        // The reserve/register throws only while the ring is being disposed, and only before the
                        // record can be sent. The completion was already registered above, so the teardown drain
                        // may have raced past its slot before the publication marker became visible, leaving the
                        // caller's completion stranded. Win the single-delivery claim and fault it here so it can
                        // never hang; if the drain already claimed it, this is a no-op. Lane accounting
                        // (tcsOffset/repliedUntil) is owned by the receive side (ProcessReplies /
                        // DisposeMessageConsumer); do not advance it here.
                        CompletionOnFault(writer, taskId);
                        throw;
                    }
                    payloadRegistered = true;

                    // Publication of the record may have raced a concurrent teardown: the completion is already
                    // registered but the send will never happen, and the receive-side drain may have passed this
                    // ticket before RegisterCompletion became visible. Fault it here (single-claim, a no-op if the
                    // drain already faulted it) before throwing so the caller can never hang.
                    if (Disposed || writer.Closed || !ReferenceEquals(writer, Volatile.Read(ref networkWriter)))
                    {
                        CompletionOnFault(writer, taskId);
                        ThrowException(disposeException);
                    }

                    writer.epoch.ProtectAndDrain();
                    writer.DrainRequests();
                }
                finally
                {
                    writer.epoch.Suspend();
                }
            }
            finally
            {
                // Inline owns no pooled buffer (payload is default); only an unregistered out-of-line buffer needs
                // returning. A registered payload is owned by the ring and freed on flush.
                if (!inline && !payloadRegistered)
                    payload.Dispose();
            }
        }

        /// <summary>
        /// Issue a command for execution without expecting a response.
        /// </summary>
        void InternalExecuteNoResponse(ReadOnlyMemory<byte> respOp, ReadOnlySpan<byte> subop, Span<byte> param1, Span<byte> param2, CancellationToken token = default)
        {
            var writer = networkWriter;
            const int arraySize = 4;
            var totalLength = checked(1 + NumUtils.CountDigits(arraySize) + 2 + respOp.Length);

            var length = subop.Length;
            totalLength = checked(totalLength + 1 + NumUtils.CountDigits(length) + 2 + length + 2);
            length = param1.Length;
            totalLength = checked(totalLength + 1 + NumUtils.CountDigits(length) + 2 + length + 2);
            length = param2.Length;
            totalLength = checked(totalLength + 1 + NumUtils.CountDigits(length) + 2 + length + 2);

            var recordSize = writer.GetRecordSize(totalLength, out var inline);

            LightRequestContext payload = default;
            var payloadRegistered = false;

            static unsafe void SerializeCommand(byte* curr, byte* end, int arraySize, ReadOnlySpan<byte> respOp,
                ReadOnlySpan<byte> subop, ReadOnlySpan<byte> param1, ReadOnlySpan<byte> param2)
            {
                if (!RespWriteUtils.TryWriteArrayLength(arraySize, ref curr, end) ||
                    !RespWriteUtils.TryWriteDirect(respOp, ref curr, end) ||
                    !RespWriteUtils.TryWriteBulkString(subop, ref curr, end) ||
                    !RespWriteUtils.TryWriteBulkString(param1, ref curr, end) ||
                    !RespWriteUtils.TryWriteBulkString(param2, ref curr, end))
                {
                    throw new InvalidOperationException("Unable to serialize the command into its reserved slot.");
                }

                if (curr != end)
                    throw new InvalidOperationException("The serialized command did not fill its reserved slot.");
            }

            try
            {
                if (!inline)
                {
                    var allocationSize = 0;
                    var admissionReserved = false;
                    try
                    {
                        allocationSize = writer.GetRequestBufferAllocationSize(totalLength);
                        if (!writer.AdmitOutOfLineRental(allocationSize, token))
                            throw new InvalidOperationException("The out-of-line rental admission queue is full.");
                        admissionReserved = true;

                        payload = writer.RentAdmittedOutOfLineBuffer(totalLength, allocationSize);
                        admissionReserved = false;
                    }
                    catch
                    {
                        if (admissionReserved)
                            writer.ReleaseOutOfLineRental(allocationSize);
                        throw;
                    }

                    unsafe
                    {
                        fixed (byte* payloadPtr = payload.Buffer)
                            SerializeCommand(payloadPtr, payloadPtr + payload.Length, arraySize, respOp.Span, subop, param1, param2);
                    }
                }

                try
                {
                    writer.epoch.Resume();

                    long address;
                    while (true)
                    {
                        token.ThrowIfCancellationRequested();
                        if (Disposed || writer.Closed || !ReferenceEquals(writer, Volatile.Read(ref networkWriter)) || !IsConnected)
                        {
                            if (!inline)
                            {
                                payload.Dispose();
                                payload = default;
                            }
                            ThrowException(disposeException);
                        }

                        if (writer.TryScheduleSend(
                            recordSize,
                            expectsResponse: false,
                            out var reservation,
                            out var flushEvent))
                        {
                            address = reservation.requestAddress;
                            break;
                        }

                        try
                        {
                            writer.epoch.Suspend();
                            flushEvent.Wait(token);
                        }
                        finally
                        {
                            writer.epoch.Resume();
                        }
                    }

                    // Fire-and-forget: no completion ticket is consumed and nothing is registered in the
                    // completion lane. A reply arriving for one of these is a protocol violation the reply
                    // reader is expected to assert on.
                    if (inline)
                    {
                        unsafe
                        {
                            var curr = writer.RegisterInlineRecord(address, totalLength);
                            SerializeCommand(curr, curr + totalLength, arraySize, respOp.Span, subop, param1, param2);
                        }
                    }
                    else
                    {
                        writer.RegisterOfflineRecord(address, payload);
                    }
                    payloadRegistered = true;

                    if (Disposed || writer.Closed || !ReferenceEquals(writer, Volatile.Read(ref networkWriter)))
                        ThrowException(disposeException);

                    writer.epoch.ProtectAndDrain();
                    writer.DrainRequests();
                }
                finally
                {
                    writer.epoch.Suspend();
                }
            }
            finally
            {
                if (!inline && !payloadRegistered)
                    payload.Dispose();
            }
        }

        static void CompleteOnFailure(TcsWrapper tcs, Exception exception)
        {
            switch (tcs.taskType)
            {
                case TaskType.StringCallback:
                    tcs.stringCallback?.Invoke(-1, null);
                    break;
                case TaskType.MemoryByteCallback:
                    tcs.memoryByteCallback?.Invoke(-1, default);
                    break;
                case TaskType.StringAsync:
                    if (exception is OperationCanceledException stringCanceled)
                        tcs.stringTcs?.TrySetCanceled(stringCanceled.CancellationToken);
                    else
                        tcs.stringTcs?.TrySetException(exception);
                    break;
                case TaskType.StringArrayAsync:
                    if (exception is OperationCanceledException stringArrayCanceled)
                        tcs.stringArrayTcs?.TrySetCanceled(stringArrayCanceled.CancellationToken);
                    else
                        tcs.stringArrayTcs?.TrySetException(exception);
                    break;
                case TaskType.MemoryByteAsync:
                    if (exception is OperationCanceledException memoryCanceled)
                        tcs.memoryByteTcs?.TrySetCanceled(memoryCanceled.CancellationToken);
                    else
                        tcs.memoryByteTcs?.TrySetException(exception);
                    break;
                case TaskType.MemoryByteArrayAsync:
                    if (exception is OperationCanceledException memoryArrayCanceled)
                        tcs.memoryByteArrayTcs?.TrySetCanceled(memoryArrayCanceled.CancellationToken);
                    else
                        tcs.memoryByteArrayTcs?.TrySetException(exception);
                    break;
                case TaskType.StringArrayCallback:
                    tcs.stringArrayCallback?.Invoke(-1, default, default);
                    break;
                case TaskType.MemoryByteArrayCallback:
                    tcs.memoryByteArrayCallback?.Invoke(-1, default, default);
                    break;
                case TaskType.LongAsync:
                    tcs.longTcs?.TrySetException(exception);
                    break;
                case TaskType.LongCallback:
                    tcs.longCallback?.Invoke(-1, default, null);
                    break;
                case TaskType.None:
                default:
                    break;
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
            if (!ReferenceEquals(session, networkHandler))
                return;

            var writer = networkWriter;
            writer?.Close();

            var c = tcsOffset;
            while (writer != null && c != writer.CompletionTail)
            {
                DisposeOffset(writer, c);
                c = (c + 1) & (int)PageOffset.kTaskMask;
            }
        }

        private void DisposeOffset(LightNetworkWriter writer, int taskId)
        {
            CompletionOnFault(writer, taskId);
            ConsumeTcsOffset(writer);
        }

        /// <summary>
        /// Fault the completion for <paramref name="taskId"/> with the teardown outcome, without advancing the
        /// reply watermark. Wins the single-delivery claim (<see cref="LightNetworkWriter.TryClaimCompletionTicket"/>)
        /// first, so a completion targeted by both the receive-side drain (<see cref="DisposeOffset"/>) and the
        /// producer whose request failed to publish is faulted exactly once — safe for both the async and
        /// callback types. No-op if the completion was not published or was already claimed by the other path.
        /// </summary>
        private static void CompletionOnFault(LightNetworkWriter writer, int taskId)
        {
            if (!writer.TryClaimCompletionTicket(taskId, out var tcs))
                return;
            CompleteOnFailure(tcs, disposeException);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ConsumeTcsOffset()
        {
            tcsOffset = (tcsOffset + 1) & (int)PageOffset.kTaskMask;
            networkWriter.AdvanceReplied(1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ConsumeTcsOffset(LightNetworkWriter writer)
        {
            tcsOffset = (tcsOffset + 1) & (int)PageOffset.kTaskMask;
            writer.AdvanceReplied(1);
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