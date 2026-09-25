// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Garnet.server.TLS;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Garnet server for TCP
    /// </summary>
    public class GarnetServerTcp : GarnetServerBase, IServerHook
    {
        readonly SocketAsyncEventArgs acceptEventArg;
        readonly Socket listenSocket;
        readonly IGarnetTlsOptions tlsOptions;
        readonly int networkSendThrottleMax;
        readonly NetworkBufferSettings networkBufferSettings;

        /// <summary>
        /// Process-wide budget for live connection buffers. Shared across all listeners so the ceiling is
        /// genuinely process-wide rather than per-endpoint.
        /// </summary>
        readonly NetworkBufferBudget networkBufferBudget;

        /// <summary>
        /// Process-wide budget for live connection buffers.
        /// </summary>
        public NetworkBufferBudget NetworkBufferBudget => networkBufferBudget;
        readonly LimitedFixedBufferPool networkPool;
        readonly string unixSocketPath;
        readonly UnixFileMode unixSocketPermission;

        // Accept loop backoff state for resource pressure errors
        const int InitialAcceptBackoffMs = 100;
        const int MaxAcceptBackoffMs = 5000;
        int acceptBackoffMs = InitialAcceptBackoffMs;

        /// <inheritdoc/>
        public override IEnumerable<IMessageConsumer> ActiveConsumers()
        {
            foreach (var kvp in activeHandlers)
            {
                var consumer = kvp.Key.Session;
                if (consumer != null)
                    yield return consumer;
            }
        }

        /// <inheritdoc/>
        public override IEnumerable<IClusterSession> ActiveClusterSessions()
        {
            foreach (var kvp in activeHandlers)
            {
                var consumer = kvp.Key.Session;
                if (consumer != null)
                    yield return ((RespServerSession)consumer).clusterSession;
            }
        }

        /// <summary>
        /// Constructor for server
        /// </summary>
        /// <param name="endpoint">Endpoint bound for listening for connections.</param>
        /// <param name="networkBufferSize"></param>
        /// <param name="tlsOptions"></param>
        /// <param name="networkSendThrottleMax"></param>
        /// <param name="connectionLimit">Process-wide connection admission control, shared across listeners. Null enforces no limit.</param>
        /// <param name="unixSocketPath"></param>
        /// <param name="unixSocketPermission"></param>
        /// <param name="networkBufferSettings">Send/receive buffer sizing. Defaults to the built-in sizes when null.</param>
        /// <param name="networkBufferPoolSize">Ceiling on idle bytes retained by the shared buffer pool. Zero uses the pool default.</param>
        /// <param name="networkBufferBudget">Process-wide budget for live connection buffers, shared across listeners. Null disables adaptation.</param>
        /// <param name="logger"></param>
        public GarnetServerTcp(
            EndPoint endpoint,
            int networkBufferSize = default,
            IGarnetTlsOptions tlsOptions = null,
            int networkSendThrottleMax = 8,
            ConnectionLimit connectionLimit = null,
            string unixSocketPath = null,
            UnixFileMode unixSocketPermission = default,
            NetworkBufferSettings networkBufferSettings = null,
            long networkBufferPoolSize = 0,
            NetworkBufferBudget networkBufferBudget = null,
            ILogger logger = null)
            : base(endpoint, networkBufferSize, logger)
        {
            // A listener given no shared limit gets its own, so it still honours CONFIG SET and so
            // that no process-wide static accumulates references to every listener ever created.
            ConnectionLimit = connectionLimit ?? new ConnectionLimit(ConnectionLimit.Unlimited);
            ConnectionLimit.Register(this);

            this.tlsOptions = tlsOptions;
            this.networkSendThrottleMax = networkSendThrottleMax;
            if (networkBufferSettings == null)
            {
                var serverBufferSize = BufferSizeUtils.ServerBufferSize(new MaxSizeSettings());
                networkBufferSettings = new NetworkBufferSettings(serverBufferSize, serverBufferSize);
            }
            this.networkBufferSettings = networkBufferSettings;
            this.networkBufferBudget = networkBufferBudget ?? NetworkBufferBudget.Disabled;
            this.networkPool = networkBufferSettings.CreateBufferPool(ownerType: PoolOwnerType.ServerNetwork, maxPooledBytes: networkBufferPoolSize, budget: this.networkBufferBudget, logger: logger);
            networkBufferSettings.Log(logger, "GarnetServerTcp");
            this.unixSocketPath = unixSocketPath;
            this.unixSocketPermission = unixSocketPermission;

            if (endpoint is UnixDomainSocketEndPoint unix)
            {
                // UDS Initialization & Cleanup
                listenSocket = new Socket(unix.AddressFamily, SocketType.Stream, ProtocolType.Unspecified);
                var socketPath = unix.ToString();
                if (File.Exists(socketPath))
                {
                    File.Delete(socketPath);
                }
            }
            else
            {
                // TCP Initialization & Port Reuse
                listenSocket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

                // Set reuse BEFORE Bind to handle TIME_WAIT states.
                listenSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);

                // On Unix, .NET's ReuseAddress sets both SO_REUSEADDR and SO_REUSEPORT.
                // Keep address reuse for restarts, but do not let two live servers share a port.
                if (OperatingSystem.IsLinux())
                    listenSocket.SetRawSocketOption(1 /* SOL_SOCKET */, 15 /* SO_REUSEPORT */, BitConverter.GetBytes(0));
                else if (OperatingSystem.IsMacOS() || OperatingSystem.IsFreeBSD())
                    listenSocket.SetRawSocketOption(0xffff /* SOL_SOCKET */, 0x0200 /* SO_REUSEPORT */, BitConverter.GetBytes(0));
            }

            acceptEventArg = new SocketAsyncEventArgs();
            acceptEventArg.Completed += AcceptEventArg_Completed;
        }

        /// <summary>
        /// Stop listening for new connections. Frees the listening port
        /// without waiting for active connections to drain.
        /// </summary>
        public override void Close()
        {
            listenSocket.Close();
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public override void Dispose()
        {
            // Close listening socket to free the port and stop accepting new connections.
            // This also prevents new connections from arriving while DisposeActiveHandlers drains existing ones.
            listenSocket.Dispose();
            base.Dispose();
            acceptEventArg.UserToken = null;
            acceptEventArg.Dispose();
            networkPool?.Dispose();
        }

        /// <summary>
        /// Start listening to incoming requests
        /// </summary>
        public override void Start()
        {
            listenSocket.Bind(EndPoint);
            if (EndPoint is UnixDomainSocketEndPoint && unixSocketPermission != default && !OperatingSystem.IsWindows())
            {
                File.SetUnixFileMode(unixSocketPath, unixSocketPermission);
            }

            listenSocket.Listen(512);
            if (!listenSocket.AcceptAsync(acceptEventArg))
                AcceptEventArg_Completed(null, acceptEventArg);
        }

        private void AcceptEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            try
            {
                do
                {
                    // HandleNewConnection returns true to continue accepting, false to stop.
                    // When the while condition exits normally (AcceptAsync returned true), an
                    // accept is already pending on IOCP — the callback fires on next connection.
                    // When HandleNewConnection returns false, the break skips the while condition
                    // entirely — no AcceptAsync is issued, and the accept loop exits.
                    // This only happens during shutdown (Tier 1 fatal errors).
                    // Tier 2 (resource pressure) uses Thread.Sleep for backoff and returns true
                    // to continue the loop. Tier 3 (transient) returns true immediately.
                    if (!HandleNewConnection(e)) break;
                    e.AcceptSocket = null;
                } while (!listenSocket.AcceptAsync(e));
            }
            // socket disposed
            catch (ObjectDisposedException) { }
        }

        private bool HandleAcceptError(SocketAsyncEventArgs e)
        {
            // Dispose any socket the failed accept may have created
            e.AcceptSocket?.Dispose();
            e.AcceptSocket = null;

            switch (e.SocketError)
            {
                // Tier 1 — Fatal: listen socket is dead, stop accepting
                case SocketError.OperationAborted:
                case SocketError.Shutdown:
                    // Clean shutdown — Dispose() already closed the listen socket,
                    // which triggered this error. Exit the accept loop silently.
                    // Don't dispose e here — GarnetServerTcp.Dispose() owns that cleanup.
                    return false;

                case SocketError.NotSocket:
                case SocketError.NotInitialized:
                case SocketError.VersionNotSupported:
                    // Fatal and not a clean shutdown — the listen socket is corrupt.
                    // Throw to crash the process. The OS will free all sockets and
                    // resources when the process terminates.
                    logger?.LogCritical("Fatal accept error, crashing: {error}", e.SocketError);
                    throw new SocketException((int)e.SocketError);

                // Tier 2 — Resource pressure: backoff before retrying
                case SocketError.TooManyOpenSockets:
                case SocketError.NoBufferSpaceAvailable:
                case SocketError.NetworkDown:
                case SocketError.SystemNotReady:
                case SocketError.ProcessLimit:
                    logger?.LogWarning("Accept backoff ({backoffMs}ms) due to resource pressure: {error}", acceptBackoffMs, e.SocketError);
                    // This will hold the IOCP thread hostage, but anyway we hold it hostage in the do while accept loop. So this is not really deflecting from design.
                    // If this is ever a concern in the future we can use a timer to schedule the next attempt instead of blocking the thread.
                    Thread.Sleep(acceptBackoffMs);
                    acceptBackoffMs = Math.Min(acceptBackoffMs * 2, MaxAcceptBackoffMs);
                    return true;

                // Tier 3 — Client-caused transient or irrelevant: log and continue
                default:
                    logger?.LogDebug("Transient accept error, continuing: {error}", e.SocketError);
                    return true;
            }
        }

        private unsafe bool HandleNewConnection(SocketAsyncEventArgs e)
        {
            if (e.SocketError != SocketError.Success)
            {
                return HandleAcceptError(e);
            }

            // Reset backoff on successful accept
            acceptBackoffMs = InitialAcceptBackoffMs;

            ServerTcpNetworkHandler handler = null;
            if (activeHandlerCount >= 0)
            {
                var currentActiveHandlerCount = Interlocked.Increment(ref activeHandlerCount);
                if (currentActiveHandlerCount > 0 && ConnectionLimit.IsWithinLimit())
                {
                    string remoteEndpointName = null;
                    try
                    {
                        // Configure the accepted socket. These can throw SocketException if the
                        // peer RST'd between accept completing and here — the socket is "successful"
                        // from the kernel's perspective but already dead.
                        if (e.AcceptSocket.LocalEndPoint is not UnixDomainSocketEndPoint)
                            e.AcceptSocket.NoDelay = true;

                        remoteEndpointName = e.AcceptSocket.RemoteEndPoint?.ToString();
                        logger?.LogDebug("Accepted TCP connection from {remoteEndpoint}", remoteEndpointName);

                        handler = new ServerTcpNetworkHandler(this, e.AcceptSocket, networkBufferSettings, networkPool, tlsOptions != null, networkSendThrottleMax: networkSendThrottleMax, logger: logger);
                        ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Network_After_GarnetServerTcp_Handler_Created);
                        if (!activeHandlers.TryAdd(handler, default))
                            throw new Exception("Unable to add handler to dictionary");
                    }
                    catch (Exception ex)
                    {
                        // We need to decrement the active handler count and dispose because the handler was not added to the activeHandlers dictionary.
                        _ = Interlocked.Decrement(ref activeHandlerCount);
                        if (handler != null)
                        {
                            logger?.LogError(ex, "Error creating and registering network handler");
                            // We did not start the handler, so we need to call DisposeResources() to clean up resources.
                            handler.DisposeResources();
                            handler.Dispose();
                        }
                        else
                        {
                            if (ex is SocketException se)
                            {
                                logger?.LogDebug("Transient socket error during connection setup (SocketErrorCode: {errorCode}), continuing", se.SocketErrorCode);
                            }
                            else
                            {
                                logger?.LogError(ex, "Unexpected error during connection setup, continuing");
                            }
                            // Handler was never created (e.g. dead socket) — dispose the socket directly
                            e.AcceptSocket?.Dispose();
                        }
                        return true;
                    }

                    try
                    {
                        IncrementConnectionsReceived();
                        ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Dispose_After_Handler_Registered_Before_Start);

                        // A TLS handshake takes several network round trips, driven by the peer. Completing it here
                        // would hold the accept loop for its whole duration, so a peer that connects and negotiates
                        // slowly - or never - stops this server from accepting any other connection. StartAsync
                        // performs the socket setup synchronously and leaves only the handshake to the thread pool.
                        var startTask = handler.StartAsync(tlsOptions?.TlsServerOptions, remoteEndpointName);
                        if (!startTask.IsCompletedSuccessfully)
                            _ = ObserveHandlerStartAsync(startTask, handler);
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "Error calling Start on network handler");

                        // Dispose the socket if we get an exception while starting.
                        // The resources will be disposed (including updating activeHandlerCount and totalConnectionsDisposed)
                        // when the handler is removed from the activeHandlers dictionary as part of socket exception
                        // handling in TcpNetworkHandlerBase.Start(), which will call NetworkHandler.DisposeImpl().
                        handler.Dispose();
                        return true;
                    }
                }
                else
                {
                    _ = Interlocked.Decrement(ref activeHandlerCount);
                    RejectConnection(e.AcceptSocket);
                }
            }
            return true;
        }

        /// <summary>
        /// Observes the result of an in-flight handler start so a failed handshake is reported and cleaned up
        /// the same way a synchronous failure is, without keeping the accept loop waiting for it.
        /// </summary>
        /// <param name="startTask">Task returned by the handler's asynchronous start.</param>
        /// <param name="handler">Handler the task belongs to.</param>
        private async Task ObserveHandlerStartAsync(Task startTask, ServerTcpNetworkHandler handler)
        {
            try
            {
                await startTask.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error calling Start on network handler");
                handler.Dispose();
            }
        }

        /// <summary>
        /// RESP error returned to a client refused because the connection limit was reached.
        /// Matches the Redis wire text so existing client error handling applies unchanged.
        /// </summary>
        static ReadOnlySpan<byte> MaxClientsReachedError => "-ERR max number of clients reached\r\n"u8;

        /// <summary>
        /// Refuse a connection that exceeded the configured connection limit, telling the client
        /// why before closing so the failure is diagnosable rather than an unexplained reset.
        /// </summary>
        /// <param name="socket">The accepted socket to refuse and dispose.</param>
        void RejectConnection(Socket socket)
        {
            IncrementConnectionsRejected();

            // Only plaintext clients can be told. Under TLS the peer has sent a ClientHello and is
            // waiting for a ServerHello, so writing a RESP error would be a protocol violation and
            // would surface as a handshake failure -- pointing the operator at certificates rather
            // than at capacity. For TLS the rejected_connections metric is the whole remedy.
            if (tlsOptions == null)
            {
                try
                {
                    // Best effort, and deliberately non-blocking: reaching the limit means the
                    // server is already under connection pressure, so a blocking write here would
                    // serialize refusals on the accept path and amplify the overload. The payload
                    // is a few dozen bytes into an empty send buffer, so it fits in practice; if it
                    // ever does not, dropping it is better than stalling the accept loop.
                    socket.Blocking = false;
                    _ = socket.Send(MaxClientsReachedError);
                }
                catch (SocketException ex)
                {
                    // Includes WouldBlock, and a peer that reset between accept and here.
                    logger?.LogDebug("Could not send connection-limit error to client (SocketErrorCode: {errorCode})", ex.SocketErrorCode);
                }
                catch (ObjectDisposedException) { }
            }

            // Graceful close: no linger is configured anywhere, so the kernel flushes anything
            // queued above before the FIN. An abortive close would discard the error.
            socket.Dispose();
        }

        /// <summary>
        /// Create session (message consumer) given incoming bytes
        /// </summary>
        /// <param name="bytes"></param>
        /// <param name="networkSender"></param>
        /// <param name="session"></param>
        /// <returns></returns>
        public bool TryCreateMessageConsumer(Span<byte> bytes, INetworkSender networkSender, out IMessageConsumer session)
        {
            session = null;

            // We need at least 4 bytes to determine session            
            if (bytes.Length < 4)
                return false;

            WireFormat protocol = WireFormat.ASCII;

            if (!GetSessionProviders().TryGetValue(protocol, out var provider))
            {
                var input = System.Text.Encoding.ASCII.GetString(bytes);
                logger?.LogError("Cannot identify wire protocol {bytes}", input);
                throw new Exception($"Unsupported incoming wire format {protocol} {input}");
            }

            if (!AddSession(protocol, ref provider, networkSender, out session))
                throw new Exception($"Unable to add session");

            return true;
        }

        /// <inheritdoc />
        public void DisposeMessageConsumer(INetworkHandler session)
        {
            if (activeHandlers.TryRemove(session, out _))
            {
                Interlocked.Decrement(ref activeHandlerCount);
                IncrementConnectionsDisposed();
                try
                {
                    session.Session?.Dispose();
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "Error disposing RespServerSession");
                }
            }
        }

        public void Purge() => networkPool.Purge();

        public string GetBufferPoolStats() => networkPool.GetStats();
    }
}