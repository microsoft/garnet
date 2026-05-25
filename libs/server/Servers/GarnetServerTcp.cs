// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
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
        readonly LimitedFixedBufferPool networkPool;
        readonly int networkConnectionLimit;
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
        /// <param name="networkConnectionLimit"></param>
        /// <param name="unixSocketPath"></param>
        /// <param name="unixSocketPermission"></param>
        /// <param name="logger"></param>
        public GarnetServerTcp(
            EndPoint endpoint,
            int networkBufferSize = default,
            IGarnetTlsOptions tlsOptions = null,
            int networkSendThrottleMax = 8,
            int networkConnectionLimit = -1,
            string unixSocketPath = null,
            UnixFileMode unixSocketPermission = default,
            ILogger logger = null)
            : base(endpoint, networkBufferSize, logger)
        {
            this.networkConnectionLimit = networkConnectionLimit;
            this.tlsOptions = tlsOptions;
            this.networkSendThrottleMax = networkSendThrottleMax;
            var serverBufferSize = BufferSizeUtils.ServerBufferSize(new MaxSizeSettings());
            this.networkBufferSettings = new NetworkBufferSettings(serverBufferSize, serverBufferSize);
            this.networkPool = networkBufferSettings.CreateBufferPool(ownerType: PoolOwnerType.ServerNetwork, logger: logger);
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

                // Set reuse BEFORE Bind to handle TIME_WAIT states
                listenSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
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
                if (currentActiveHandlerCount > 0 && (networkConnectionLimit == -1 || currentActiveHandlerCount <= networkConnectionLimit))
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
                        handler.Start(tlsOptions?.TlsServerOptions, remoteEndpointName);
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
                    e.AcceptSocket.Dispose();
                }
            }
            return true;
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