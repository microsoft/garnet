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
        Timer acceptRetryTimer;

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
            this.networkPool = networkBufferSettings.CreateBufferPool(logger: logger);
            this.unixSocketPath = unixSocketPath;
            this.unixSocketPermission = unixSocketPermission;

            listenSocket = endpoint switch
            {
                UnixDomainSocketEndPoint unix => new Socket(unix.AddressFamily, SocketType.Stream, ProtocolType.Unspecified),

                _ => new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            };

            acceptEventArg = new SocketAsyncEventArgs();
            acceptEventArg.Completed += AcceptEventArg_Completed;
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();
            listenSocket.Dispose();
            acceptRetryTimer?.Dispose();
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
                    /*
                    When the while condition exits normally(AcceptAsync returned true), an
                    accept is already pending on IOCP — the callback will fire on next connection.But when
                    HandleNewConnection returns false, the break skips the while entirely — no AcceptAsync is issued,
                    nothing is pending.
                    Without ScheduleAcceptRetry calling AcceptAsync after a delay, the accept loop would be permanently dead.
                    */
                    if (!HandleNewConnection(e)) break;
                    e.AcceptSocket = null;
                } while (!listenSocket.AcceptAsync(e));
            }
            // socket disposed
            catch (ObjectDisposedException) { }
        }

        private bool HandleAcceptError(SocketAsyncEventArgs e)
        {
            switch (e.SocketError)
            {
                // Tier 1 — Fatal: listen socket is dead, stop accepting
                case SocketError.OperationAborted:
                case SocketError.NotSocket:
                case SocketError.Shutdown:
                case SocketError.NotInitialized:
                case SocketError.VersionNotSupported:
                    if (!Disposed || e.SocketError != SocketError.OperationAborted)
                    {
                        logger?.LogCritical("Fatal accept error, stopping accept loop: {error}", e.SocketError);
                    }
                    e.Dispose();
                    return false;

                // Tier 2 — Resource pressure: backoff before retrying
                case SocketError.TooManyOpenSockets:
                case SocketError.NoBufferSpaceAvailable:
                case SocketError.NetworkDown:
                case SocketError.SystemNotReady:
                case SocketError.ProcessLimit:
                    logger?.LogWarning("Accept backoff ({backoffMs}ms) due to resource pressure: {error}", acceptBackoffMs, e.SocketError);
                    ScheduleAcceptRetry(e);
                    return false;

                // Tier 3 — Client-caused transient or irrelevant: log and continue
                default:
                    logger?.LogDebug("Transient accept error, continuing: {error}", e.SocketError);
                    return true;
            }
        }

        private void ScheduleAcceptRetry(SocketAsyncEventArgs e)
        {
            // Schedules the accept loop to resume after a backoff delay.
            // Use a Timer so we don't block the IOCP thread pool thread.
            acceptRetryTimer?.Dispose();
            acceptRetryTimer = new Timer(_ =>
            {
                // Best effort guard against race with Dispose(): Timer.Dispose() does not
                // guarantee an in-flight callback has finished, so this callback
                // can fire after the server has started disposing.
                // if disposed happens after check then try catch handles it.
                if (Disposed) return;

                try
                {
                    e.AcceptSocket = null;
                    if (!listenSocket.AcceptAsync(e))
                        AcceptEventArg_Completed(null, e);
                }
                catch (ObjectDisposedException) { }
            }, null, acceptBackoffMs, Timeout.Infinite);

            // Exponential backoff with cap
            acceptBackoffMs = Math.Min(acceptBackoffMs * 2, MaxAcceptBackoffMs);
        }

        private unsafe bool HandleNewConnection(SocketAsyncEventArgs e)
        {
            if (e.SocketError != SocketError.Success)
            {
                // returning false here breaks the accept loop in AcceptEventArg_Completed.
                // if the accept loop was broken due to backpressure/resource related errors
                // a timer will reschedule kicking off the loop again.
                return HandleAcceptError(e);
            }

            // Reset backoff on successful accept
            acceptBackoffMs = InitialAcceptBackoffMs;

            if (e.AcceptSocket.LocalEndPoint is not UnixDomainSocketEndPoint)
                e.AcceptSocket.NoDelay = true;

            // Ok to create new event args on accept because we assume a connection to be long-running
            string remoteEndpointName = e.AcceptSocket.RemoteEndPoint?.ToString();
            logger?.LogDebug("Accepted TCP connection from {remoteEndpoint}", remoteEndpointName);

            ServerTcpNetworkHandler handler = null;
            if (activeHandlerCount >= 0)
            {
                var currentActiveHandlerCount = Interlocked.Increment(ref activeHandlerCount);
                if (currentActiveHandlerCount > 0 && (networkConnectionLimit == -1 || currentActiveHandlerCount <= networkConnectionLimit))
                {
                    try
                    {
                        handler = new ServerTcpNetworkHandler(this, e.AcceptSocket, networkBufferSettings, networkPool, tlsOptions != null, networkSendThrottleMax: networkSendThrottleMax, logger: logger);
                        ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Network_After_GarnetServerTcp_Handler_Created);
                        if (!activeHandlers.TryAdd(handler, default))
                            throw new Exception("Unable to add handler to dictionary");
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "Error creating and registering network handler");

                        // We need to decrement the active handler count and dispose because the handler was not added to the activeHandlers dictionary.
                        _ = Interlocked.Decrement(ref activeHandlerCount);
                        // We did not start the handler, so we need to call DisposeResources() to clean up resources.
                        handler?.DisposeResources();
                        // Dispose the handler
                        handler?.Dispose();
                        return true;
                    }

                    try
                    {
                        IncrementConnectionsReceived();
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