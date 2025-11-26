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
        volatile bool isListening;

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
            isListening = true;
            if (!listenSocket.AcceptAsync(acceptEventArg))
                AcceptEventArg_Completed(null, acceptEventArg);
        }

        /// <inheritdoc />
        public override void StopListening()
        {
            if (!isListening)
                return;

            isListening = false;
            try
            {
                // Close the listen socket to stop accepting new connections
                // This will cause any pending AcceptAsync to complete with an error
                listenSocket.Close();
                logger?.LogInformation("Stopped accepting new connections on {endpoint}", EndPoint);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "Error closing listen socket on {endpoint}", EndPoint);
            }
        }

        private void AcceptEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            try
            {
                do
                {
                    // Check isListening flag before processing and before calling AcceptAsync again
                    if (!isListening) break;

                    if (!HandleNewConnection(e)) break;
                    e.AcceptSocket = null;
                } while (isListening && !listenSocket.AcceptAsync(e));
            }
            // socket disposed
            catch (ObjectDisposedException) { }
        }

        private unsafe bool HandleNewConnection(SocketAsyncEventArgs e)
        {
            if (e.SocketError != SocketError.Success)
            {
                e.Dispose();
                return false;
            }

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