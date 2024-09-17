// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
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
        readonly Socket servSocket;
        readonly IGarnetTlsOptions tlsOptions;
        readonly int networkSendThrottleMax;
        readonly NetworkBuffers networkBuffers;

        public IPEndPoint GetEndPoint
        {
            get
            {
                var ip = string.IsNullOrEmpty(Address) ? IPAddress.Any : IPAddress.Parse(Address);
                return new IPEndPoint(ip, Port);
            }
        }

        /// <summary>
        /// Get active consumers
        /// </summary>
        public override IEnumerable<IMessageConsumer> ActiveConsumers()
        {
            foreach (var kvp in activeHandlers)
            {
                var consumer = kvp.Key.Session;
                if (consumer != null)
                    yield return consumer;
            }
        }

        /// <summary>
        /// Get active consumers
        /// </summary>
        public IEnumerable<IClusterSession> ActiveClusterSessions()
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
        /// <param name="address"></param>
        /// <param name="port"></param>
        /// <param name="networkBufferSize"></param>
        /// <param name="tlsOptions"></param>
        /// <param name="networkSendThrottleMax"></param>
        /// <param name="logger"></param>
        public GarnetServerTcp(string address, int port, int networkBufferSize = default, IGarnetTlsOptions tlsOptions = null, int networkSendThrottleMax = 8, ILogger logger = null)
            : base(address, port, networkBufferSize, logger)
        {
            this.tlsOptions = tlsOptions;
            this.networkSendThrottleMax = networkSendThrottleMax;
            var serverBufferSize = BufferSizeUtils.ServerBufferSize(new MaxSizeSettings());
            this.networkBuffers = new NetworkBuffers(serverBufferSize, serverBufferSize).Allocate(logger: logger);
            servSocket = new Socket(GetEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            acceptEventArg = new SocketAsyncEventArgs();
            acceptEventArg.Completed += AcceptEventArg_Completed;
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();
            servSocket.Dispose();
            acceptEventArg.UserToken = null;
            acceptEventArg.Dispose();
            networkBuffers.Dispose();
        }

        /// <summary>
        /// Start listening to incoming requests
        /// </summary>
        public override void Start()
        {
            var endPoint = GetEndPoint;
            servSocket.Bind(endPoint);
            servSocket.Listen(512);
            if (!servSocket.AcceptAsync(acceptEventArg))
                AcceptEventArg_Completed(null, acceptEventArg);
        }

        private void AcceptEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            try
            {
                do
                {
                    if (!HandleNewConnection(e)) break;
                    e.AcceptSocket = null;
                } while (!servSocket.AcceptAsync(e));
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

            e.AcceptSocket.NoDelay = true;

            // Ok to create new event args on accept because we assume a connection to be long-running
            string remoteEndpointName = e.AcceptSocket.RemoteEndPoint?.ToString();
            logger?.LogDebug("Accepted TCP connection from {remoteEndpoint}", remoteEndpointName);


            ServerTcpNetworkHandler handler = null;
            if (activeHandlerCount >= 0)
            {
                if (Interlocked.Increment(ref activeHandlerCount) > 0)
                {
                    try
                    {
                        handler = new ServerTcpNetworkHandler(this, e.AcceptSocket, networkBuffers, tlsOptions != null, networkSendThrottleMax: networkSendThrottleMax, logger: logger);
                        if (!activeHandlers.TryAdd(handler, default))
                            throw new Exception("Unable to add handler to dictionary");

                        handler.Start(tlsOptions?.TlsServerOptions, remoteEndpointName);
                        incr_conn_recv();
                        return true;
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "Error starting network handler");
                        Interlocked.Decrement(ref activeHandlerCount);
                        handler?.Dispose();
                    }
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
                incr_conn_disp();
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

        public bool Purge()
        {
            networkBuffers.Purge();
            return true;
        }

        public string GetBufferPoolStats() => networkBuffers.GetStats();
    }
}