// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net.Sockets;
using Garnet.networking;
using Microsoft.Extensions.Logging;

namespace Garnet.common
{
    /// <summary>
    /// TCP network handler
    /// </summary>
    /// <typeparam name="TServerHook"></typeparam>
    public abstract class TcpNetworkHandler<TServerHook> : TcpNetworkHandlerBase<TServerHook, GarnetTcpNetworkSender>
        where TServerHook : IServerHook
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="networkSendTimeout">Longest a send waits for an earlier send to complete before the connection is closed. Zero or negative waits indefinitely.</param>
        /// <param name="tlsHandshakeTimeout">Longest a server-side TLS handshake may take before the connection is closed. Zero or negative waits indefinitely.</param>
        public TcpNetworkHandler(TServerHook serverHook, Socket socket, NetworkBufferSettings networkBufferSettings, LimitedFixedBufferPool networkPool, bool useTLS, IMessageConsumer messageConsumer = null, int networkSendThrottleMax = 8, ILogger logger = null, TimeSpan networkSendTimeout = default, TimeSpan tlsHandshakeTimeout = default)
            : base(serverHook, new GarnetTcpNetworkSender(socket, networkBufferSettings, networkPool, networkSendThrottleMax, networkSendTimeout), socket, networkBufferSettings, networkPool, useTLS, messageConsumer: messageConsumer, logger: logger, tlsHandshakeTimeout: tlsHandshakeTimeout)
        {
        }
    }
}