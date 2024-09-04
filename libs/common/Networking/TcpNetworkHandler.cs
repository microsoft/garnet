// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
        public TcpNetworkHandler(TServerHook serverHook, Socket socket, NetworkBuffers networkBuffers, bool useTLS, IMessageConsumer messageConsumer = null, LimitedFixedBufferPool recvNetworkPool = null, int networkSendThrottleMax = 8, ILogger logger = null)
            : base(serverHook, new GarnetTcpNetworkSender(socket, networkBuffers.bufferPool, networkSendThrottleMax), socket, networkBuffers, useTLS, messageConsumer: messageConsumer, logger: logger)
        {
        }
    }
}