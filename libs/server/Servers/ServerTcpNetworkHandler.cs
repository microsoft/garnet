// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net.Sockets;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    internal sealed class ServerTcpNetworkHandler : TcpNetworkHandler<GarnetServerTcp>
    {
        public ServerTcpNetworkHandler(GarnetServerTcp serverHook, Socket socket, LimitedFixedBufferPool sendNetworkPool, bool useTLS, LimitedFixedBufferPool recvNetworkPool = null, int networkSendThrottleMax = 8, ILogger logger = null)
            : base(serverHook, socket, sendNetworkPool, useTLS, null, recvNetworkPool: recvNetworkPool, networkSendThrottleMax: networkSendThrottleMax, logger: logger)
        {
        }
    }
}