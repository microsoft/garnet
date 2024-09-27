// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net.Sockets;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    internal sealed class ServerTcpNetworkHandler : TcpNetworkHandler<GarnetServerTcp>
    {
        public ServerTcpNetworkHandler(GarnetServerTcp serverHook, Socket socket, NetworkBufferSettings networkBufferSettings, LimitedFixedBufferPool networkPool, bool useTLS, int networkSendThrottleMax = 8, ILogger logger = null)
            : base(serverHook, socket, networkBufferSettings, networkPool, useTLS, null, networkSendThrottleMax: networkSendThrottleMax, logger: logger)
        {
        }
    }
}