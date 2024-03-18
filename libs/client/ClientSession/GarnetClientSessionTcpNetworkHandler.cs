// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net.Sockets;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;

namespace Garnet.client
{
    sealed class GarnetClientSessionTcpNetworkHandler : TcpNetworkHandlerBase<GarnetClientSession, GarnetTcpNetworkSender>
    {
        public GarnetClientSessionTcpNetworkHandler(GarnetClientSession serverHook, Socket socket, LimitedFixedBufferPool networkPool, bool useTLS, IMessageConsumer messageConsumer, int networkSendThrottleMax = 8, ILogger logger = null)
            : base(serverHook, new GarnetTcpNetworkSender(socket, networkPool, networkSendThrottleMax), socket, networkPool, useTLS, messageConsumer, logger)
        {
        }

        public byte[] RawTransportBuffer => transportReceiveBuffer;
    }
}