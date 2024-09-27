// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net.Sockets;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;

namespace Garnet.client
{
    sealed class GarnetClientTcpNetworkHandler : TcpNetworkHandlerBase<GarnetClient, ClientTcpNetworkSender>
    {
        public GarnetClientTcpNetworkHandler(GarnetClient serverHook, Action<object> callback, Socket socket, NetworkBufferSettings networkBufferSettings, LimitedFixedBufferPool networkPool, bool useTLS, IMessageConsumer messageConsumer, int networkSendThrottleMax = 8, ILogger logger = null)
            : base(serverHook, new ClientTcpNetworkSender(socket, callback, networkBufferSettings, networkPool, networkSendThrottleMax), socket, networkBufferSettings, networkPool, useTLS, messageConsumer: messageConsumer, logger: logger)
        {
        }

        public byte[] RawTransportBuffer => transportReceiveBuffer;
    }
}