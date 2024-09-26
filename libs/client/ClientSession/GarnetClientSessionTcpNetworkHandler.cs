﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net.Sockets;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;

namespace Garnet.client
{
    sealed class GarnetClientSessionTcpNetworkHandler : TcpNetworkHandlerBase<GarnetClientSession, GarnetTcpNetworkSender>
    {
        public GarnetClientSessionTcpNetworkHandler(GarnetClientSession serverHook, Socket socket, NetworkBufferSettings networkBufferSettings, LimitedFixedBufferPool networkPool, bool useTLS, IMessageConsumer messageConsumer, int networkSendThrottleMax = 8, ILogger logger = null)
            : base(serverHook, new GarnetTcpNetworkSender(socket, networkBufferSettings, networkPool, networkSendThrottleMax), socket, networkBufferSettings, networkPool, useTLS, messageConsumer: messageConsumer, logger: logger)
        {
        }

        public byte[] RawTransportBuffer => transportReceiveBuffer;
    }
}