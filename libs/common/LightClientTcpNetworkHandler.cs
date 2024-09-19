// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net.Sockets;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    internal sealed class LightClientTcpNetworkHandler : TcpNetworkHandler<LightClient>
    {
        public LightClientTcpNetworkHandler(LightClient serverHook, Socket socket, NetworkBufferSpecs networkBufferSpecs, LimitedFixedBufferPool networkPool, bool useTLS, IMessageConsumer messageConsumer, int networkSendThrottleMax = 8, ILogger logger = null)
            : base(serverHook, socket, networkBufferSpecs, networkPool, useTLS, messageConsumer, networkSendThrottleMax: networkSendThrottleMax, logger: logger)
        {
        }

        public byte[] RawTransportBuffer => transportReceiveBuffer;
    }
}