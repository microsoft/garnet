// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net.Sockets;
using System.Threading;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;

namespace Garnet.client
{
    sealed class GarnetClientSessionTcpNetworkHandler : TcpNetworkHandlerBase<GarnetClientSession, GarnetTcpNetworkSender>
    {
        int clientSendBufferReleased;

        public GarnetClientSessionTcpNetworkHandler(GarnetClientSession serverHook, Socket socket, NetworkBufferSettings networkBufferSettings, LimitedFixedBufferPool networkPool, bool useTLS, IMessageConsumer messageConsumer, int networkSendThrottleMax = 8, ILogger logger = null)
            : base(serverHook, new GarnetTcpNetworkSender(socket, networkBufferSettings, networkPool, networkSendThrottleMax), socket, networkBufferSettings, networkPool, useTLS, messageConsumer: messageConsumer, logger: logger)
        {
            // GCS caches plaintext pointers between commands. Retain before Start can launch receive cleanup;
            // the mono-threaded GCS owner releases only after its synchronous sender has unwound.
            RetainTransportSendBuffer();
        }

        internal void ReleaseClientSendBuffer()
        {
            if (Interlocked.Exchange(ref clientSendBufferReleased, 1) == 0)
                ReleaseTransportSendBuffer();
        }

        public byte[] RawTransportBuffer => transportReceiveBuffer;
    }
}