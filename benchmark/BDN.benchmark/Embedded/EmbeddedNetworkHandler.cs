// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;

namespace Embedded.server
{
    internal class EmbeddedNetworkHandler : NetworkHandler<GarnetServerEmbedded, EmbeddedNetworkSender>
    {
        public EmbeddedNetworkHandler(GarnetServerEmbedded serverHook, EmbeddedNetworkSender networkSender, NetworkBufferSettings networkBufferSettings, LimitedFixedBufferPool networkPool, bool useTLS, IMessageConsumer messageConsumer = null, ILogger logger = null) : base(serverHook, networkSender, networkBufferSettings, networkPool, useTLS, messageConsumer, logger)
        {
        }

        public override string RemoteEndpointName => throw new NotImplementedException();
        public override string LocalEndpointName => throw new NotImplementedException();
        public override void Dispose()
        {
            DisposeImpl();
        }

        public override bool TryClose() => throw new NotImplementedException();

        public unsafe void Send(byte[] buffer, byte* bufferPtr, int length)
        {
            networkReceiveBuffer = buffer;
            networkReceiveBufferPtr = bufferPtr;
            OnNetworkReceive(length);

            // We should have consumed the entire buffer
            Debug.Assert(networkBytesRead == 0);
            Debug.Assert(networkReadHead == 0);
        }
    }
}