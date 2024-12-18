// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Embedded.perftest;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;

namespace BDN.benchmark.Network
{
    class BdnNetworkHandler : NetworkHandler<GarnetServerEmbedded, DummyNetworkSender>
    {
        public BdnNetworkHandler(GarnetServerEmbedded serverHook, DummyNetworkSender networkSender, NetworkBufferSettings networkBufferSettings, LimitedFixedBufferPool networkPool, bool useTLS, IMessageConsumer messageConsumer = null, ILogger logger = null) : base(serverHook, networkSender, networkBufferSettings, networkPool, useTLS, messageConsumer, logger)
        {
        }

        public override string RemoteEndpointName => throw new NotImplementedException();
        public override string LocalEndpointName => throw new NotImplementedException();
        public override void Dispose()
        {
        }

        public override bool TryClose() => throw new NotImplementedException();

        public unsafe void Send(byte[] buffer, byte* bufferPtr, int length)
        {
            networkReceiveBuffer = buffer;
            networkReceiveBufferPtr = bufferPtr;
            OnNetworkReceive(length);
        }
    }
}