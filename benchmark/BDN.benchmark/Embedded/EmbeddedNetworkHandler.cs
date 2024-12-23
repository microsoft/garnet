// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading.Tasks;
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

        public void Send(Request request)
        {
            networkReceiveBuffer = request.buffer;
            unsafe { networkReceiveBufferPtr = request.bufferPtr; }

            OnNetworkReceive(request.buffer.Length);

            Debug.Assert(networkBytesRead == 0);
            Debug.Assert(networkReadHead == 0);
        }
    }
}