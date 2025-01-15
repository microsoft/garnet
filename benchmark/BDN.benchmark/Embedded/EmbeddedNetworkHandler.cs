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
        readonly bool useTLS;

        public EmbeddedNetworkHandler(GarnetServerEmbedded serverHook, EmbeddedNetworkSender networkSender, NetworkBufferSettings networkBufferSettings, LimitedFixedBufferPool networkPool, bool useTLS, IMessageConsumer messageConsumer = null, ILogger logger = null) : base(serverHook, networkSender, networkBufferSettings, networkPool, useTLS, messageConsumer, logger)
        {
            this.useTLS = useTLS;
        }

        public override string RemoteEndpointName => throw new NotImplementedException();
        public override string LocalEndpointName => throw new NotImplementedException();
        public override void Dispose()
        {
            DisposeImpl();
        }

        public override bool TryClose() => throw new NotImplementedException();

        public async ValueTask Send(Request request)
        {
            networkReceiveBuffer = request.buffer;
            unsafe { networkReceiveBufferPtr = request.bufferPtr; }

            if (useTLS)
                await OnNetworkReceiveWithTLSAsync(request.buffer.Length);
            else
                OnNetworkReceiveWithoutTLS(request.buffer.Length);

            Debug.Assert(networkBytesRead == 0);
            Debug.Assert(networkReadHead == 0);
        }
    }
}