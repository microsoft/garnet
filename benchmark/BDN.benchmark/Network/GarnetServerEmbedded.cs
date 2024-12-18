// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Embedded.perftest;
using Garnet.networking;
using Garnet.server;

namespace BDN.benchmark.Network
{
    class GarnetServerEmbedded : GarnetServerBase, IServerHook
    {
        readonly EmbeddedRespServer server;

        public GarnetServerEmbedded(EmbeddedRespServer server) : base("0.0.0.0", 0, 1 << 10)
        {
            this.server = server;
        }

        public void DisposeMessageConsumer(INetworkHandler session)
        {
            session.Dispose();
        }

        public override void Start()
        {
        }

        public bool TryCreateMessageConsumer(Span<byte> bytesReceived, INetworkSender networkSender, out IMessageConsumer session)
        {
            session = server.GetRespSession(networkSender);
            return true;
        }
    }
}