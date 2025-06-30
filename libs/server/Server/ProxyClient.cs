// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Threading;
using Garnet.networking;

namespace Garnet.server
{
    internal class ProxyClient
    {
        readonly ShardedSessionProxy proxy;
        readonly INetworkSender source;
        readonly List<SessionPacket> ongoingPackets;

        public ProxyClient(ShardedSessionProxy proxy, INetworkSender source)
        {
            this.proxy = proxy;
            this.source = source;
            ongoingPackets = [];
        }

        public void Send(int destination, byte[] request)
        {
            var packet = new SessionPacket
            {
                request = request,
                completed = new SemaphoreSlim(0)
            };
            ongoingPackets.Add(packet);
            proxy.Forward(destination, packet);
        }

        public void Complete()
        {
            foreach (var packet in ongoingPackets)
            {
                packet.completed.Wait();
                source.SendResponse(packet.response, 0, packet.response.Length, null);
                packet.completed.Dispose();
            }
            ongoingPackets.Clear();
        }
    }
}