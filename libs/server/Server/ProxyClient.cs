// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using Garnet.networking;

namespace Garnet.server
{
    internal unsafe class ProxyClient
    {
        readonly ShardedSessionProxy proxy;
        readonly INetworkSender source;
        readonly List<SessionPacket> ongoingPackets;
        readonly Random random;
        public ProxyClient(ShardedSessionProxy proxy, INetworkSender source)
        {
            this.proxy = proxy;
            this.source = source;
            ongoingPackets = [];
            random = new Random();
        }

        public void Send(int destination, ArgSlice request)
        {
            var packet = new SessionPacket
            {
                request = request,
                completed = new SemaphoreSlim(0)
            };
            ongoingPackets.Add(packet);
            if (destination == -1)
            {
                // Randomly select a shard if no specific destination is provided
                destination = random.Next(proxy.NumShards);
            }
            proxy.Forward(destination, packet);
        }

        public int Complete(ref byte* dcurr, ref byte* dend)
        {
            int readHead = 0;
            foreach (var packet in ongoingPackets)
            {
                packet.completed.Wait();
                WriteDirectLarge(new ReadOnlySpan<byte>(packet.response), ref dcurr, ref dend);
                readHead += packet.readHead;
                packet.completed.Dispose();
            }
            ongoingPackets.Clear();
            return readHead;
        }

        private void WriteDirectLarge(ReadOnlySpan<byte> src, ref byte* dcurr, ref byte* dend)
        {
            // Repeat while we have bytes left to write
            while (src.Length > 0)
            {
                // Compute space left on output buffer
                int destSpace = (int)(dend - dcurr);

                // Fast path if there is enough space
                if (src.Length <= destSpace)
                {
                    src.CopyTo(new Span<byte>(dcurr, src.Length));
                    dcurr += src.Length;
                    break;
                }

                // Adjust number of bytes to copy, to space left on output buffer, then copy
                src.Slice(0, destSpace).CopyTo(new Span<byte>(dcurr, destSpace));
                dcurr += destSpace;
                src = src.Slice(destSpace);

                // Send and reset output buffer
                Send(source.GetResponseObjectHead(), dcurr);
                source.GetResponseObject();
                dcurr = source.GetResponseObjectHead();
                dend = source.GetResponseObjectTail();
            }
        }

        private void Send(byte* d, byte* dcurr)
        {
            if ((int)(dcurr - d) > 0)
            {
                int sendBytes = (int)(dcurr - d);
                source.SendResponse((int)(d - source.GetResponseObjectHead()), sendBytes);
            }
        }
    }
}