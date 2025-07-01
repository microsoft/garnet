// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Sharded RESP server session host
    /// </summary>
    internal sealed class ShardedSessionProxy
    {
        readonly EmbeddedRespServer[] servers;
        readonly AsyncQueue<SessionPacket>[] shardInputs;

        public int NumShards => servers.Length;

        public ShardedSessionProxy(GarnetServerOptions opts)
        {
            int numShards = opts.IntraNodeShards;
            servers = new EmbeddedRespServer[numShards];
            shardInputs = new AsyncQueue<SessionPacket>[numShards];
            opts.IntraNodeShards = 0;
            for (var i = 0; i < numShards; i++)
            {
                var _i = i;
                shardInputs[i] = new AsyncQueue<SessionPacket>();
                servers[i] = new EmbeddedRespServer(opts, null, new GarnetServerEmbedded());
                _ = Task.Run(async () => await ShardRunner(_i, CancellationToken.None));
            }
            opts.IntraNodeShards = numShards;
        }

        async Task ShardRunner(int shardId, CancellationToken token = default)
        {
            var server = servers[shardId];
            var networkHandler = server.GetNetworkHandler();
            var session = server.GetRespSession(out var embeddedNetworkSender);

            var inputQueue = shardInputs[shardId];

            while (true)
            {
                if (!inputQueue.TryDequeue(out var nextInput))
                {
                    try
                    {
                        nextInput = await inputQueue.DequeueAsync(token);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }

                unsafe
                {
                    // Process the request
                    nextInput.readHead = session.TryConsumeMessages(nextInput.request.ptr, nextInput.request.length);
                    nextInput.response = embeddedNetworkSender.GetResponse().ToArray();
                    nextInput.completed.Release();
                }
            }
        }

        public void Forward(int destination, SessionPacket packet)
        {
            shardInputs[destination].Enqueue(packet);
        }
    }
}