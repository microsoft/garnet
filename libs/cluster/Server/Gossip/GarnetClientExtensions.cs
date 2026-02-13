// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;

namespace Garnet.cluster
{
    internal static partial class GarnetClientExtensions
    {
        static readonly Memory<byte> GOSSIP = "GOSSIP"u8.ToArray();
        static readonly Memory<byte> WITHMEET = "WITHMEET"u8.ToArray();

        static Memory<byte> PUBLISH => "PUBLISH"u8.ToArray();
        static Memory<byte> SPUBLISH => "SPUBLISH"u8.ToArray();

        /// <summary>
        /// Send config
        /// </summary>
        /// <param name="client"></param>
        /// <param name="data"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static Task<MemoryResult<byte>> Gossip(this GarnetClient client, Memory<byte> data, CancellationToken cancellationToken = default)
            => client.ExecuteForMemoryResultWithCancellationAsync(GarnetClient.CLUSTER, [GOSSIP, data], cancellationToken);

        /// <summary>
        /// Send config
        /// </summary>
        /// <param name="client"></param>
        /// <param name="data"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static Task<MemoryResult<byte>> GossipWithMeet(this GarnetClient client, Memory<byte> data, CancellationToken cancellationToken = default)
            => client.ExecuteForMemoryResultWithCancellationAsync(GarnetClient.CLUSTER, [GOSSIP, WITHMEET, data], cancellationToken);

        /// <summary>
        /// Issue stop writes to primary node
        /// </summary>
        /// <param name="client"></param>
        /// <param name="nodeid"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <seealso cref="T:Garnet.client.GarnetClientSession.NetworkClusterFailStopWrites"/>
        public static async Task<string> ExecuteClusterFailStopWrites(this GarnetClient client, Memory<byte> nodeid, CancellationToken cancellationToken = default)
            => await client.ExecuteForStringResultWithCancellationAsync(GarnetClient.CLUSTER, [CmdStrings.failstopwrites.ToArray(), nodeid], cancellationToken).ConfigureAwait(false);

        /// <summary>
        /// Acquire replication offset of primary. Used to delay failover until the calling replica catches up.
        /// </summary>
        /// <param name="client"></param>
        /// <param name="primaryReplicationOffset"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <seealso cref="T:Garnet.client.GarnetClientSession.NetworkClusterFailReplicationOffset"/>
        public static async Task<string> ExecuteClusterFailReplicationOffset(this GarnetClient client, AofAddress primaryReplicationOffset, CancellationToken cancellationToken = default)
        {
            var args = new Memory<byte>[] {
                CmdStrings.failreplicationoffset.ToArray(),
                Encoding.ASCII.GetBytes(primaryReplicationOffset.ToString())
            };
            return await client.ExecuteForStringResultWithCancellationAsync(GarnetClient.CLUSTER, args, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Issue CLUSTER PUBLISH
        /// </summary>
        /// <param name="client"></param>
        /// <param name="cmd"></param>
        /// <param name="channel"></param>
        /// <param name="message"></param>
        /// <param name="cancellationToken"></param>
        /// <seealso cref="T:Garnet.client.GarnetClientSession.NetworkClusterPublish"/>
        public static void ExecuteClusterPublishNoResponse(this GarnetClient client, RespCommand cmd, ref Span<byte> channel, ref Span<byte> message, CancellationToken cancellationToken = default)
            => client.ExecuteNoResponse(GarnetClient.CLUSTER, RespCommand.PUBLISH == cmd ? GarnetClient.PUBLISH : GarnetClient.SPUBLISH, ref channel, ref message, cancellationToken);
    }
}