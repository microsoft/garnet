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
    internal static class GarnetClientExtensions
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
        /// Send stop writes to primary
        /// </summary>
        /// <param name="client"></param>
        /// <param name="nodeid"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<long> failstopwrites(this GarnetClient client, Memory<byte> nodeid, CancellationToken cancellationToken = default)
            => await client.ExecuteForLongResultWithCancellationAsync(GarnetClient.CLUSTER, [CmdStrings.failstopwrites.ToArray(), nodeid], cancellationToken).ConfigureAwait(false);

        /// <summary>
        /// Send request to await for replication offset sync with replica
        /// </summary>
        /// <param name="client"></param>
        /// <param name="primaryReplicationOffset"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<long> ExecuteClusterFailReplicationOffset(this GarnetClient client, long primaryReplicationOffset, CancellationToken cancellationToken = default)
        {
            var args = new Memory<byte>[] {
                CmdStrings.failreplicationoffset.ToArray(),
                Encoding.ASCII.GetBytes(primaryReplicationOffset.ToString())
            };
            return await client.ExecuteForLongResultWithCancellationAsync(GarnetClient.CLUSTER, args, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Publishes a message to a specified channel in a clustered Garnet environment without waiting for a server
        /// response.
        /// </summary>
        /// <param name="client">The Garnet client instance used to send the publish command.</param>
        /// <param name="cmd">The RESP command to execute. Must be either PUBLISH or SPUBLISH.</param>
        /// <param name="channel">A span containing the channel name to which the message will be published.</param>
        /// <param name="message">A span containing the message to publish to the channel.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the operation.</param>
        public static void ExecuteClusterPublishNoResponse(this GarnetClient client, RespCommand cmd, ref Span<byte> channel, ref Span<byte> message, CancellationToken cancellationToken = default)
            => client.ExecuteNoResponse(GarnetClient.CLUSTER, RespCommand.PUBLISH == cmd ? GarnetClient.PUBLISH : GarnetClient.SPUBLISH, ref channel, ref message, cancellationToken);
    }
}