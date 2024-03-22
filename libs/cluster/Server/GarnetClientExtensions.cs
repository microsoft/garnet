// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;

namespace Garnet.cluster
{
    internal static partial class GarnetClientExtensions
    {
        static readonly Memory<byte> GOSSIP = "GOSSIP"u8.ToArray();
        static readonly Memory<byte> WITHMEET = "WITHMEET"u8.ToArray();

        /// <summary>
        /// Send config
        /// </summary>
        /// <param name="client"></param>
        /// <param name="data"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static Task<MemoryResult<byte>> Gossip(this GarnetClient client, Memory<byte> data, CancellationToken cancellationToken = default)
            => client.ExecuteForMemoryResultWithCancellationAsync(GarnetClient.CLUSTER, new Memory<byte>[] { GOSSIP, data }, cancellationToken);

        /// <summary>
        /// Send config
        /// </summary>
        /// <param name="client"></param>
        /// <param name="data"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static Task<MemoryResult<byte>> GossipWithMeet(this GarnetClient client, Memory<byte> data, CancellationToken cancellationToken = default)
            => client.ExecuteForMemoryResultWithCancellationAsync(GarnetClient.CLUSTER, new Memory<byte>[] { GOSSIP, WITHMEET, data }, cancellationToken);

        /// <summary>
        /// Send stop writes to primary
        /// </summary>
        /// <param name="client"></param>
        /// <param name="nodeid"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<long> failstopwrites(this GarnetClient client, Memory<byte> nodeid, CancellationToken cancellationToken = default)
            => await client.ExecuteForLongResultWithCancellationAsync(GarnetClient.CLUSTER, new Memory<byte>[] { CmdStrings.failstopwrites.ToArray(), nodeid }, cancellationToken).ConfigureAwait(false);

        /// <summary>
        /// Send request for failover authorization to all primaries
        /// </summary>
        /// <param name="client"></param>
        /// <param name="nodeid"></param>
        /// <param name="requestedEpoch"></param>
        /// <param name="claimedSlots"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<long> failauthreq(this GarnetClient client, byte[] nodeid, long requestedEpoch, byte[] claimedSlots, CancellationToken cancellationToken = default)
        {
            var args = new Memory<byte>[] {
                CmdStrings.failauthreq.ToArray(),
                nodeid,
                BitConverter.GetBytes(requestedEpoch),
                claimedSlots
            };
            return await client.ExecuteForLongResultWithCancellationAsync(GarnetClient.CLUSTER, args, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Send request to await for replication offset sync with replica
        /// </summary>
        /// <param name="client"></param>
        /// <param name="primaryReplicationOffset"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<long> failreplicationoffset(this GarnetClient client, long primaryReplicationOffset, CancellationToken cancellationToken = default)
        {
            var args = new Memory<byte>[] {
                CmdStrings.failreplicationoffset.ToArray(),
                Encoding.ASCII.GetBytes(primaryReplicationOffset.ToString())
            };
            return await client.ExecuteForLongResultWithCancellationAsync(GarnetClient.CLUSTER, args, cancellationToken).ConfigureAwait(false);
        }
    }
}