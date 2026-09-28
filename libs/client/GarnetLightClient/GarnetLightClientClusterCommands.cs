// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;

namespace Garnet.client
{
    public sealed partial class GarnetLightClient
    {
        /// <summary>
        /// CLUSTER command, resp formatted.
        /// </summary>
        public static readonly Memory<byte> CLUSTER = "$7\r\nCLUSTER\r\n"u8.ToArray();

        /// <summary>
        /// PING command, resp formatted.
        /// </summary>
        public static readonly Memory<byte> PING = "$4\r\nPING\r\n"u8.ToArray();

        /// <summary>
        /// PUBLISH command, resp formatted.
        /// </summary>
        public static readonly Memory<byte> PUBLISH = "$7\r\nPUBLISH\r\n"u8.ToArray();

        static readonly Memory<byte> GOSSIP = "GOSSIP"u8.ToArray();
        static readonly Memory<byte> WITHMEET = "WITHMEET"u8.ToArray();

        /// <summary>
        /// PUBLISH sub-token used when forwarding messages inside a cluster.
        /// </summary>
        static ReadOnlySpan<byte> PUBLISH_SUB => "PUBLISH"u8;

        /// <summary>
        /// SPUBLISH sub-token used when forwarding sharded messages inside a cluster.
        /// </summary>
        static ReadOnlySpan<byte> SPUBLISH_SUB => "SPUBLISH"u8;

        /// <summary>
        /// Issue a <c>PING</c> and return the server reply (typically <c>PONG</c>).
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task that completes with the ping reply</returns>
        public Task<string> PingAsync(CancellationToken token = default)
            => ExecuteForStringResultWithCancellationAsync(PING, (System.Collections.Generic.ICollection<Memory<byte>>)null, token);

        /// <summary>
        /// Send a serialized cluster configuration via <c>CLUSTER GOSSIP</c> and return the peer's response
        /// (its serialized configuration, or an empty payload when nothing changed).
        /// </summary>
        /// <param name="data">Serialized cluster configuration; may be empty to send a gossip ping.</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task that completes with the peer's binary response</returns>
        public Task<MemoryResult<byte>> GossipAsync(Memory<byte> data, CancellationToken token = default)
            => ExecuteForMemoryResultWithCancellationAsync(CLUSTER, [GOSSIP, data], token);

        /// <summary>
        /// Send a serialized cluster configuration via <c>CLUSTER GOSSIP WITHMEET</c>, forcing the peer to
        /// trust and merge the configuration, and return its binary response.
        /// </summary>
        /// <param name="data">Serialized cluster configuration; may be empty to send a gossip ping.</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task that completes with the peer's binary response</returns>
        public Task<MemoryResult<byte>> GossipWithMeetAsync(Memory<byte> data, CancellationToken token = default)
            => ExecuteForMemoryResultWithCancellationAsync(CLUSTER, [GOSSIP, WITHMEET, data], token);

        /// <summary>
        /// Publish <paramref name="message"/> to <paramref name="channel"/> and return the number of clients
        /// that received it.
        /// </summary>
        /// <param name="channel">Channel to publish to</param>
        /// <param name="message">Message payload</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task that completes with the receiver count</returns>
        public Task<long> PublishAsync(Memory<byte> channel, Memory<byte> message, CancellationToken token = default)
            => ExecuteForLongResultWithCancellationAsync(PUBLISH, [channel, message], token);

        /// <summary>
        /// Forward a published message to a peer node without expecting a response (fire-and-forget). Mirrors
        /// the cluster's internal <c>CLUSTER PUBLISH</c> forwarding path; the peer must not reply.
        /// </summary>
        /// <param name="channel">Channel the message was published to</param>
        /// <param name="message">Message payload</param>
        /// <param name="token">Cancellation token</param>
        public void ClusterPublishNoResponse(Span<byte> channel, Span<byte> message, CancellationToken token = default)
            => ExecuteNoResponse(CLUSTER, PUBLISH_SUB, channel, message, token);

        /// <summary>
        /// Forward a sharded published message to a peer node without expecting a response (fire-and-forget).
        /// Mirrors the cluster's internal <c>CLUSTER SPUBLISH</c> forwarding path; the peer must not reply.
        /// </summary>
        /// <param name="channel">Channel the message was published to</param>
        /// <param name="message">Message payload</param>
        /// <param name="token">Cancellation token</param>
        public void ClusterSPublishNoResponse(Span<byte> channel, Span<byte> message, CancellationToken token = default)
            => ExecuteNoResponse(CLUSTER, SPUBLISH_SUB, channel, message, token);
    }
}