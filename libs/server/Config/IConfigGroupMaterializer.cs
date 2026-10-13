// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.client;

namespace Garnet.server
{
    /// <summary>
    /// Materializes a logical configuration group into its physical options struct by reading the group's
    /// individual parameter slots from <see cref="RuntimeServerConfig"/> on demand (logical-to-physical).
    /// <para/>
    /// Implemented as a <see langword="struct"/> so <see cref="RuntimeServerConfig.MaterializeConfigGroup{TGroup, TMaterializer}"/>
    /// is specialized by the JIT over the concrete materializer: the <see cref="Materialize"/> call is
    /// devirtualized and inlinable, and <typeparamref name="TGroup"/> is returned by value — so materializing
    /// a group allocates nothing (no boxing, no delegate, no backing dictionary).
    /// </summary>
    /// <typeparam name="TGroup">Physical options type produced from the group's parameter slots.</typeparam>
    public interface IConfigGroupMaterializer<out TGroup>
    {
        /// <summary>
        /// Reads the group's parameter slots from <paramref name="config"/> and constructs the options struct.
        /// </summary>
        /// <param name="config">Runtime configuration holding the group's parameter slots.</param>
        /// <returns>The materialized options for the group.</returns>
        TGroup Materialize(RuntimeServerConfig config);
    }

    /// <summary>
    /// Materializes the GarnetLightClient (gossip) network-writer options from their runtime slots. The
    /// cross-field constraints (power-of-two sizes, divisibility) are enforced by the
    /// <see cref="LightNetworkWriterOptions"/> constructor at materialization time, not when each slot is set.
    /// </summary>
    public readonly struct GossipClientConfigMaterializer : IConfigGroupMaterializer<LightNetworkWriterOptions>
    {
        /// <inheritdoc/>
        public LightNetworkWriterOptions Materialize(RuntimeServerConfig config) => new(
            networkBufferSizeBytes: config.GetInt(ServerConfigType.GOSSIP_NETWORK_BUFFER_SIZE_BYTES),
            requestPageSizeBytes: config.GetInt(ServerConfigType.GOSSIP_REQUEST_PAGE_SIZE_BYTES),
            requestPageCount: config.GetInt(ServerConfigType.GOSSIP_REQUEST_PAGE_COUNT),
            maxOutstandingRequests: config.GetInt(ServerConfigType.GOSSIP_MAX_OUTSTANDING_REQUESTS),
            maxOutstandingCompletions: config.GetInt(ServerConfigType.GOSSIP_MAX_OUTSTANDING_COMPLETIONS),
            maxConcurrentNetworkSends: config.GetInt(ServerConfigType.GOSSIP_MAX_CONCURRENT_NETWORK_SENDS),
            maxOutOfLineRentedBytes: config.GetLong(ServerConfigType.GOSSIP_MAX_OUT_OF_LINE_RENTED_BYTES),
            flushResultAllocationMode: config.GetEnum<FlushResultAllocationMode>(ServerConfigType.GOSSIP_FLUSH_RESULT_ALLOCATION_MODE));
    }
}