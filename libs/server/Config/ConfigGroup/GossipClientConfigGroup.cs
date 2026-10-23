// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.client;

namespace Garnet.server
{
    /// <summary>
    /// The GOSSIP_CLIENT configuration group: materializes the gossip <see cref="GarnetLightClient"/>
    /// network-writer options from their runtime slots. Cross-field constraints (power-of-two sizes,
    /// divisibility) are validated by the <see cref="LightNetworkWriterOptions"/> constructor at
    /// materialization time, not when each slot is individually set.
    /// </summary>
    internal sealed class GossipClientConfigGroup : ConfigGroupBase<LightNetworkWriterOptions>
    {
        /// <inheritdoc/>
        public override ServerConfigGroupType GetGroupType() => ServerConfigGroupType.GOSSIP_CLIENT;

        /// <inheritdoc/>
        protected override LightNetworkWriterOptions Materialize(RuntimeServerConfig config) => new(
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