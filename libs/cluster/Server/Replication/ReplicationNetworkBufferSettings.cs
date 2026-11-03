// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IDisposable
    {
        /// <summary>
        /// NetworkBufferSettings for the buffer pool maintained by the ReplicationManager
        /// </summary>
        // Reserve size classes, not buffers, for the entire runtime-adjustable chunk range.
        internal NetworkBufferSettings networkBufferSettings => NetworkBufferSettings.GetInclusive([
            CreateRSSNetworkBufferSettings(GarnetServerOptions.ReplicaDiskBasedSyncChunkSizeBytesMax),
            GetIRSNetworkBufferSettings, GetAofSyncNetworkBufferSettings]);

        /// <summary>
        /// Network pool maintained by the ReplicationManager
        /// </summary>
        readonly LimitedFixedBufferPool networkPool;
        public LimitedFixedBufferPool GetNetworkPool => networkPool;

        /// <summary>
        /// NetworkBufferSettings for the replica sync session clients
        /// </summary>
        const int rssInitialReceiveBufferSize = 1 << 12;
        const int rssMinimumSendBufferSize = 1 << 20;
        public NetworkBufferSettings GetRSSNetworkBufferSettings => CreateRSSNetworkBufferSettings(
            clusterProvider.storeWrapper.runtimeConfig.GetReplicaDiskBasedSyncSettings().ChunkSize);

        internal static NetworkBufferSettings CreateRSSNetworkBufferSettings(int chunkSize) => new(
            Math.Max(rssMinimumSendBufferSize, 2 * chunkSize),
            rssInitialReceiveBufferSize);

        /// <summary>
        /// NetworkBufferSettings for the client used for InitiateReplicaSync (ReplicateReceiveCheckpoint.cs)
        /// </summary>
        const int irsSendBufferSize = 1 << 17;
        const int irsInitialReceiveBufferSize = 1 << 17;
        public NetworkBufferSettings GetIRSNetworkBufferSettings { get; } = new(irsSendBufferSize, irsInitialReceiveBufferSize);

        /// <summary>
        /// NetworkBufferSettings for the AOF sync task clients
        /// NOTE: double buffer size for send page to ensure payload (command header + page size) always fits into client buffer.
        /// </summary>
        int aofSyncSendBufferSize => 2 << clusterProvider.storeWrapper.serverOptions.AofPageSizeBits();
        const int aofSyncInitialReceiveBufferSize = 1 << 17;
        public NetworkBufferSettings GetAofSyncNetworkBufferSettings => new(aofSyncSendBufferSize, aofSyncInitialReceiveBufferSize);

        void ValidateNetworkBufferSettings()
        {
            if (!networkPool.Validate(GetRSSNetworkBufferSettings))
                logger?.LogWarning("NetworkBufferSettings for ReplicaSyncSession do not allow for buffer re-use with configured NetworkPool");

            if (!networkPool.Validate(GetIRSNetworkBufferSettings))
                logger?.LogWarning("NetworkBufferSettings for InitiateReplicaSync do not allow for buffer re-use with configured NetworkPool");

            if (!networkPool.Validate(GetAofSyncNetworkBufferSettings))
                logger?.LogWarning("NetworkBufferSettings for AofSyncTask do not allow for buffer re-use with configured NetworkPool");
        }
    }
}