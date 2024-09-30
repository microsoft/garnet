// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IDisposable
    {
        /// <summary>
        /// NetworkBufferSettings for the buffer pool maintained by the ReplicationManager
        /// </summary>
        const int defaultSendBufferSize = 1 << 22;
        const int defaultInitialReceiveBufferSize = 1 << 12;
        readonly NetworkBufferSettings networkBufferSettings = new(defaultSendBufferSize, defaultInitialReceiveBufferSize);

        /// <summary>
        /// Network pool maintained by the ReplicationManager
        /// </summary>
        readonly LimitedFixedBufferPool networkPool;
        public LimitedFixedBufferPool GetNetworkPool => networkPool;

        /// <summary>
        /// NetworkBufferSettings for the replica sync session clients
        /// </summary>
        const int rssSendBufferSize = 1 << 20;
        const int rssInitialReceiveBufferSize = 1 << 12;
        public NetworkBufferSettings GetRSSNetworkBufferSettings { get; } = new(rssSendBufferSize, rssInitialReceiveBufferSize);

        /// <summary>
        /// NetworkBufferSettings for the client used for InitiateReplicaSync (ReplicateReceiveCheckpoint.cs)
        /// </summary>
        const int irsSendBufferSize = 1 << 17;
        const int irsInitialReceiveBufferSize = 1 << 17;
        public NetworkBufferSettings GetIRSNetworkBufferSettings { get; } = new(irsSendBufferSize, irsInitialReceiveBufferSize);

        /// <summary>
        /// NetworkBufferSettings for the AOF sync task clients
        /// </summary>
        const int aofSyncSendBufferSize = 1 << 22;
        const int aofSyncInitialReceiveBufferSize = 1 << 17;
        public NetworkBufferSettings GetAofSyncNetworkBufferSettings { get; } = new(aofSyncSendBufferSize, aofSyncInitialReceiveBufferSize);

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