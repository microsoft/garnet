// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.networking;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IDisposable
    {
        /// <summary>
        /// Replica replay driver store instance
        /// </summary>
        public ReplicaReplayDriverStore ReplicaReplayDriverStore;

        /// <summary>
        /// Initialize replica replay driver
        /// </summary>
        /// <param name="physicalSublogIdx"></param>
        /// <param name="networkSender"></param>
        /// <returns>True if re</returns>
        public bool InitializeReplicaReplayDriver(int physicalSublogIdx, INetworkSender networkSender)
        {
            if (ReplicaReplayDriverStore.GetReplayDriver(physicalSublogIdx) != null)
                return false;

            ReplicaReplayDriverStore.AddReplicaReplayDriver(physicalSublogIdx, networkSender);
            return true;
        }

        /// <summary>
        /// Resets the state of the replica replay driver store
        /// </summary>
        public void ResetReplicaReplayDriverStore()
        {
            ReplicaReplayDriverStore?.Dispose();
            ReplicaReplayDriverStore = new ReplicaReplayDriverStore(clusterProvider, logger);
        }
    }
}