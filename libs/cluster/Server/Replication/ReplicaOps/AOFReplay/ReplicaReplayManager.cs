// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
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
        /// <param name="sublogsInitialized">Completes once every sublog's driver has been initialized; see <see cref="ReplicaReplayDriverStore.AddReplicaReplayDriverAsync"/>. Default when this returns false.</param>
        /// <returns>True if the driver was initialized by this call, false if it was already initialized</returns>
        public bool InitializeReplicaReplayDriver(int physicalSublogIdx, INetworkSender networkSender, out ValueTask<bool> sublogsInitialized)
        {
            if (ReplicaReplayDriverStore.GetReplayDriver(physicalSublogIdx) != null)
            {
                sublogsInitialized = default;
                return false;
            }

            sublogsInitialized = ReplicaReplayDriverStore.AddReplicaReplayDriverAsync(physicalSublogIdx, networkSender);
            return true;
        }

        /// <summary>
        /// Resets the state of the replica replay driver store. Completes once the replica-only background tasks have
        /// stopped, which takes as long as the slowest of them takes to observe its cancellation.
        /// </summary>
        public async ValueTask ResetReplicaReplayDriverStoreAsync()
        {
            ReplicaReplayDriverStore?.Dispose();
            await clusterProvider.storeWrapper.SuspendReplicaOnlyTasksAsync().ConfigureAwait(false);
            if (clusterProvider.IsReplica())
                clusterProvider.storeWrapper.StartReplicaTasks();
            ReplicaReplayDriverStore = new ReplicaReplayDriverStore(clusterProvider, logger);
        }
    }
}