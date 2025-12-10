// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.networking;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IDisposable
    {
        /// <summary>
        /// Replica replay task group
        /// </summary>
        public ReplicaReplayDriverGroup replicaReplayTaskGroup;

        /// <summary>
        /// Initialize replica replay group
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="networkSender"></param>
        /// <param name="replicaReplayTaskGroup"></param>
        /// <returns></returns>
        public bool InitializeReplicaReplayGroup(int sublogIdx, INetworkSender networkSender, out ReplicaReplayDriverGroup replicaReplayTaskGroup)
        {
            replicaReplayTaskGroup = null;
            if (this.replicaReplayTaskGroup.GetReplayDriver(sublogIdx) != null)
                return false;

            this.replicaReplayTaskGroup.AddReplicaReplayDriver(sublogIdx, networkSender);
            replicaReplayTaskGroup = this.replicaReplayTaskGroup;

            return true;
        }

        /// <summary>
        /// Resets the state of the replica replay group
        /// </summary>
        public void ResetReplicaReplayGroup()
        {
            replicaReplayTaskGroup?.Dispose();
            replicaReplayTaskGroup = new ReplicaReplayDriverGroup(clusterProvider, logger);
        }
    }
}