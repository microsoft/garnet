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
        public ReplicaReplayTaskGroup replicaReplayTaskGroup;

        /// <summary>
        /// Initialize replica replay group
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="networkSender"></param>
        /// <param name="replicaReplayTaskGroup"></param>
        /// <returns></returns>
        public bool InitializeReplicaReplayTask(int sublogIdx, INetworkSender networkSender, out ReplicaReplayTaskGroup replicaReplayTaskGroup)
        {
            replicaReplayTaskGroup = null;
            if (this.replicaReplayTaskGroup[sublogIdx] != null)
                return false;

            this.replicaReplayTaskGroup.AddReplicaReplayTask(sublogIdx, networkSender);
            replicaReplayTaskGroup = this.replicaReplayTaskGroup;

            return true;
        }
    }
}