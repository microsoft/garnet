// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.networking;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IDisposable
    {
        /// <summary>
        /// Replica replay task group
        /// </summary>
        ReplicaReplayTaskGroup replicaReplayTaskGroup = null;

        bool IsReplayTaskGroupInitialized => replicaReplayTaskGroup != null && replicaReplayTaskGroup.IsInitialized;

        /// <summary>
        /// Initialize and return replica replay task
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="networkSender"></param>
        /// <param name="_replicaReplayTaskGroup"></param>
        /// <returns></returns>
        public bool InitializeReplayTaskGroup(int sublogIdx, INetworkSender networkSender, out ReplicaReplayTaskGroup _replicaReplayTaskGroup)
        {
            _replicaReplayTaskGroup = null;
            if (IsReplayTaskGroupInitialized)
                return false;

            _ = Interlocked.CompareExchange(ref replicaReplayTaskGroup, new ReplicaReplayTaskGroup(clusterProvider, logger), null);
            replicaReplayTaskGroup.CreateReplicaReplayTask(sublogIdx, networkSender);
            _replicaReplayTaskGroup = this.replicaReplayTaskGroup;
            return true;
        }

        /// <summary>
        /// Dispose replica replay tasks
        /// </summary>
        public void DisposeReplayTaskGroup()
        {
            var currentReplicaReplayTaskGroup = this.replicaReplayTaskGroup;
            Interlocked.CompareExchange(ref replicaReplayTaskGroup, null, currentReplicaReplayTaskGroup)?.Dispose();
        }
    }
}