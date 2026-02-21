// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal class ReplicaReplayDriverStore(ClusterProvider clusterProvider, ILogger logger)
    {
        readonly ClusterProvider clusterProvider = clusterProvider;
        readonly ILogger logger = logger;

        /// <summary>
        /// Get replay driver for given sublogIdx
        /// </summary>
        /// <param name="physicalSublogIdx"></param>
        /// <returns></returns>
        public ReplicaReplayDriver GetReplayDriver(int physicalSublogIdx)
            => replicaReplayDrivers[physicalSublogIdx];

        /// <summary>
        /// Replay task instances per sublog (used with ShardedLog)
        /// </summary>
        readonly ReplicaReplayDriver[] replicaReplayDrivers = new ReplicaReplayDriver[clusterProvider.serverOptions.AofPhysicalSublogCount];

        /// <summary>
        /// Replay barrier used to coordinate connection of replay tasks
        /// </summary>
        readonly Barrier barrier = new(clusterProvider.serverOptions.AofPhysicalSublogCount);

        /// <summary>
        /// Disposed lock
        /// </summary>
        private SingleWriterMultiReaderLock _lock = new();

        /// <summary>
        /// Disposed flag
        /// </summary>
        private bool disposed = false;

        /// <summary>
        /// Cancellation token source for replay task group
        /// </summary>
        readonly CancellationTokenSource cts = new();

        /// <summary>
        /// Add replica replay driver to this store
        /// </summary>
        /// <param name="physicalSublogIdx"></param>
        /// <param name="networkSender"></param>
        public void AddReplicaReplayDriver(int physicalSublogIdx, INetworkSender networkSender)
        {
            try
            {
                _lock.ReadLock();
                if (disposed)
                    return;
                replicaReplayDrivers[physicalSublogIdx] = new ReplicaReplayDriver(physicalSublogIdx, clusterProvider, networkSender, cts, logger);
                _ = barrier.SignalAndWait(clusterProvider.serverOptions.ReplicaSyncTimeout, cts.Token);
            }
            finally
            {
                _lock.ReadUnlock();
            }
        }

        /// <summary>
        /// Dispose replica replay task group
        /// </summary>
        public void Dispose()
        {
            try
            {
                _lock.WriteLock();
                if (disposed)
                    return;
                disposed = true;
            }
            finally
            {
                _lock.WriteUnlock();
            }

            cts.Cancel();
            var replicaReplayTasks = replicaReplayDrivers;
            if (replicaReplayTasks != null)
            {
                for (var i = 0; i < replicaReplayTasks.Length; i++)
                    replicaReplayTasks[i]?.Dispose();
            }
            cts.Dispose();
        }
    }
}