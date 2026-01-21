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
        /// <param name="sublogIdx"></param>
        /// <returns></returns>
        public ReplicaReplayDriver GetReplayDriver(int sublogIdx)
            => replicaReplayDrivers[sublogIdx];

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
        public SingleWriterMultiReaderLock _lock = new();

        /// <summary>
        /// Disposed flag
        /// </summary>
        public bool _disposed = false;

        /// <summary>
        /// Cancellation token source for replay task group
        /// </summary>
        readonly CancellationTokenSource cts = new();

        /// <summary>
        /// Add replica replay task to this group
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="networkSender"></param>
        public void AddReplicaReplayDriver(int sublogIdx, INetworkSender networkSender)
        {
            replicaReplayDrivers[sublogIdx] = new ReplicaReplayDriver(sublogIdx, clusterProvider, networkSender, cts, logger);
            _ = barrier.SignalAndWait(clusterProvider.serverOptions.ReplicaSyncTimeout, cts.Token);
        }

        /// <summary>
        /// Dispose replica replay task group
        /// </summary>
        public void Dispose()
        {
            try
            {
                _lock.WriteLock();
                if (_disposed) return;
                _disposed = true;
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