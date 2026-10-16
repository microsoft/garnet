// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;
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
        /// Rendezvous of the sublog connections, so that the primary starts streaming only once every sublog has
        /// connected. Null when a single sublog is used.
        /// </summary>
        readonly AsyncRendezvous rendezvous = clusterProvider.serverOptions.AofPhysicalSublogCount > 1 ? new(clusterProvider.serverOptions.AofPhysicalSublogCount) : null;

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
        /// <returns>
        /// Completes with true once every sublog has been added and with false when the replica sync timeout elapsed
        /// first, or this store was already disposed. Faults when the store is disposed while it is pending. The wait
        /// is registered under the lock but must be awaited outside it, so that <see cref="Dispose"/> is never held
        /// up by a sublog that has yet to connect.
        /// </returns>
        public ValueTask<bool> AddReplicaReplayDriverAsync(int physicalSublogIdx, INetworkSender networkSender)
        {
            try
            {
                _lock.ReadLock();
                if (disposed)
                    return new ValueTask<bool>(false);
                replicaReplayDrivers[physicalSublogIdx] = new ReplicaReplayDriver(physicalSublogIdx, clusterProvider, networkSender, cts, logger);
                if (rendezvous == null)
                    return new ValueTask<bool>(true);

                // The token is read under the lock because Dispose disposes the source once it has taken the write lock
                return rendezvous.SignalAndWaitAsync(clusterProvider.serverOptions.ReplicaSyncTimeout, cts.Token);
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