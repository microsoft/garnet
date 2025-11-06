// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal class ReplicaReplayTaskGroup(ClusterProvider clusterProvider, ILogger logger)
    {
        readonly ClusterProvider clusterProvider = clusterProvider;
        readonly ILogger logger = logger;

        /// <summary>
        /// Indexer
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public ReplicaReplayTask this[int i]
        {
            get
            {
                return replicaReplayTasks[i];
            }
        }

        /// <summary>
        /// Replay task instances per sublog (used with ShardedLog)
        /// </summary>
        readonly ReplicaReplayTask[] replicaReplayTasks = new ReplicaReplayTask[clusterProvider.serverOptions.AofSublogCount];

        /// <summary>
        /// Replay barrier used to coordinate connection of replay tasks
        /// </summary>
        readonly Barrier barrier = new Barrier(clusterProvider.serverOptions.AofSublogCount);

        /// <summary>
        /// Disposed lock
        /// </summary>
        SingleWriterMultiReaderLock _disposed = new();

        /// <summary>
        /// Cancellation token source for replay task group
        /// </summary>
        readonly CancellationTokenSource cts = new();

        /// <summary>
        /// Initialized flag
        /// </summary>
        public bool IsInitialized { get; private set; } = false;

        /// <summary>
        /// Add replica replay task to this group
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="networkSender"></param>
        public void AddReplicaReplayTask(int sublogIdx, INetworkSender networkSender)
        {
            replicaReplayTasks[sublogIdx] = new ReplicaReplayTask(sublogIdx, clusterProvider, networkSender, cts, logger);
            _ = barrier.SignalAndWait(clusterProvider.serverOptions.ReplicaSyncTimeout, cts.Token);
            if (!_disposed.TryReadLock())
                throw new GarnetException("Failed to add replica replay task");
        }

        /// <summary>
        /// Trigger cancel of replay tasks.
        /// </summary>
        /// <param name="isReplicating">Indicates if call is from replicating task</param>
        public void Cancel(bool isReplicating)
        {
            if (isReplicating)
                _disposed.ReadUnlock();
            cts.Cancel();
        }

        /// <summary>
        /// Dispose replica replay task group
        /// </summary>
        public void Dispose()
        {
            cts.Cancel();
            if (!_disposed.TryWriteLock())
                return;
            var replicaReplayTasks = this.replicaReplayTasks;
            if (replicaReplayTasks != null)
            {
                for (var i = 0; i < replicaReplayTasks.Length; i++)
                    replicaReplayTasks[i]?.Dispose();
            }
            cts.Dispose();
        }
    }
}