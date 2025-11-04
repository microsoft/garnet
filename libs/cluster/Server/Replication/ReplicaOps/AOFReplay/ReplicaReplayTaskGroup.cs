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
        ReplicaReplayTask[] replicaReplayTasks = null;

        /// <summary>
        /// Replica replay barrier used to coordinate connection and dispose of replica replay tasks
        /// </summary>
        EventBarrier replicaReplayBarrier = null;

        /// <summary>
        /// Replica task group lock
        /// </summary>
        SingleWriterMultiReaderLock _lock = new();

        /// <summary>
        /// Cancellation token source for replay task group
        /// </summary>
        CancellationTokenSource cts = null;

        /// <summary>
        /// Initialized flag
        /// </summary>
        public bool IsInitialized { get; private set; } = false;

        /// <summary>
        /// Dispose replica replay task group
        /// </summary>
        public void Dispose()
        {
            cts.Cancel();
            var replicaReplayTasks = this.replicaReplayTasks;
            if (replicaReplayTasks != null)
            {
                for (var i = 0; i < replicaReplayTasks.Length; i++)
                    replicaReplayTasks[i]?.Dispose();
                this.replicaReplayTasks = null;
            }
            cts.Dispose();
        }


        /// <summary>
        /// Create replica replay task
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="networkSender"></param>
        public void CreateReplicaReplayTask(int sublogIdx, INetworkSender networkSender)
        {
            // Need to synchronize only for sharded log
            if (clusterProvider.serverOptions.AofSublogCount > 1)
            {
                Initialize();
                // Signal and wait for all tasks to connect
                var _replicaReplayBarrier = this.replicaReplayBarrier;
                if (_replicaReplayBarrier.SignalAndWait(clusterProvider.serverOptions.ReplicaSyncTimeout, cts.Token))
                {
                    replicaReplayBarrier = null;
                    _replicaReplayBarrier.Set();
                    IsInitialized = true;
                }
            }
            else
            {
                Initialize();
                IsInitialized = true;
            }

            replicaReplayTasks[sublogIdx] = new ReplicaReplayTask(sublogIdx, clusterProvider, networkSender, cts, logger);

            void Initialize()
            {
                var sublogCount = clusterProvider.serverOptions.AofSublogCount;
                try
                {
                    _lock.WriteLock();
                    if (this.replicaReplayTasks == null)
                    {
                        replicaReplayTasks = new ReplicaReplayTask[sublogCount];
                        replicaReplayBarrier = new EventBarrier(sublogCount);
                        cts = new();
                    }
                }
                finally
                {
                    _lock.WriteUnlock();
                }
            }
        }
    }
}