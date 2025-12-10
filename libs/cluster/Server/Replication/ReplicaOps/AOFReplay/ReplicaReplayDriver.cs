// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed class ReplicaReplayDriver : IDisposable
    {
        SingleWriterMultiReaderLock activeReplay;
        readonly ClusterProvider clusterProvider;
        readonly ReplicaReplaySubtask[] replicaReplaySubtasks;
        readonly CancellationTokenSource cts;

        static ReplicaReplaySubtask[] CreateReplaySubtasks(
            int sublogIdx,
            ReplicaReplayDriver replicaReplayDriver,
            ClusterProvider clusterProvider,
            INetworkSender respSessionNetworkSender,
            CancellationTokenSource cts, ILogger logger)
        {
            var replaySubtaskCount = clusterProvider.serverOptions.AofReplaySubtaskCount;
            return [.. Enumerable.Range(0, replaySubtaskCount).Select(
                subtaskIdx => new ReplicaReplaySubtask(sublogIdx, subtaskIdx, replicaReplayDriver, clusterProvider, respSessionNetworkSender, cts, logger))];
        }

        /// <summary>
        /// Indexer
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public ReplicaReplaySubtask this[int i]
        {
            get
            {
                return replicaReplaySubtasks[i];
            }
        }

        public ReplicaReplayDriver(int sublogIdx, ClusterProvider clusterProvider, INetworkSender respSessionNetworkSender, CancellationTokenSource cts, ILogger logger = null)
        {
            this.clusterProvider = clusterProvider;
            this.replicaReplaySubtasks = CreateReplaySubtasks(sublogIdx, this, clusterProvider, respSessionNetworkSender, cts, logger);
            this.cts = cts;
        }

        /// <summary>
        /// Initialize all replay subtasks
        /// </summary>
        /// <param name="startAddress"></param>
        public void InitializeReplaySubtasks(long startAddress)
        {
            try
            {
                activeReplay.WriteLock();
                foreach (var subtask in replicaReplaySubtasks)
                    subtask.Run(startAddress);
            }
            finally
            {
                activeReplay.WriteUnlock();
            }
        }

        public unsafe void Consume(byte* record, int recordLength, long currentAddress, long nextAddress, bool isProtected)
            => Parallel.ForEach(replicaReplaySubtasks, subtask => subtask.Consume(record, recordLength, currentAddress, nextAddress, isProtected));

        /// <summary>
        /// Throttle primary
        /// </summary>
        public void ThrottlePrimary()
        {
            var replicationOffsetMaxLag = clusterProvider.storeWrapper.serverOptions.ReplicationOffsetMaxLag;
            var replicationOffset = clusterProvider.replicationManager.ReplicationOffset;
            var tailAddress = clusterProvider.storeWrapper.appendOnlyFile.Log.TailAddress;
            while (replicationOffsetMaxLag != -1 && tailAddress.AggregateDiff(replicationOffset) > replicationOffsetMaxLag)
            {
                cts.Token.ThrowIfCancellationRequested();
                _ = Thread.Yield();

                tailAddress = clusterProvider.storeWrapper.appendOnlyFile.Log.TailAddress;
                replicationOffset = clusterProvider.replicationManager.ReplicationOffset;
            }
        }

        /// <summary>
        /// Resume replay to prevent dispose
        /// NOTE:
        ///     Used with sync replication to ensure dispose of the store
        ///     does not interfere with ongoing replay op
        /// </summary>
        /// <returns></returns>
        public bool ResumeReplay()
            => activeReplay.TryReadLock();

        /// <summary>
        /// Indicate resume replay to prevent dispose
        /// </summary>
        /// <returns></returns>
        public void SuspendReplay()
            => activeReplay.ReadUnlock();

        /// <summary>
        /// Dispose all subtasks for this driver
        /// </summary>
        public void Dispose()
        {
            activeReplay.WriteLock();
            foreach (var subtask in replicaReplaySubtasks)
                subtask?.Dispose();
        }
    }
}