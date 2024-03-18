// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Garnet.client;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    /// <summary>
    /// Storage provider for AOF tasks
    /// </summary>
    internal class AofTaskStore : IDisposable
    {
        readonly ClusterProvider clusterProvider;
        readonly ILogger logger;
        readonly int logPageSizeBits, logPageSizeMask;
        readonly long TruncateLagAddress;

        AofSyncTaskInfo[] tasks;
        int numTasks;
        SingleWriterMultiReaderLock _lock;
        bool _disposed;
        public int Count => numTasks;
        long TruncatedUntil;

        public AofTaskStore(ClusterProvider clusterProvider, int initialSize = 1, ILogger logger = null)
        {
            this.clusterProvider = clusterProvider;
            this.logger = logger;
            tasks = new AofSyncTaskInfo[initialSize];
            numTasks = 0;
            if (clusterProvider.storeWrapper.appendOnlyFile != null)
            {
                logPageSizeBits = clusterProvider.storeWrapper.appendOnlyFile.UnsafeGetLogPageSizeBits();
                int logPageSize = 1 << logPageSizeBits;
                logPageSizeMask = logPageSize - 1;
                if (clusterProvider.serverOptions.MainMemoryReplication)
                    clusterProvider.storeWrapper.appendOnlyFile.SafeTailShiftCallback = SafeTailShiftCallback;
                TruncateLagAddress = clusterProvider.storeWrapper.appendOnlyFile.UnsafeGetReadOnlyLagAddress() - 2 * logPageSize;
            }
            TruncatedUntil = 0;
        }

        internal long AofTruncatedUntil => TruncatedUntil;

        internal void SafeTailShiftCallback(long oldTailAddress, long newTailAddress)
        {
            long oldPage = oldTailAddress >> logPageSizeBits;
            long newPage = newTailAddress >> logPageSizeBits;
            // Call truncate only once per page
            if (oldPage != newPage)
            {
                // Truncate 2 pages after ReadOnly mark, so that we have sufficient time to shift begin before we flush
                long truncateUntilAddress = (newTailAddress & ~logPageSizeMask) - TruncateLagAddress;
                // Do not truncate beyond new tail (to handle corner cases)
                if (truncateUntilAddress > newTailAddress) truncateUntilAddress = newTailAddress;
                if (truncateUntilAddress > 0)
                    SafeTruncateAof(truncateUntilAddress);
            }
        }

        public List<(string, string)> GetReplicaInfo(long PrimaryReplicationOffset)
        {
            // secondary0: ip=127.0.0.1,port=7001,state=online,offset=56,lag=0
            List<(string, string)> replicaInfo = new List<(string, string)>();

            _lock.ReadLock();
            var current = clusterProvider.clusterManager.CurrentConfig;
            try
            {
                if (_disposed) return replicaInfo;

                for (int i = 0; i < numTasks; i++)
                {
                    var cr = tasks[i];
                    var replicaId = cr.remoteNodeId;
                    var (address, port) = current.GetWorkerAddressFromNodeId(replicaId);
                    var state = cr.garnetClient.IsConnected ? "online" : "offline";
                    long offset = cr.previousAddress;
                    long lag = offset - PrimaryReplicationOffset;
                    var count = replicaInfo.Count;
                    replicaInfo.Add(($"slave{count}", $"ip={address},port={port},state={state},offset={offset},lag={lag}"));
                }
            }
            finally
            {
                _lock.ReadUnlock();
            }
            return replicaInfo;
        }

        public void Dispose()
        {
            _lock.WriteLock();
            try
            {
                _disposed = true;
                for (int i = 0; i < numTasks; i++)
                {
                    var task = tasks[i];
                    task.Dispose();
                }
                numTasks = 0;
                Array.Clear(tasks);
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }

        public bool TryAddReplicationTask(string remoteNodeId, long startAddress, out AofSyncTaskInfo aofSyncTaskInfo)
        {
            aofSyncTaskInfo = null;

            if (startAddress == 0) startAddress = ReplicationManager.kFirstValidAofAddress;
            bool success = false;
            var current = clusterProvider.clusterManager.CurrentConfig;
            var (address, port) = current.GetWorkerAddressFromNodeId(remoteNodeId);

            // Create AofSyncTask
            try
            {
                aofSyncTaskInfo = new AofSyncTaskInfo(
                    clusterProvider,
                    this,
                    current.GetLocalNodeId(),
                    remoteNodeId,
                    new GarnetClientSession(address, port, clusterProvider.serverOptions.TlsOptions?.TlsClientOptions, authUsername: clusterProvider.ClusterUsername, authPassword: clusterProvider.ClusterPassword, 1 << 22, logger: logger),
                    new CancellationTokenSource(),
                    startAddress,
                    logger);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An error occurred at TryAddReplicationTask task creation for {remoteNodeId}", remoteNodeId);
                return false;
            }

            Debug.Assert(aofSyncTaskInfo != null);

            // Lock to prevent add/remove tasks and truncate operations
            _lock.WriteLock();
            try
            {
                if (_disposed) return success;

                // Possible AOF data loss: { using null AOF device } OR { main memory replication AND no on-demand checkpoints }
                bool possibleAofDataLoss = clusterProvider.serverOptions.UseAofNullDevice ||
                    (clusterProvider.serverOptions.MainMemoryReplication && !clusterProvider.serverOptions.OnDemandCheckpoint);

                // Fail adding the task if truncation has happened, and we are not in possibleAofDataLoss mode
                if (startAddress < TruncatedUntil && !possibleAofDataLoss)
                {
                    logger?.LogWarning("AOF sync task for {remoteNodeId}, with start address {startAddress}, could not be added, local AOF is truncated until {truncatedUntil}", remoteNodeId, startAddress, TruncatedUntil);
                    return success;
                }

                // Iterate array of existing tasks and update associated task if it already exists
                for (int i = 0; i < numTasks; i++)
                {
                    var t = tasks[i];
                    Debug.Assert(t != null);
                    if (t.remoteNodeId == remoteNodeId)
                    {
                        tasks[i] = aofSyncTaskInfo;
                        t.Dispose();
                        success = true;
                        break;
                    }
                }

                // If task did not exist we add it here
                if (!success)
                {
                    if (numTasks == tasks.Length)
                    {
                        var old_tasks = tasks;
                        var _tasks = new AofSyncTaskInfo[tasks.Length * 2];
                        Array.Copy(tasks, _tasks, tasks.Length);
                        tasks = _tasks;
                        Array.Clear(old_tasks);
                    }
                    tasks[numTasks++] = aofSyncTaskInfo;
                    success = true;
                }
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An error occurred at TryAddReplicationTask task addition for {remoteNodeId}", remoteNodeId);
            }
            finally
            {
                _lock.WriteUnlock();
                if (!success)
                {
                    aofSyncTaskInfo?.Dispose();
                    aofSyncTaskInfo = null;
                }
            }

            return success;
        }

        public bool TryRemove(AofSyncTaskInfo aofSyncTask)
        {
            // Lock addition of new tasks
            _lock.WriteLock();

            bool success = false;
            try
            {
                if (_disposed) return success;

                for (int i = 0; i < numTasks; i++)
                {
                    var t = tasks[i];
                    Debug.Assert(t != null);
                    if (t == aofSyncTask)
                    {
                        tasks[i] = null;
                        if (i < numTasks - 1)
                        {
                            // Swap the last task into the free slot
                            tasks[i] = tasks[numTasks - 1];
                            tasks[numTasks - 1] = null;
                        }
                        // Reduce the number of tasks
                        numTasks--;

                        // Kill the task
                        t.Dispose();
                        success = true;
                        break;
                    }
                }
            }
            finally
            {
                _lock.WriteUnlock();
            }
            return success;
        }

        /// <summary>
        /// Safely truncate iterator
        /// </summary>
        /// <param name="CheckpointCoveredAofAddress"></param>
        /// <returns></returns>
        public long SafeTruncateAof(long CheckpointCoveredAofAddress = long.MaxValue)
        {
            _lock.WriteLock();

            if (_disposed)
            {
                _lock.WriteUnlock();
                return -1;
            }

            // Calculate min address of all iterators
            long TruncatedUntil = CheckpointCoveredAofAddress;
            for (int i = 0; i < numTasks; i++)
            {
                Debug.Assert(tasks[i] != null);
                if (tasks[i].previousAddress < TruncatedUntil)
                    TruncatedUntil = tasks[i].previousAddress;
            }

            //Inform that we have logically truncatedUntil
            Tsavorite.core.Utility.MonotonicUpdate(ref this.TruncatedUntil, TruncatedUntil, out _);
            //Release lock early
            _lock.WriteUnlock();

            if (TruncatedUntil > 0 && TruncatedUntil < long.MaxValue)
            {
                if (clusterProvider.serverOptions.MainMemoryReplication)
                {
                    clusterProvider.storeWrapper.appendOnlyFile?.UnsafeShiftBeginAddress(TruncatedUntil, snapToPageStart: true, truncateLog: true, noFlush: true);
                }
                else
                {
                    clusterProvider.storeWrapper.appendOnlyFile?.TruncateUntil(TruncatedUntil);
                    clusterProvider.storeWrapper.appendOnlyFile?.Commit();
                }
            }
            return TruncatedUntil;
        }

        public int CountConnectedReplicas()
        {
            int count = 0;
            _lock.ReadLock();
            try
            {
                if (_disposed) return 0;

                for (int i = 0; i < numTasks; i++)
                {
                    var t = tasks[i];
                    count += t.garnetClient.IsConnected ? 1 : 0;
                }
            }
            finally
            {
                _lock.ReadUnlock();
            }
            return count;
        }

        public void UpdateTruncatedUntil(long truncatedUntil)
        {
            _lock.WriteLock();
            Tsavorite.core.Utility.MonotonicUpdate(ref TruncatedUntil, truncatedUntil, out _);
            _lock.WriteUnlock();
        }
    }
}