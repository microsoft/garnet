// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    /// <summary>
    /// Storage provider for AOF tasks
    /// </summary>
    internal sealed class AofTaskStore : IDisposable
    {
        readonly ClusterProvider clusterProvider;
        readonly ILogger logger;
        readonly int logPageSizeBits, logPageSizeMask;

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
                if (clusterProvider.serverOptions.FastAofTruncate)
                    clusterProvider.storeWrapper.appendOnlyFile.SafeTailShiftCallback = SafeTailShiftCallback;
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
                // Truncate 2 pages above ReadOnly mark, so that we have sufficient time to shift begin before we flush.
                // Make sure this is page-aligned, in case we go to a non-page-aligned ReadOnlyAddress.
                var truncateUntilAddress = clusterProvider.storeWrapper.appendOnlyFile.UnsafeGetReadOnlyAddressAbove(newTailAddress, numPagesAbove: 2);
                if (truncateUntilAddress > 0)
                    SafeTruncateAof(truncateUntilAddress);
            }
        }

        public List<RoleInfo> GetReplicaInfo(long PrimaryReplicationOffset)
        {
            // secondary0: ip=127.0.0.1,port=7001,state=online,offset=56,lag=0
            List<RoleInfo> replicaInfo = new(numTasks);

            _lock.ReadLock();
            var current = clusterProvider.clusterManager.CurrentConfig;
            try
            {
                if (_disposed) return replicaInfo;

                for (int i = 0; i < numTasks; i++)
                {
                    var cr = tasks[i];
                    var (address, port) = current.GetWorkerAddressFromNodeId(cr.remoteNodeId);

                    replicaInfo.Add(new()
                    {
                        address = address,
                        port = port,
                        replication_state = cr.garnetClient.IsConnected ? "online" : "offline",
                        replication_offset = cr.previousAddress,
                        replication_lag = cr.previousAddress - PrimaryReplicationOffset
                    });
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
            try
            {
                _lock.WriteLock();
                _disposed = true;
                for (var i = 0; i < numTasks; i++)
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
            var success = false;
            var current = clusterProvider.clusterManager.CurrentConfig;
            var (address, port) = current.GetWorkerAddressFromNodeId(remoteNodeId);

            // If address is null or port is not valid, we cannot create a task
            if (address == null || port <= 0 || ExceptionInjectionHelper.TriggerCondition(ExceptionInjectionType.Replication_Failed_To_AddAofSyncTask_UnknownNode))
                throw new GarnetException($"Failed to create AOF sync task for {remoteNodeId} with address {address} and port {port}");

            // Create AofSyncTask
            try
            {
                aofSyncTaskInfo = new AofSyncTaskInfo(
                    clusterProvider,
                    this,
                    current.LocalNodeId,
                    remoteNodeId,
                    new GarnetClientSession(
                        new IPEndPoint(IPAddress.Parse(address), port),
                        clusterProvider.replicationManager.GetAofSyncNetworkBufferSettings,
                        clusterProvider.replicationManager.GetNetworkPool,
                        tlsOptions: clusterProvider.serverOptions.TlsOptions?.TlsClientOptions,
                        authUsername: clusterProvider.ClusterUsername,
                        authPassword: clusterProvider.ClusterPassword,
                        logger: logger),
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

                // Fail adding the task if truncation has happened, and we are not in AllowDataLoss mode
                if (startAddress < TruncatedUntil && !clusterProvider.AllowDataLoss)
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

        public bool TryAddReplicationTasks(ReplicaSyncSession[] replicaSyncSessions, long startAddress)
        {
            var current = clusterProvider.clusterManager.CurrentConfig;
            var success = true;
            if (startAddress == 0) startAddress = ReplicationManager.kFirstValidAofAddress;

            // First iterate through all sync sessions and add an AOF sync task
            // All tasks will be
            foreach (var rss in replicaSyncSessions)
            {
                if (rss == null) continue;
                var replicaNodeId = rss.replicaSyncMetadata.originNodeId;
                var (address, port) = current.GetWorkerAddressFromNodeId(replicaNodeId);

                // If address is null or port is not valid, we cannot create a task
                if (address == null || port <= 0)
                    throw new GarnetException($"Failed to create AOF sync task for {replicaNodeId} with address {address} and port {port}");

                try
                {
                    rss.AddAofSyncTask(new AofSyncTaskInfo(
                        clusterProvider,
                        this,
                        current.LocalNodeId,
                        replicaNodeId,
                        new GarnetClientSession(
                            new IPEndPoint(IPAddress.Parse(address), port),
                            clusterProvider.replicationManager.GetAofSyncNetworkBufferSettings,
                            clusterProvider.replicationManager.GetNetworkPool,
                            tlsOptions: clusterProvider.serverOptions.TlsOptions?.TlsClientOptions,
                            authUsername: clusterProvider.ClusterUsername,
                            authPassword: clusterProvider.ClusterPassword,
                            logger: logger),
                        startAddress,
                        logger));
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "{method} creating AOF sync task for {replicaNodeId} failed", nameof(TryAddReplicationTasks), replicaNodeId);
                    return false;
                }
            }

            _lock.WriteLock();
            try
            {
                if (_disposed) return false;

                // Fail adding the task if truncation has happened
                if (startAddress < TruncatedUntil && !clusterProvider.AllowDataLoss)
                {
                    logger?.LogError("{method} failed to add tasks for AOF sync {startAddress} {truncatedUntil}", nameof(TryAddReplicationTasks), startAddress, TruncatedUntil);
                    return false;
                }

                foreach (var rss in replicaSyncSessions)
                {
                    if (rss == null) continue;

                    var added = false;
                    // Find if AOF sync task already exists
                    for (var i = 0; i < numTasks; i++)
                    {
                        var t = tasks[i];
                        Debug.Assert(t != null);
                        if (t.remoteNodeId == rss.replicaNodeId)
                        {
                            tasks[i] = rss.AofSyncTask;
                            t.Dispose();
                            added = true;
                            break;
                        }
                    }

                    if (added) continue;

                    // If AOF sync task did not exist and was not added we added below
                    // Check if array can hold a new AOF sync task
                    if (numTasks == tasks.Length)
                    {
                        var old_tasks = tasks;
                        var _tasks = new AofSyncTaskInfo[tasks.Length * 2];
                        Array.Copy(tasks, _tasks, tasks.Length);
                        tasks = _tasks;
                        Array.Clear(old_tasks);
                    }
                    // Add new AOF sync task
                    tasks[numTasks++] = rss.AofSyncTask;
                }

                success = true;
            }
            finally
            {
                _lock.WriteUnlock();

                if (!success)
                {
                    foreach (var rss in replicaSyncSessions)
                    {
                        if (rss == null) continue;
                        rss.AofSyncTask?.Dispose();
                    }
                }
            }

            return true;
        }

        public bool TryRemove(AofSyncTaskInfo aofSyncTask)
        {
            // Lock addition of new tasks
            _lock.WriteLock();

            var success = false;
            try
            {
                if (_disposed) return success;

                for (var i = 0; i < numTasks; i++)
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

            // Inform that we have logically truncatedUntil
            _ = Tsavorite.core.Utility.MonotonicUpdate(ref this.TruncatedUntil, TruncatedUntil, out _);
            // Release lock early
            _lock.WriteUnlock();

            if (TruncatedUntil is > 0 and < long.MaxValue)
            {
                if (clusterProvider.serverOptions.FastAofTruncate)
                {
                    clusterProvider.storeWrapper.appendOnlyFile?.UnsafeShiftBeginAddress(TruncatedUntil, snapToPageStart: true, truncateLog: true);
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
            var count = 0;
            _lock.ReadLock();
            try
            {
                if (_disposed) return 0;

                for (var i = 0; i < numTasks; i++)
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