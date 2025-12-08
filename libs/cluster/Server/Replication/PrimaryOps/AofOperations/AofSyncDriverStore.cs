// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    /// <summary>
    /// Storage provider for AOF tasks
    /// </summary>
    internal sealed class AofSyncDriverStore : IDisposable
    {
        internal sealed class LogShiftTailCallback(int logIdx, AofSyncDriverStore store)
        {
            readonly int logIdx = logIdx;
            readonly AofSyncDriverStore store = store;

            internal void SafeTailShiftCallback(long oldTailAddress, long newTailAddress)
            {
                var oldPage = oldTailAddress >> store.logPageSizeBits;
                var newPage = newTailAddress >> store.logPageSizeBits;
                // Call truncate only once per page
                if (oldPage != newPage)
                {
                    // Truncate 2 pages after ReadOnly mark, so that we have sufficient time to shift begin before we flush
                    var truncateUntilAddress = (newTailAddress & ~store.logPageSizeMask) - store.TruncateLagAddress;
                    // Do not truncate beyond new tail (to handle corner cases)
                    if (truncateUntilAddress > newTailAddress) truncateUntilAddress = newTailAddress;
                    if (truncateUntilAddress > 0)
                        _ = store.SafeTruncateAof(truncateUntilAddress, logIdx);
                }
            }
        }

        readonly ClusterProvider clusterProvider;
        readonly ILogger logger;
        readonly int logPageSizeBits, logPageSizeMask;
        readonly long TruncateLagAddress;

        AofSyncDriver[] syncDrivers;
        int numDrivers;
        SingleWriterMultiReaderLock _lock;
        bool _disposed;
        internal AofAddress TruncatedUntil;

        public int AofSyncDriverCount => numDrivers;

        public AofSyncDriverStore(ClusterProvider clusterProvider, int initialSize = 1, ILogger logger = null)
        {
            this.clusterProvider = clusterProvider;
            this.logger = logger;
            syncDrivers = new AofSyncDriver[initialSize];
            numDrivers = 0;
            if (clusterProvider.storeWrapper.appendOnlyFile != null)
            {
                logPageSizeBits = clusterProvider.storeWrapper.appendOnlyFile.Log.UnsafeGetLogPageSizeBits();
                var logPageSize = 1 << logPageSizeBits;
                logPageSizeMask = logPageSize - 1;
                TruncateLagAddress = clusterProvider.storeWrapper.appendOnlyFile.Log.UnsafeGetReadOnlyAddressLagOffset() - 2 * logPageSize;
                if (clusterProvider.serverOptions.FastAofTruncate)
                {
                    for (var i = 0; i < clusterProvider.serverOptions.AofSublogCount; i++)
                    {
                        var logShiftTailCallback = new LogShiftTailCallback(i, this);
                        clusterProvider.storeWrapper.appendOnlyFile.SetLogShiftTailCallback(i, logShiftTailCallback.SafeTailShiftCallback);
                    }
                }
            }
            TruncatedUntil = AofAddress.Create(clusterProvider.serverOptions.AofSublogCount, 0);
        }

        /// <summary>
        /// Safely truncate AOF sublog
        /// </summary>
        /// <param name="truncateUntil"></param>
        /// <param name="sublogIdx"></param>
        /// <returns></returns>
        long SafeTruncateAof(long truncateUntil, int sublogIdx)
        {
            _lock.WriteLock();

            if (_disposed)
            {
                _lock.WriteUnlock();
                return -1;
            }

            // Calculate min address of all iterators
            var TruncatedUntil = truncateUntil;
            for (var i = 0; i < numDrivers; i++)
            {
                Debug.Assert(syncDrivers[i] != null, $"syncDriver cannot be null at {nameof(SafeTruncateAof)}");
                if (syncDrivers[i].PreviousAddress[sublogIdx] < TruncatedUntil)
                    TruncatedUntil = syncDrivers[i].PreviousAddress[sublogIdx];
            }

            // Inform that we have logically truncatedUntil
            this.TruncatedUntil.MonotonicUpdate(TruncatedUntil, sublogIdx);
            // Release lock early
            _lock.WriteUnlock();

            if (clusterProvider.serverOptions.FastAofTruncate)
            {
                clusterProvider.storeWrapper.appendOnlyFile?.Log.GetSubLog(sublogIdx).UnsafeShiftBeginAddress(TruncatedUntil, snapToPageStart: true, truncateLog: true);
            }
            else
            {
                clusterProvider.storeWrapper.appendOnlyFile?.Log.GetSubLog(sublogIdx).TruncateUntil(TruncatedUntil);
                clusterProvider.storeWrapper.appendOnlyFile?.Log.Commit();
            }

            return TruncatedUntil;
        }

        /// <summary>
        /// Safely truncate AOF until provided address by checking against active AofSyncDrivers
        /// </summary>
        /// <param name="truncateUntil"></param>
        public void SafeTruncateAof(AofAddress truncateUntil)
        {
            _lock.WriteLock();

            if (_disposed)
            {
                _lock.WriteUnlock();
                return;
            }

            // Calculate min address of all iterators
            var TruncatedUntil = truncateUntil;
            for (var i = 0; i < numDrivers; i++)
            {
                Debug.Assert(syncDrivers[i] != null, $"syncDriver cannot be null {nameof(SafeTruncateAof)}");
                var previousAddress = syncDrivers[i].PreviousAddress;
                for (var sublogIdx = 0; sublogIdx < previousAddress.Length; sublogIdx++)
                {
                    if (previousAddress[sublogIdx] < TruncatedUntil[sublogIdx])
                        TruncatedUntil[sublogIdx] = previousAddress[sublogIdx];
                }
            }

            // Inform that we have logically truncatedUntil
            this.TruncatedUntil.MonotonicUpdate(ref TruncatedUntil);
            // Release lock early
            _lock.WriteUnlock();

            if (clusterProvider.serverOptions.FastAofTruncate)
            {
                clusterProvider.storeWrapper.appendOnlyFile?.Log.UnsafeShiftBeginAddress(TruncatedUntil, snapToPageStart: true, truncateLog: true);
            }
            else
            {
                clusterProvider.storeWrapper.appendOnlyFile?.Log.TruncateUntil(TruncatedUntil);
                clusterProvider.storeWrapper.appendOnlyFile?.Log.Commit();
            }
        }

        /// <summary>
        /// Get relevant information for all attached replicas
        /// </summary>
        /// <param name="PrimaryReplicationOffset"></param>
        /// <returns></returns>
        public List<RoleInfo> GetReplicaInfo(AofAddress PrimaryReplicationOffset)
        {
            // secondary0: ip=127.0.0.1,port=7001,state=online,offset=56,lag=0
            List<RoleInfo> replicaInfo = new(numDrivers);

            _lock.ReadLock();
            var current = clusterProvider.clusterManager.CurrentConfig;
            try
            {
                if (_disposed) return replicaInfo;

                for (var i = 0; i < numDrivers; ++i)
                {
                    var cr = syncDrivers[i];
                    var (address, port) = current.GetWorkerAddressFromNodeId(cr.RemoteNodeId);
                    replicaInfo.Add(new()
                    {
                        address = address,
                        port = port,
                        replication_state = cr.IsConnected ? "online" : "offline",
                        replication_offset = cr.PreviousAddress,
                        replication_lag = cr.PreviousAddress.Diff(PrimaryReplicationOffset)
                    });
                }
            }
            finally
            {
                _lock.ReadUnlock();
            }
            return replicaInfo;
        }

        /// <summary>
        /// Dispose the AofSyncDriverStore
        /// </summary>
        public void Dispose()
        {
            try
            {
                _lock.WriteLock();
                if (_disposed) return;
                _disposed = true;
                for (var i = 0; i < numDrivers; i++)
                {
                    var task = syncDrivers[i];
                    task.Dispose();
                }
                numDrivers = 0;
                Array.Clear(syncDrivers);
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }

        /// <summary>
        /// Add a new AofSyncDriver
        /// </summary>
        /// <param name="remoteNodeId"></param>
        /// <param name="startAddress"></param>
        /// <param name="aofSyncDriver"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        public bool TryAddReplicationDriver(string remoteNodeId, ref AofAddress startAddress, out AofSyncDriver aofSyncDriver)
        {
            aofSyncDriver = null;

            startAddress.SetValueIf(ReplicationManager.kFirstValidAofAddress, 0);
            var success = false;
            var current = clusterProvider.clusterManager.CurrentConfig;
            var (address, port) = current.GetWorkerAddressFromNodeId(remoteNodeId);

            // If address is null or port is not valid, we cannot create a task
            if (address == null || port <= 0 || ExceptionInjectionHelper.TriggerCondition(ExceptionInjectionType.Replication_Failed_To_AddAofSyncTask_UnknownNode))
                throw new GarnetException($"Failed to create AOF sync task for {remoteNodeId} with address {address} and port {port}");

            // Create AofSyncTask
            try
            {
                aofSyncDriver = new AofSyncDriver(
                    clusterProvider,
                    this,
                    current.LocalNodeId,
                    remoteNodeId,
                    new IPEndPoint(IPAddress.Parse(address), port),
                    ref startAddress,
                    logger);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An error occurred at TryAddReplicationTask task creation for {remoteNodeId}", remoteNodeId);
                return false;
            }

            Debug.Assert(aofSyncDriver != null, $"aofSyncTaskInfo should not be null {nameof(TryAddReplicationDriver)}");

            // Lock to prevent add/remove tasks and truncate operations
            _lock.WriteLock();
            try
            {
                if (_disposed) return success;

                // Fail adding the task if truncation has happened, and we are not in AllowDataLoss mode
                if (startAddress.AnyLesser(TruncatedUntil) && !clusterProvider.AllowDataLoss)
                {
                    logger?.LogWarning("AOF sync task for {remoteNodeId}, with start address {startAddress}, could not be added, local AOF is truncated until {truncatedUntil}", remoteNodeId, startAddress, TruncatedUntil);
                    return success;
                }

                // Iterate array of existing tasks and update associated task if it already exists
                for (var i = 0; i < numDrivers; i++)
                {
                    var t = syncDrivers[i];
                    Debug.Assert(t != null, "syncDriver should not be null");
                    if (t.RemoteNodeId == remoteNodeId)
                    {
                        syncDrivers[i] = aofSyncDriver;
                        t.Dispose();
                        success = true;
                        break;
                    }
                }

                // If task did not exist we add it here
                if (!success)
                {
                    if (numDrivers == syncDrivers.Length)
                    {
                        var old_tasks = syncDrivers;
                        var _tasks = new AofSyncDriver[syncDrivers.Length * 2];
                        Array.Copy(syncDrivers, _tasks, syncDrivers.Length);
                        syncDrivers = _tasks;
                        Array.Clear(old_tasks);
                    }
                    syncDrivers[numDrivers++] = aofSyncDriver;
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
                    aofSyncDriver?.Dispose();
                    aofSyncDriver = null;
                }
            }

            return success;
        }

        /// <summary>
        /// Add AofSyncDrivers collection
        /// </summary>
        /// <param name="replicaSyncSessions"></param>
        /// <param name="startAddress"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        public bool TryAddReplicationDrivers(ReplicaSyncSession[] replicaSyncSessions, ref AofAddress startAddress)
        {
            var current = clusterProvider.clusterManager.CurrentConfig;
            var success = true;
            startAddress.SetValueIf(ReplicationManager.kFirstValidAofAddress, 0);

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
                    rss.AddAofSyncTask(new AofSyncDriver(
                        clusterProvider,
                        this,
                        current.LocalNodeId,
                        replicaNodeId,
                        new IPEndPoint(IPAddress.Parse(address), port),
                        ref startAddress,
                        logger));
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "{method} creating AOF sync task for {replicaNodeId} failed", nameof(TryAddReplicationDrivers), replicaNodeId);
                    return false;
                }
            }

            _lock.WriteLock();
            try
            {
                if (_disposed) return false;

                // Fail adding the task if truncation has happened
                if (startAddress.AnyLesser(TruncatedUntil) && !clusterProvider.AllowDataLoss)
                {
                    logger?.LogError("{method} failed to add tasks for AOF sync {startAddress} {truncatedUntil}", nameof(TryAddReplicationDrivers), startAddress, TruncatedUntil);
                    return false;
                }

                foreach (var rss in replicaSyncSessions)
                {
                    if (rss == null) continue;

                    var added = false;
                    // Find if AOF sync task already exists
                    for (var i = 0; i < numDrivers; i++)
                    {
                        var t = syncDrivers[i];
                        Debug.Assert(t != null, $"syncDrive should not be null {nameof(TryAddReplicationDrivers)}");
                        if (t.RemoteNodeId == rss.replicaNodeId)
                        {
                            syncDrivers[i] = rss.AofSyncDriver;
                            t.Dispose();
                            added = true;
                            break;
                        }
                    }

                    if (added) continue;

                    // If AOF sync task did not exist and was not added we added below
                    // Check if array can hold a new AOF sync task
                    if (numDrivers == syncDrivers.Length)
                    {
                        var old_tasks = syncDrivers;
                        var _tasks = new AofSyncDriver[syncDrivers.Length * 2];
                        Array.Copy(syncDrivers, _tasks, syncDrivers.Length);
                        syncDrivers = _tasks;
                        Array.Clear(old_tasks);
                    }
                    // Add new AOF sync task
                    syncDrivers[numDrivers++] = rss.AofSyncDriver;
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
                        rss.AofSyncDriver?.Dispose();
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Remove provided AofSyncDriver
        /// </summary>
        /// <param name="aofSyncDriver"></param>
        /// <returns></returns>
        public bool TryRemove(AofSyncDriver aofSyncDriver)
        {
            // Lock addition of new tasks
            _lock.WriteLock();

            var success = false;
            try
            {
                if (_disposed) return success;

                for (var i = 0; i < numDrivers; i++)
                {
                    var t = syncDrivers[i];
                    Debug.Assert(t != null, $"syncDriver should not be null at {nameof(TryRemove)}");
                    if (t == aofSyncDriver)
                    {
                        syncDrivers[i] = null;
                        if (i < numDrivers - 1)
                        {
                            // Swap the last task into the free slot
                            syncDrivers[i] = syncDrivers[numDrivers - 1];
                            syncDrivers[numDrivers - 1] = null;
                        }
                        // Reduce the number of tasks
                        numDrivers--;

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
        /// Count the number of AofSyncDriver connections
        /// </summary>
        /// <returns></returns>
        public int CountConnectedReplicas()
        {
            var count = 0;
            _lock.ReadLock();
            try
            {
                if (_disposed) return 0;

                for (var i = 0; i < numDrivers; i++)
                {
                    var t = syncDrivers[i];
                    count += t.IsConnected ? 1 : 0;
                }
            }
            finally
            {
                _lock.ReadUnlock();
            }
            return count;
        }

        /// <summary>
        /// Update TruncatedUntil address
        /// </summary>
        /// <param name="truncatedUntil"></param>
        public void UpdateTruncatedUntil(AofAddress truncatedUntil)
        {
            _lock.WriteLock();
            this.TruncatedUntil.MonotonicUpdate(ref truncatedUntil);
            _lock.WriteUnlock();
        }

        /// <summary>
        /// Remove and dispose all active aof sync drivers
        /// </summary>
        public void RemoveAll()
        {
            try
            {
                _lock.WriteLock();
                if (_disposed) return;
                for (var i = 0; i < numDrivers; i++)
                {
                    var task = syncDrivers[i];
                    task.Dispose();
                }
                numDrivers = 0;
                Array.Clear(syncDrivers);
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }
    }
}