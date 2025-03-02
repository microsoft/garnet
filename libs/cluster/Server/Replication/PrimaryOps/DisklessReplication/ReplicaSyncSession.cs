﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class ReplicaSyncSession
    {
        SyncStatusInfo ssInfo;
        Task<bool> flushTask;
        bool sendMainStore = false;
        bool sendObjectStore = false;
        bool outOfRangeAof = false;
        bool fullSync = false;

        /// <summary>
        /// Get the associated aof sync task instance with this replica sync session
        /// </summary>
        public AofSyncTaskInfo AofSyncTask { get; private set; } = null;

        public bool IsConnected => AofSyncTask != null && AofSyncTask.IsConnected;

        public bool Failed => ssInfo.syncStatus == SyncStatus.FAILED;

        public bool InProgress => ssInfo.syncStatus == SyncStatus.INPROGRESS;

        public SyncStatusInfo GetSyncStatusInfo => ssInfo;

        public long currentStoreVersion;

        public long currentObjectStoreVersion;

        /// <summary>
        /// Pessimistic checkpoint covered AOF address
        /// </summary>
        public long checkpointCoveredAofAddress;

        #region NetworkMethods
        /// <summary>
        /// Connect client
        /// </summary>
        public void Connect()
        {
            if (!AofSyncTask.IsConnected)
                AofSyncTask.garnetClient.Connect();
        }

        /// <summary>
        /// Execute async command
        /// </summary>
        /// <param name="commands"></param>
        /// <returns></returns>
        public Task<string> ExecuteAsync(params string[] commands)
        {
            WaitForFlush().GetAwaiter().GetResult();
            return AofSyncTask.garnetClient.ExecuteAsync(commands);
        }

        /// <summary>
        /// Initialize iteration buffer
        /// </summary>
        public void InitializeIterationBuffer()
        {
            WaitForFlush().GetAwaiter().GetResult();
            AofSyncTask.garnetClient.InitializeIterationBuffer();
        }

        /// <summary>
        /// Set Cluster Sync header
        /// </summary>
        /// <param name="isMainStore"></param>
        public void SetClusterSyncHeader(bool isMainStore)
        {
            WaitForFlush().GetAwaiter().GetResult();
            if (AofSyncTask.garnetClient.NeedsInitialization)
                AofSyncTask.garnetClient.SetClusterSyncHeader(clusterProvider.clusterManager.CurrentConfig.LocalNodeId, isMainStore: isMainStore);
        }

        /// <summary>
        /// Try write main store key value pair
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="task"></param>
        /// <returns></returns>
        public bool TryWriteKeyValueSpanByte(ref SpanByte key, ref SpanByte value, out Task<string> task)
        {
            WaitForFlush().GetAwaiter().GetResult();
            return AofSyncTask.garnetClient.TryWriteKeyValueSpanByte(ref key, ref value, out task);
        }

        /// <summary>
        /// Try write object store key value pair
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="expiration"></param>
        /// <param name="task"></param>
        /// <returns></returns>
        public bool TryWriteKeyValueByteArray(byte[] key, byte[] value, long expiration, out Task<string> task)
        {
            WaitForFlush().GetAwaiter().GetResult();
            return AofSyncTask.garnetClient.TryWriteKeyValueByteArray(key, value, expiration, out task);
        }

        /// <summary>
        /// Send and reset iteration buffer
        /// </summary>
        /// <returns></returns>
        public void SendAndResetIterationBuffer()
        {
            WaitForFlush().GetAwaiter().GetResult();
            SetFlushTask(AofSyncTask.garnetClient.SendAndResetIterationBuffer());
        }
        #endregion

        /// <summary>
        /// Associated aof sync task instance with this replica sync session
        /// </summary>
        /// <param name="aofSyncTask"></param>
        public void AddAofSyncTask(AofSyncTaskInfo aofSyncTask) => AofSyncTask = aofSyncTask;

        /// <summary>
        /// Set status of replica sync session
        /// </summary>
        /// <param name="status"></param>
        /// <param name="error"></param>
        public void SetStatus(SyncStatus status, string error = null)
        {
            ssInfo.error = error;
            // NOTE: set this last to signal state change
            ssInfo.syncStatus = status;
        }

        /// <summary>
        /// Set network flush task for checkpoint snapshot stream data
        /// </summary>
        /// <param name="task"></param>
        public void SetFlushTask(Task<string> task)
        {
            if (task != null)
            {
                flushTask = task.ContinueWith(resp =>
                {
                    if (!resp.Result.Equals("OK", StringComparison.Ordinal))
                    {
                        logger?.LogError("ReplicaSyncSession: {errorMsg}", resp.Result);
                        SetStatus(SyncStatus.FAILED, resp.Result);
                        return false;
                    }
                    return true;
                }, TaskContinuationOptions.OnlyOnRanToCompletion).WaitAsync(timeout, token);
            }
        }

        /// <summary>
        /// Wait for network buffer flush
        /// </summary>
        /// <returns></returns>
        public async Task WaitForFlush()
        {
            try
            {
                if (flushTask != null) _ = await flushTask;
                flushTask = null;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "{method}", $"{nameof(ReplicaSyncSession.WaitForFlush)}");
                SetStatus(SyncStatus.FAILED, "Flush task faulted");
            }
        }

        /// <summary>
        /// Wait until sync of checkpoint is completed
        /// </summary>
        /// <returns></returns>
        public async Task WaitForSyncCompletion()
        {
            try
            {
                while (ssInfo.syncStatus is not SyncStatus.SUCCESS and not SyncStatus.FAILED)
                {
                    token.ThrowIfCancellationRequested();
                    await Task.Yield();
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "{method} failed waiting for sync", nameof(WaitForSyncCompletion));
                SetStatus(SyncStatus.FAILED, "Wait for sync task faulted");
            }
        }

        /// <summary>
        /// Should stream
        /// </summary>
        /// <returns></returns>
        public bool NeedToFullSync()
        {
            // TODO: consolidate disk-based logic if possible
            return clusterProvider.serverOptions.ReplicaDisklessSync ?
                ShouldStreamDisklessCheckpoint() : throw new NotImplementedException();

            bool ShouldStreamDisklessCheckpoint()
            {
                var localPrimaryReplId = clusterProvider.replicationManager.PrimaryReplId;
                var sameHistory = localPrimaryReplId.Equals(replicaSyncMetadata.currentPrimaryReplId, StringComparison.Ordinal);
                sendMainStore = !sameHistory || replicaSyncMetadata.currentStoreVersion != currentStoreVersion;
                sendObjectStore = !sameHistory || replicaSyncMetadata.currentObjectStoreVersion != currentObjectStoreVersion;

                var aofBeginAddress = clusterProvider.storeWrapper.appendOnlyFile.BeginAddress;
                var aofTailAddress = clusterProvider.storeWrapper.appendOnlyFile.TailAddress;
                outOfRangeAof = replicaSyncMetadata.currentAofTailAddress < aofBeginAddress || replicaSyncMetadata.currentAofTailAddress > aofTailAddress;

                // We need to stream checkpoint if any of the following conditions are met:
                // 1. Replica has different history than primary
                // 2. Replica has different main store version than primary
                // 3. Replica has different object store version than primary
                // 4. Replica has truncated AOF
                fullSync = sendMainStore || sendObjectStore || outOfRangeAof;
                return fullSync;
            }
        }

        /// <summary>
        /// Begin syncing AOF to the replica
        /// </summary>
        public async Task BeginAofSync()
        {
            var aofSyncTask = AofSyncTask;
            try
            {
                var mmr = clusterProvider.serverOptions.FastAofTruncate;
                var aofNull = clusterProvider.serverOptions.UseAofNullDevice;

                var currentAofBeginAddress = fullSync ? checkpointCoveredAofAddress : aofSyncTask.StartAddress;
                var currentAofTailAddress = clusterProvider.storeWrapper.appendOnlyFile.TailAddress;

                var recoverSyncMetadata = new SyncMetadata(
                    fullSync: fullSync,
                    originNodeRole: clusterProvider.clusterManager.CurrentConfig.LocalNodeRole,
                    originNodeId: clusterProvider.clusterManager.CurrentConfig.LocalNodeId,
                    currentPrimaryReplId: clusterProvider.replicationManager.PrimaryReplId,
                    currentStoreVersion: currentStoreVersion,
                    currentObjectStoreVersion: currentObjectStoreVersion,
                    currentAofBeginAddress: currentAofBeginAddress,
                    currentAofTailAddress: currentAofTailAddress,
                    currentReplicationOffset: clusterProvider.replicationManager.ReplicationOffset,
                    checkpointEntry: null);

                var result = await aofSyncTask.garnetClient.ExecuteAttachSync(recoverSyncMetadata.ToByteArray());
                if (!long.TryParse(result, out var syncFromAofAddress))
                {
                    logger?.LogError("Failed to parse syncFromAddress at {method}", nameof(BeginAofSync));
                    SetStatus(SyncStatus.FAILED, "Failed to parse recovery offset");
                    return;
                }

                logger?.LogSyncMetadata(LogLevel.Trace, "BeginAofSync", replicaSyncMetadata, recoverSyncMetadata);

                // We have already added the iterator for the covered address above but replica might request an address
                // that is ahead of the covered address so we should start streaming from that address in order not to
                // introduce duplicate insertions.
                if (!clusterProvider.replicationManager.TryAddReplicationTask(replicaSyncMetadata.originNodeId, syncFromAofAddress, out aofSyncTask))
                    throw new GarnetException("Failed trying to try update replication task");
                if (!clusterProvider.replicationManager.TryConnectToReplica(replicaSyncMetadata.originNodeId, syncFromAofAddress, aofSyncTask, out _))
                    throw new GarnetException("Failed connecting to replica for aofSync");
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "{method}", $"{nameof(ReplicaSyncSession.BeginAofSync)}");
                SetStatus(SyncStatus.FAILED, "Failed to begin AOF sync");
                AofSyncTask?.Dispose();
            }
        }
    }
}