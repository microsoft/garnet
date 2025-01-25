// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
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
        AofSyncTaskInfo aofSyncTask = null;

        bool sendMainStore = false;
        bool sendObjectStore = false;
        bool truncatedAof = false;
        bool fullSync = false;

        public bool IsConnected => aofSyncTask != null && aofSyncTask.IsConnected;

        public bool Failed => ssInfo.syncStatus == SyncStatus.FAILED;

        public SyncStatusInfo GetSyncStatusInfo => ssInfo;

        public long currentStoreVersion;

        public long currentObjectStoreVersion;

        /// <summary>
        /// LogError
        /// </summary>
        public void LogError()
        {
            logger?.LogError("{msg} > " +
                "originNodeId: {originNodeId}, " +
                "currentPrimaryReplId: {currentPrimaryReplId}, " +
                "currentAofBeginAddress: {currentAofBeginAddress}, " +
                "currentAofTailAddress: {currentAofTailAddress}, ",
                ssInfo.error,
                replicaSyncMetadata.originNodeId,
                replicaSyncMetadata.currentPrimaryReplId,
                replicaSyncMetadata.currentAofBeginAddress,
                replicaSyncMetadata.currentAofTailAddress);
        }

        #region NetworkMethods
        /// <summary>
        /// Connect client
        /// </summary>
        public void Connect()
        {
            if (!aofSyncTask.IsConnected)
                aofSyncTask.garnetClient.Connect();
        }

        /// <summary>
        /// Execute async command
        /// </summary>
        /// <param name="commands"></param>
        /// <returns></returns>
        public Task<string> ExecuteAsync(params string[] commands)
        {
            if (flushTask != null) WaitForFlush().GetAwaiter().GetResult();
            return aofSyncTask.garnetClient.ExecuteAsync(commands);
        }

        /// <summary>
        /// Set Cluster Sync header
        /// </summary>
        /// <param name="isMainStore"></param>
        public void SetClysterSyncHeader(bool isMainStore)
        {
            if (flushTask != null) WaitForFlush().GetAwaiter().GetResult();
            if (aofSyncTask.garnetClient.NeedsInitialization)
                aofSyncTask.garnetClient.SetClusterSyncHeader(clusterProvider.clusterManager.CurrentConfig.LocalNodeId, isMainStore: isMainStore);
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
            if (flushTask != null) WaitForFlush().GetAwaiter().GetResult();
            return aofSyncTask.garnetClient.TryWriteKeyValueSpanByte(ref key, ref value, out task);
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
            if (flushTask != null) WaitForFlush().GetAwaiter().GetResult();
            return aofSyncTask.garnetClient.TryWriteKeyValueByteArray(key, value, expiration, out task);
        }

        /// <summary>
        /// Send and reset iteration buffer
        /// </summary>
        /// <returns></returns>
        public void SendAndResetIterationBuffer(TimeSpan timeout, CancellationToken token)
        {
            if (flushTask != null) WaitForFlush().GetAwaiter().GetResult();
            SetFlushTask(aofSyncTask.garnetClient.SendAndResetIterationBuffer(), timeout: timeout, token: token);
        }
        #endregion

        /// <summary>
        /// Associated aof sync task instance with this replica sync session
        /// </summary>
        /// <param name="aofSyncTask"></param>
        public void AddAofSyncTask(AofSyncTaskInfo aofSyncTask) => this.aofSyncTask = aofSyncTask;

        /// <summary>
        /// Get the associated aof sync task instance with this replica sync session
        /// </summary>
        public AofSyncTaskInfo GetAofSyncTask => aofSyncTask;

        /// <summary>
        /// Wait until sync of checkpoint is completed
        /// </summary>
        /// <returns></returns>
        public async Task CompletePending()
        {
            while (ssInfo.syncStatus == SyncStatus.INPROGRESS)
                await Task.Yield();
        }

        /// <summary>
        /// Should stream
        /// </summary>
        /// <returns></returns>
        public bool ShouldStreamCheckpoint()
        {
            // TODO: implement disk-based logic if possible
            return clusterProvider.serverOptions.ReplicaDisklessSync ?
                ShouldStreamDisklessCheckpoint() : true;

            bool ShouldStreamDisklessCheckpoint()
            {
                var localPrimaryReplId = clusterProvider.replicationManager.PrimaryReplId;
                var sameHistory = localPrimaryReplId.Equals(replicaSyncMetadata.currentPrimaryReplId, StringComparison.Ordinal);
                sendMainStore = !sameHistory || replicaSyncMetadata.currentStoreVersion != currentStoreVersion;
                sendObjectStore = !sameHistory || replicaSyncMetadata.currentObjectStoreVersion != currentObjectStoreVersion;
                truncatedAof = replicaSyncMetadata.currentAofTailAddress < aofSyncTask.StartAddress;

                // We need to stream checkpoint if any of the following conditions are met:
                // 1. Replica has different history than primary
                // 2. Replica has different main store version than primary
                // 3. Replica has different object store version than primary
                // 4. Replica has truncated AOF
                fullSync = sendMainStore || sendObjectStore || truncatedAof;
                return fullSync;
            }
        }

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
        /// <param name="timeout"></param>
        /// <param name="token"></param>
        public void SetFlushTask(Task<string> task, TimeSpan timeout, CancellationToken token)
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

        /// <summary>
        /// Wait for network buffer flush
        /// </summary>
        /// <returns></returns>
        public async Task WaitForFlush()
        {
            try
            {
                _ = await flushTask;
                flushTask = null;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "{method}", $"{nameof(ReplicaSyncSession.WaitForFlush)}");
                SetStatus(SyncStatus.FAILED, "Flush task faulted");
            }
        }

        /// <summary>
        /// Begin syncing AOF to the replica
        /// </summary>
        public async Task BeginAofSync()
        {
            var mmr = clusterProvider.serverOptions.MainMemoryReplication;
            var aofNull = clusterProvider.serverOptions.UseAofNullDevice;

            var canReplayFromAddress = aofSyncTask.StartAddress;
            var replicaAofBeginAddress = replicaSyncMetadata.currentAofBeginAddress;
            var replicaAofTailAddress = replicaSyncMetadata.currentAofTailAddress;

            var currentAofBeginAddress = canReplayFromAddress;
            var currentTailAofAddress = clusterProvider.storeWrapper.appendOnlyFile.TailAddress;

            // TODO:
            // If partial sync we need to calculate the beginAddress and endAddress of replica AOF
            // to match the primary AOF
            if (!fullSync)
            {

            }

            var recoverSyncMetadata = new SyncMetadata(
                fullSync: fullSync,
                originNodeRole: clusterProvider.clusterManager.CurrentConfig.LocalNodeRole,
                originNodeId: clusterProvider.clusterManager.CurrentConfig.LocalNodeId,
                currentPrimaryReplId: clusterProvider.replicationManager.PrimaryReplId,
                currentStoreVersion: currentStoreVersion,
                currentObjectStoreVersion: currentObjectStoreVersion,
                currentAofBeginAddress: currentAofBeginAddress,
                currentAofTailAddress: currentTailAofAddress,
                currentReplicationOffset: clusterProvider.replicationManager.ReplicationOffset,
                checkpointEntry: null);

            var result = await aofSyncTask.garnetClient.ExecuteAttachSync(recoverSyncMetadata.ToByteArray());
            if (!long.TryParse(result, out var syncFromAofAddress))
            {
                logger?.LogError("Failed to parse syncFromAddress at {method}", nameof(BeginAofSync));
                SetStatus(SyncStatus.FAILED, "Failed to parse recovery offset");
                return;
            }

            // We have already added the iterator for the covered address above but replica might request an address
            // that is ahead of the covered address so we should start streaming from that address in order not to
            // introduce duplicate insertions.
            if (!clusterProvider.replicationManager.TryAddReplicationTask(replicaSyncMetadata.originNodeId, syncFromAofAddress, out var aofSyncTaskInfo))
                throw new GarnetException("Failed trying to try update replication task");
            if (!clusterProvider.replicationManager.TryConnectToReplica(replicaSyncMetadata.originNodeId, syncFromAofAddress, aofSyncTaskInfo, out _))
                throw new GarnetException("Failed connecting to replica for aofSync");
        }
    }
}