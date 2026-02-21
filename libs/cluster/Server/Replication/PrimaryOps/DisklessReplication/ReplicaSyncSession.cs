// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class ReplicaSyncSession
    {
        SyncStatusInfo ssInfo;
        Task<bool> flushTask;
        bool fullSync = false;

        /// <summary>
        /// Get the associated aof sync task instance with this replica sync session
        /// </summary>
        public AofSyncDriver AofSyncDriver { get; private set; } = null;

        public bool IsConnected => AofSyncDriver != null && AofSyncDriver.IsConnected;

        public bool Failed => ssInfo.syncStatus == SyncStatus.FAILED;

        public bool InProgress => ssInfo.syncStatus == SyncStatus.INPROGRESS;

        public SyncStatusInfo GetSyncStatusInfo => ssInfo;

        public long currentStoreVersion;

        /// <summary>
        /// Pessimistic checkpoint covered AOF address
        /// </summary>
        public AofAddress checkpointCoveredAofAddress;

        #region NetworkMethods
        /// <summary>
        /// Connect client
        /// </summary>
        public void Connect()
            => AofSyncDriver.ConnectClients();

        /// <summary>
        /// Issue FlushAll
        /// </summary>
        /// <returns></returns>
        public Task<string> IssueFlushAllAsync()
        {
            WaitForFlush().GetAwaiter().GetResult();
            return AofSyncDriver.IssuesFlushAll();
        }

        /// <summary>
        /// Initialize iteration buffer
        /// </summary>
        public void InitializeIterationBuffer()
        {
            WaitForFlush().GetAwaiter().GetResult();
            AofSyncDriver.InitializeIterationBuffer();
        }

        /// <summary>
        /// Set Cluster Sync header
        /// </summary>
        public void SetClusterSyncHeader()
        {
            WaitForFlush().GetAwaiter().GetResult();
            AofSyncDriver.InitializeIfNeeded();
        }

        /// <summary>
        /// Try to write the span of an entire record.
        /// </summary>
        /// <returns></returns>
        public bool TryWriteRecordSpan(ReadOnlySpan<byte> recordSpan, out Task<string> task)
        {
            WaitForFlush().GetAwaiter().GetResult();
            return AofSyncDriver.TryWriteRecordSpan(recordSpan, out task);
        }

        /// <summary>
        /// Send and reset iteration buffer
        /// </summary>
        /// <returns></returns>
        public void SendAndResetIterationBuffer()
        {
            WaitForFlush().GetAwaiter().GetResult();
            SetFlushTask(AofSyncDriver.SendAndResetIterationBuffer());
        }
        #endregion

        /// <summary>
        /// Associated aof sync task instance with this replica sync session
        /// </summary>
        /// <param name="aofSyncDriver"></param>
        public void AddAofSyncTask(AofSyncDriver aofSyncDriver) => AofSyncDriver = aofSyncDriver;

        /// <summary>
        /// Set status of replica sync session
        /// </summary>
        /// <param name="status"></param>
        /// <param name="error"></param>
        public void SetStatus(SyncStatus status, string error = null)
        {
            ssInfo.error ??= error;
            // NOTE: set this after error to signal complete state change
            ssInfo.syncStatus = status;

            // Signal Release for WaitForSyncCompletion call
            switch (status)
            {
                case SyncStatus.SUCCESS:
                    _ = signalCompletion.Release();
                    break;
                case SyncStatus.FAILED:
                    _ = clusterProvider.replicationManager.AofSyncDriverStore.TryRemove(AofSyncDriver);
                    _ = signalCompletion.Release();
                    break;
            }
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
                }, TaskContinuationOptions.OnlyOnRanToCompletion).WaitAsync(storeWrapper.serverOptions.ReplicaSyncTimeout, token);
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
                await signalCompletion.WaitAsync(token);
                Debug.Assert(ssInfo.syncStatus is SyncStatus.SUCCESS or SyncStatus.FAILED);
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
            var localPrimaryReplId = clusterProvider.replicationManager.PrimaryReplId;
            var sameHistory = localPrimaryReplId.Equals(replicaSyncMetadata.currentPrimaryReplId, StringComparison.Ordinal);
            var sendMainStore = !sameHistory || replicaSyncMetadata.currentStoreVersion != currentStoreVersion;

            var aofBeginAddress = clusterProvider.storeWrapper.appendOnlyFile.Log.BeginAddress;
            var aofTailAddress = clusterProvider.storeWrapper.appendOnlyFile.Log.TailAddress;
            var outOfRangeAof = replicaSyncMetadata.currentAofTailAddress.IsOutOfRange(aofBeginAddress, aofTailAddress);

            var aofTooLarge = aofTailAddress.AggregateDiff(replicaSyncMetadata.currentAofTailAddress) > clusterProvider.serverOptions.ReplicaDisklessSyncFullSyncAofThresholdValue();

            // We need to stream checkpoint if any of the following conditions are met:
            // 1. Replica has different history than primary
            // 2. Replica has different store version than primary
            // 3. Replica has truncated AOF
            // 4. The AOF to be replayed in case of a partial sync is larger than the specified threshold
            fullSync = sendMainStore || outOfRangeAof || aofTooLarge;
            return fullSync;
        }

        /// <summary>
        /// Begin syncing AOF to the replica
        /// </summary>
        public async Task BeginAofSync()
        {
            var aofSyncDriver = AofSyncDriver;
            try
            {
                var currentAofBeginAddress = fullSync ? checkpointCoveredAofAddress : aofSyncDriver.StartAddress;
                var currentAofTailAddress = clusterProvider.storeWrapper.appendOnlyFile.Log.TailAddress;

                var recoverSyncMetadata = new SyncMetadata(
                    fullSync: fullSync,
                    originNodeRole: clusterProvider.clusterManager.CurrentConfig.LocalNodeRole,
                    originNodeId: clusterProvider.clusterManager.CurrentConfig.LocalNodeId,
                    currentPrimaryReplId: clusterProvider.replicationManager.PrimaryReplId,
                    currentStoreVersion: currentStoreVersion,
                    currentAofBeginAddress: currentAofBeginAddress,
                    currentAofTailAddress: currentAofTailAddress,
                    currentReplicationOffset: clusterProvider.replicationManager.ReplicationOffset,
                    checkpointEntry: null);

                var result = await aofSyncDriver.ExecuteAttachSync(recoverSyncMetadata);
                var syncFromAddress = AofAddress.FromString(result);

                logger?.LogSyncMetadata(LogLevel.Trace, "BeginAofSync", replicaSyncMetadata, recoverSyncMetadata);

                // Check what happens if we fail after recovery and start AOF stream
                ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Replication_Fail_Before_Background_AOF_Stream_Task_Start);

                // We have already added the iterator for the covered address above but replica might request an address
                // that is ahead of the covered address so we should start streaming from that address in order not to
                // introduce duplicate insertions.
                if (!clusterProvider.replicationManager.AofSyncDriverStore.TryAddReplicationDriver(replicaSyncMetadata.originNodeId, ref syncFromAddress, out aofSyncDriver))
                    throw new GarnetException("Failed trying to try update replication task");
                if (!clusterProvider.replicationManager.TryConnectToReplica(replicaSyncMetadata.originNodeId, ref syncFromAddress, aofSyncDriver, out _))
                    throw new GarnetException("Failed connecting to replica for aofSync");
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "{method}", $"{nameof(ReplicaSyncSession.BeginAofSync)}");
                SetStatus(SyncStatus.FAILED, ex.Message);
            }
        }
    }
}