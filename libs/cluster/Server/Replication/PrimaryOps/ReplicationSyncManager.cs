// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    /// <summary>
    /// Unified coordinator for replica synchronization.
    /// Supports both diskless (streaming snapshot) and disk-based (file broadcast) modes.
    /// </summary>
    internal sealed partial class ReplicationSyncManager
    {
        SingleWriterMultiReaderLock syncInProgress;
        CancellationTokenSource cts;
        readonly TimeSpan replicaSyncTimeout;
        readonly bool isDisklessSync;
        readonly ILogger logger;

        public ReplicaSyncSessionTaskStore GetSessionStore { get; }

        public int NumSessions { get; private set; }

        public ReplicaSyncSession[] Sessions { get; private set; }

        public ClusterProvider ClusterProvider { get; }

        SingleWriterMultiReaderLock disposed;

        public ReplicationSyncManager(ClusterProvider clusterProvider, ILogger logger = null)
        {
            GetSessionStore = new ReplicaSyncSessionTaskStore(clusterProvider.storeWrapper, clusterProvider, logger);
            ClusterProvider = clusterProvider;
            this.logger = logger;

            var opts = clusterProvider.serverOptions;
            replicaSyncTimeout = opts.ReplicaSyncTimeout;
            isDisklessSync = opts.ReplicaDisklessSync;
            cts = new();
        }

        public void Dispose()
        {
            cts?.Cancel();
            syncInProgress.WriteLock();
            disposed.WriteLock();
            cts?.Dispose();
            cts = null;
        }

        /// <summary>
        /// Check if sync session is active
        /// </summary>
        public bool IsActive(int offset)
        {
            if (Sessions[offset] == null)
                return false;

            if (!Sessions[offset].IsConnected)
            {
                Sessions[offset].SetStatus(SyncStatus.FAILED, "Connection broken");
                Sessions[offset] = null;
                return false;
            }
            return true;
        }

        /// <summary>
        /// Wait for network flush to complete for all sessions
        /// </summary>
        public async Task WaitForFlushAsync()
        {
            for (var i = 0; i < NumSessions; i++)
            {
                if (!IsActive(i)) continue;
                await Sessions[i].WaitForFlushAsync().ConfigureAwait(false);
                if (Sessions[i].Failed) Sessions[i] = null;
            }
        }

        /// <summary>
        /// Create and add ReplicaSyncSession to store (diskless path)
        /// </summary>
        public bool AddReplicaSyncSession(SyncMetadata replicaSyncMetadata, out ReplicaSyncSession replicaSyncSession)
        {
            replicaSyncSession = new ReplicaSyncSession(
                ClusterProvider.storeWrapper,
                ClusterProvider,
                replicaAofBeginAddress: default,
                replicaAofTailAddress: default,
                replicaSyncMetadata,
                cts.Token,
                logger: logger);
            replicaSyncSession.SetStatus(SyncStatus.INITIALIZING);
            try
            {
                syncInProgress.ReadLock();
                return GetSessionStore.TryAddReplicaSyncSession(replicaSyncSession);
            }
            finally
            {
                syncInProgress.ReadUnlock();
            }
        }

        /// <summary>
        /// Create and add ReplicaSyncSession to store (disk-based path)
        /// </summary>
        public bool AddDiskbasedReplicaSyncSession(
            string replicaNodeId,
            string replicaAssignedPrimaryId,
            CheckpointEntry replicaCheckpointEntry,
            AofAddress replicaAofBeginAddress,
            AofAddress replicaAofTailAddress,
            out ReplicaSyncSession replicaSyncSession)
        {
            replicaSyncSession = new ReplicaSyncSession(
                ClusterProvider.storeWrapper,
                ClusterProvider,
                replicaAofBeginAddress,
                replicaAofTailAddress,
                replicaSyncMetadata: null,
                cts.Token,
                replicaNodeId,
                replicaAssignedPrimaryId,
                replicaCheckpointEntry,
                logger: logger);
            replicaSyncSession.SetStatus(SyncStatus.INITIALIZING);
            try
            {
                syncInProgress.ReadLock();
                return GetSessionStore.TryAddReplicaSyncSession(replicaSyncSession);
            }
            finally
            {
                syncInProgress.ReadUnlock();
            }
        }

        /// <summary>
        /// Start sync session (called per-replica; leader spawns the main driver)
        /// </summary>
        public async Task<SyncStatusInfo> ReplicationSyncDriverAsync(ReplicaSyncSession replicaSyncSession)
        {
            try
            {
                var isLeader = GetSessionStore.IsFirst(replicaSyncSession);
                // Give opportunity to other replicas to attach
                if (ClusterProvider.serverOptions.ReplicaDisklessSyncDelay > 0 && isLeader)
                    Thread.Sleep(TimeSpan.FromSeconds(ClusterProvider.serverOptions.ReplicaDisklessSyncDelay));

                // Signal syncing in-progress
                replicaSyncSession.SetStatus(SyncStatus.INPROGRESS);

                // Only the leader spawns the driver
                if (isLeader)
                {
                    using SemaphoreSlim signalCompletion = new(0);
                    _ = Task.Run(() => SnapshotDispatchDriverAsync(signalCompletion));
                    await signalCompletion.WaitAsync().ConfigureAwait(false);
                }

                // Wait for main sync driver to complete
                await replicaSyncSession.WaitForSyncCompletionAsync().ConfigureAwait(false);

                if (replicaSyncSession.Failed)
                {
                    var status = replicaSyncSession.GetSyncStatusInfo;
                    var msg = $"{status.syncStatus}:{status.error}";
                    if (replicaSyncSession.replicaSyncMetadata != null)
                        logger?.LogSyncMetadata(LogLevel.Error, msg, replicaSyncSession.replicaSyncMetadata);
                    else
                        logger?.LogError("{msg} for replica {nodeId}", msg, replicaSyncSession.RemoteNodeId);
                    return status;
                }

                // For diskless mode, start AOF sync per-replica after checkpoint streaming completes.
                // For disk-based mode, AOF sync is handled inside FinalizeReplicaRecoverAsync.
                if (isDisklessSync)
                    await replicaSyncSession.BeginAofSyncAsync().ConfigureAwait(false);

                return replicaSyncSession.GetSyncStatusInfo;
            }
            finally
            {
                replicaSyncSession.Dispose();
            }
        }

        /// <summary>
        /// Coordinates the main synchronization process across replica sessions.
        /// Branches based on sync mode: diskless (streaming snapshot) or disk-based (file broadcast).
        /// </summary>
        async Task SnapshotDispatchDriverAsync(SemaphoreSlim signalCompletion)
        {
            var syncInProgressAcquired = false;
            try
            {
                syncInProgressAcquired = syncInProgress.TryWriteLock();
                if (!syncInProgressAcquired)
                    throw new GarnetException("Failed to acquire write syncInProgress lock!");

                NumSessions = GetSessionStore.GetNumSessions();
                Sessions = GetSessionStore.GetSessions();

                // Wait for all replicas to reach InProgress state
                for (var i = 0; i < NumSessions; i++)
                {
                    while (!Sessions[i].InProgress)
                        await Task.Yield();
                }

                // Dispatch to mode-specific flow
                if (isDisklessSync)
                    await DisklessSyncFlowAsync().ConfigureAwait(false);
                else
                    await DiskbasedSyncFlowAsync().ConfigureAwait(false);

                // Notify sync sessions of success
                for (var i = 0; i < NumSessions; i++)
                    Sessions[i]?.SetStatus(SyncStatus.SUCCESS);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "{method} faulted", nameof(SnapshotDispatchDriverAsync));
                for (var i = 0; i < NumSessions; i++)
                    Sessions[i]?.SetStatus(SyncStatus.FAILED, ex.Message);
            }
            finally
            {
                GetSessionStore.Clear();

                if (syncInProgressAcquired)
                    syncInProgress.WriteUnlock();

                signalCompletion.Release();
            }
        }
    }
}