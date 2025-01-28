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
    internal sealed class ReplicationSyncManager
    {
        SingleWriterMultiReaderLock syncInProgress;
        readonly CancellationTokenSource cts;
        readonly TimeSpan clusterTimeout;
        readonly ILogger logger;

        public ReplicaSyncSessionTaskStore GetSessionStore { get; }

        public ClusterProvider ClusterProvider { get; }

        public ReplicationSyncManager(ClusterProvider clusterProvider, ILogger logger = null)
        {
            GetSessionStore = new ReplicaSyncSessionTaskStore(clusterProvider.storeWrapper, clusterProvider, logger);
            ClusterProvider = clusterProvider;
            this.logger = logger;

            var opts = clusterProvider.serverOptions;
            clusterTimeout = opts.ClusterTimeout <= 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(opts.ClusterTimeout);
            cts = new();
        }

        public void Dispose()
        {
            cts.Cancel();
            cts.Dispose();
            syncInProgress.WriteLock();
        }

        /// <summary>
        /// Begin background replica sync session
        /// </summary>
        /// <param name="replicaSyncMetadata">Replica sync metadata</param>
        /// <param name="replicaSyncSession">Replica sync session created</param>
        /// <returns></returns>
        public bool AddSyncSession(SyncMetadata replicaSyncMetadata, out ReplicaSyncSession replicaSyncSession)
        {
            replicaSyncSession = new ReplicaSyncSession(ClusterProvider.storeWrapper, ClusterProvider, replicaSyncMetadata, logger: logger);
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
        /// Start sync session
        /// </summary>
        /// <param name="replicaSyncSession"></param>
        /// <returns></returns>
        public async Task<SyncStatusInfo> MainDisklessSync(ReplicaSyncSession replicaSyncSession)
        {
            try
            {
                // Give opportunity to other replicas to attach for streaming sync
                if (ClusterProvider.serverOptions.ReplicaDisklessSyncDelay > 0)
                    Thread.Sleep(TimeSpan.FromSeconds(ClusterProvider.serverOptions.ReplicaDisklessSyncDelay));

                // Started syncing
                replicaSyncSession.SetStatus(SyncStatus.INPROGRESS);

                // Only one thread will acquire this lock
                if (syncInProgress.OneWriteLock())
                {
                    // Launch a background task to sync the attached replicas using streaming snapshot
                    _ = Task.Run(() => StreamingSnapshotSync());
                }

                // Wait for main sync task to complete
                await replicaSyncSession.WaitForSyncCompletion();

                // If session faulted return early
                if (replicaSyncSession.Failed)
                {
                    replicaSyncSession.LogError();
                    replicaSyncSession.Dispose();
                    return replicaSyncSession.GetSyncStatusInfo;
                }

                await replicaSyncSession.BeginAofSync();

                return replicaSyncSession.GetSyncStatusInfo;
            }
            finally
            {
                replicaSyncSession.Dispose();
            }
        }

        // Main streaming snapshot task
        async Task StreamingSnapshotSync()
        {
            // Parameters for sync operation
            var disklessRepl = ClusterProvider.serverOptions.ReplicaDisklessSync;
            var disableObjects = ClusterProvider.serverOptions.DisableObjects;

            // Replica sync session
            var numSessions = GetSessionStore.GetNumSessions();
            var sessions = GetSessionStore.GetSessions();

            try
            {
                // Take lock to ensure no other task will be taking a checkpoint
                while (!ClusterProvider.storeWrapper.TryPauseCheckpoints())
                    await Task.Yield();

                // Get sync metadata for checkpoint
                await PrepareForSync();

                // Stream checkpoint to replicas
                await StreamDisklessCheckpoint();
            }
            finally
            {
                // Notify sync session of success success
                for (var i = 0; i < numSessions; i++)
                    sessions[i]?.SetStatus(SyncStatus.SUCCESS);

                // Clear array of sync sessions
                GetSessionStore.Clear();

                // Release checkpoint lock
                ClusterProvider.storeWrapper.ResumeCheckpoints();

                // Unlock sync session lock
                syncInProgress.WriteUnlock();
            }

            // Acquire checkpoint and lock AOF if possible
            async Task PrepareForSync()
            {
                if (disklessRepl)
                {
                    #region pauseAofTruncation
                    while (true)
                    {
                        // Calculate minimum address from which replicas should start streaming from
                        var syncFromAddress = ClusterProvider.storeWrapper.appendOnlyFile.TailAddress;

                        // Lock AOF address for sync streaming
                        if (ClusterProvider.replicationManager.TryAddReplicationTasks(GetSessionStore.GetSessions(), syncFromAddress))
                            break;

                        // Retry if failed to lock AOF address because truncation occurred
                        await Task.Yield();
                    }
                    #endregion

                    #region initializeConnection
                    for (var i = 0; i < numSessions; i++)
                    {
                        try
                        {
                            // Initialize connections
                            sessions[i].Connect();

                            // Set store version to operate on
                            sessions[i].currentStoreVersion = ClusterProvider.storeWrapper.store.CurrentVersion;
                            sessions[i].currentObjectStoreVersion = disableObjects ? -1 : ClusterProvider.storeWrapper.objectStore.CurrentVersion;

                            // If checkpoint is not needed mark this sync session as complete
                            // to avoid waiting for other replicas which may need to receive the latest checkpoint
                            if (!sessions[i].NeedToFullSync())
                            {
                                sessions[i]?.SetStatus(SyncStatus.SUCCESS, "Partial sync");
                                sessions[i] = null;
                            }
                            else
                            {
                                // Reset replica database in preparation for full sync
                                sessions[i].SetFlushTask(sessions[i].ExecuteAsync(["FLUSHALL"]), timeout: clusterTimeout, cts.Token);
                                await sessions[i].WaitForFlush();
                                if (sessions[i].Failed) sessions[i] = null;
                            }
                        }
                        catch (Exception ex)
                        {
                            sessions[i]?.SetStatus(SyncStatus.FAILED, ex.Message);
                            sessions[i] = null;
                        }

                    }
                    #endregion
                }
            }
        }

        async Task StreamDisklessCheckpoint()
        {
            // Main snapshot iterator manager
            var manager = new SnapshotIteratorManager(this, clusterTimeout, cts.Token, logger);

            // Iterate through main store
            var mainStoreResult = await ClusterProvider.storeWrapper.store.
                TakeFullCheckpointAsync(CheckpointType.StreamingSnapshot, streamingSnapshotIteratorFunctions: manager.mainStoreSnapshotIterator);

            if (!ClusterProvider.serverOptions.DisableObjects)
            {
                // Iterate through object store
                var objectStoreResult = await ClusterProvider.storeWrapper.objectStore.TakeFullCheckpointAsync(CheckpointType.StreamingSnapshot, streamingSnapshotIteratorFunctions: manager.objectStoreSnapshotIterator);
            }
        }
    }
}