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

        public int NumSessions { get; private set; }

        public ReplicaSyncSession[] Sessions { get; private set; }

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
            replicaSyncSession = new ReplicaSyncSession(ClusterProvider.storeWrapper, ClusterProvider, replicaSyncMetadata, clusterTimeout, cts.Token, logger: logger);
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

        public async Task WaitForFlush()
        {
            for (var i = 0; i < NumSessions; i++)
            {
                // Wait for network flush
                await Sessions[i].WaitForFlush();
                if (Sessions[i].Failed) Sessions[i] = null;
            }
        }

        public bool IsActiveSyncSession(int offset)
        {
            // Check if session is null if an error occurred earlier and session was broken
            if (Sessions[offset] == null)
                return false;

            // Check if connection is still healthy
            if (!Sessions[offset].IsConnected)
            {
                Sessions[offset].SetStatus(SyncStatus.FAILED, "Connection broken");
                Sessions[offset] = null;
                return false;
            }
            return true;
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
                var isLeader = GetSessionStore.IsFirst(replicaSyncSession);
                if (isLeader)
                {
                    // Launch a background task to sync the attached replicas using streaming snapshot
                    _ = Task.Run(() => StreamingSnapshotSync());
                }

                // Wait for main sync task to complete
                await replicaSyncSession.WaitForSyncCompletion();

                // If session faulted return early
                if (replicaSyncSession.Failed)
                {
                    var status = replicaSyncSession.GetSyncStatusInfo;
                    var msg = $"{status.syncStatus}:{status.error}";
                    logger?.LogSyncMetadata(LogLevel.Error, msg, replicaSyncSession.replicaSyncMetadata);
                    return status;
                }

                // Start AOF sync background task for this replica
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

            try
            {
                // Lock to avoid the addition of new replica sync sessions while sync is in progress
                syncInProgress.WriteLock();

                // Get sync session info
                NumSessions = GetSessionStore.GetNumSessions();
                Sessions = GetSessionStore.GetSessions();

                // Wait for all replicas to reach initializing state
                for (var i = 0; i < NumSessions; i++)
                {
                    while (!Sessions[i].InProgress)
                        await Task.Yield();
                }

                // Take lock to ensure no other task will be taking a checkpoint
                while (!ClusterProvider.storeWrapper.TryPauseCheckpoints())
                    await Task.Yield();

                // Get sync metadata for checkpoint
                await PrepareForSync();

                // Stream checkpoint to replicas
                await TakeStreamingCheckpoint();

                // Notify sync session of success success
                for (var i = 0; i < NumSessions; i++)
                    Sessions[i]?.SetStatus(SyncStatus.SUCCESS);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "{method} faulted", nameof(StreamingSnapshotSync));
                for (var i = 0; i < NumSessions; i++)
                    Sessions[i]?.SetStatus(SyncStatus.FAILED, ex.Message);
            }
            finally
            {
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
                        // Minimum address that we can serve assuming aof-locking and no aof-null-device
                        var minServiceableAofAddress = ClusterProvider.storeWrapper.appendOnlyFile.BeginAddress;

                        // Lock AOF address for sync streaming
                        if (ClusterProvider.replicationManager.TryAddReplicationTasks(GetSessionStore.GetSessions(), minServiceableAofAddress))
                            break;

                        // Retry if failed to lock AOF address because truncation occurred
                        await Task.Yield();
                    }
                    #endregion

                    #region initializeConnection
                    for (var i = 0; i < NumSessions; i++)
                    {
                        try
                        {
                            // Initialize connections
                            Sessions[i].Connect();

                            // Set store version to operate on
                            Sessions[i].currentStoreVersion = ClusterProvider.storeWrapper.store.CurrentVersion;
                            Sessions[i].currentObjectStoreVersion = disableObjects ? -1 : ClusterProvider.storeWrapper.objectStore.CurrentVersion;

                            // If checkpoint is not needed mark this sync session as complete
                            // to avoid waiting for other replicas which may need to receive the latest checkpoint
                            if (!Sessions[i].NeedToFullSync())
                            {
                                Sessions[i]?.SetStatus(SyncStatus.SUCCESS, "Partial sync");
                                Sessions[i] = null;
                            }
                            else
                            {
                                // Reset replica database in preparation for full sync
                                Sessions[i].SetFlushTask(Sessions[i].ExecuteAsync(["FLUSHALL"]));
                            }
                        }
                        catch (Exception ex)
                        {
                            Sessions[i]?.SetStatus(SyncStatus.FAILED, ex.Message);
                            Sessions[i] = null;
                        }
                    }

                    await WaitForFlush();
                    #endregion
                }
            }

            // Stream Diskless
            async Task TakeStreamingCheckpoint()
            {
                // Main snapshot iterator manager
                var manager = new SnapshotIteratorManager(this, cts.Token, logger);

                // Iterate through main store
                var mainStoreResult = await ClusterProvider.storeWrapper.store.
                    TakeFullCheckpointAsync(CheckpointType.StreamingSnapshot, streamingSnapshotIteratorFunctions: manager.mainStoreSnapshotIterator).
                    AsTask().WaitAsync(clusterTimeout, cts.Token);

                if (!ClusterProvider.serverOptions.DisableObjects)
                {
                    // Iterate through object store
                    var objectStoreResult = await ClusterProvider.storeWrapper.objectStore.
                        TakeFullCheckpointAsync(CheckpointType.StreamingSnapshot, streamingSnapshotIteratorFunctions: manager.objectStoreSnapshotIterator).
                        AsTask().WaitAsync(clusterTimeout, cts.Token);
                }

                ClusterProvider.replicationManager.SafeTruncateAof(manager.CheckpointCoveredAddress);
            }
        }
    }
}