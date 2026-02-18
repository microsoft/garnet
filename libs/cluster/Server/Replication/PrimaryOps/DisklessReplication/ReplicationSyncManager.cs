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
        CancellationTokenSource cts;
        readonly TimeSpan replicaSyncTimeout;
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
            cts = new();
        }

        public void Dispose()
        {
            // Return if original value is true, hence already disposed
            disposed.WriteLock();
            cts?.Cancel();
            cts?.Dispose();
            cts = null;
            syncInProgress.WriteLock();
        }

        /// <summary>
        /// Check if sync session is active
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public bool IsActive(int offset)
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
        /// Wait for network flush to complete for all sessions
        /// </summary>
        /// <returns></returns>
        public async Task WaitForFlush()
        {
            for (var i = 0; i < NumSessions; i++)
            {
                if (!IsActive(i)) continue;
                // Wait for network flush
                await Sessions[i].WaitForFlush();
                if (Sessions[i].Failed) Sessions[i] = null;
            }
        }

        /// <summary>
        /// Create and add ReplicaSyncSession to store
        /// </summary>
        /// <param name="replicaSyncMetadata">Replica sync metadata</param>
        /// <param name="replicaSyncSession">Replica sync session created</param>
        /// <returns></returns>
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
        /// Start sync session
        /// </summary>
        /// <param name="replicaSyncSession"></param>
        /// <returns></returns>
        public async Task<SyncStatusInfo> ReplicationSyncDriver(ReplicaSyncSession replicaSyncSession)
        {
            try
            {
                var isLeader = GetSessionStore.IsFirst(replicaSyncSession);
                // Give opportunity to other replicas to attach for streaming sync
                // Only leader waits because it is the one that initiates the sync driver, so everybody else will wait for it to complete.
                if (ClusterProvider.serverOptions.ReplicaDisklessSyncDelay > 0 && isLeader)
                    Thread.Sleep(TimeSpan.FromSeconds(ClusterProvider.serverOptions.ReplicaDisklessSyncDelay));

                // Signal syncing in-progress
                replicaSyncSession.SetStatus(SyncStatus.INPROGRESS);

                // Only one thread should be the leader who initiates the sync driver.
                // This will be the task added first in the replica sync session array.
                if (isLeader)
                {
                    using SemaphoreSlim signalCompletion = new(0);
                    // Launch a background task to sync the attached replicas using streaming snapshot
                    _ = Task.Run(() => MainStreamingSnapshotDriver(signalCompletion));
                    await signalCompletion.WaitAsync();
                }

                // Wait for main sync driver to complete
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

        /// <summary>
        /// Coordinates the main streaming snapshot synchronization process across replica sessions.
        /// </summary>
        /// <param name="signalCompletion">A semaphore used to signal completion of the main synchronization task. The method releases this semaphore
        /// when the synchronization process finishes.</param>
        /// <returns>A task that represents the asynchronous operation of the streaming snapshot synchronization driver.</returns>
        /// <exception cref="GarnetException">Thrown if the streaming checkpoint operation fails during synchronization.</exception>
        async Task MainStreamingSnapshotDriver(SemaphoreSlim signalCompletion)
        {
            // Parameters for sync operation
            try
            {
                // Lock to avoid the addition of new replica sync sessions while sync is in progress
                syncInProgress.WriteLock();

                // Get sync session info
                NumSessions = GetSessionStore.GetNumSessions();
                Sessions = GetSessionStore.GetSessions();

                // Wait for all replicas to reach InProgress state
                for (var i = 0; i < NumSessions; i++)
                {
                    while (!Sessions[i].InProgress)
                        await Task.Yield();
                }

                // Take lock to ensure no other task will be taking a checkpoint
                while (!ClusterProvider.storeWrapper.TryPauseCheckpoints())
                    await Task.Yield();

                // Choose to perform a full sync or not
                var fullSync = await PrepareForSync();

                // If at least one replica requires a full sync, take a streaming checkpoint
                // NOTE:
                //      This is operation does not block all waiting tasks, only those that require full sync.
                //      It is possible that some replicas may not require a full sync and can continue with partial sync.
                //      See #chooseBetweenFullAndPartialSync
                if (fullSync)
                    await TakeStreamingCheckpoint();

                // Notify sync session of success success
                for (var i = 0; i < NumSessions; i++)
                    Sessions[i]?.SetStatus(SyncStatus.SUCCESS);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "{method} faulted", nameof(MainStreamingSnapshotDriver));
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

                // Release to indicate completion of the main sync task
                signalCompletion.Release();
            }

            // Acquire checkpoint and lock AOF if possible
            async Task<bool> PrepareForSync()
            {
                // Try to lock AOF address to avoid truncation
                #region pauseAofTruncation
                while (true)
                {
                    // Minimum address that we can serve assuming aof-locking and no aof-null-device
                    var minServiceableAofAddress = ClusterProvider.storeWrapper.appendOnlyFile.Log.BeginAddress;

                    // Lock AOF address for sync streaming
                    // If clusterProvider.allowDataLoss is set the addition never fails,
                    // otherwise failure occurs if AOF has been truncated beyond minServiceableAofAddress
                    if (ClusterProvider.replicationManager.AofSyncDriverStore.TryAddReplicationDrivers(GetSessionStore.GetSessions(), ref minServiceableAofAddress))
                        break;

                    // Retry if failed to lock AOF address because truncation occurred
                    await Task.Yield();
                }
                #endregion

                #region chooseBetweenFullAndPartialSync
                var fullSync = false;
                for (var i = 0; i < NumSessions; i++)
                {
                    try
                    {
                        // Initialize connections
                        Sessions[i].Connect();

                        // Set store version to operate on
                        Sessions[i].currentStoreVersion = ClusterProvider.storeWrapper.store.CurrentVersion;

                        // If checkpoint is not needed mark this sync session as complete
                        // to avoid waiting for other replicas which may need to receive the latest checkpoint
                        if (!Sessions[i].NeedToFullSync())
                        {
                            Sessions[i]?.SetStatus(SyncStatus.SUCCESS);
                            Sessions[i] = null;
                        }
                        else
                        {
                            // Reset replica database in preparation for full sync
                            Sessions[i].SetFlushTask(Sessions[i].IssueFlushAllAsync());
                            fullSync = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        Sessions[i]?.SetStatus(SyncStatus.FAILED, ex.Message);
                        Sessions[i] = null;
                    }
                }

                // Flush network buffers for all active sessions
                await WaitForFlush();
                #endregion

                return fullSync;
            }

            // Stream Diskless
            async Task TakeStreamingCheckpoint()
            {
                // Store snapshot iterator manager
                var manager = new SnapshotIteratorManager(this, cts.Token, logger);

                // Iterate through store
                var mainStoreCheckpointTask = ClusterProvider.storeWrapper.store.
                    TakeFullCheckpointAsync(CheckpointType.StreamingSnapshot, cancellationToken: cts.Token, streamingSnapshotIteratorFunctions: manager.StoreSnapshotIterator);

                var (success, _) = await WaitOrDie(checkpointTask: mainStoreCheckpointTask, iteratorManager: manager);
                if (!success)
                    throw new GarnetException("Main store checkpoint stream failed!");

                // Note: We do not truncate the AOF here as this was just a "virtual" checkpoint
                // WaitOrDie is needed here to check if streaming checkpoint is making progress.
                // We cannot use a timeout on the cancellationToken because we don't know in total how long the streaming checkpoint will take
                async ValueTask<(bool success, Guid token)> WaitOrDie(ValueTask<(bool success, Guid token)> checkpointTask, SnapshotIteratorManager iteratorManager)
                {
                    try
                    {
                        var timeout = replicaSyncTimeout;
                        var delay = TimeSpan.FromSeconds(1);
                        while (true)
                        {
                            // Check if cancellation requested
                            cts.Token.ThrowIfCancellationRequested();

                            // Wait for stream sync to make some progress
                            await Task.Delay(delay);

                            // Trigger exception to test reset cts mechanism
                            ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Replication_Diskless_Sync_Reset_Cts);

                            // Check if checkpoint has completed
                            if (checkpointTask.IsCompleted)
                                return await checkpointTask;

                            // Check if we made some progress
                            timeout = !manager.IsProgressing() ? timeout.Subtract(delay) : replicaSyncTimeout;

                            // Throw timeout equals to zero
                            if (timeout.TotalSeconds <= 0)
                                throw new TimeoutException("Streaming snapshot checkpoint timed out");
                        }
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "{method} faulted", nameof(WaitOrDie));
                        for (var i = 0; i < NumSessions; i++)
                            Sessions[i]?.SetStatus(SyncStatus.FAILED, ex.Message);
                        cts.Cancel();
                    }

                    // At this point we failed through a timeout or any other exception
                    // so try to reset token.
                    // No race here because the only other cancellation will happen at dispose only
                    try
                    {
                        _ = await checkpointTask;
                    }
                    finally
                    {
                        var readLock = disposed.TryReadLock();
                        if (readLock && !cts.TryReset())
                        {
                            cts.Dispose();
                            cts = new();
                        }

                        if (readLock)
                            disposed.ReadUnlock();
                    }

                    return (false, default);
                }
            }
        }
    }
}