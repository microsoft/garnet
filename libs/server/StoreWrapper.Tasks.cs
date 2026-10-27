// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    public sealed partial class StoreWrapper
    {
        /// <summary>
        /// Background task manager instance
        /// </summary>
        internal readonly TaskManager taskManager;

        readonly SemaphoreSlim checkpointFrequencyChanged = new(0, 1);

        /// <summary>
        /// Get task manager instance
        /// </summary>
        public TaskManager TaskManager => taskManager;

        async Task AutoCheckpointBasedOnAofSizeLimitAsync(long aofSizeLimit, CancellationToken token = default, ILogger logger = null)
        {
            try
            {
                while (true)
                {
                    await Task.Delay(TimeSpan.FromSeconds(runtimeConfig.GetInt(ServerConfigType.AOF_SIZE_LIMIT_ENFORCE_FREQUENCY)), token).ConfigureAwait(false);
                    if (token.IsCancellationRequested) break;

                    await databaseManager.TaskCheckpointBasedOnAofSizeLimitAsync(aofSizeLimit, token, logger).ConfigureAwait(false);
                }
            }
            catch (TaskCanceledException) when (token.IsCancellationRequested)
            {
                // Suppress the exception if the task was cancelled because of store wrapper disposal
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Exception received at AutoCheckpointTask");
            }
        }

        async Task ScheduledCheckpointTaskAsync(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    if (TryStopDisabledScheduledCheckpointTask())
                        break;

                    if (!await WaitForScheduledCheckpointAsync(token).ConfigureAwait(false))
                        continue;

                    if (token.IsCancellationRequested || runtimeConfig.GetInt(ServerConfigType.CHECKPOINT_FREQ) <= 0 ||
                        (clusterProvider?.IsReplica() ?? false))
                        continue;

                    var status = await TakeCheckpointAsync(false, dbId: -1, token: token, logger: logger).ConfigureAwait(false);
                    if (status != CheckpointStatus.Success)
                    {
                        if (status == CheckpointStatus.AlreadyInProgress)
                            logger?.LogDebug("Scheduled checkpoint skipped for at least one database: a checkpoint is already in progress.");
                        else
                            logger?.LogWarning("Scheduled checkpoint did not complete; will retry on the next interval.");
                    }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "Scheduled checkpoint failed; will retry on the next interval.");
                }
            }
        }

        async Task<bool> WaitForScheduledCheckpointAsync(CancellationToken token)
        {
            var remainingSecs = runtimeConfig.GetInt(ServerConfigType.CHECKPOINT_FREQ);
            if (remainingSecs <= 0)
            {
                await checkpointFrequencyChanged.WaitAsync(token).ConfigureAwait(false);
                return false;
            }

            while (remainingSecs > 0)
            {
                var delaySecs = Math.Min(remainingSecs, 86400);
                if (await checkpointFrequencyChanged.WaitAsync(TimeSpan.FromSeconds(delaySecs), token).ConfigureAwait(false))
                    return false;
                remainingSecs -= delaySecs;
            }

            return true;
        }

        /// <summary>
        /// Make the scheduler observe a CONFIG SET of the checkpoint interval without waiting for a running checkpoint.
        /// </summary>
        /// <param name="intervalChanged">Whether the interval differs from the previous value.</param>
        internal void NotifyCheckpointFrequencyChanged(bool intervalChanged)
        {
            lock (taskLifecycleLock)
            {
                if (CanStartPrimaryTasks && runtimeConfig.GetInt(ServerConfigType.CHECKPOINT_FREQ) > 0 &&
                    !taskManager.IsRegistered(TaskType.ScheduledCheckpointTask))
                    taskManager.RegisterAndRun(TaskType.ScheduledCheckpointTask, ScheduledCheckpointTaskAsync);
            }

            if (!intervalChanged)
                return;

            try
            {
                // A pending notification already wakes the scheduler to read the latest value.
                if (checkpointFrequencyChanged.CurrentCount == 0)
                    checkpointFrequencyChanged.Release();
            }
            catch (SemaphoreFullException)
            {
                // A concurrent CONFIG SET released first.
            }
            catch (ObjectDisposedException)
            {
                // The server is shutting down.
            }
        }

        bool TryStopDisabledScheduledCheckpointTask()
        {
            lock (taskLifecycleLock)
            {
                if (runtimeConfig.GetInt(ServerConfigType.CHECKPOINT_FREQ) > 0)
                    return false;

                taskManager.TryUnregister(TaskType.ScheduledCheckpointTask);
                return true;
            }
        }

        async Task CommitTaskAsync(int commitFrequencyMs, CancellationToken token = default, ILogger logger = null)
        {
            try
            {
                while (true)
                {
                    if (token.IsCancellationRequested) break;

#if DEBUG
                    await ExceptionInjectionHelper.ResetAndWaitAsync(ExceptionInjectionType.Aof_Pause_Before_Periodic_Commit).ConfigureAwait(false);
#endif
                    // Local commits must finish admitted writes before replica synchronization can change AOF addresses.
                    if ((clusterProvider?.IsReplica() ?? false) && primaryTaskDrain.IsCompleted)
                    {
                        await Task.Delay(commitFrequencyMs, token).ConfigureAwait(false);
                    }
                    else
                    {
                        await databaseManager.CommitToAofAsync(token, logger).ConfigureAwait(false);

                        await Task.Delay(commitFrequencyMs, token).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                // Suppress the exception if the task was cancelled because the store wrapper was disposed
                // or because the node transitioned to a replica and primary-only tasks were suspended
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "CommitTask exception received.");
            }
        }

        /// <summary>
        /// Run <paramref name="runCycle"/> every <paramref name="interval"/> until <paramref name="token"/> is
        /// cancelled. A cycle that throws is logged and retried on the next interval, so a transient fault
        /// cannot disable a maintenance task for the lifetime of the process. Cancellation ends the loop
        /// without logging.
        /// </summary>
        /// <param name="runCycle">The work to perform on each cycle.</param>
        /// <param name="interval">Delay between cycles.</param>
        /// <param name="taskDescription">Task name used in the failure log message.</param>
        /// <param name="logger">Logger for failed cycles, may be null.</param>
        /// <param name="token">Cancellation token that ends the loop.</param>
        internal static async Task RunMaintenanceLoopAsync(Func<CancellationToken, ValueTask> runCycle,
            TimeSpan interval, string taskDescription, ILogger logger, CancellationToken token)
        {
            try
            {
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        await runCycle(token).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        // Every cycle failure is classified here rather than in an exception filter, so one that
                        // races shutdown is still handled as a cycle failure: the outer handlers recognize
                        // cancellation by exception type and treat anything else as terminal. Cancellation makes
                        // the failure shutdown noise, so the loop ends without logging it.
                        if (!token.IsCancellationRequested)
                            logger?.LogError(ex, "Unknown exception received for {taskDescription}. Retrying on the next cycle.", taskDescription);
                    }

                    await Task.Delay(interval, token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                // Suppress the exception if the task was cancelled because the store wrapper was disposed
                // or because the node transitioned to a replica and primary-only tasks were suspended
            }
            catch (Exception ex)
            {
                logger?.LogCritical(ex, "Unknown exception received for {taskDescription}. The task won't be resumed.", taskDescription);
            }
        }

        Task CompactionTaskAsync(int compactionFrequencySecs, CancellationToken token = default)
        {
            GarnetAsserts.Assert(compactionFrequencySecs > 0);

            return RunMaintenanceLoopAsync(async (cancellationToken) =>
            {
                await databaseManager.DoCompactionAsync(cancellationToken, logger).ConfigureAwait(false);

                if (!runtimeConfig.GetBool(ServerConfigType.COMPACTION_FORCE_DELETE))
                    logger?.LogInformation("NOTE: Take a checkpoint (SAVE/BGSAVE) in order to actually delete the older data segments (files) from disk");
                else
                    logger?.LogInformation("NOTE: Compaction will delete files, make sure checkpoint/recovery is not being used");
            }, TimeSpan.FromSeconds(compactionFrequencySecs), "background compaction task", logger, token);
        }

        Task ObjectCollectTaskAsync(int objectCollectFrequencySecs, CancellationToken token = default)
        {
            GarnetAsserts.Assert(objectCollectFrequencySecs > 0);

            if (serverOptions.DisableObjects)
            {
                logger?.LogWarning("ExpiredObjectCollectionFrequencySecs option is configured but Object store is disabled. Stopping the background hash collect task.");
                return Task.CompletedTask;
            }

            return RunMaintenanceLoopAsync((_) =>
            {
#if DEBUG
#pragma warning disable VSTHRD103 // The rendezvous models an in-flight synchronous collection cycle.
                ExceptionInjectionHelper.ResetAndWait(ExceptionInjectionType.Failover_Pause_Background_Object_Collection);
#pragma warning restore VSTHRD103
#endif
                databaseManager.ExecuteObjectCollection();
                return ValueTask.CompletedTask;
            }, TimeSpan.FromSeconds(objectCollectFrequencySecs), "background hash collect task", logger, token);
        }

        Task ExpiredKeyDeletionScanTaskAsync(int expiredKeyDeletionScanFrequencySecs, CancellationToken token = default)
        {
            GarnetAsserts.Assert(expiredKeyDeletionScanFrequencySecs > 0);

            return RunMaintenanceLoopAsync((_) =>
            {
                databaseManager.ExpiredKeyDeletionScan();
                return ValueTask.CompletedTask;
            }, TimeSpan.FromSeconds(expiredKeyDeletionScanFrequencySecs), "background expired key deletion scan task", logger, token);
        }

        /// <summary>
        /// Grows indexes of both main store and object store if current size is too small.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        private async Task IndexAutoGrowTaskAsync(CancellationToken token)
        {
            try
            {
                var allIndexesMaxedOut = false;

                while (!allIndexesMaxedOut)
                {
                    if (token.IsCancellationRequested) break;

                    await Task.Delay(TimeSpan.FromSeconds(serverOptions.IndexResizeFrequencySecs), token).ConfigureAwait(false);

                    allIndexesMaxedOut = await databaseManager.GrowIndexesIfNeededAsync(token).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, $"{nameof(IndexAutoGrowTaskAsync)} exception received");
            }
        }

        #region Task management API

        /// <summary>
        /// Drains admitted cluster sessions, then stops primary maintenance with periodic commits last.
        /// Callers must close write admission and release their session epoch before awaiting.
        /// </summary>
        /// <param name="token">Cancellation token for draining admitted sessions; cancellation leaves commits running.</param>
        /// <returns></returns>
        public Task SuspendPrimaryOnlyTasksAsync(CancellationToken token = default)
        {
            lock (taskLifecycleLock)
            {
                if (!primaryTaskDrain.IsCompleted)
                    return primaryTaskDrain;
                return primaryTaskDrain = SuspendAsync();
            }

            async Task SuspendAsync()
            {
                // Publish the suspension before awaiting or entering task lifecycle callbacks.
                await Task.Yield();
                if (clusterProvider != null &&
                    !await clusterProvider.BumpAndWaitForEpochTransitionAsync(token).ConfigureAwait(false))
                    return;
                if (token.IsCancellationRequested)
                    return;
                await taskManager.CancelAsync(TaskPlacementCategory.Primary).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Finishes draining before allowing restarts, and restores tasks only if the node remains primary.
        /// </summary>
        public async Task ResumePrimaryTasksAfterFailoverAsync()
        {
            Task drain;
            lock (taskLifecycleLock)
                drain = primaryTaskDrain;
            await drain.ConfigureAwait(false);
            lock (taskLifecycleLock)
            {
                if (CanStartPrimaryTasks)
                    StartPrimaryTasks();
            }
        }

        /// <summary>
        /// Suspend background task that may interfere with the primary store.
        /// </summary>
        /// <returns></returns>
        public Task SuspendReplicaOnlyTasksAsync()
        {
            return taskManager.CancelAsync(TaskPlacementCategory.Replica);
        }

        /// <summary>
        /// Serializes background primary-task lifecycle changes (start / kill / restart) so concurrent
        /// CONFIG SET calls and failover transitions cannot race on the same task.
        /// </summary>
        readonly object taskLifecycleLock = new();
        volatile Task primaryTaskDrain = Task.CompletedTask;

        bool CanStartPrimaryTasks => !disposed && !(clusterProvider?.IsWritePaused ?? false) && primaryTaskDrain.IsCompleted &&
            !(clusterProvider?.IsReplica() ?? false);

        /// <summary>
        /// Reconcile a background primary task with the current runtime configuration after a CONFIG SET:
        /// stop the running instance (so a killed or retuned task does not keep its old interval) and, when
        /// this node is not a replica, restart it if the new value re-enables it. Runs synchronously on the
        /// CONFIG SET thread under <see cref="taskLifecycleLock"/> so concurrent updates are serialized.
        /// Primary tasks are intentionally not started on a replica; a later failover starts them from the
        /// (now updated) runtime configuration through <see cref="StartPrimaryTasks"/>.
        /// </summary>
        /// <param name="taskType">Primary task to reconcile.</param>
        internal void ReconcilePrimaryTask(TaskType taskType)
        {
            lock (taskLifecycleLock)
            {
                if ((clusterProvider?.IsWritePaused ?? false) || !primaryTaskDrain.IsCompleted)
                    return;

                AsyncUtils.BlockingWait(taskManager.CancelAsync(taskType));

                if (!CanStartPrimaryTasks)
                    return;

                switch (taskType)
                {
                    case TaskType.CommitTask:
                        var commitFrequencyMs = runtimeConfig.GetInt(ServerConfigType.AOF_COMMIT_FREQ);
                        if (commitFrequencyMs > 0 && serverOptions.EnableAOF)
                            taskManager.RegisterAndRun(TaskType.CommitTask, token => CommitTaskAsync(commitFrequencyMs, token, logger));
                        break;
                    case TaskType.ObjectCollectTask:
                        var objectCollectFrequencySecs = runtimeConfig.GetInt(ServerConfigType.EXPIRED_OBJECT_COLLECTION_FREQ);
                        if (objectCollectFrequencySecs > 0)
                            taskManager.RegisterAndRun(TaskType.ObjectCollectTask, token => ObjectCollectTaskAsync(objectCollectFrequencySecs, token));
                        break;
                    case TaskType.ExpiredKeyDeletionTask:
                        var expiredKeyDeletionFrequencySecs = runtimeConfig.GetInt(ServerConfigType.EXPIRED_KEY_DELETION_SCAN_FREQ);
                        if (expiredKeyDeletionFrequencySecs > 0)
                            taskManager.RegisterAndRun(TaskType.ExpiredKeyDeletionTask, token => ExpiredKeyDeletionScanTaskAsync(expiredKeyDeletionFrequencySecs, token));
                        break;
                }
            }
        }

        #endregion

        #region Task registration

        void StartBackgroundTasks()
        {
            if (clusterProvider == null || clusterProvider.IsPrimary())
                StartPrimaryTasks();
            else if (clusterProvider.IsReplica())
                StartReplicaTasks();

            StartGenericNodeTasks();
        }

        /// <summary>
        /// Start background maintenance tasks that should only run when this node is a primary
        /// </summary>
        /// <returns></returns>
        public void StartPrimaryTasks()
        {
            lock (taskLifecycleLock)
            {
                if (!CanStartPrimaryTasks)
                    return;

                if (serverOptions.AofSizeLimit.Length > 0)
                {
                    var aofSizeLimitBytes = 1L << serverOptions.AofSizeLimitSizeBits();
                    taskManager.RegisterAndRun(TaskType.AofSizeLimitTask, (token) => AutoCheckpointBasedOnAofSizeLimitAsync(aofSizeLimitBytes, token, logger));
                }

                var commitFrequencyMs = runtimeConfig.GetInt(ServerConfigType.AOF_COMMIT_FREQ);
                if (commitFrequencyMs > 0 && serverOptions.EnableAOF)
                    taskManager.RegisterAndRun(TaskType.CommitTask, token => CommitTaskAsync(commitFrequencyMs, token, logger));

                if (runtimeConfig.GetInt(ServerConfigType.CHECKPOINT_FREQ) > 0 &&
                    !taskManager.IsRegistered(TaskType.ScheduledCheckpointTask))
                    taskManager.RegisterAndRun(TaskType.ScheduledCheckpointTask, ScheduledCheckpointTaskAsync);

                if (serverOptions.CompactionFrequencySecs > 0 && serverOptions.CompactionType != LogCompactionType.None)
                {
                    taskManager.RegisterAndRun(TaskType.CompactionTask, (token) => CompactionTaskAsync(serverOptions.CompactionFrequencySecs, token));
                }

                var objectCollectFrequencySecs = runtimeConfig.GetInt(ServerConfigType.EXPIRED_OBJECT_COLLECTION_FREQ);
                if (objectCollectFrequencySecs > 0)
                    taskManager.RegisterAndRun(TaskType.ObjectCollectTask, token => ObjectCollectTaskAsync(objectCollectFrequencySecs, token));

                var expiredKeyDeletionFrequencySecs = runtimeConfig.GetInt(ServerConfigType.EXPIRED_KEY_DELETION_SCAN_FREQ);
                if (expiredKeyDeletionFrequencySecs > 0)
                    taskManager.RegisterAndRun(TaskType.ExpiredKeyDeletionTask, token => ExpiredKeyDeletionScanTaskAsync(expiredKeyDeletionFrequencySecs, token));
            }
        }

        /// <summary>
        /// Start background maintenance tasks that should only be run when this node is a replica.
        /// </summary>
        public void StartReplicaTasks()
        {
            if (serverOptions.EnableVectorSetPreview)
            {
                _ = taskManager.RegisterAndRun(TaskType.VectorReplicationReplayTask, token => DefaultDatabase.VectorManager.StartReplicationTasksAsync(token));
            }
        }

        /// <summary>
        /// Start background maintenance generic tasks
        /// </summary>
        public void StartGenericNodeTasks()
        {
            if (serverOptions.AdjustedIndexMaxCacheLines > 0 || serverOptions.AdjustedObjectStoreIndexMaxCacheLines > 0)
            {
                taskManager.RegisterAndRun(TaskType.IndexAutoGrowTask, (token) => IndexAutoGrowTaskAsync(token));
            }
        }

        #endregion
    }
}