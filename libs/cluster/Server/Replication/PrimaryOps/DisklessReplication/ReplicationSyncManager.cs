// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationSyncManager
    {
        /// <summary>
        /// Diskless sync flow: pauses checkpoints, takes streaming snapshot, broadcasts via store iteration.
        /// </summary>
        async Task DisklessSyncFlowAsync()
        {
            try
            {
                // Take lock to ensure no other task will be taking a checkpoint
                while (!ClusterProvider.storeWrapper.TryPauseCheckpoints())
                    await Task.Yield();

                var fullSync = await PrepareForDisklessSyncAsync().ConfigureAwait(false);

                if (fullSync)
                    await TakeStreamingCheckpointAsync().ConfigureAwait(false);
            }
            finally
            {
                ClusterProvider.storeWrapper.ResumeCheckpoints();
            }
        }

        async Task<bool> PrepareForDisklessSyncAsync()
        {
            // Try to lock AOF address to avoid truncation
            while (true)
            {
                var minServiceableAofAddress = ClusterProvider.storeWrapper.appendOnlyFile.Log.BeginAddress;

                if (ClusterProvider.replicationManager.AofSyncDriverStore.TryAddReplicationDrivers(GetSessionStore.GetSessions(), ref minServiceableAofAddress))
                    break;

                await Task.Yield();
            }

            var fullSync = false;
            for (var i = 0; i < NumSessions; i++)
            {
                try
                {
                    await Sessions[i].ConnectAsync().ConfigureAwait(false);

                    Sessions[i].currentStoreVersion = ClusterProvider.storeWrapper.store.CurrentVersion;

                    if (!Sessions[i].NeedToFullSync())
                    {
                        Sessions[i]?.SetStatus(SyncStatus.SUCCESS);
                        Sessions[i] = null;
                    }
                    else
                    {
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

            await WaitForFlushAsync().ConfigureAwait(false);
            return fullSync;
        }

        async Task TakeStreamingCheckpointAsync()
        {
            var manager = new SnapshotIteratorManager(this, cts.Token, logger);

            var mainStoreCheckpointTask = ClusterProvider.storeWrapper.store.
                TakeFullCheckpointAsync(CheckpointType.StreamingSnapshot, cancellationToken: cts.Token, streamingSnapshotIteratorFunctions: manager.StoreSnapshotIterator);

            var (success, _) = await WaitOrDieAsync(checkpointTask: mainStoreCheckpointTask, iteratorManager: manager).ConfigureAwait(false);
            if (!success)
                throw new GarnetException("Main store checkpoint stream failed!");

            async ValueTask<(bool success, Guid token)> WaitOrDieAsync(ValueTask<(bool success, Guid token)> checkpointTask, SnapshotIteratorManager iteratorManager)
            {
                try
                {
                    var timeout = replicaSyncTimeout;
                    var delay = TimeSpan.FromSeconds(1);
                    while (true)
                    {
                        cts.Token.ThrowIfCancellationRequested();

                        await Task.Delay(delay).ConfigureAwait(false);

                        ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Replication_Diskless_Sync_Reset_Cts);

                        if (checkpointTask.IsCompleted)
                            return await checkpointTask.ConfigureAwait(false);

                        timeout = !manager.IsProgressing() ? timeout.Subtract(delay) : replicaSyncTimeout;

                        if (timeout.TotalSeconds <= 0)
                            throw new TimeoutException("Streaming snapshot checkpoint timed out");
                    }
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "{method} faulted", nameof(WaitOrDieAsync));
                    for (var i = 0; i < NumSessions; i++)
                        Sessions[i]?.SetStatus(SyncStatus.FAILED, ex.Message);
                    await cts.CancelAsync().ConfigureAwait(false);
                }

                try
                {
                    _ = await checkpointTask.ConfigureAwait(false);
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