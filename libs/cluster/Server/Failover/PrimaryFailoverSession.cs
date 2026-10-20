// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class FailoverSession : IDisposable
    {
        private async Task<string> CheckReplicaSyncAsync(GarnetClient gclient, AofAddress syncTarget)
        {
            try
            {
                if (!gclient.IsConnected)
                    await gclient.ConnectAsync(cts.Token).WaitAsync(cts.Token).ConfigureAwait(false);

                cts.Token.ThrowIfCancellationRequested();
                return await gclient.ExecuteClusterFailReplicationOffsetAsync(syncTarget).WaitAsync(clusterTimeout, cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cts.IsCancellationRequested)
            {
                return null;
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An error occurred at CheckReplicaSync while waiting for first replica sync");
                return null;
            }
        }

        private async Task<GarnetClient> WaitForFirstReplicaSyncAsync(AofAddress syncTarget)
        {
            var tasks = new Task<string>[clients.Length];
            for (var i = 0; i < clients.Length; i++)
                tasks[i] = CheckReplicaSyncAsync(clients[i], syncTarget);

            var completedTask = await Task.WhenAny(tasks).WaitAsync(cts.Token).ConfigureAwait(false);
            var reply = await completedTask.ConfigureAwait(false);
            cts.Token.ThrowIfCancellationRequested();
            return reply != null && AofAddress.FromString(reply).EqualsAll(syncTarget)
                ? clients[Array.IndexOf(tasks, completedTask)]
                : null;
        }

        private async Task<bool> InitiateReplicaTakeOverAsync(GarnetClient gclient)
        {
            try
            {
                if (!gclient.IsConnected)
                    await gclient.ConnectAsync(cts.Token).WaitAsync(cts.Token).ConfigureAwait(false);

                cts.Token.ThrowIfCancellationRequested();
                return await gclient.Failover(FailoverOption.TAKEOVER, cts.Token).WaitAsync(clusterTimeout, cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cts.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An error occurred at CheckReplicaSync while waiting for first replica sync");
                return false;
            }
        }

        private bool TryPauseCheckpointsForFailover()
        {
            // Park this background failover worker on contention to avoid timer allocations and busy yielding.
            var spinner = ConfiguredSpinWait.CpuLocal();
            var backoff = new ExponentialBackoff(TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(50));
            while (!cts.IsCancellationRequested)
            {
                if (clusterProvider.storeWrapper.TryPauseCheckpoints())
                    return true;

                if (spinner.TryWait())
                    continue;

                Thread.Sleep(backoff.RecordFailure());
            }
            return false;
        }

        public async Task<bool> BeginAsyncPrimaryFailoverAsync()
        {
            var checkpointsPaused = false;
            var writesPaused = false;
            var primaryTasksSuspended = false;
            try
            {
                // Bound only the period holding application writes; omission keeps the pause infinite.
                if (failoverTimeout != Timeout.InfiniteTimeSpan)
                    cts.CancelAfter(failoverTimeout);
                cts.Token.ThrowIfCancellationRequested();
                status = FailoverStatus.ISSUING_PAUSE_WRITES;
                clusterProvider.failoverManager.PauseWrites(true);
                writesPaused = true;
                primaryTasksSuspended = true;
                await clusterProvider.storeWrapper.SuspendPrimaryTasksForFailoverAsync().WaitAsync(cts.Token).ConfigureAwait(false);
                _ = await clusterProvider.BumpAndWaitForEpochTransitionAsync(cts.Token).ConfigureAwait(false);
                cts.Token.ThrowIfCancellationRequested();

                checkpointsPaused = TryPauseCheckpointsForFailover();
                cts.Token.ThrowIfCancellationRequested();
                if (!checkpointsPaused)
                    return false;

                var syncTarget = clusterProvider.storeWrapper.appendOnlyFile.Log.TailAddress;
                status = FailoverStatus.WAITING_FOR_SYNC;
                var newPrimary = await WaitForFirstReplicaSyncAsync(syncTarget).ConfigureAwait(false);
                if (newPrimary == null) return false;
                cts.Token.ThrowIfCancellationRequested();

                status = FailoverStatus.TAKING_OVER_AS_PRIMARY;
                var endpoint = (IPEndPoint)newPrimary.EndPoint;
                var replicaId = oldConfig.GetWorkerNodeIdFromAddress(endpoint.Address.ToString(), endpoint.Port);
                clusterProvider.clusterManager.TryStopWrites(replicaId);
                // Replica checkpoint callbacks no longer append markers. Unblock checkpoint waiters before draining their epochs.
                clusterProvider.storeWrapper.ResumeCheckpoints();
                checkpointsPaused = false;
                // Drain requests that captured the primary configuration while writes were paused.
                _ = await clusterProvider.BumpAndWaitForEpochTransitionAsync(cts.Token).ConfigureAwait(false);
                cts.Token.ThrowIfCancellationRequested();

                var success = await InitiateReplicaTakeOverAsync(newPrimary).ConfigureAwait(false);
                if (!success) return false;
            }
            catch (OperationCanceledException) when (cts.IsCancellationRequested)
            {
                logger?.LogWarning("Primary failover canceled or timed out while writes were paused");
                return false;
            }
            catch (Exception ex)
            {
                logger?.LogWarning("BeginAsyncPrimaryFailover exception {exceptionMsg}", ex.Message);
                return false;
            }
            finally
            {
                try
                {
                    if (checkpointsPaused)
                        clusterProvider.storeWrapper.ResumeCheckpoints();
                }
                finally
                {
                    if (writesPaused)
                        clusterProvider.failoverManager.PauseWrites(false);
                    // Release writers on cancellation even if a maintenance cycle is still draining.
                    if (primaryTasksSuspended)
                        await clusterProvider.storeWrapper.ResumePrimaryTasksAfterFailoverAsync().ConfigureAwait(false);
                    status = FailoverStatus.NO_FAILOVER;
                }
            }
            return true;
        }
    }
}