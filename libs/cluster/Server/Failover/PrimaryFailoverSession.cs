// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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
                    await gclient.ConnectAsync().ConfigureAwait(false);

                return await gclient.ExecuteClusterFailReplicationOffsetAsync(syncTarget).WaitAsync(clusterTimeout, cts.Token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An error occurred at CheckReplicaSync while waiting for first replica sync");
                throw new Exception(ex.Message);
            }
        }

        private async Task<GarnetClient> WaitForFirstReplicaSyncAsync(AofAddress syncTarget)
        {
            if (clients.Length > 1)
            {
                var tasks = new Task<string>[clients.Length + 1];

                var tcount = 0;
                foreach (var _gclient in clients)
                    tasks[tcount++] = CheckReplicaSyncAsync(_gclient, syncTarget);

                tasks[clients.Length] = DelayToDefaultAsync(failoverTimeout);
                var completedTask = await Task.WhenAny(tasks).ConfigureAwait(false);

                // No replica was able to catch up with primary so timeout
                if (completedTask == tasks[clients.Length])
                {
                    logger?.LogError("WaitForReplicasSync timeout");
                    return null;
                }

                // Return client for replica that has caught up with replication primary
                for (var i = 0; i < tasks.Length; i++)
                {
                    var replicationOffset = AofAddress.FromString(await tasks[i].ConfigureAwait(false));
                    if (completedTask == tasks[i] && replicationOffset.EqualsAll(syncTarget))
                        return clients[i];
                }
                return null;
            }
            else
            {
                var syncTask = CheckReplicaSyncAsync(clients[0], syncTarget);
                var timeoutTask = Task.Delay(failoverTimeout, cts.Token);
                var completedTask = await Task.WhenAny(syncTask, timeoutTask).ConfigureAwait(false);

                // Replica trying to failover did not caught up on time so timeout
                if (completedTask == timeoutTask)
                {
                    logger?.LogError("WaitForFirstReplicaSync timeout");
                    return null;
                }

                var replicationOffset = AofAddress.FromString(await syncTask.ConfigureAwait(false));
                if (!replicationOffset.EqualsAll(syncTarget))
                    return null;
                else
                    return clients[0];
            }

            static async Task<string> DelayToDefaultAsync(TimeSpan failoverTimeout)
            {
                await Task.Delay(failoverTimeout).ConfigureAwait(false);

                return default;
            }
        }

        private async Task<bool> InitiateReplicaTakeOverAsync(GarnetClient gclient)
        {
            try
            {
                if (!gclient.IsConnected)
                    await gclient.ConnectAsync().ConfigureAwait(false);

                return await gclient.Failover(FailoverOption.TAKEOVER).WaitAsync(clusterTimeout, cts.Token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An error occurred at CheckReplicaSync while waiting for first replica sync");
                return false;
            }
        }

        private bool TryPauseCheckpointsForFailover()
        {
            var started = Stopwatch.GetTimestamp();
            // Park this background failover worker on contention to avoid timer allocations and busy yielding.
            var spinner = ConfiguredSpinWait.CpuLocal();
            var backoff = new ExponentialBackoff(TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(50));
            while (!cts.IsCancellationRequested)
            {
                var remaining = failoverTimeout - Stopwatch.GetElapsedTime(started);
                if (remaining <= TimeSpan.Zero)
                {
                    logger?.LogWarning("Primary failover timed out waiting to pause checkpoints");
                    return false;
                }

                if (clusterProvider.storeWrapper.TryPauseCheckpoints())
                    return true;

                if (spinner.TryWait())
                    continue;

                var delay = backoff.RecordFailure();
                Thread.Sleep(delay < remaining ? delay : remaining);
            }
            return false;
        }

        public async Task<bool> BeginAsyncPrimaryFailoverAsync()
        {
            var checkpointsPaused = false;
            try
            {
                status = FailoverStatus.ISSUING_PAUSE_WRITES;
                clusterProvider.failoverManager.PauseWrites(true);
                if (!await clusterProvider.BumpAndWaitForEpochTransitionAsync().ConfigureAwait(false) || cts.IsCancellationRequested)
                    return false;

                checkpointsPaused = TryPauseCheckpointsForFailover();
                if (!checkpointsPaused)
                    return false;

                var syncTarget = clusterProvider.storeWrapper.appendOnlyFile.Log.TailAddress;
                status = FailoverStatus.WAITING_FOR_SYNC;
                var newPrimary = await WaitForFirstReplicaSyncAsync(syncTarget).ConfigureAwait(false);
                if (newPrimary == null) return false;

                status = FailoverStatus.TAKING_OVER_AS_PRIMARY;
                var endpoint = (IPEndPoint)newPrimary.EndPoint;
                var replicaId = oldConfig.GetWorkerNodeIdFromAddress(endpoint.Address.ToString(), endpoint.Port);
                clusterProvider.clusterManager.TryStopWrites(replicaId);
                // Replica checkpoint callbacks no longer append markers. Unblock checkpoint waiters before draining their epochs.
                clusterProvider.storeWrapper.ResumeCheckpoints();
                checkpointsPaused = false;
                // Drain requests that captured the primary configuration while writes were paused.
                if (!await clusterProvider.BumpAndWaitForEpochTransitionAsync().ConfigureAwait(false) || cts.IsCancellationRequested)
                    return false;

                var success = await InitiateReplicaTakeOverAsync(newPrimary).ConfigureAwait(false);
                if (!success) return false;
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
                    clusterProvider.failoverManager.PauseWrites(false);
                    status = FailoverStatus.NO_FAILOVER;
                }
            }
            return true;
        }
    }
}