// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class FailoverSession : IDisposable
    {
        private async Task<string> CheckReplicaSyncAsync(GarnetClient gclient)
        {
            try
            {
                if (!gclient.IsConnected)
                    await gclient.ConnectAsync().ConfigureAwait(false);

                return await gclient.ExecuteClusterFailReplicationOffsetAsync(clusterProvider.replicationManager.ReplicationOffset).WaitAsync(clusterTimeout, cts.Token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An error occurred at CheckReplicaSync while waiting for first replica sync");
                throw new Exception(ex.Message);
            }
        }

        private async Task<GarnetClient> WaitForFirstReplicaSyncAsync()
        {
            if (clients.Length > 1)
            {
                var tasks = new Task<string>[clients.Length + 1];

                var tcount = 0;
                foreach (var _gclient in clients)
                    tasks[tcount++] = CheckReplicaSyncAsync(_gclient);

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
                    if (completedTask == tasks[i] && replicationOffset.EqualsAll(clusterProvider.replicationManager.ReplicationOffset))
                        return clients[i];
                }
                return null;
            }
            else
            {
                var syncTask = CheckReplicaSyncAsync(clients[0]);
                var timeoutTask = Task.Delay(failoverTimeout, cts.Token);
                var completedTask = await Task.WhenAny(syncTask, timeoutTask).ConfigureAwait(false);

                // Replica trying to failover did not caught up on time so timeout
                if (completedTask == timeoutTask)
                {
                    logger?.LogError("WaitForFirstReplicaSync timeout");
                    return null;
                }

                var replicationOffset = AofAddress.FromString(await syncTask.ConfigureAwait(false));
                if (!replicationOffset.EqualsAll(clusterProvider.replicationManager.ReplicationOffset))
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

        public async Task<bool> BeginAsyncPrimaryFailoverAsync()
        {
            try
            {
                // Change local node role to suspend any write workload
                status = FailoverStatus.ISSUING_PAUSE_WRITES;
                var localId = clusterProvider.clusterManager.CurrentConfig.LocalNodeId;
                var oldRole = clusterProvider.clusterManager.CurrentConfig.LocalNodeRole;
                var replicas = clusterProvider.clusterManager.CurrentConfig.GetReplicaIds(localId);
                clusterProvider.clusterManager.TryStopWrites(replicas[0]);
                _ = await clusterProvider.BumpAndWaitForEpochTransitionAsync().ConfigureAwait(false);

                status = FailoverStatus.WAITING_FOR_SYNC;
                var newPrimary = await WaitForFirstReplicaSyncAsync().ConfigureAwait(false);
                if (newPrimary == null) return false;

                status = FailoverStatus.TAKING_OVER_AS_PRIMARY;
                var success = await InitiateReplicaTakeOverAsync(newPrimary).ConfigureAwait(false);
                if (!success) return false;
            }
            catch (Exception ex)
            {
                logger?.LogWarning("BeginAsyncPrimaryFailover exception {exceptionMsg}", ex.Message);
            }
            finally
            {
                status = FailoverStatus.NO_FAILOVER;
            }
            return true;
        }
    }
}