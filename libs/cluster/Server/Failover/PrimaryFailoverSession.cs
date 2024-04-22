﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class FailoverSession : IDisposable
    {
        private Task<long> CheckReplicaSync(GarnetClient gclient)
        {
            try
            {
                if (!gclient.IsConnected)
                    gclient.Connect();

                return gclient.failreplicationoffset(clusterProvider.replicationManager.ReplicationOffset).WaitAsync(clusterTimeout, cts.Token);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An error occurred at CheckReplicaSync while waiting for first replica sync");
                throw new Exception(ex.Message);
            }
        }

        private async Task<GarnetClient> WaitForFirstReplicaSync()
        {
            if (clients.Length > 1)
            {
                Task<long>[] tasks = new Task<long>[clients.Length + 1];

                int tcount = 0;
                foreach (var _gclient in clients)
                    tasks[tcount++] = CheckReplicaSync(_gclient);

                tasks[clients.Length] = Task.Delay(failoverTimeout).ContinueWith(_ => default(long));
                var completedTask = await Task.WhenAny(tasks);

                //No replica was able to catch up with primary so timeout
                if (completedTask == tasks[clients.Length])
                {
                    logger?.LogError("WaitForReplicasSync timeout");
                    return null;
                }

                //Return client for replica that has caught up with replication primary
                for (int i = 0; i < tasks.Length; i++)
                {
                    if (completedTask == tasks[i] && tasks[i].Result == clusterProvider.replicationManager.ReplicationOffset)
                        return clients[i];
                }
                return null;
            }
            else
            {
                var syncTask = CheckReplicaSync(clients[0]);
                var timeoutTask = Task.Delay(failoverTimeout, cts.Token);
                var completedTask = await Task.WhenAny(syncTask, timeoutTask);

                //Replica trying to failover did not caught up on time so timeout
                if (completedTask == timeoutTask)
                {
                    logger?.LogError("WaitForFirstReplicaSync timeout");
                    return null;
                }

                if (syncTask.Result != clusterProvider.replicationManager.ReplicationOffset)
                    return null;
                else
                    return clients[0];
            }
        }

        private async Task<bool> InitiateReplicaTakeOver(GarnetClient gclient)
        {
            try
            {
                if (!gclient.IsConnected)
                    gclient.Connect();

                return await gclient.Failover(FailoverOption.TAKEOVER).WaitAsync(clusterTimeout, cts.Token);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An error occurred at CheckReplicaSync while waiting for first replica sync");
                return false;
            }
        }

        public async Task<bool> BeginAsyncPrimaryFailover()
        {
            try
            {
                //Change local node role to suspend any write workload
                status = FailoverStatus.ISSUING_PAUSE_WRITES;
                var localId = clusterProvider.clusterManager.CurrentConfig.LocalNodeId;
                var oldRole = clusterProvider.clusterManager.CurrentConfig.LocalNodeRole;
                var replicas = clusterProvider.clusterManager.CurrentConfig.GetReplicaIds(localId);
                clusterProvider.clusterManager.TryStopWrites(replicas[0]);
                clusterProvider.WaitForConfigTransition();

                status = FailoverStatus.WAITING_FOR_SYNC;
                GarnetClient newPrimary = await WaitForFirstReplicaSync();
                if (newPrimary == null) return false;

                status = FailoverStatus.TAKING_OVER_AS_PRIMARY;
                var success = await InitiateReplicaTakeOver(newPrimary);
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