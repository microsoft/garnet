// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class FailoverSession : IDisposable
    {
        /// <summary>
        /// Send stop writes message to PRIMARY
        /// </summary>
        /// <returns>PRIMARY replication offset</returns>
        private async Task<long> SendStopWrites()
        {
            try
            {
                if (!clients[0].IsConnected) await clients[0].ConnectAsync().WaitAsync(failoverTimeout, cts.Token);
                var localIdBytes = Encoding.ASCII.GetBytes(clusterProvider.clusterManager.CurrentConfig.GetLocalNodeId());
                return await clients[0].failstopwrites(localIdBytes).WaitAsync(failoverTimeout, cts.Token);
            }
            catch (Exception ex)
            {
                logger?.LogError("SendStopWrites failed with msg: {msg}", ex.Message);
                return -1;
            }
        }

        /// <summary>
        /// Wait for replica to catch up with PRIMARY
        /// </summary>
        /// <param name="primaryReplicationOffset"></param>
        /// <returns></returns>
        private async Task<bool> AwaitReplicationSync(long primaryReplicationOffset)
        {
            while (primaryReplicationOffset > clusterProvider.replicationManager.ReplicationOffset)
            {
                if (FailoverTimeout)
                {
                    logger?.LogError("AwaitReplicationSync timed out failoverStart: {failoverStart} failoverEnd: {failoverEnd} diff: {diff} UtcNow: {UtcNow}", failoverStart, failoverEnd, failoverEnd - failoverStart, DateTimeOffset.UtcNow.Ticks);
                    return false;
                }
                await Task.Yield();
            }
            return true;
        }

        /// <summary>
        /// Send failover authorization request
        /// </summary>
        /// <param name="requestedEpoch"></param>
        /// <returns></returns>
        private async Task<bool> SendFailoverAuthReq(long requestedEpoch)
        {
            Task<long>[] tasks = new Task<long>[clients.Length + 1];
            //Skip primary connection if FORCE option is set
            var nodeIdBytes = Encoding.ASCII.GetBytes(clusterProvider.clusterManager.CurrentConfig.GetLocalNodeId());
            var primaryId = clusterProvider.clusterManager.CurrentConfig.GetLocalNodeId();
            var claimedSlots = clusterProvider.clusterManager.CurrentConfig.GetClaimedSlotsFromNodeId(primaryId);
            int firstClientIndex = option == FailoverOption.FORCE ? 1 : 0;

            if (firstClientIndex == 1) tasks[0] = Task.CompletedTask.ContinueWith(x => 0L);
            for (int i = firstClientIndex; i < clients.Length; i++)
            {
                try
                {
                    if (!clients[i].IsConnected) clients[i].Connect();
                    tasks[i] = clients[i].failauthreq(nodeIdBytes, requestedEpoch, claimedSlots).WaitAsync(failoverTimeout, cts.Token);
                }
                catch (Exception ex)
                {
                    logger?.LogWarning("SendFailoverAuthReq error at connection initialization {msg}", ex.Message);
                }
            }

            //Wait for all or timeout
            tasks[clients.Length] = Task.Delay(failoverTimeout).ContinueWith(_ => default(long));
            var t = await Task.WhenAll(tasks);

            int majority = (clients.Length - firstClientIndex / 2) + 1;
            int votes = 0;
            for (int i = firstClientIndex; i < clients.Length; i++)
            {
                var task = tasks[i];
                votes += task.Status == TaskStatus.RanToCompletion && task.Result > 0 ? 1 : 0;
            }
            return votes >= majority;
        }

        private async Task AttachReplicas(ClusterConfig oldConfig, ClusterConfig newConfig)
        {
            var oldPrimaryId = oldConfig.GetLocalNodePrimaryId();
            var replicaEndpoints = newConfig.GetReplicaEndpoints(oldPrimaryId);
            var localAddress = newConfig.GetLocalNodeIp();
            var localPort = newConfig.GetLocalNodePort();
            Task<string>[] tasks = new Task<string>[replicaEndpoints.Count];
            GarnetClient[] clients = new GarnetClient[replicaEndpoints.Count];
            int count = 0;
            try
            {
                foreach (var replicaEndpoint in replicaEndpoints)
                {
                    var address = replicaEndpoint.Item1;
                    var port = replicaEndpoint.Item2;
                    clients[0] = new(address, port, clusterProvider.serverOptions.TlsOptions?.TlsClientOptions, authUsername: clusterProvider.ClusterUsername, authPassword: clusterProvider.ClusterPassword, logger: logger);
                    // Connect to replica
                    await clients[0].ConnectAsync().WaitAsync(failoverTimeout, cts.Token);
                    // Gossip new configuration to ack new primary
                    var resp = await clients[0].Gossip(newConfig.ToByteArray()).WaitAsync(failoverTimeout, cts.Token);

                    // Try to merge if package is non-zero
                    if (resp.Length > 0)
                    {
                        var other = ClusterConfig.FromByteArray(resp.Span.ToArray());

                        // Merge config if receiving config is from a trusted node
                        if (newConfig.IsKnown(other.GetLocalNodeId()))
                            clusterProvider.clusterManager.TryMerge(other);
                        else
                            logger?.LogWarning("Received gossip from unknown node: {node-id}", other.GetLocalNodeId());
                    }
                    resp.Dispose();

                    //3. Send replicaof to make recv node a replica of this node
                    tasks[count++] = clients[0].ReplicaOf(localAddress, localPort).WaitAsync(failoverTimeout, cts.Token);
                }

                await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "AttachReplicas threw an error");
            }
            finally
            {
                for (int i = 0; i < tasks.Length; i++)
                {
                    var task = tasks[i];
                    if (task == null) continue;
                    if (task.Status != TaskStatus.RanToCompletion || !task.Result.Equals("OK"))
                        logger?.LogError("AttachReplicas task failed with status: {taskStatus} {address} {port} {resp}", task.Status, replicaEndpoints[i].Item1, replicaEndpoints[i].Item2, task.Result);
                    clients[0].Dispose();
                }
            }
        }

        /// <summary>
        /// REPLICA main failover task
        /// </summary>
        /// <returns></returns>
        public async Task<bool> BeginAsyncReplicaFailover()
        {
            // CLUSTER FAILOVER OPTIONS
            // FORCE: Do not await for the primary since it might be unreachable
            // TAKEOVER: Same as force but also do not await for voting from other primaries
            try
            {
                if (option != FailoverOption.FORCE && option != FailoverOption.TAKEOVER)
                {
                    //1. Issue stop write to the primary
                    status = FailoverStatus.ISSUING_PAUSE_WRITES;
                    long primaryReplicationOffset = await SendStopWrites();
                    if (primaryReplicationOffset < 0)
                    {
                        logger?.LogError("Failed at {status}, primaryReplicationOffset: {primaryReplicationOffset}", status, primaryReplicationOffset);
                        return false;
                    }

                    //2. wait for replica to catch up with primary
                    status = FailoverStatus.WAITING_FOR_SYNC;
                    if (!await AwaitReplicationSync(primaryReplicationOffset))
                    {
                        logger?.LogError("Failed at {status} failoverTimeout: {failoverTimeout}s)", FailoverStatus.WAITING_FOR_SYNC, failoverTimeout.TotalSeconds);
                        return false;
                    }
                }

                //If TAKEOVER option is set skip voting
                if (option != FailoverOption.TAKEOVER)
                {
                    //TODO: implement voting
                    //3. Generate new epoch and request a vote for the epoch
                    //status = FailoverStatus.FAILOVER_IN_PROGRESS;
                    //while (true)
                    //{
                    //    var requestedEpoch = storeWrapper.clusterManager.TryBumpCurrentClusterEpoch();
                    //    var success = await SendFailoverAuthReq(requestedEpoch);
                    //    if (success || FailoverTimeout) break;
                    //}
                }

                //4. Take over as primary and inform old primary
                status = FailoverStatus.TAKING_OVER_AS_PRIMARY;

                //make replica syncing unavailable by setting recovery flag
                clusterProvider.replicationManager.recovering = true;
                clusterProvider.WaitForConfigTransition();

                //Update replicationIds and replicationOffset2
                clusterProvider.replicationManager.TryUpdateForFailover();

                var oldConfig = clusterProvider.clusterManager.CurrentConfig;
                //Initialize checkpoint history
                clusterProvider.replicationManager.InitializeCheckpointStore();
                clusterProvider.clusterManager.TryTakeOverForPrimary();
                clusterProvider.WaitForConfigTransition();

                //Disable recovering as now we have become a primary
                clusterProvider.replicationManager.recovering = false;

                var newConfig = clusterProvider.clusterManager.CurrentConfig;
                //Inform new primary of the configuration change
                if (option != FailoverOption.FORCE && option != FailoverOption.TAKEOVER)
                {
                    var byteArray = clusterProvider.clusterManager.CurrentConfig.ToByteArray();
                    try
                    {
                        _ = await clients[0].Gossip(byteArray);
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError("Failed to send updated config to old primary {exceptionMessage}", ex.Message);
                    }
                }

                //Attach to old replicas
                await AttachReplicas(oldConfig, newConfig);

                return true;
            }
            finally
            {
                status = FailoverStatus.NO_FAILOVER;
            }
        }
    }
}