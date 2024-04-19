// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class FailoverSession : IDisposable
    {
        bool useGossipConnections = false;


        /// <summary>
        /// Helper method to re-use gossip connection to perform the failover
        /// </summary>
        /// <param name="nodeId">Node-id to use for search the connection array</param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        private GarnetClient GetOrAddConnection(string nodeId)
        {
            _ = clusterProvider.clusterManager.clusterConnectionStore.GetConnection(nodeId, out var gsn);

            // If connection not available try to initialize it
            if (gsn == null)
            {
                var (address, port) = currentConfig.GetEndpointFromNodeId(nodeId);
                gsn = new GarnetServerNode(
                    clusterProvider,
                    address,
                    port,
                    clusterProvider.storeWrapper.serverOptions.TlsOptions?.TlsClientOptions,
                    logger: logger);

                // Try add connection to the connection store
                if (!clusterProvider.clusterManager.clusterConnectionStore.AddConnection(gsn))
                {
                    // If failed to add dispose connection resources
                    gsn.Dispose();
                    // Retry to get established connection if it was added after our first attempt
                    _ = clusterProvider.clusterManager.clusterConnectionStore.GetConnection(nodeId, out gsn);
                }

                // Final check fail, if connection is not established.
                if (gsn == null)
                    throw new GarnetException($"Connection not established to node {nodeId}");
            }

            gsn.Initialize();

            return gsn.Client;
        }

        /// <summary>
        /// Helper method to establish connection towards remote node
        /// </summary>
        /// <param name="nodeId">Id of node to create connection for</param>
        /// <returns></returns>
        private GarnetClient CreateConnection(string nodeId)
        {
            var (address, port) = currentConfig.GetEndpointFromNodeId(nodeId);
            var client = new GarnetClient(
                address,
                port,
                clusterProvider.serverOptions.TlsOptions?.TlsClientOptions,
                authUsername: clusterProvider.ClusterUsername,
                authPassword: clusterProvider.ClusterPassword, logger: logger);

            try
            {
                if (!client.IsConnected)
                    client.ReconnectAsync().WaitAsync(failoverTimeout, cts.Token).GetAwaiter().GetResult();

                return client;
            }
            catch (Exception ex)
            {
                if (!useGossipConnections)
                    client?.Dispose();
                logger?.LogError(ex, "ReplicaFailoverSession.CreateConnection");
                return null;
            }
        }

        private GarnetClient GetConnection(string nodeId)
        {
            return useGossipConnections ? GetOrAddConnection(nodeId) : CreateConnection(nodeId);
        }

        /// <summary>
        /// Send stop writes message to PRIMARY
        /// </summary>
        /// <returns>True on success, false otherwise</returns>
        private async Task<bool> PauseWritesAndWaitForSync()
        {
            var primaryId = currentConfig.GetLocalNodePrimaryId();
            var client = GetConnection(primaryId);
            try
            {
                if (client == null)
                {
                    logger?.LogError("Failed to initialize connection to primary {primaryId}", primaryId);
                    return false;
                }

                // Issue stop writes to the primary
                status = FailoverStatus.ISSUING_PAUSE_WRITES;
                var localIdBytes = Encoding.ASCII.GetBytes(currentConfig.GetLocalNodeId());
                var primaryReplicationOffset = await client.failstopwrites(localIdBytes).WaitAsync(failoverTimeout, cts.Token);

                // Wait for replica to catch up
                status = FailoverStatus.WAITING_FOR_SYNC;
                while (primaryReplicationOffset > clusterProvider.replicationManager.ReplicationOffset)
                {
                    // Fail if upper bound time for failover has been reached
                    if (FailoverTimeout)
                    {
                        logger?.LogError("AwaitReplicationSync timed out failoverStart");
                        return false;
                    }
                    await Task.Yield();
                }

                return true;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "PauseWritesAndWaitForSync Error");
                return false;
            }
            finally
            {
                if (!useGossipConnections)
                    client?.Dispose();
            }
        }

        /// <summary>
        /// Perform series of steps to update local config and take ownership of primary slots.
        /// </summary>
        private void TakeOverAsPrimary()
        {
            // Take over as primary and inform old primary
            status = FailoverStatus.TAKING_OVER_AS_PRIMARY;

            // Make replica syncing unavailable by setting recovery flag
            clusterProvider.replicationManager.recovering = true;
            _ = clusterProvider.WaitForConfigTransition();

            // Update replicationIds and replicationOffset2
            clusterProvider.replicationManager.TryUpdateForFailover();

            // Initialize checkpoint history
            clusterProvider.replicationManager.InitializeCheckpointStore();
            clusterProvider.clusterManager.TryTakeOverForPrimary();
            _ = clusterProvider.WaitForConfigTransition();

            // Disable recovering as now we have become a primary
            clusterProvider.replicationManager.recovering = false;
        }

        /// <summary>
        /// Issue gossip and attach request to replica
        /// </summary>
        /// <param name="replicaId">Replica-id to issue gossip and attache request</param>
        /// <param name="configByteArray">Serialized local cluster config data</param>
        /// <returns></returns>
        private async Task BroadcastConfigAndRequestAttach(string replicaId, byte[] configByteArray)
        {
            var newConfig = clusterProvider.clusterManager.CurrentConfig;
            var client = GetConnection(replicaId);

            try
            {
                if (client == null)
                {
                    logger?.LogError("Failed to initialize connection to replica {primaryId}", replicaId);
                    return;
                }

                // Force send updated config to replica
                await client.Gossip(configByteArray).ContinueWith(t =>
                {
                    var resp = t.Result;
                    try
                    {
                        var current = clusterProvider.clusterManager.CurrentConfig;
                        if (resp.Length > 0)
                        {
                            clusterProvider.clusterManager.gossipStats.UpdateGossipBytesRecv(resp.Length);
                            var returnedConfigArray = resp.Span.ToArray();
                            var other = ClusterConfig.FromByteArray(returnedConfigArray);

                            // Check if gossip is from a node that is known and trusted before merging
                            if (current.IsKnown(other.GetLocalNodeId()))
                                _ = clusterProvider.clusterManager.TryMerge(ClusterConfig.FromByteArray(returnedConfigArray));
                            else
                                logger?.LogWarning("Received gossip from unknown node: {node-id}", other.GetLocalNodeId());
                        }
                        resp.Dispose();
                    }
                    catch (Exception ex)
                    {
                        logger?.LogCritical(ex, "IssueAttachReplicas faulted");
                    }
                    finally
                    {
                        resp.Dispose();
                    }
                }, TaskContinuationOptions.RunContinuationsAsynchronously).WaitAsync(failoverTimeout, cts.Token);

                var localAddress = currentConfig.GetLocalNodeIp();
                var localPort = currentConfig.GetLocalNodePort();

                // Ask replica to attach and sync
                var replicaOfResp = await client.ReplicaOf(localAddress, localPort).WaitAsync(failoverTimeout, cts.Token);

                // Check if response for attach succeeded
                if (!replicaOfResp.Equals("OK"))
                    logger?.LogWarning("IssueAttachReplicas Error: {replicaId} {replicaOfResp}", replicaId, replicaOfResp);
            }
            finally
            {
                if (!useGossipConnections)
                    client?.Dispose();
            }
        }

        /// <summary>
        /// Issue attach message to remote replicas
        /// </summary>
        /// <returns></returns>
        private async Task IssueAttachReplicas()
        {
            // Get information of local node from newConfig
            var newConfig = clusterProvider.clusterManager.CurrentConfig;
            // Get replica ids for old primary from old configuration
            var oldPrimaryId = currentConfig.GetLocalNodePrimaryId();
            var replicaIds = newConfig.GetReplicaIds(oldPrimaryId);
            var configByteArray = newConfig.ToByteArray();
            var attachReplicaTasks = new List<Task>();

            // If DEFAULT failover try to make old primary replica of this new primary
            if (option is FailoverOption.DEFAULT)
            {
                // TODO: enable primary to replica failover
                replicaIds.Add(oldPrimaryId);
            }

            // Issue gossip and attach request to replicas
            foreach (var replicaId in replicaIds)
            {
                try
                {
                    attachReplicaTasks.Add(Task.Run(async () => await BroadcastConfigAndRequestAttach(replicaId, configByteArray)));
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "IssueAttachReplicas Error");
                }
            }

            // Wait for tasks to complete
            if (attachReplicaTasks.Count > 0)
            {
                try
                {
                    await Task.WhenAll(attachReplicaTasks);
                }
                catch (Exception ex)
                {
                    logger?.LogWarning(ex, "WaitingForAttachToComplete Error");
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
                // Issue stop writes and on ack wait for replica to catch up
                if (option is FailoverOption.DEFAULT && !await PauseWritesAndWaitForSync())
                {
                    return false;
                }

                // If TAKEOVER option is set skip voting
                if (option is FailoverOption.DEFAULT or FailoverOption.FORCE)
                {
                    //TODO: implement voting
                }

                // Transition to primary role
                TakeOverAsPrimary();

                // Attach to old replicas, and old primary if DEFAULT option
                await IssueAttachReplicas();

                return true;
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "BeginAsyncReplicaFailover Error");
                return false;
            }
            finally
            {
                status = FailoverStatus.NO_FAILOVER;
            }
        }
    }
}