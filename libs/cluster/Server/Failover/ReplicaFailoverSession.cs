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
        /// <summary>
        /// Connection to primary if reachable
        /// </summary>
        GarnetClient primaryClient = null;

        /// <summary>
        /// Send page size for GarnetClient
        /// </summary>
        const int sendPageSize = 1 << 17;

        /// <summary>
        /// Helper method to establish connection towards remote node
        /// </summary>
        /// <param name="nodeId">Id of node to create connection for</param>
        /// <returns></returns>
        private async Task<GarnetClient> CreateConnectionAsync(string nodeId)
        {
            var endpoint = oldConfig.GetEndpointFromNodeId(nodeId);
            var client = new GarnetClient(
                endpoint,
                clusterProvider.serverOptions.TlsOptions?.TlsClientOptions,
                sendPageSize: sendPageSize,
                maxOutstandingTasks: 8,
                authUsername: clusterProvider.ClusterUsername,
                authPassword: clusterProvider.ClusterPassword, logger: logger);

            try
            {
                if (!client.IsConnected)
                    await client.ReconnectAsync().WaitAsync(failoverTimeout, cts.Token);

                return client;
            }
            catch (Exception ex)
            {
                client?.Dispose();
                logger?.LogError(ex, "ReplicaFailoverSession.CreateConnection");
                return null;
            }
        }

        /// <summary>
        /// Acquire a connection to the node identified by given node-id.
        /// </summary>
        /// <param name="nodeId"></param>
        /// <returns></returns>
        private Task<GarnetClient> GetConnectionAsync(string nodeId)
            => CreateConnectionAsync(nodeId);

        /// <summary>
        /// Send stop writes message to PRIMARY
        /// </summary>
        /// <returns>True on success, false otherwise</returns>
        private async Task<bool> PauseWritesAndWaitForSync()
        {
            var primaryId = oldConfig.LocalNodePrimaryId;
            var client = await GetConnectionAsync(primaryId);
            try
            {
                if (client == null)
                {
                    logger?.LogError("Failed to initialize connection to primary {primaryId}", primaryId);
                    return false;
                }

                // Cache connection for use with next operations
                primaryClient = client;

                // Issue stop writes to the primary
                status = FailoverStatus.ISSUING_PAUSE_WRITES;
                var localIdBytes = Encoding.ASCII.GetBytes(oldConfig.LocalNodeId);
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
        }

        /// <summary>
        /// Perform series of steps to update local config and take ownership of primary slots.
        /// </summary>
        private bool TakeOverAsPrimary()
        {
            // Take over as primary and inform old primary
            status = FailoverStatus.TAKING_OVER_AS_PRIMARY;
            var acquiredLock = false;

            try
            {
                // Make replica syncing unavailable by setting recovery flag
                if (!clusterProvider.replicationManager.BeginRecovery(RecoveryStatus.ClusterFailover, upgradeLock: false))
                {
                    logger?.LogWarning($"{nameof(TakeOverAsPrimary)}: {{logMessage}}", Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_CANNOT_ACQUIRE_RECOVERY_LOCK));
                    return false;
                }
                acquiredLock = true;
                _ = clusterProvider.BumpAndWaitForEpochTransition();

                // Take over slots from old primary
                if (!clusterProvider.clusterManager.TryTakeOverForPrimary())
                {
                    logger?.LogWarning($"{nameof(TakeOverAsPrimary)}: {{logMessage}}", Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_CANNOT_TAKEOVER_FROM_PRIMARY));
                    return false;
                }

                // Update replicationIds and replicationOffset2
                clusterProvider.replicationManager.TryUpdateForFailover();

                // Reset replay iterators
                clusterProvider.replicationManager.ResetReplayIterator();

                // Initialize checkpoint history
                if (!clusterProvider.replicationManager.InitializeCheckpointStore())
                    logger?.LogWarning("Failed acquiring latest memory checkpoint metadata at {method}", nameof(TakeOverAsPrimary));
                _ = clusterProvider.BumpAndWaitForEpochTransition();

                // Resume all background maintenance that were possibly shutdown when this node became a replica
                clusterProvider.storeWrapper.StartPrimaryTasks();
            }
            finally
            {
                // Disable recovering as now this node has become a primary or failed in its attempt earlier
                if (acquiredLock) clusterProvider.replicationManager.EndRecovery(RecoveryStatus.NoRecovery, downgradeLock: false);
            }

            return true;
        }

        /// <summary>
        /// Issue gossip and attach request to replica
        /// </summary>
        /// <param name="replicaId">Replica-id to issue gossip and attache request</param>
        /// <param name="configByteArray">Serialized local cluster config data</param>
        /// <returns></returns>
        private async Task BroadcastConfigAndRequestAttach(string replicaId, byte[] configByteArray)
        {
            var oldPrimaryId = oldConfig.LocalNodePrimaryId;
            var newConfig = clusterProvider.clusterManager.CurrentConfig;
            var client = oldPrimaryId.Equals(replicaId) ? primaryClient : await GetConnectionAsync(replicaId);

            try
            {
                if (client == null)
                {
                    logger?.LogError("Failed to initialize connection to replica {replicaId}", replicaId);
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
                            if (current.IsKnown(other.LocalNodeId))
                                _ = clusterProvider.clusterManager.TryMerge(ClusterConfig.FromByteArray(returnedConfigArray));
                            else
                                logger?.LogWarning("Received gossip from unknown node: {node-id}", other.LocalNodeId);
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

                var localAddress = oldConfig.LocalNodeIp;
                var localPort = oldConfig.LocalNodePort;

                // Ask replica to attach and sync
                var replicaOfResp = await client.ReplicaOf(localAddress, localPort).WaitAsync(failoverTimeout, cts.Token);

                // Check if response for attach succeeded
                if (!replicaOfResp.Equals("OK"))
                    logger?.LogWarning("IssueAttachReplicas Error: {replicaId} {replicaOfResp}", replicaId, replicaOfResp);
            }
            finally
            {
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
            var oldPrimaryId = oldConfig.LocalNodePrimaryId;
            var replicaIds = newConfig.GetReplicaIds(oldPrimaryId);
            var configByteArray = newConfig.ToByteArray();
            var attachReplicaTasks = new List<Task>();

            // If DEFAULT failover try to make old primary replica of this new primary
            if (option is FailoverOption.DEFAULT)
            {
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
                if (!TakeOverAsPrimary())
                {
                    // Request primary to be reset to original state only if DEFAULT option was used
                    if (primaryClient != null)
                        _ = await primaryClient?.failstopwrites(Array.Empty<byte>()).WaitAsync(failoverTimeout, cts.Token);
                    return false;
                }

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
                primaryClient?.Dispose();
                status = FailoverStatus.NO_FAILOVER;
            }
        }
    }
}