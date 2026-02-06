// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    /// <summary>
    /// Cluster manager
    /// </summary>
    internal sealed partial class ClusterManager : IDisposable
    {
        /// <summary>
        /// Add worker with specified slots
        /// Update existing only if new config epoch is larger or current config epoch is zero
        /// </summary>
        public bool TryInitializeLocalWorker(
            string nodeId,
            string address,
            int port,
            long configEpoch,
            NodeRole role,
            string replicaOfNodeId,
            string hostname)
        {
            while (true)
            {
                var current = currentConfig;
                var newConfig = current.InitializeLocalWorker(nodeId, address, port, configEpoch, role, replicaOfNodeId, hostname);
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
            return true;
        }

        /// <summary>
        /// Try remove worker through the forget command.
        /// </summary>
        /// <param name="nodeid"></param>
        /// <param name="expirySeconds"></param>
        /// <param name="errorMessage">The ASCII encoded error message if the method returned <see langword="false"/>; otherwise <see langword="default"/></param>
        public bool TryRemoveWorker(string nodeid, int expirySeconds, out ReadOnlySpan<byte> errorMessage)
        {
            try
            {
                SuspendConfigMerge();
                errorMessage = default;
                while (true)
                {
                    var current = currentConfig;

                    if (current.LocalNodeId.Equals(nodeid, StringComparison.OrdinalIgnoreCase))
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_CANNOT_FORGET_MYSELF;
                        return false;
                    }

                    if (current.GetNodeRoleFromNodeId(nodeid) == NodeRole.UNASSIGNED)
                    {
                        errorMessage = Encoding.ASCII.GetBytes($"ERR I don't know about node {nodeid}.");
                        return false;
                    }

                    if (current.LocalNodeRole == NodeRole.REPLICA && current.LocalNodePrimaryId.Equals(nodeid, StringComparison.OrdinalIgnoreCase))
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_CANNOT_FORGET_MY_PRIMARY;
                        return false;
                    }

                    var newConfig = current.RemoveWorker(nodeid);
                    var expiry = DateTimeOffset.UtcNow.Ticks + TimeSpan.FromSeconds(expirySeconds).Ticks;
                    _ = workerBanList.AddOrUpdate(nodeid, expiry, (key, oldValue) => expiry);
                    if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                        break;
                }
                FlushConfig();
                return true;
            }
            finally
            {
                ResumeConfigMerge();
            }
        }

        /// <summary>
        /// Reset cluster config and generated new node id if HARD reset specified
        /// </summary>
        /// <param name="soft"></param>
        /// <param name="expirySeconds"></param>
        /// <returns></returns>
        public ReadOnlySpan<byte> TryReset(bool soft, int expirySeconds = 60)
        {
            try
            {
                SuspendConfigMerge();

                // Reset recovery operations before proceeding with reset
                clusterProvider.replicationManager.ResetRecovery();

                var resp = CmdStrings.RESP_OK;
                while (true)
                {
                    var current = currentConfig;
                    var localSlots = current.GetSlotList(1);
                    if (clusterProvider.storeWrapper.HasKeysInSlots(localSlots))
                    {
                        return CmdStrings.RESP_ERR_RESET_WITH_KEYS_ASSIGNED;
                    }

                    this.clusterConnectionStore.CloseAll();

                    var newNodeId = soft ? current.LocalNodeId : Generator.CreateHexId();
                    var endpoint = clusterProvider.storeWrapper.GetClusterEndpoint();
                    var address = endpoint.Address.ToString();
                    var port = endpoint.Port;
                    var hostname = serverOptions.ClusterAnnounceHostname;

                    var configEpoch = soft ? current.LocalNodeConfigEpoch : 0;
                    var expiry = DateTimeOffset.UtcNow.Ticks + TimeSpan.FromSeconds(expirySeconds).Ticks;
                    var newConfig = new ClusterConfig().InitializeLocalWorker(
                        newNodeId,
                        address,
                        port,
                        configEpoch: configEpoch,
                        role: NodeRole.PRIMARY,
                        replicaOfNodeId: null,
                        hostname: string.IsNullOrEmpty(hostname) ? "" : hostname);
                    if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                        break;
                }
                FlushConfig();
                return resp;
            }
            finally
            {
                ResumeConfigMerge();
            }
        }

        /// <summary>
        /// Try to make this node a replica of node with nodeid
        /// </summary>
        /// <param name="nodeid"></param>
        /// <param name="force">If false, checks if node is clean (i.e. is PRIMARY without any assigned nodes) before making changes.</param>
        /// <param name="upgradeLock">If true, allows for a <see cref="RecoveryStatus.ReadRole"/> read lock to be upgraded to <see cref="RecoveryStatus.ClusterReplicate"/>.</param>
        /// <param name="errorMessage">The ASCII encoded error response if the method returned <see langword="false"/>; otherwise <see langword="default"/></param>
        /// <param name="logger"></param>
        public bool TryAddReplica(string nodeid, bool force, bool upgradeLock, out ReadOnlySpan<byte> errorMessage, ILogger logger = null)
        {
            Debug.Assert(
                !upgradeLock || clusterProvider.replicationManager.currentRecoveryStatus == RecoveryStatus.ReadRole,
                "Lock upgrades are only allowed if caller holds a ReadRole lock"
            );

            errorMessage = default;
            while (true)
            {
                var current = CurrentConfig;
                if (current.LocalNodeId.Equals(nodeid, StringComparison.OrdinalIgnoreCase))
                {
                    errorMessage = CmdStrings.RESP_ERR_GENERIC_CANNOT_REPLICATE_SELF;
                    logger?.LogError($"{nameof(TryAddReplica)}: {{logMessage}}", Encoding.ASCII.GetString(errorMessage));
                    return false;
                }

                if (!force && current.LocalNodeRole != NodeRole.PRIMARY)
                {
                    logger?.LogError("ERR I am already replica of {localNodePrimaryId}", current.LocalNodePrimaryId);
                    errorMessage = Encoding.ASCII.GetBytes($"ERR I am already replica of {current.LocalNodePrimaryId}.");
                    return false;
                }

                if (!force && current.HasAssignedSlots(1))
                {
                    errorMessage = CmdStrings.RESP_ERR_GENERIC_CANNOT_MAKE_REPLICA_WITH_ASSIGNED_SLOTS;
                    logger?.LogError($"{nameof(TryAddReplica)}: {{logMessage}}", Encoding.ASCII.GetString(errorMessage));
                    return false;
                }

                var workerId = current.GetWorkerIdFromNodeId(nodeid);
                if (workerId == 0)
                {
                    errorMessage = Encoding.ASCII.GetBytes($"ERR I don't know about node {nodeid}.");
                    logger?.LogError("ERR I don't know about node {nodeid}.", nodeid);
                    return false;
                }

                if (current.GetNodeRoleFromNodeId(nodeid) != NodeRole.PRIMARY)
                {
                    logger?.LogError("ERR Trying to replicate node ({nodeid}) that is not a primary.", nodeid);
                    errorMessage = Encoding.ASCII.GetBytes($"ERR Trying to replicate node ({nodeid}) that is not a primary.");
                    return false;
                }

                // Transition to recovering state
                // Only one caller will succeed in becoming a replica for the provided node-id
                if (!clusterProvider.replicationManager.BeginRecovery(RecoveryStatus.ClusterReplicate, upgradeLock))
                {
                    logger?.LogError($"{nameof(TryAddReplica)}: {{logMessage}}", Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_CANNOT_ACQUIRE_RECOVERY_LOCK));
                    errorMessage = CmdStrings.RESP_ERR_GENERIC_CANNOT_ACQUIRE_RECOVERY_LOCK;
                    return false;
                }

                var newConfig = currentConfig.MakeReplicaOf(nodeid).BumpLocalNodeConfigEpoch();
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;

                // If we reach here then we failed to update config so we need to suspend recovery and retry to update the config
                if (upgradeLock)
                {
                    clusterProvider.replicationManager.EndRecovery(RecoveryStatus.ReadRole, downgradeLock: true);
                }
                else
                {
                    clusterProvider.replicationManager.EndRecovery(RecoveryStatus.NoRecovery, downgradeLock: false);
                }
            }

            clusterProvider.storeWrapper.SuspendPrimaryOnlyTasks().Wait();
            clusterProvider.storeWrapper.StartReplicaTasks();

            FlushConfig();
            return true;
        }

        /// <summary>
        /// List replicas of specified primary with given nodeid
        /// </summary>
        /// <param name="nodeid"> Node-id string</param>
        /// <param name="clusterProvider">ClusterProvider instance</param>
        public List<string> ListReplicas(string nodeid, ClusterProvider clusterProvider)
        {
            var current = CurrentConfig;
            return current.GetReplicas(nodeid, clusterProvider);
        }
    }
}