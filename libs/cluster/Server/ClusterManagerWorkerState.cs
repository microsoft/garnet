﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
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
            long currentConfigEpoch,
            long lastVotedConfigEpoch,
            NodeRole role,
            string replicaOfNodeId,
            string hostname)
        {
            while (true)
            {
                var current = currentConfig;
                var newConfig = current.InitializeLocalWorker(nodeId, address, port, configEpoch, currentConfigEpoch, lastVotedConfigEpoch, role, replicaOfNodeId, hostname);
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
                PauseConfigMerge();
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
                UnpauseConfigMerge();
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
                PauseConfigMerge();
                var resp = CmdStrings.RESP_OK;

                while (true)
                {
                    var current = currentConfig;
                    var newNodeId = soft ? current.LocalNodeId : Generator.CreateHexId();
                    var address = current.LocalNodeIp;
                    var port = current.LocalNodePort;

                    var configEpoch = soft ? current.LocalNodeConfigEpoch : 0;
                    var currentConfigEpoch = soft ? current.LocalNodeCurrentConfigEpoch : 0;
                    var lastVotedConfigEpoch = soft ? current.LocalNodeLastVotedEpoch : 0;

                    var expiry = DateTimeOffset.UtcNow.Ticks + TimeSpan.FromSeconds(expirySeconds).Ticks;
                    foreach (var nodeId in current.GetRemoteNodeIds())
                        _ = workerBanList.AddOrUpdate(nodeId, expiry, (key, oldValue) => expiry);

                    var newConfig = new ClusterConfig().InitializeLocalWorker(
                        newNodeId,
                        address,
                        port,
                        configEpoch: configEpoch,
                        currentConfigEpoch: currentConfigEpoch,
                        lastVotedConfigEpoch: lastVotedConfigEpoch,
                        role: NodeRole.PRIMARY,
                        replicaOfNodeId: null,
                        Format.GetHostName());
                    if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                        break;
                }
                FlushConfig();
                return resp;
            }
            finally
            {
                UnpauseConfigMerge();
            }
        }

        /// <summary>
        /// Try to make this node a replica of node with nodeid
        /// </summary>
        /// <param name="nodeid"></param>
        /// <param name="force">Check if node is clean (i.e. is PRIMARY without any assigned nodes)</param>
        /// <param name="recovering"></param>
        /// <param name="errorMessage">The ASCII encoded error response if the method returned <see langword="false"/>; otherwise <see langword="default"/></param>
        /// <param name="logger"></param>
        public bool TryAddReplica(string nodeid, bool force, ref bool recovering, out ReadOnlySpan<byte> errorMessage, ILogger logger = null)
        {
            errorMessage = default;
            while (true)
            {
                var current = CurrentConfig;
                if (current.LocalNodeId.Equals(nodeid, StringComparison.OrdinalIgnoreCase))
                {
                    errorMessage = CmdStrings.RESP_ERR_GENERIC_CANNOT_REPLICATE_SELF;
                    logger?.LogError(Encoding.ASCII.GetString(errorMessage));
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
                    logger?.LogError(Encoding.ASCII.GetString(errorMessage));
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

                recovering = true;
                var newConfig = currentConfig.MakeReplicaOf(nodeid);
                newConfig = newConfig.BumpLocalNodeConfigEpoch();
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
            return true;
        }

        /// <summary>
        /// List replicas of specified primary with given nodeid
        /// </summary>
        /// <param name="nodeid"></param>
        public List<string> ListReplicas(string nodeid)
        {
            var current = CurrentConfig;
            return current.GetReplicas(nodeid);
        }
    }
}