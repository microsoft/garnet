// Copyright (c) Microsoft Corporation.
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
        /// Remove worker through the forget command.
        /// </summary>
        /// <param name="nodeid"></param>
        /// <param name="expirySeconds"></param>
        public ReadOnlySpan<byte> TryRemoveWorker(string nodeid, int expirySeconds)
        {
            try
            {
                PauseConfigMerge();
                ReadOnlySpan<byte> resp = CmdStrings.RESP_OK;
                while (true)
                {
                    var current = currentConfig;

                    if (current.GetLocalNodeId().Equals(nodeid))
                        return new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes("-ERR I tried hard but I can't forget myself.\r\n"));

                    if (current.GetNodeRoleFromNodeId(nodeid) == NodeRole.UNASSIGNED)
                        return new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR I don't know about node {nodeid}.\r\n"));

                    if (current.GetLocalNodeRole() == NodeRole.REPLICA && current.GetLocalNodePrimaryId().Equals(nodeid))
                        return new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Can't forget my primary!\r\n"));

                    var newConfig = current.RemoveWorker(nodeid);
                    long expiry = DateTimeOffset.UtcNow.Ticks + TimeSpan.FromSeconds(expirySeconds).Ticks;
                    workerBanList.AddOrUpdate(nodeid, expiry, (key, oldValue) => expiry);
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
                ReadOnlySpan<byte> resp = CmdStrings.RESP_OK;

                while (true)
                {
                    var current = currentConfig;
                    string newNodeId = soft ? current.GetLocalNodeId() : Generator.CreateHexId();
                    string address = current.GetLocalNodeIp();
                    int port = current.GetLocalNodePort();

                    long configEpoch = soft ? current.GetLocalNodeConfigEpoch() : 0;
                    long currentConfigEpoch = soft ? current.GetLocalNodeCurrentConfigEpoch() : 0;
                    long lastVotedConfigEpoch = soft ? current.GetLocalNodeLastVotedEpoch() : 0;

                    long expiry = DateTimeOffset.UtcNow.Ticks + TimeSpan.FromSeconds(expirySeconds).Ticks;
                    foreach (var nodeId in current.GetRemoteNodeIds())
                        workerBanList.AddOrUpdate(nodeId, expiry, (key, oldValue) => expiry);

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
        /// Make this node a replica of node with nodeid
        /// </summary>
        /// <param name="nodeid"></param>
        /// <param name="force">Check if node is clean (i.e. is PRIMARY without any assigned nodes)</param>
        /// <param name="recovering"></param>
        /// <param name="resp"></param>
        /// <param name="logger"></param>
        public bool TryAddReplica(string nodeid, bool force, ref bool recovering, out ReadOnlySpan<byte> resp, ILogger logger = null)
        {
            resp = CmdStrings.RESP_OK;
            while (true)
            {
                var current = CurrentConfig;
                //(error) ERR Can't replicate myself
                if (current.GetLocalNodeId().Equals(nodeid))
                {
                    logger?.LogError("-ERR Can't replicate myself");
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes("-ERR Can't replicate myself.\r\n"));
                    return false;
                }

                if (!force && current.GetLocalNodeRole() != NodeRole.PRIMARY)
                {
                    logger?.LogError("-ERR I am already replica of {localNodePrimaryId}", current.GetLocalNodePrimaryId());
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR I am already replica of {current.GetLocalNodePrimaryId()}.\r\n"));
                    return false;
                }

                if (!force && current.HasAssignedSlots(1))
                {
                    logger?.LogError("-ERR To set a master the node must be empty and without assigned slots.");
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes("-ERR To set a master the node must be empty and without assigned slots.\r\n"));
                    return false;
                }

                int workerId = current.GetWorkerIdFromNodeId(nodeid);
                if (workerId == 0)
                {
                    logger?.LogError("-ERR I don't know about node {nodeid}.", nodeid);
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR I don't know about node {nodeid}.\r\n"));
                    return false;
                }

                if (current.GetNodeRoleFromNodeId(nodeid) != NodeRole.PRIMARY)
                {
                    logger?.LogError("-ERR Trying to replicate node ({nodeid}) that is not a primary.", nodeid);
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Trying to replicate node ({nodeid}) that is not a primary.\r\n"));
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