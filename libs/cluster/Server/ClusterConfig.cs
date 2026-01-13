// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable IDE0005
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
#pragma warning restore IDE0005
using System.Runtime.CompilerServices;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using System.Net;
using System.Text;
using Garnet.server;

namespace Garnet.cluster
{
    /// <summary>
    /// Cluster configuration
    /// </summary>
    internal sealed partial class ClusterConfig
    {
        /// <summary>
        /// Reserved offset in workers array
        /// </summary>
        public const int RESERVED_WORKER_ID = 0;

        /// <summary>
        /// Reserved Worker offset in workers array.
        /// </summary>
        public const int LOCAL_WORKER_ID = 1;

        /// <summary>
        /// Minimum hash slot value.
        /// </summary>
        public const int MIN_HASH_SLOT_VALUE = 0;

        /// <summary>
        /// Maximum hash slot value.
        /// </summary>
        public const int MAX_HASH_SLOT_VALUE = 16384;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="slot"></param>
        /// <returns></returns>
        public static bool OutOfRange(int slot) => slot >= MAX_HASH_SLOT_VALUE || slot < MIN_HASH_SLOT_VALUE;

        /// <summary>
        /// Num of workers assigned
        /// </summary>
        public int NumWorkers => workers.Length - 1;

        readonly HashSlot[] slotMap;
        readonly Worker[] workers;

        /// <summary>
        /// Create default cluster config
        /// </summary>
        public ClusterConfig()
        {
            slotMap = new HashSlot[MAX_HASH_SLOT_VALUE];
            for (var i = 0; i < MAX_HASH_SLOT_VALUE; i++)
            {
                slotMap[i]._state = SlotState.OFFLINE;
                slotMap[i]._workerId = 0;
            }
            workers = new Worker[2];
            InitializeUnassignedWorker();
        }

        /// <summary>
        /// Create cluster config
        /// </summary>
        /// <param name="slotMap"></param>
        /// <param name="workers"></param>
        public ClusterConfig(HashSlot[] slotMap, Worker[] workers)
        {
            this.slotMap = slotMap;
            this.workers = workers;
            InitializeUnassignedWorker();
        }

        public ClusterConfig Copy()
        {
            var newSlotMap = new HashSlot[slotMap.Length];
            var newWorkers = new Worker[workers.Length];
            Array.Copy(workers, newWorkers, workers.Length);
            Array.Copy(slotMap, newSlotMap, slotMap.Length);
            return new ClusterConfig(newSlotMap, newWorkers);
        }

        /// <summary>
        /// Initialize the worker at index 0 as unassigned.
        /// </summary>
        private void InitializeUnassignedWorker()
        {
            workers[RESERVED_WORKER_ID].Address = "unassigned";
            workers[RESERVED_WORKER_ID].Port = 0;
            workers[RESERVED_WORKER_ID].Nodeid = null;
            workers[RESERVED_WORKER_ID].ConfigEpoch = 0;
            workers[RESERVED_WORKER_ID].Role = NodeRole.UNASSIGNED;
            workers[RESERVED_WORKER_ID].ReplicaOfNodeId = null;
            workers[RESERVED_WORKER_ID].ReplicationOffset = 0;
            workers[RESERVED_WORKER_ID].hostname = null;
        }

        /// <summary>
        /// Initialize local worker with provided information
        /// </summary>
        /// <param name="nodeId">Local worker node-id.</param>
        /// <param name="address">Local worker IP address.</param>
        /// <param name="port">Local worker port.</param>
        /// <param name="configEpoch">Local worker config epoch.</param>
        /// <param name="role">Local worker role.</param>
        /// <param name="replicaOfNodeId">Local worker primary id.</param>
        /// <param name="hostname">Local worker hostname.</param>
        /// <returns>Instance of local config with update local worker info.</returns>
        public ClusterConfig InitializeLocalWorker(
            string nodeId,
            string address,
            int port,
            long configEpoch,
            NodeRole role,
            string replicaOfNodeId,
            string hostname)
        {
            var newWorkers = new Worker[workers.Length];
            Array.Copy(workers, newWorkers, workers.Length);
            newWorkers[LOCAL_WORKER_ID].Address = address;
            newWorkers[LOCAL_WORKER_ID].Port = port;
            newWorkers[LOCAL_WORKER_ID].Nodeid = nodeId;
            newWorkers[LOCAL_WORKER_ID].ConfigEpoch = configEpoch;
            newWorkers[LOCAL_WORKER_ID].Role = role;
            newWorkers[LOCAL_WORKER_ID].ReplicaOfNodeId = replicaOfNodeId;
            newWorkers[LOCAL_WORKER_ID].ReplicationOffset = 0;
            newWorkers[LOCAL_WORKER_ID].hostname = hostname;
            return new ClusterConfig(slotMap, newWorkers);
        }

        /// <summary>
        /// Check if workerId has assigned slots
        /// </summary>
        /// <param name="workerId">Offset in worker list.</param>
        /// <returns>True if worker has assigned slots, false otherwise.</returns>
        public bool HasAssignedSlots(ushort workerId)
        {
            for (ushort i = 0; i < 16384; i++)
            {
                if (slotMap[i].workerId == workerId)
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Check if the provided  slot is local from the perspective of the local config.
        /// 1. Local slots are assigned to workerId = 1
        /// 2. Local slots which are in migrating state are pointing to the target node thus workerdId != 1. 
        ///     However, we still need to redirect traffic as if the workerId == 1 until migration completes
        /// 3. Local slots for a replica are those slots served by its primary only for read operations
        /// </summary>
        /// <param name="slot">Slot to check</param>
        /// <param name="readWriteSession">Used to override write restrictions for non-local slots that are replicas of the slot owner</param>
        /// <returns>True if slot is owned by this node, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsLocal(ushort slot, bool readWriteSession = true)
            => slotMap[slot].workerId == LOCAL_WORKER_ID || IsLocalExpensive(slot, readWriteSession);

        /// <summary>
        /// If slot in MIGRATE state then it must have been set by original owner, so we keep treating it like a local slot and serve requests if the key has not yet migrated.
        /// If it is a read command and this is a replica the associated slot should be assigned to this node's primary in order for the read request to be served.
        /// </summary>
        /// <param name="slot"></param>
        /// <param name="readWriteSession"></param>
        /// <returns></returns>
        private bool IsLocalExpensive(ushort slot, bool readWriteSession)
            => slotMap[slot]._state == SlotState.MIGRATING ||
            (readWriteSession &&
            workers[1].Role == NodeRole.REPLICA &&
            slotMap[slot]._workerId > 1 &&
            LocalNodePrimaryId != null &&
            workers[slotMap[slot]._workerId].Nodeid.Equals(LocalNodePrimaryId, StringComparison.OrdinalIgnoreCase));

        /// <summary>
        /// Check if specified node-id belongs to a node in our local config.
        /// </summary>
        /// <param name="nodeid">Node id to search for.</param>
        /// <returns>True if node-id in worker list, false otherwise.</returns>
        public bool IsKnown(string nodeid)
        {
            for (var i = 1; i <= NumWorkers; i++)
                if (workers[i].Nodeid.Equals(nodeid, StringComparison.OrdinalIgnoreCase))
                    return true;
            return false;
        }

        /// <summary>
        /// Check if local node is a PRIMARY node
        /// </summary>
        public bool IsPrimary => LocalNodeRole == NodeRole.PRIMARY;

        /// <summary>
        /// Check if local node is a REPLICA node
        /// </summary>
        public bool IsReplica => LocalNodeRole == NodeRole.REPLICA;

        #region GetLocalNodeInfo
        /// <summary>
        /// Get local node ip
        /// </summary>
        /// <returns>IP of local worker</returns>
        public string LocalNodeIp => workers[LOCAL_WORKER_ID].Address;

        /// <summary>
        /// Get local node port
        /// </summary>
        /// <returns>Port of local worker</returns>
        public int LocalNodePort => workers[LOCAL_WORKER_ID].Port;

        /// <summary>
        /// Get local node ID
        /// </summary>
        /// <returns>Node-id of local worker.</returns>
        public string LocalNodeId => workers[LOCAL_WORKER_ID].Nodeid;

        /// <summary>
        /// NOTE: Use this only for logging not comparison
        /// Get short local node ID
        /// </summary>
        /// <returns>Short node-id of local worker.</returns>
        public string LocalNodeIdShort => workers[LOCAL_WORKER_ID].Nodeid.Substring(0, 8);

        /// <summary>
        /// Get local node role
        /// </summary>
        /// <returns>Role of local node.</returns>
        public NodeRole LocalNodeRole => workers[LOCAL_WORKER_ID].Role;

        /// <summary>
        /// Get nodeid of primary.
        /// </summary>
        /// <returns>Primary-id of the node this node is replicating.</returns>
        public string LocalNodePrimaryId => workers[LOCAL_WORKER_ID].ReplicaOfNodeId;

        /// <summary>
        /// Get config epoch for local worker.
        /// </summary>
        /// <returns>Config epoch of local node.</returns>
        public long LocalNodeConfigEpoch => workers[LOCAL_WORKER_ID].ConfigEpoch;

        /// <summary>
        /// Return endpoint of primary if this node is a replica.
        /// </summary>
        /// <returns>Returns primary endpoints if this node is a replica, otherwise (null,-1)</returns>
        public (string address, int port) GetLocalNodePrimaryAddress() => GetWorkerAddressFromNodeId(workers[LOCAL_WORKER_ID].ReplicaOfNodeId);

        /// <summary>
        /// Get local node replicas
        /// </summary>
        /// <returns>Returns a list of node-ids representing the replicas that replicate this node.</returns>
        public List<string> GetLocalNodeReplicaIds() => GetReplicaIds(LocalNodeId);

        /// <summary>
        /// Get list of endpoints for all replicas of this node.
        /// </summary>
        /// <returns>List of (address,port) pairs.</returns>
        public List<IPEndPoint> GetLocalNodeReplicaEndpoints()
        {
            List<IPEndPoint> replicas = new();
            for (ushort i = 2; i < workers.Length; i++)
            {
                var replicaOf = workers[i].ReplicaOfNodeId;
                if (replicaOf != null && replicaOf.Equals(workers[LOCAL_WORKER_ID].Nodeid, StringComparison.OrdinalIgnoreCase))
                    replicas.Add(new(IPAddress.Parse(workers[i].Address), workers[i].Port));
            }
            return replicas;
        }

        /// <summary>
        /// Return all primary endpoints. Used from replica that is becoming a primary during a failover.
        /// </summary>
        /// <param name="includeMyPrimaryFirst"></param>
        /// <returns>List of pairs (address,port) representing known primary endpoints</returns>
        public List<IPEndPoint> GetLocalNodePrimaryEndpoints(bool includeMyPrimaryFirst = false)
        {
            var myPrimaryId = includeMyPrimaryFirst ? LocalNodePrimaryId : "";
            var primaries = new List<IPEndPoint>();
            for (ushort i = 2; i < workers.Length; i++)
            {
                if (workers[i].Role == NodeRole.PRIMARY && !workers[i].Nodeid.Equals(myPrimaryId, StringComparison.OrdinalIgnoreCase))
                    primaries.Add(new(IPAddress.Parse(workers[i].Address), workers[i].Port));

                if (workers[i].Nodeid.Equals(myPrimaryId, StringComparison.OrdinalIgnoreCase))
                    primaries.Insert(0, new(IPAddress.Parse(workers[i].Address), workers[i].Port));
            }
            return primaries;
        }

        /// <summary>
        /// Retrieve a list of slots served by this node's primary.
        /// </summary>
        /// <returns>List of slots.</returns>
        public List<int> GetLocalPrimarySlots()
        {
            var primaryId = LocalNodePrimaryId;
            List<int> slots = [];

            if (primaryId != null)
            {
                for (var i = 0; i < MAX_HASH_SLOT_VALUE; i++)
                {
                    if (slotMap[i].workerId > 0 && workers[slotMap[i].workerId].Nodeid.Equals(primaryId, StringComparison.OrdinalIgnoreCase))
                        slots.Add(i);
                }
            }
            return slots;
        }

        /// <summary>
        /// Find maximum config epoch from local config
        /// </summary>
        /// <returns>Integer representing max config epoch value.</returns>
        public long GetMaxConfigEpoch()
        {
            long mx = 0;
            for (var i = 1; i <= NumWorkers; i++)
                mx = Math.Max(workers[i].ConfigEpoch, mx);
            return mx;
        }

        /// <summary>
        /// Retrieve list of all known node ids.
        /// </summary>
        /// <returns>List of strings representing known node ids.</returns>
        public List<string> GetRemoteNodeIds()
        {
            var remoteNodeIds = new List<string>();
            for (int i = 2; i < workers.Length; i++)
                remoteNodeIds.Add(workers[i].Nodeid);
            return remoteNodeIds;
        }
        #endregion

        #region GetFromNodeId
        /// <summary>
        /// Get worker id from node id.
        /// </summary>
        /// <param name="nodeId"></param>
        /// <returns>Integer representing offset of worker in worker list.</returns>
        public ushort GetWorkerIdFromNodeId(string nodeId)
        {
            for (ushort i = 1; i <= NumWorkers; i++)
            {
                if (workers[i].Nodeid.Equals(nodeId, StringComparison.OrdinalIgnoreCase))
                    return i;
            }
            return 0;
        }

        /// <summary>
        /// Get role from node-id.
        /// </summary>
        /// <param name="nodeId"></param>
        /// <returns>Node role type</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public NodeRole GetNodeRoleFromNodeId(string nodeId) => workers[GetWorkerIdFromNodeId(nodeId)].Role;

        /// <summary>
        /// Get worker from node-id.
        /// </summary>
        /// <param name="nodeId"></param>
        /// <returns>Worker struct</returns>
        public Worker GetWorkerFromNodeId(string nodeId) => workers[GetWorkerIdFromNodeId(nodeId)];

        /// <summary>
        /// Get worker (IP address and port) for node-id.
        /// </summary>
        /// <param name="nodeId"></param>
        /// <returns>Pair of (string,int) representing worker endpoint.</returns>
        public (string address, int port) GetWorkerAddressFromNodeId(string nodeId)
        {
            if (nodeId == null)
                return (null, -1);
            var workerId = GetWorkerIdFromNodeId(nodeId);
            return workerId == 0 ? (null, -1) : (workers[workerId].Address, workers[workerId].Port);
        }

        /// <summary>
        /// Get hostname from node-id.
        /// </summary>
        /// <param name="nodeId"></param>
        /// <returns>String representing node's hostname.</returns>
        public string GetHostNameFromNodeId(string nodeId)
        {
            if (nodeId == null)
                return null;
            var workerId = GetWorkerIdFromNodeId(nodeId);
            return workerId == 0 ? null : workers[workerId].hostname;
        }
        #endregion

        #region GetFromSlot

        /// <summary>
        /// Check if slot is set as IMPORTING
        /// </summary>
        /// <param name="slot">Slot number.</param>
        /// <returns>True if slot is in IMPORTING state, false otherwise.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsImportingSlot(ushort slot) => slotMap[slot]._state == SlotState.IMPORTING;

        /// <summary>
        /// Check if slot is set as MIGRATING
        /// </summary>
        /// <param name="slot">Slot number.</param>
        /// <returns>True if slot is in MIGRATING state, false otherwise.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsMigratingSlot(ushort slot) => slotMap[slot]._state == SlotState.MIGRATING;

        /// <summary>
        /// Get slot state
        /// </summary>
        /// <param name="slot">Slot number.</param>
        /// <returns>SlotState type</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public SlotState GetState(ushort slot) => slotMap[slot]._state;

        /// <summary>
        /// Get worker offset in worker list from slot.
        /// </summary>
        /// <param name="slot">Slot number.</param>
        /// <returns>Integer offset in worker list.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetWorkerIdFromSlot(ushort slot) => slotMap[slot].workerId;

        /// <summary>
        /// Get node-id of slot owner.
        /// </summary>
        /// <param name="slot">Slot number.</param>
        /// <returns>String node-id</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string GetNodeIdFromSlot(ushort slot) => workers[GetWorkerIdFromSlot(slot)].Nodeid;

        /// <summary>
        /// Get node-id of slot owner.
        /// </summary>
        /// <param name="slot">Slot number.</param>
        /// <returns>String node-id</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string GetOwnerIdFromSlot(ushort slot) => workers[slotMap[slot]._workerId].Nodeid;

        /// <summary>
        /// Get endpoint of slot owner.
        /// </summary>
        /// <param name="slot">Slot number.</param>
        /// <returns>Pair of (string,integer) representing endpoint.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (string endpoint, int port) GetEndpointFromSlot(ushort slot, ClusterPreferredEndpointType type)
        {
            var workerId = GetWorkerIdFromSlot(slot);

            return (GetEndpointByPreferredType(workerId, type), workers[workerId].Port);
        }

        /// <summary>
        /// Get endpoint of node to which slot is migrating.
        /// </summary>
        /// <param name="slot">Slot number.</param>
        /// <returns>Pair of (string,integer) representing endpoint.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (string endpoint, int port) AskEndpointFromSlot(ushort slot, ClusterPreferredEndpointType type)
        {
            var workerId = slotMap[slot]._workerId;

            return (GetEndpointByPreferredType(workerId, type), workers[workerId].Port);
        }

        private string GetEndpointByPreferredType(int workerId, ClusterPreferredEndpointType type)
        {
            return type switch
            {
                ClusterPreferredEndpointType.Ip => workers[workerId].Address,
                ClusterPreferredEndpointType.Hostname => string.IsNullOrEmpty(workers[workerId].hostname) ? "?" : workers[workerId].hostname,
                _ => "?"
            };
        }

        /// <summary>
        /// Get endpoint of node from node-id.
        /// </summary>
        /// <param name="nodeid">Node-id.</param>
        /// <returns>Pair of (string,integer) representing endpoint.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IPEndPoint GetEndpointFromNodeId(string nodeid)
        {
            var workerId = GetWorkerIdFromNodeId(nodeid);
            return new(IPAddress.Parse(workers[workerId].Address), workers[workerId].Port);
        }
        #endregion

        /// <summary>
        /// Get formatted (using CLUSTER NODES format) cluster info.
        /// </summary>
        /// <returns>Formatted string.</returns>
        public string GetClusterInfo(ClusterProvider clusterProvider)
        {
            var nodesStringBuilder = new StringBuilder();
            for (ushort i = 1; i <= NumWorkers; i++)
            {
                var info = default(ConnectionInfo);
                _ = clusterProvider?.clusterManager?.GetConnectionInfo(workers[i].Nodeid, out info);
                GetNodeInfo(i, info, nodesStringBuilder);
            }
            return nodesStringBuilder.ToString();
        }

        /// <summary>
        /// Get formatted (using CLUSTER NODES format) worker info.
        /// </summary>
        /// <param name="workerId">Offset of worker in the worker list.</param>
        /// <param name="info">Connection information for the corresponding worker.</param>
        /// <returns>Formatted string.</returns>
        public string GetNodeInfo(ushort workerId, ConnectionInfo info)
        {
            //<id>
            //<ip:port@cport[,hostname[,auxiliary_field=value]*]>
            //<flags>
            //<primary>
            //<ping-sent>
            //<pong-recv>
            //<config-epoch>
            //<link-state>
            //<slot> <slot> ... <slot>
            var nodeInfoStringBuilder = new StringBuilder();
            GetNodeInfo(workerId, info, nodeInfoStringBuilder);
            return nodeInfoStringBuilder.ToString();
        }

        private void GetNodeInfo(ushort workerId, ConnectionInfo info, StringBuilder nodeInfoStringBuilder)
        {
            _ = nodeInfoStringBuilder
                .Append(workers[workerId].Nodeid).Append(' ')
                .Append(workers[workerId].Address).Append(':').Append(workers[workerId].Port)
                .Append('@').Append(workers[workerId].Port + 10000).Append(',').Append(workers[workerId].hostname).Append(' ')
                .Append(workerId == 1 ? "myself," : "")
                .Append(workers[workerId].Role == NodeRole.PRIMARY ? "master" : "slave").Append(' ')
                .Append(workers[workerId].Role == NodeRole.REPLICA ? workers[workerId].ReplicaOfNodeId : '-').Append(' ')
                .Append(info.ping).Append(' ')
                .Append(info.pong).Append(' ')
                .Append(workers[workerId].ConfigEpoch).Append(' ')
                .Append(info.connected || workerId == 1 ? "connected" : "disconnected");
            AppendSlotRange(nodeInfoStringBuilder, workerId);
            AppendSpecialStates(nodeInfoStringBuilder, workerId);
            _ = nodeInfoStringBuilder.Append('\n');

            void AppendSlotRange(StringBuilder stringBuilder, uint workerId)
            {
                ushort start = ushort.MaxValue, end = 0;
                for (ushort i = 0; i < MAX_HASH_SLOT_VALUE; i++)
                {
                    if (slotMap[i].workerId == workerId)
                    {
                        if (i < start) start = i;
                        if (i > end) end = i;
                    }
                    else
                    {
                        if (start != ushort.MaxValue)
                        {
                            if (end == start) stringBuilder.Append($" {start}");
                            else stringBuilder.Append($" {start}-{end}");
                            start = ushort.MaxValue;
                            end = 0;
                        }
                    }
                }
                if (start != ushort.MaxValue)
                {
                    if (end == start) stringBuilder.Append($" {start}");
                    else stringBuilder.Append($" {start}-{end}");
                }
            }

            void AppendSpecialStates(StringBuilder stringBuilder, uint workerId)
            {
                // Only print special states for local node
                if (workerId != LOCAL_WORKER_ID) return;
                for (var slot = 0; slot < slotMap.Length; slot++)
                {
                    var _workerId = slotMap[slot]._workerId;
                    var _state = slotMap[slot]._state;

                    if (_state == SlotState.STABLE) continue;
                    if (_workerId > NumWorkers) continue;

                    var _nodeId = workers[_workerId].Nodeid;
                    if (_nodeId == null) continue;

                    stringBuilder.Append(_state switch
                    {
                        SlotState.MIGRATING => $" [{slot}->-{_nodeId}]",
                        SlotState.IMPORTING => $" [{slot}-<-{_nodeId}]",
                        _ => ""
                    });
                }
                return;
            }
        }

        /// <summary>
        /// Get shard slot ranges for worker.
        /// </summary>
        /// <param name="workerId">Offset of worker in worker list.</param>
        /// <returns>List of pairs representing slot ranges.</returns>
        public List<(ushort, ushort)> GetShardRanges(int workerId)
        {
            List<(ushort, ushort)> ranges = new();
            var startRange = ushort.MaxValue;
            ushort endRange;
            for (ushort i = 0; i < MAX_HASH_SLOT_VALUE + 1; i++)
            {
                if (i < slotMap.Length && slotMap[i].workerId == workerId)
                    startRange = startRange == ushort.MaxValue ? i : startRange;
                else if (startRange != ushort.MaxValue)
                {
                    endRange = (ushort)(i - 1);
                    ranges.Add(new(startRange, endRange));
                    startRange = ushort.MaxValue;
                }
            }
            return ranges;
        }

        /// <summary>
        /// Get worker offset in worker list for replicas of the given worker offset.
        /// </summary>
        /// <param name="workerId">Offset of worker in worker list.</param>
        /// <returns>List of worker offsets.</returns>
        public List<int> GetWorkerReplicas(int workerId)
        {
            var primaryId = workers[workerId].Nodeid;
            List<int> replicaWorkerIds = [];
            for (ushort i = 1; i <= NumWorkers; i++)
            {
                var replicaOf = workers[i].ReplicaOfNodeId;
                if (replicaOf != null && replicaOf.Equals(primaryId, StringComparison.OrdinalIgnoreCase))
                    replicaWorkerIds.Add(i);
            }
            return replicaWorkerIds;
        }

        /// <summary>
        /// Get formatted (using CLUSTER SHARDS format) cluster config information.
        /// </summary>
        /// <param name="clusterConnection"></param>
        /// <returns>RESP formatted string</returns>
        public string GetShardsInfo(GarnetClusterConnectionStore clusterConnection)
        {
            var shardsInfo = "";
            var shardCount = 0;
            for (ushort i = 1; i <= NumWorkers; i++)
            {
                if (workers[i].Role == NodeRole.PRIMARY)
                {
                    var shardRanges = GetShardRanges(i);
                    var replicaWorkerIds = GetWorkerReplicas(i);
                    shardsInfo += CreateFormattedShardInfo(i, shardRanges, replicaWorkerIds);
                    shardCount++;
                }
            }
            shardsInfo = $"*{shardCount}\r\n" + shardsInfo;
            return shardsInfo;

            string CreateFormattedShardInfo(int primaryWorkerId, List<(ushort, ushort)> shardRanges, List<int> replicaWorkerIds)
            {
                var shardInfo = $"*4\r\n";
                shardInfo += $"$5\r\nslots\r\n";
                shardInfo += $"*{shardRanges.Count * 2}\r\n";
                for (var i = 0; i < shardRanges.Count; i++)
                {
                    var range = shardRanges[i];
                    shardInfo += $":{range.Item1}\r\n";
                    shardInfo += $":{range.Item2}\r\n";
                }

                shardInfo += $"$5\r\nnodes\r\n";
                shardInfo += $"*{1 + replicaWorkerIds.Count}\r\n";
                if (primaryWorkerId == 1)
                    shardInfo += CreateFormattedNodeInfo(primaryWorkerId, true);
                else
                {
                    _ = clusterConnection.GetConnectionInfo(workers[primaryWorkerId].Nodeid, out var info);
                    shardInfo += CreateFormattedNodeInfo(primaryWorkerId, info.connected);
                }
                foreach (var id in replicaWorkerIds)
                {
                    _ = clusterConnection.GetConnectionInfo(workers[id].Nodeid, out var info);
                    shardInfo += CreateFormattedNodeInfo(id, info.connected);
                }

                return shardInfo;

                string CreateFormattedNodeInfo(int workerId, bool connected)
                {
                    var nodeInfo = "*12\r\n";
                    nodeInfo += "$2\r\nid\r\n";
                    nodeInfo += $"$40\r\n{workers[workerId].Nodeid}\r\n";
                    nodeInfo += "$4\r\nport\r\n";
                    nodeInfo += $":{workers[workerId].Port}\r\n";
                    nodeInfo += "$7\r\naddress\r\n";
                    nodeInfo += $"${workers[workerId].Address.Length}\r\n{workers[workerId].Address}\r\n";
                    nodeInfo += "$4\r\nrole\r\n";
                    nodeInfo += $"${workers[workerId].Role.ToString().Length}\r\n{workers[workerId].Role}\r\n";
                    nodeInfo += "$18\r\nreplication-offset\r\n";
                    nodeInfo += $":{workers[workerId].ReplicationOffset}\r\n";
                    nodeInfo += "$6\r\nhealth\r\n";
                    nodeInfo += connected ? "$6\r\nonline\r\n" : "$7\r\noffline\r\n";
                    return nodeInfo;
                }
            }
        }

        private string CreateFormattedSlotInfo(
            int slotStart,
            int slotEnd,
            string ipAddress,
            int port,
            string nodeid,
            string hostname,
            List<string> replicaIds,
            ClusterPreferredEndpointType preferredEndpointType)
        {
            int countA = replicaIds.Count == 0 ? 3 : 3 + replicaIds.Count;
            var rangeInfo = $"*{countA}\r\n";

            rangeInfo += $":{slotStart}\r\n";
            rangeInfo += $":{slotEnd}\r\n";

            rangeInfo += CreateNodeNetworkingInfo(ipAddress, port, nodeid, hostname, preferredEndpointType);

            foreach (var replicaId in replicaIds)
            {
                var (replicaIp, replicaPort) = GetWorkerAddressFromNodeId(replicaId);
                var replicaHostname = GetHostNameFromNodeId(replicaId);
                rangeInfo += CreateNodeNetworkingInfo(replicaIp, replicaPort, replicaId, replicaHostname, preferredEndpointType);
            }

            return rangeInfo;
        }

        private string CreateNodeNetworkingInfo(
            string ipAddress,
            int port,
            string nodeid,
            string hostname,
            ClusterPreferredEndpointType preferredEndpointType)
        {
            var nodeInfo = "*4\r\n";

            var preferredEndpoint = preferredEndpointType switch
            {
                ClusterPreferredEndpointType.Ip => string.IsNullOrEmpty(ipAddress) ? null : ipAddress,
                ClusterPreferredEndpointType.Hostname => string.IsNullOrEmpty(hostname) ? "?" : hostname,
                ClusterPreferredEndpointType.UnknownEndpoint => null,
                _ => null
            };

            nodeInfo += FormatValueOrNull(preferredEndpoint);
            nodeInfo += $":{port}\r\n";
            nodeInfo += $"${nodeid.Length}\r\n{nodeid}\r\n";

            var metaPairs = new List<(string, string)>();
            if (preferredEndpointType != ClusterPreferredEndpointType.Ip)
            {
                metaPairs.Add(("ip", ipAddress));
            }
            if (preferredEndpointType != ClusterPreferredEndpointType.Hostname)
            {
                metaPairs.Add(("hostname", hostname));
            }

            nodeInfo += $"*{metaPairs.Count * 2}\r\n";
            foreach (var (k, v) in metaPairs)
            {
                nodeInfo += $"${k.Length}\r\n{k}\r\n";
                nodeInfo += FormatValueOrNull(v);
            }

            return nodeInfo;
        }

        private string FormatValueOrNull(string value)
        {
            if (string.IsNullOrEmpty(value)) return "$-1\r\n";
            return $"${value.Length}\r\n{value}\r\n";
        }

        /// <summary>
        /// Get formatted (using CLUSTER SLOTS format) cluster config info.
        /// Ip, endpoint is ip address
        /// hostname may be null and included in metadata
        /// Hostname, endpoint is hostname
        /// if hostname is not existing, endpoint is "?"
        /// metadata includes ip address
        /// UnknownEndpoint, endpoint is null
        /// hostname and ip address are included in metadata
        /// </summary>
        /// <returns>Formatted string.</returns>
        public string GetSlotsInfo(ClusterPreferredEndpointType preferredEndpointType)
        {
            string completeSlotInfo = "";
            int slotRanges = 0;
            int slotStart;
            int slotEnd;

            for (slotStart = 0; slotStart < slotMap.Length; slotStart++)
            {
                if (slotMap[slotStart]._state == SlotState.OFFLINE)
                    continue;

                for (slotEnd = slotStart; slotEnd < slotMap.Length; slotEnd++)
                {
                    if (slotMap[slotEnd]._state == SlotState.OFFLINE || slotMap[slotStart].workerId != slotMap[slotEnd].workerId)
                        break;
                }

                int currSlotWorkerId = slotMap[slotStart].workerId;
                var address = workers[currSlotWorkerId].Address;
                var port = workers[currSlotWorkerId].Port;
                var nodeid = workers[currSlotWorkerId].Nodeid;
                var hostname = workers[currSlotWorkerId].hostname;
                var replicas = GetReplicaIds(nodeid);
                slotEnd--;
                completeSlotInfo += CreateFormattedSlotInfo(slotStart, slotEnd, address, port, nodeid, hostname, replicas, preferredEndpointType);
                slotRanges++;
                slotStart = slotEnd;
            }
            completeSlotInfo = $"*{slotRanges}\r\n" + completeSlotInfo;
            //Console.WriteLine(completeSlotInfo);

            return completeSlotInfo;
        }

        /// <summary>
        /// Retrieve a list of slots served by this node.
        /// </summary>
        /// <returns>List of slots.</returns>
        public List<int> GetSlotList(ushort workerId)
        {
            List<int> result = [];
            for (var i = 0; i < MAX_HASH_SLOT_VALUE; i++)
                if (slotMap[i].workerId == workerId) result.Add(i);
            return result;
        }

        /// <summary>
        /// Get Replicas for node-id.
        /// </summary>
        /// <param name="nodeid">Node-id string.</param>
        /// <param name="clusterProvider">ClusterProvider instance.</param>
        /// <returns></returns>
        public List<string> GetReplicas(string nodeid, ClusterProvider clusterProvider)
        {
            List<string> replicas = [];
            for (ushort i = 1; i < workers.Length; i++)
            {
                var nodeId = workers[i].Nodeid;
                var replicaOf = workers[i].ReplicaOfNodeId;
                if (replicaOf != null && replicaOf.Equals(nodeid, StringComparison.OrdinalIgnoreCase))
                {
                    var info = default(ConnectionInfo);
                    _ = clusterProvider?.clusterManager?.GetConnectionInfo(nodeId, out info);
                    replicas.Add(GetNodeInfo(i, info));
                }
            }
            return replicas;
        }

        /// <summary>
        /// Get all know node ids
        /// </summary>
        public void GetAllNodeIds(out List<(string NodeId, IPEndPoint EndPoint)> allNodeIds)
        {
            allNodeIds = [];
            for (ushort i = 2; i < workers.Length; i++)
                allNodeIds.Add((workers[i].Nodeid, new IPEndPoint(IPAddress.Parse(workers[i].Address), workers[i].Port)));
        }

        /// <summary>
        /// Get node-ids for nodes in the local shard
        /// </summary>
        /// <returns></returns>
        public void GetNodeIdsForShard(out List<(string NodeId, IPEndPoint EndPoint)> shardNodeIds)
        {
            var primaryId = LocalNodeRole == NodeRole.PRIMARY ? LocalNodeId : workers[1].ReplicaOfNodeId;
            shardNodeIds = [];
            for (ushort i = 2; i < workers.Length; i++)
            {
                var replicaOf = workers[i].ReplicaOfNodeId;
                if (primaryId != null && ((replicaOf != null && replicaOf.Equals(primaryId, StringComparison.OrdinalIgnoreCase)) || primaryId.Equals(workers[i].Nodeid)))
                    shardNodeIds.Add((workers[i].Nodeid, new IPEndPoint(IPAddress.Parse(workers[i].Address), workers[i].Port)));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="nodeid"></param>
        /// <returns></returns>
        public List<string> GetReplicaIds(string nodeid)
        {
            List<string> replicas = [];
            for (ushort i = 1; i < workers.Length; i++)
            {
                var replicaOf = workers[i].ReplicaOfNodeId;
                if (replicaOf != null && replicaOf.Equals(nodeid, StringComparison.OrdinalIgnoreCase))
                    replicas.Add(workers[i].Nodeid);
            }
            return replicas;
        }

        public List<(string, int)> GetReplicaEndpoints(string nodeid)
        {
            List<(string, int)> replicaEndpoints = [];
            for (ushort i = 1; i < workers.Length; i++)
            {
                var replicaOf = workers[i].ReplicaOfNodeId;
                if (replicaOf != null && replicaOf.Equals(nodeid, StringComparison.OrdinalIgnoreCase))
                    replicaEndpoints.Add(new(workers[i].Address, workers[i].Port));
            }
            return replicaEndpoints;
        }

        /// <summary>
        /// Get worker (IP address and port) for workerId
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (string address, int port) GetWorkerAddress(ushort workerId)
        {
            var w = workers[workerId];
            return (w.Address, w.Port);
        }

        /// <summary>
        /// Return list of triples containing node-id,address,port.
        /// </summary>
        /// <returns>List of triplets.</returns>
        public List<(string, string, int)> GetWorkerInfoForGossip()
        {
            List<(string, string, int)> result = [];
            for (var i = 2; i < workers.Length; i++)
                result.Add((workers[i].Nodeid, workers[i].Address, workers[i].Port));
            return result;
        }

        /// <summary>
        /// Return count of slots in given state.
        /// </summary>
        /// <param name="slotState">SlotState type.</param>
        /// <returns>Integer representing count of slots in given state.</returns>
        public int GetSlotCountForState(SlotState slotState)
        {
            var count = 0;
            for (var i = 0; i < slotMap.Length; i++)
                count += slotMap[i]._state == slotState ? 1 : 0;
            return count;
        }

        /// <summary>
        /// Return number of primary nodes.
        /// </summary>
        /// <returns>Integer representing number of primary nodes.</returns>
        public int GetPrimaryCount()
        {
            var count = 0;
            for (ushort i = 1; i <= NumWorkers; i++)
            {
                var w = workers[i];
                count += w.Role == NodeRole.PRIMARY ? 1 : 0;
            }
            return count;
        }

        /// <summary>
        /// Get worker (IP address and port) for node-id.
        /// </summary>
        /// <param name="address">IP address string.</param>
        /// <param name="port">Port number.</param>
        /// <returns>String representing node-id matching endpoint.</returns>
        public string GetWorkerNodeIdFromAddress(string address, int port)
        {
            for (ushort i = 1; i <= NumWorkers; i++)
            {
                var w = workers[i];
                if (w.Address == address && w.Port == port)
                    return w.Nodeid;
            }
            return null;
        }

        /// <summary>
        /// Update replication offset lazily.
        /// </summary>
        /// <param name="newReplicationOffset">Long of new replication offset.</param>
        public void LazyUpdateLocalReplicationOffset(long newReplicationOffset)
            => workers[LOCAL_WORKER_ID].ReplicationOffset = newReplicationOffset;

        /// <summary>
        /// Merging incoming configuration from gossip with local configuration copy.
        /// </summary>
        /// <param name="senderConfig">Sender config object.</param>
        /// <param name="workerBanList">Worker ban list used to prevent merging.</param>
        /// <param name="logger">Logger instance</param>
        /// <returns>Cluster config object.</returns>
        public ClusterConfig Merge(ClusterConfig senderConfig, ConcurrentDictionary<string, long> workerBanList, ILogger logger = null)
        {
            var localId = LocalNodeId;
            var newConfig = this;
            for (ushort i = 1; i < senderConfig.NumWorkers + 1; i++)
            {
                // Do not update local node config
                if (localId.Equals(senderConfig.workers[i].Nodeid, StringComparison.OrdinalIgnoreCase))
                    continue;

                // Skip any nodes scheduled for deletion
                if (workerBanList.ContainsKey(senderConfig.workers[i].Nodeid))
                    continue;

                newConfig = newConfig.MergeWorkerInfo(senderConfig.workers[i]);
            }

            return newConfig.MergeSlotMap(senderConfig, logger);
        }

        private ClusterConfig MergeWorkerInfo(Worker worker)
        {
            ushort workerId = RESERVED_WORKER_ID;
            // Find workerId offset from my local configuration
            for (var i = 1; i < workers.Length; i++)
            {
                if (workers[i].Nodeid.Equals(worker.Nodeid, StringComparison.OrdinalIgnoreCase))
                {
                    // Update only if received config epoch is strictly greater
                    if (worker.ConfigEpoch <= workers[i].ConfigEpoch) return this;
                    workerId = (ushort)i;
                    break;
                }
            }

            var newWorkers = workers;
            // Check if we need to add worker to the known workers list
            if (workerId == RESERVED_WORKER_ID)
            {
                newWorkers = new Worker[workers.Length + 1];
                workerId = (ushort)workers.Length;
                Array.Copy(workers, newWorkers, workers.Length);
            }

            // Insert or update worker information
            newWorkers[workerId].Address = worker.Address;
            newWorkers[workerId].Port = worker.Port;
            newWorkers[workerId].Nodeid = worker.Nodeid;
            newWorkers[workerId].ConfigEpoch = worker.ConfigEpoch;
            newWorkers[workerId].Role = worker.Role;
            newWorkers[workerId].ReplicaOfNodeId = worker.ReplicaOfNodeId;
            newWorkers[workerId].hostname = worker.hostname;

            return new(slotMap, newWorkers);
        }

        public ClusterConfig MergeSlotMap(ClusterConfig senderConfig, ILogger logger = null)
        {
            // Track if update happened to avoid expensive merge and FlushConfig operation when possible
            var updated = false;
            var senderSlotMap = senderConfig.slotMap;
            var assignToWorkerId = GetWorkerIdFromNodeId(senderConfig.LocalNodeId);

            // Create a copy of the local slotMap
            var newSlotMap = new HashSlot[MAX_HASH_SLOT_VALUE];
            Array.Copy(slotMap, newSlotMap, senderSlotMap.Length);
            for (var i = 0; i < MAX_HASH_SLOT_VALUE; i++)
            {
                var currentOwnerId = newSlotMap[i].workerId;

                // Process this slot information when it is in stable state
                if (senderSlotMap[i]._state != SlotState.STABLE)
                    continue;

                // Skip processing this slot information when the sender is not the claimant of the slot and is a primary                
                if (senderSlotMap[i]._workerId != LOCAL_WORKER_ID && senderConfig.IsPrimary)
                {
                    var currentOwnerNodeId = workers[currentOwnerId].Nodeid;
                    // Sender does not own node but local node believes it does
                    // This can happen if epoch collision occurred at the sender and its epoch got bumped,
                    // in that case slot state should be set to offline to give the opportunity to the actual owner to claim the slot.
                    // Otherwise the sender will falsely remain the owner and its epoch will be greater than that of the new owner and the new owner
                    // will not be able to claim the slot without outside intervention
                    if (currentOwnerNodeId != null && currentOwnerNodeId.Equals(senderConfig.LocalNodeId, StringComparison.OrdinalIgnoreCase))
                    {
                        logger?.LogWarning("MergeReset: {senderConfig.LocalNodeIdShort} > {i} > {LocalNodeIdShort}", senderConfig.LocalNodeIdShort, i, LocalNodeIdShort);
                        newSlotMap[i]._workerId = RESERVED_WORKER_ID;
                        newSlotMap[i]._state = SlotState.OFFLINE;
                    }
                    continue;
                }

                if (senderConfig.IsPrimary)
                {
                    // Sender is claimant of this node and is a primary, hence it can update the state of this slot, if its config epoch is higher than the old epoch.
                    if (senderConfig.LocalNodeConfigEpoch != 0 && workers[currentOwnerId].ConfigEpoch >= senderConfig.LocalNodeConfigEpoch)
                        continue;
                }
                else if (currentOwnerId != RESERVED_WORKER_ID) // Possibly multiple replicas may enter this but only the old primary should succeed in the event of a planned failover.
                {
                    // This should guarantee that only the old primary should proceed with re-assigning the slots to the replica that is taking over
                    // Scenario 4 nodes A,B,C,D for which B,C are replicas of A and B takes over from A,
                    // then due to delay D will receive a gossip from A,B,C in any order.
                    if (!workers[currentOwnerId].Nodeid.Equals(senderConfig.LocalNodeId))
                        continue;
                    assignToWorkerId = GetWorkerIdFromNodeId(senderConfig.LocalNodePrimaryId);
                }

                // Update happened only if workerId or state changed
                // NOTE: this avoids message flooding when sender epoch equals zero
                updated = newSlotMap[i]._workerId != assignToWorkerId || newSlotMap[i]._state != SlotState.STABLE;

                // Update ownership of node
                newSlotMap[i]._workerId = assignToWorkerId;
                newSlotMap[i]._state = SlotState.STABLE;
            }

            return updated ? new(newSlotMap, workers) : this;
        }

        /// <summary>
        /// Remove worker
        /// </summary>
        /// <param name="nodeid">Node-id string.</param>
        /// <returns>ClusterConfig object with updates.</returns>
        public ClusterConfig RemoveWorker(string nodeid)
        {
            ushort workerId = 0;
            for (var i = 1; i < workers.Length; i++)
            {
                if (workers[i].Nodeid.Equals(nodeid, StringComparison.OrdinalIgnoreCase))
                {
                    workerId = (ushort)i;
                    break;
                }
            }

            var newSlotMap = new HashSlot[MAX_HASH_SLOT_VALUE];
            Array.Copy(slotMap, newSlotMap, slotMap.Length);
            for (var i = 0; i < newSlotMap.Length; i++)
            {
                // Node being removed is owner of slot
                if (newSlotMap[i]._state == SlotState.STABLE && newSlotMap[i].workerId == workerId)
                {
                    Debug.Assert(newSlotMap[i]._workerId != LOCAL_WORKER_ID);
                    newSlotMap[i]._workerId = RESERVED_WORKER_ID;
                    newSlotMap[i]._state = SlotState.OFFLINE;
                }
                // Node being removed is target node for migration and this is the source node
                else if (newSlotMap[i]._state == SlotState.MIGRATING && newSlotMap[i]._workerId == workerId)
                {
                    Debug.Assert(newSlotMap[i].workerId == LOCAL_WORKER_ID);
                    newSlotMap[i]._workerId = LOCAL_WORKER_ID;
                    newSlotMap[i]._state = SlotState.STABLE;
                }
                // Node being remove is source node for migration and this is the target node
                else if (newSlotMap[i]._state == SlotState.IMPORTING && workers[newSlotMap[i]._workerId].Nodeid.Equals(nodeid, StringComparison.OrdinalIgnoreCase))
                {
                    newSlotMap[i]._workerId = RESERVED_WORKER_ID;
                    newSlotMap[i]._state = SlotState.OFFLINE;
                }
                // Every other node with greater workerId need to decrement its offset
                else if (newSlotMap[i].workerId > workerId)
                {
                    newSlotMap[i]._workerId--;
                }
            }

            var newWorkers = new Worker[workers.Length - 1];
            Array.Copy(workers, 0, newWorkers, 0, workerId);
            if (workers.Length - 1 != workerId)
                Array.Copy(workers, workerId + 1, newWorkers, workerId, workers.Length - workerId - 1);

            return new ClusterConfig(newSlotMap, newWorkers);
        }

        /// <summary>
        /// Make this worker replica of a node with specified node Id.
        /// </summary>
        /// <param name="nodeid">String node-id of primary.</param>
        /// <returns>ClusterConfig object with updates.</returns>
        public ClusterConfig MakeReplicaOf(string nodeid)
        {
            var newWorkers = new Worker[workers.Length];
            Array.Copy(workers, newWorkers, workers.Length);

            newWorkers[LOCAL_WORKER_ID].ReplicaOfNodeId = nodeid;
            newWorkers[LOCAL_WORKER_ID].Role = NodeRole.REPLICA;
            return new ClusterConfig(slotMap, newWorkers);
        }

        /// <summary>
        /// Set role of local worker.
        /// </summary>
        /// <param name="role"></param>
        /// <returns>ClusterConfig object with updates.</returns>
        public ClusterConfig SetLocalWorkerRole(NodeRole role)
        {
            var newWorkers = new Worker[workers.Length];
            Array.Copy(workers, newWorkers, workers.Length);

            newWorkers[LOCAL_WORKER_ID].Role = role;
            return new ClusterConfig(slotMap, newWorkers);
        }

        /// <summary>
        /// Take over for primary.
        /// </summary>
        /// <returns>ClusterConfig object with updates.</returns>
        public ClusterConfig TakeOverFromPrimary()
        {
            var newWorkers = new Worker[workers.Length];
            Array.Copy(workers, newWorkers, workers.Length);
            newWorkers[LOCAL_WORKER_ID].Role = NodeRole.PRIMARY;
            newWorkers[LOCAL_WORKER_ID].ReplicaOfNodeId = null;

            var slots = GetLocalPrimarySlots();
            var newSlotMap = new HashSlot[MAX_HASH_SLOT_VALUE];
            Array.Copy(slotMap, newSlotMap, slotMap.Length);
            foreach (var slot in slots)
            {
                newSlotMap[slot]._workerId = LOCAL_WORKER_ID;
                newSlotMap[slot]._state = SlotState.STABLE;
            }

            return new ClusterConfig(newSlotMap, newWorkers);
        }

        /// <summary>
        /// Try to make local node owner of list of slots given.
        /// </summary>
        /// <param name="slots">Slots to assign.</param>
        /// <param name="slotAssigned">Slot already assigned if any during this bulk op.</param>
        /// <param name="config">ClusterConfig object with updates</param>
        /// <param name="state">SlotState type to be set.</param>
        /// <returns><see langword="false"/> if slot already owned by someone else according to a message received from the gossip protocol; otherwise <see langword="true"/>.</returns>
        public bool TryAddSlots(HashSet<int> slots, out int slotAssigned, out ClusterConfig config, SlotState state = SlotState.STABLE)
        {
            slotAssigned = -1;
            config = null;

            var newSlotMap = new HashSlot[16384];
            Array.Copy(slotMap, newSlotMap, slotMap.Length);
            if (slots != null)
            {
                foreach (var slot in slots)
                {
                    if (newSlotMap[slot].workerId != 0)
                    {
                        slotAssigned = slot;
                        return false;
                    }
                    newSlotMap[slot]._workerId = LOCAL_WORKER_ID;
                    newSlotMap[slot]._state = state;
                }
            }

            config = new ClusterConfig(newSlotMap, workers);
            return true;
        }

        /// <summary>
        /// Assign slots to workerId
        /// </summary>
        /// <param name="slots"></param>
        /// <param name="workerId"></param>
        /// <param name="state"></param>
        /// <returns></returns>
        public ClusterConfig AssignSlots(List<int> slots, ushort workerId, SlotState state)
        {
            var newSlotMap = new HashSlot[16384];
            Array.Copy(slotMap, newSlotMap, slotMap.Length);
            foreach (var slot in slots)
            {
                newSlotMap[slot]._workerId = workerId;
                newSlotMap[slot]._state = state;
            }
            return new ClusterConfig(newSlotMap, workers);
        }

        /// <summary>
        /// Try to remove slots from this local node.
        /// </summary>
        /// <param name="slots">Slots to be removed.</param>
        /// <param name="notLocalSlot">The slot number that is not local.</param>
        /// <param name="config">ClusterConfig object with updates</param>
        /// <returns><see langword="false"/> if a slot provided is not local; otherwise <see langword="true"/>.</returns>
        public bool TryRemoveSlots(HashSet<int> slots, out int notLocalSlot, out ClusterConfig config)
        {
            notLocalSlot = -1;
            config = null;

            var newSlotMap = new HashSlot[16384];
            Array.Copy(slotMap, newSlotMap, slotMap.Length);
            if (slots != null)
            {
                foreach (var slot in slots)
                {
                    if (newSlotMap[slot].workerId == 0)
                    {
                        notLocalSlot = slot;
                        return false;
                    }
                    newSlotMap[slot]._workerId = 0;
                    newSlotMap[slot]._state = SlotState.OFFLINE;
                }
            }

            config = new ClusterConfig(newSlotMap, workers);
            return true;
        }

        /// <summary>
        /// Update local slot state.
        /// </summary>
        /// <param name="slot">Slot number to update state</param>
        /// <param name="workerId">Worker offset information associated with slot.</param>
        /// <param name="state">SlotState type</param>
        /// <returns>ClusterConfig object with updates.</returns>
        public ClusterConfig UpdateSlotState(int slot, int workerId, SlotState state)
        {
            var newSlotMap = new HashSlot[16384];
            Array.Copy(slotMap, newSlotMap, slotMap.Length);

            newSlotMap[slot]._workerId = (ushort)workerId;
            newSlotMap[slot]._state = state;
            return new ClusterConfig(newSlotMap, workers);
        }

        /// <summary>
        /// Update slot states in bulk.
        /// </summary>
        /// <param name="slots">Slot numbers to update state.</param>
        /// <param name="workerId">Worker offset information associated with slot.</param>
        /// <param name="state">SlotState type</param>
        /// <returns>ClusterConfig object with updates.</returns>        
        public ClusterConfig UpdateMultiSlotState(HashSet<int> slots, int workerId, SlotState state)
        {
            var newSlotMap = new HashSlot[MAX_HASH_SLOT_VALUE];
            Array.Copy(slotMap, newSlotMap, slotMap.Length);

            foreach (var slot in slots)
            {
                newSlotMap[slot]._workerId = (ushort)workerId;
                newSlotMap[slot]._state = state;
            }
            return new ClusterConfig(newSlotMap, workers);
        }

        public ClusterConfig ResetMultiSlotState(HashSet<int> slots)
        {
            var newSlotMap = new HashSlot[MAX_HASH_SLOT_VALUE];
            Array.Copy(slotMap, newSlotMap, slotMap.Length);

            foreach (ushort slot in slots)
            {
                var slotState = GetState(slot);
                var workerId = slotState == SlotState.MIGRATING ? LOCAL_WORKER_ID : GetWorkerIdFromSlot(slot);
                newSlotMap[slot]._workerId = (ushort)workerId;
                newSlotMap[slot]._state = SlotState.STABLE;
            }
            return new ClusterConfig(newSlotMap, workers);
        }

        /// <summary>
        /// Update config epoch for worker in new version of config.
        /// </summary>
        /// <param name="configEpoch">Config epoch value to set.</param>
        /// <returns>ClusterConfig object with updates.</returns>
        public ClusterConfig SetLocalWorkerConfigEpoch(long configEpoch)
        {
            var newWorkers = new Worker[workers.Length];
            Array.Copy(workers, newWorkers, workers.Length);

            // Ensure epoch is zero and monotonicity
            if (workers[LOCAL_WORKER_ID].ConfigEpoch == 0 && workers[LOCAL_WORKER_ID].ConfigEpoch < configEpoch)
            {
                newWorkers[LOCAL_WORKER_ID].ConfigEpoch = configEpoch;
            }
            else
                return null;

            return new ClusterConfig(slotMap, newWorkers);
        }

        /// <summary>
        /// Increment local config epoch without consensus
        /// </summary>
        /// <returns>ClusterConfig object with updates.</returns>
        public ClusterConfig BumpLocalNodeConfigEpoch()
        {
            var maxConfigEpoch = GetMaxConfigEpoch();
            var newWorkers = new Worker[workers.Length];
            Array.Copy(workers, newWorkers, workers.Length);
            newWorkers[1].ConfigEpoch = maxConfigEpoch + 1;
            return new ClusterConfig(slotMap, newWorkers);
        }

        /// <summary>
        /// Check if sender has same local worker epoch as the receiver node and resolve collision.
        /// </summary>
        /// <param name="senderConfig">Incoming configuration object.</param>
        /// <param name="logger"></param>
        /// <returns>ClusterConfig object with updates.</returns>
        public ClusterConfig HandleConfigEpochCollision(ClusterConfig senderConfig, ILogger logger = null)
        {
            var localNodeConfigEpoch = LocalNodeConfigEpoch;
            var senderConfigEpoch = senderConfig.LocalNodeConfigEpoch;

            // If incoming config epoch different than local don't need to do anything
            if (localNodeConfigEpoch != senderConfigEpoch)
                return this;

            var senderNodeId = senderConfig.LocalNodeId;
            var localNodeId = LocalNodeId;

            // If remoteNodeId is lesser than localNodeId do nothing
            if (senderNodeId.CompareTo(localNodeId) <= 0) return this;

            logger?.LogWarning("Epoch Collision {localNodeConfigEpoch} <> {senderConfigEpoch} [{LocalNodeIp}:{LocalNodePort},{localNodeId}] [{senderIp}:{senderPort},{senderNodeId}]",
                localNodeConfigEpoch,
                senderConfigEpoch,
                LocalNodeIp,
                LocalNodePort,
                LocalNodeIdShort,
                senderConfig.LocalNodeIp,
                senderConfig.LocalNodePort,
                senderConfig.LocalNodeIdShort);

            return BumpLocalNodeConfigEpoch();
        }
    }
}