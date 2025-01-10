﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable IDE0005
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
#pragma warning restore IDE0005
using System.Runtime.CompilerServices;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    /// <summary>
    /// Cluster configuration
    /// </summary>
    internal sealed partial class ClusterConfig
    {
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
            for (int i = 0; i < MAX_HASH_SLOT_VALUE; i++)
            {
                slotMap[i]._state = SlotState.OFFLINE;
                slotMap[i]._workerId = 0;
            }
            workers = new Worker[2];
            workers[0].Address = "unassigned";
            workers[0].Port = 0;
            workers[0].Nodeid = null;
            workers[0].ConfigEpoch = 0;
            workers[0].Role = NodeRole.UNASSIGNED;
            workers[0].ReplicaOfNodeId = null;
            workers[0].ReplicationOffset = 0;
            workers[0].hostname = null;
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
            newWorkers[1].Address = address;
            newWorkers[1].Port = port;
            newWorkers[1].Nodeid = nodeId;
            newWorkers[1].ConfigEpoch = configEpoch;
            newWorkers[1].Role = role;
            newWorkers[1].ReplicaOfNodeId = replicaOfNodeId;
            newWorkers[1].ReplicationOffset = 0;
            newWorkers[1].hostname = hostname;
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
            => slotMap[slot].workerId == 1 || IsLocalExpensive(slot, readWriteSession);

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
        public string LocalNodeIp => workers[1].Address;

        /// <summary>
        /// Get local node port
        /// </summary>
        /// <returns>Port of local worker</returns>
        public int LocalNodePort => workers[1].Port;

        /// <summary>
        /// Get local node ID
        /// </summary>
        /// <returns>Node-id of local worker.</returns>
        public string LocalNodeId => workers[1].Nodeid;

        /// <summary>
        /// NOTE: Use this only for logging not comparison
        /// Get short local node ID
        /// </summary>
        /// <returns>Short node-id of local worker.</returns>
        public string LocalNodeIdShort => workers[1].Nodeid.Substring(0, 8);

        /// <summary>
        /// Get local node role
        /// </summary>
        /// <returns>Role of local node.</returns>
        public NodeRole LocalNodeRole => workers[1].Role;

        /// <summary>
        /// Get nodeid of primary.
        /// </summary>
        /// <returns>Primary-id of the node this node is replicating.</returns>
        public string LocalNodePrimaryId => workers[1].ReplicaOfNodeId;

        /// <summary>
        /// Get config epoch for local worker.
        /// </summary>
        /// <returns>Config epoch of local node.</returns>
        public long LocalNodeConfigEpoch => workers[1].ConfigEpoch;

        /// <summary>
        /// Return endpoint of primary if this node is a replica.
        /// </summary>
        /// <returns>Returns primary endpoints if this node is a replica, otherwise (null,-1)</returns>
        public (string address, int port) GetLocalNodePrimaryAddress() => GetWorkerAddressFromNodeId(workers[1].ReplicaOfNodeId);

        /// <summary>
        /// Get local node replicas
        /// </summary>
        /// <returns>Returns a list of node-ids representing the replicas that replicate this node.</returns>
        public List<string> GetLocalNodeReplicaIds() => GetReplicaIds(LocalNodeId);

        /// <summary>
        /// Get list of endpoints for all replicas of this node.
        /// </summary>
        /// <returns>List of (address,port) pairs.</returns>
        public List<(string, int)> GetLocalNodeReplicaEndpoints()
        {
            List<(string, int)> replicas = new();
            for (ushort i = 2; i < workers.Length; i++)
            {
                var replicaOf = workers[i].ReplicaOfNodeId;
                if (replicaOf != null && replicaOf.Equals(workers[1].Nodeid, StringComparison.OrdinalIgnoreCase))
                    replicas.Add((workers[i].Address, workers[i].Port));
            }
            return replicas;
        }

        /// <summary>
        /// Return all primary endpoints. Used from replica that is becoming a primary during a failover.
        /// </summary>
        /// <param name="includeMyPrimaryFirst"></param>
        /// <returns>List of pairs (address,port) representing known primary endpoints</returns>
        public List<(string, int)> GetLocalNodePrimaryEndpoints(bool includeMyPrimaryFirst = false)
        {
            string myPrimaryId = includeMyPrimaryFirst ? LocalNodePrimaryId : "";
            List<(string, int)> primaries = new();
            for (ushort i = 2; i < workers.Length; i++)
            {
                if (workers[i].Role == NodeRole.PRIMARY && !workers[i].Nodeid.Equals(myPrimaryId, StringComparison.OrdinalIgnoreCase))
                    primaries.Add((workers[i].Address, workers[i].Port));

                if (workers[i].Nodeid.Equals(myPrimaryId, StringComparison.OrdinalIgnoreCase))
                    primaries.Insert(0, (workers[i].Address, workers[i].Port));
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
            List<string> remoteNodeIds = new List<string>();
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

        private static void slotBitmapSetBit(ref byte[] bitmap, int pos)
        {
            int BYTE = (pos / 8);
            int BIT = pos & 7;
            bitmap[BYTE] |= (byte)(1 << BIT);
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
        public (string address, int port) GetEndpointFromSlot(ushort slot)
        {
            var workerId = GetWorkerIdFromSlot(slot);
            return (workers[workerId].Address, workers[workerId].Port);
        }

        /// <summary>
        /// Get endpoint of node to which slot is migrating.
        /// </summary>
        /// <param name="slot">Slot number.</param>
        /// <returns>Pair of (string,integer) representing endpoint.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (string address, int port) AskEndpointFromSlot(ushort slot)
        {
            var workerId = slotMap[slot]._workerId;
            return (workers[workerId].Address, workers[workerId].Port);
        }

        /// <summary>
        /// Get endpoint of node from node-id.
        /// </summary>
        /// <param name="nodeid">Node-id.</param>
        /// <returns>Pair of (string,integer) representing endpoint.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (string address, int port) GetEndpointFromNodeId(string nodeid)
        {
            var workerId = GetWorkerIdFromNodeId(nodeid);
            return (workers[workerId].Address, workers[workerId].Port);
        }
        #endregion

        /// <summary>
        /// Get formatted (using CLUSTER NODES format) cluster info.
        /// </summary>
        /// <returns>Formatted string.</returns>
        public string GetClusterInfo(ClusterProvider clusterProvider)
        {
            var nodes = "";
            for (ushort i = 1; i <= NumWorkers; i++)
            {
                var info = default(ConnectionInfo);
                _ = clusterProvider?.clusterManager?.GetConnectionInfo(workers[i].Nodeid, out info);
                nodes += GetNodeInfo(i, info);
            }
            return nodes;
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

            return $"{workers[workerId].Nodeid} " +
                $"{workers[workerId].Address}:{workers[workerId].Port}@{workers[workerId].Port + 10000},{workers[workerId].hostname} " +
                $"{(workerId == 1 ? "myself," : "")}{(workers[workerId].Role == NodeRole.PRIMARY ? "master" : "slave")} " +
                $"{(workers[workerId].Role == NodeRole.REPLICA ? workers[workerId].ReplicaOfNodeId : "-")} " +
                $"{info.ping} " +
                $"{info.pong} " +
                $"{workers[workerId].ConfigEpoch} " +
                $"{(info.connected || workerId == 1 ? "connected" : "disconnected")}" +
                $"{GetSlotRange(workerId)}" +
                $"{GetSpecialStates(workerId)}\n";
        }

        private string GetSpecialStates(ushort workerId)
        {
            // Only print special states for local node
            if (workerId != 1) return "";
            var specialStates = "";
            for (var slot = 0; slot < slotMap.Length; slot++)
            {
                var _workerId = slotMap[slot]._workerId;
                var _state = slotMap[slot]._state;

                if (_state == SlotState.STABLE) continue;
                if (_workerId > NumWorkers) continue;

                var _nodeId = workers[_workerId].Nodeid;
                if (_nodeId == null) continue;

                specialStates += _state switch
                {
                    SlotState.MIGRATING => $" [{slot}->-{_nodeId}]",
                    SlotState.IMPORTING => $" [{slot}-<-{_nodeId}]",
                    _ => ""
                };
            }
            return specialStates;
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

        private string CreateFormattedNodeInfo(int workerId)
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
            nodeInfo += $"$6\r\nonline\r\n";
            return nodeInfo;
        }

        private string CreateFormattedShardInfo(int primaryWorkerId, List<(ushort, ushort)> shardRanges, List<int> replicaWorkerIds)
        {
            var shardInfo = $"*4\r\n";

            shardInfo += $"$5\r\nslots\r\n";//1

            shardInfo += $"*{shardRanges.Count * 2}\r\n";//2
            for (int i = 0; i < shardRanges.Count; i++)
            {
                var range = shardRanges[i];
                shardInfo += $":{range.Item1}\r\n";
                shardInfo += $":{range.Item2}\r\n";
            }

            shardInfo += $"$5\r\nnodes\r\n";//3

            shardInfo += $"*{1 + replicaWorkerIds.Count}\r\n";//4
            shardInfo += CreateFormattedNodeInfo(primaryWorkerId);
            foreach (var id in replicaWorkerIds)
                shardInfo += CreateFormattedNodeInfo(id);

            return shardInfo;
        }

        /// <summary>
        /// Get formatted (using CLUSTER SHARDS format) cluster config information.
        /// </summary>
        /// <returns>RESP formatted string</returns>
        public string GetShardsInfo()
        {
            string shardsInfo = "";
            int shardCount = 0;
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
        }

        private string CreateFormattedSlotInfo(int slotStart, int slotEnd, string address, int port, string nodeid, string hostname, List<string> replicaIds)
        {
            int countA = replicaIds.Count == 0 ? 3 : 3 + replicaIds.Count;
            var rangeInfo = $"*{countA}\r\n";

            rangeInfo += $":{slotStart}\r\n";
            rangeInfo += $":{slotEnd}\r\n";
            rangeInfo += $"*4\r\n${address.Length}\r\n{address}\r\n:{port}\r\n${nodeid.Length}\r\n{nodeid}\r\n";
            rangeInfo += $"*2\r\n$8\r\nhostname\r\n${hostname.Length}\r\n{hostname}\r\n";

            foreach (var replicaId in replicaIds)
            {
                var (replicaAddress, replicaPort) = GetWorkerAddressFromNodeId(replicaId);
                var replicaHostname = GetHostNameFromNodeId(replicaId);

                rangeInfo += $"*4\r\n${replicaAddress.Length}\r\n{replicaAddress}\r\n:{replicaPort}\r\n${replicaId.Length}\r\n{replicaId}\r\n";
                rangeInfo += $"*2\r\n$8\r\nhostname\r\n${replicaHostname.Length}\r\n{replicaHostname}\r\n";
            }
            return rangeInfo;
        }

        /// <summary>
        /// Get formatted (using CLUSTER SLOTS format) cluster config info.
        /// </summary>
        /// <returns>Formatted string.</returns>
        public string GetSlotsInfo()
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
                completeSlotInfo += CreateFormattedSlotInfo(slotStart, slotEnd, address, port, nodeid, hostname, replicas);
                slotRanges++;
                slotStart = slotEnd;
            }
            completeSlotInfo = $"*{slotRanges}\r\n" + completeSlotInfo;
            //Console.WriteLine(completeSlotInfo);

            return completeSlotInfo;
        }

        private string GetSlotRange(ushort workerId)
        {
            string result = "";
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
                        if (end == start) result += $" {start}";
                        else result += $" {start}-{end}";
                        start = ushort.MaxValue;
                        end = 0;
                    }
                }
            }
            if (start != ushort.MaxValue)
            {
                if (end == start) result += $" {start}";
                else result += $" {start}-{end}";
            }
            return result;
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
                var replicaOf = workers[i].ReplicaOfNodeId;
                if (replicaOf != null && replicaOf.Equals(nodeid, StringComparison.OrdinalIgnoreCase))
                {
                    var info = default(ConnectionInfo);
                    _ = clusterProvider?.clusterManager?.GetConnectionInfo(replicaOf, out info);
                    replicas.Add(GetNodeInfo(i, info));
                }
            }
            return replicas;
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
            => workers[1].ReplicationOffset = newReplicationOffset;

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
            ushort workerId = 0;
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
            if (workerId == 0)
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
            var senderWorkerId = GetWorkerIdFromNodeId(senderConfig.LocalNodeId);

            // Create a copy of the local slotMap
            var newSlotMap = new HashSlot[MAX_HASH_SLOT_VALUE];
            Array.Copy(slotMap, newSlotMap, senderSlotMap.Length);
            for (var i = 0; i < MAX_HASH_SLOT_VALUE; i++)
            {
                var currentOwnerId = newSlotMap[i].workerId;

                // Process this slot information when it is in stable state
                if (senderSlotMap[i]._state != SlotState.STABLE)
                    continue;

                // Process this slot information when sender is claimant of this slot
                if (senderSlotMap[i]._workerId != 1)
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
                        newSlotMap[i]._workerId = 0;
                        newSlotMap[i]._state = SlotState.OFFLINE;
                    }
                    continue;
                }

                // Process this slot information when config epoch of original owner is greater than config epoch of sender
                if (senderConfig.LocalNodeConfigEpoch != 0 && workers[currentOwnerId].ConfigEpoch >= senderConfig.LocalNodeConfigEpoch)
                    continue;

                // Update happened only if workerId or state changed
                // NOTE: this avoids message flooding when sender epoch equals zero
                updated = newSlotMap[i]._workerId != senderWorkerId || newSlotMap[i]._state != SlotState.STABLE;

                // Update ownership of node
                newSlotMap[i]._workerId = senderWorkerId;
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
                    Debug.Assert(newSlotMap[i]._workerId != 1);
                    newSlotMap[i]._workerId = 0;
                    newSlotMap[i]._state = SlotState.OFFLINE;
                }
                // Node being removed is target node for migration and this is the source node
                else if (newSlotMap[i]._state == SlotState.MIGRATING && newSlotMap[i]._workerId == workerId)
                {
                    Debug.Assert(newSlotMap[i].workerId == 1);
                    newSlotMap[i]._workerId = 1;
                    newSlotMap[i]._state = SlotState.STABLE;
                }
                // Node being remove is source node for migration and this is the target node
                else if (newSlotMap[i]._state == SlotState.IMPORTING && workers[newSlotMap[i]._workerId].Nodeid.Equals(nodeid, StringComparison.OrdinalIgnoreCase))
                {
                    newSlotMap[i]._workerId = 0;
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

            newWorkers[1].ReplicaOfNodeId = nodeid;
            newWorkers[1].Role = NodeRole.REPLICA;
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

            newWorkers[1].Role = role;
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
            newWorkers[1].Role = NodeRole.PRIMARY;
            newWorkers[1].ReplicaOfNodeId = null;

            var slots = GetLocalPrimarySlots();
            var newSlotMap = new HashSlot[MAX_HASH_SLOT_VALUE];
            Array.Copy(slotMap, newSlotMap, slotMap.Length);
            foreach (var slot in slots)
            {
                newSlotMap[slot]._workerId = 1;
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
                    newSlotMap[slot]._workerId = 1;
                    newSlotMap[slot]._state = state;
                }
            }

            config = new ClusterConfig(newSlotMap, workers);
            return true;
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
                var workerId = slotState == SlotState.MIGRATING ? 1 : GetWorkerIdFromSlot(slot);
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
            if (workers[1].ConfigEpoch == 0 && workers[1].ConfigEpoch < configEpoch)
            {
                newWorkers[1].ConfigEpoch = configEpoch;
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
        /// <returns>ClusterConfig object with updates.</returns>
        public ClusterConfig HandleConfigEpochCollision(ClusterConfig senderConfig, ILogger logger = null)
        {
            var localNodeConfigEpoch = LocalNodeConfigEpoch;
            var senderConfigEpoch = senderConfig.LocalNodeConfigEpoch;

            // If incoming config epoch different than local don't need to do anything
            if (localNodeConfigEpoch != senderConfigEpoch || !IsPrimary || !senderConfig.IsPrimary)
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