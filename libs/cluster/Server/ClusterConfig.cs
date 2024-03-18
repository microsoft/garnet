// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable IDE0005
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
#pragma warning restore IDE0005
using System.Runtime.CompilerServices;

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
        public static readonly int MIN_HASH_SLOT_VALUE = 0;

        /// <summary>
        /// Maximum hash slot value.
        /// </summary>
        public static readonly int MAX_HASH_SLOT_VALUE = 16384;

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
            workers[0].address = "unassigned";
            workers[0].port = 0;
            workers[0].nodeid = null;
            workers[0].configEpoch = 0;
            workers[0].lastVotedConfigEpoch = 0;
            workers[0].role = NodeRole.UNASSIGNED;
            workers[0].replicaOfNodeId = null;
            workers[0].replicationOffset = 0;
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
        /// <param name="currentConfigEpoch">Local worker current config epoch.</param>
        /// <param name="lastVotedConfigEpoch">Local worker last voted epoch.</param>
        /// <param name="role">Local worker role.</param>
        /// <param name="replicaOfNodeId">Local worker primary id.</param>
        /// <param name="hostname">Local worker hostname.</param>
        /// <returns>Instance of local config with update local worker info.</returns>
        public ClusterConfig InitializeLocalWorker(
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
            Worker[] newWorkers = new Worker[workers.Length];
            Array.Copy(workers, newWorkers, workers.Length);
            newWorkers[1].address = address;
            newWorkers[1].port = port;
            newWorkers[1].nodeid = nodeId;
            newWorkers[1].configEpoch = configEpoch;
            newWorkers[1].lastVotedConfigEpoch = currentConfigEpoch;
            newWorkers[1].lastVotedConfigEpoch = lastVotedConfigEpoch;
            newWorkers[1].role = role;
            newWorkers[1].replicaOfNodeId = replicaOfNodeId;
            newWorkers[1].replicationOffset = 0;
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
        /// <param name="readCommand">If we are checking as a read command. Used to override check if READWRITE is specified</param>
        /// <returns>True if slot is owned by this node, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsLocal(ushort slot, bool readCommand = true)
            => slotMap[slot].workerId == 1 || IsLocalExpensive(slot, readCommand);

        private bool IsLocalExpensive(ushort slot, bool readCommand)
            => (readCommand && workers[1].role == NodeRole.REPLICA && workers[slotMap[slot]._workerId].nodeid == GetLocalNodePrimaryId()) ||
            slotMap[slot]._state == SlotState.MIGRATING;

        /// <summary>
        /// Check if specified node-id belongs to a node in our local config.
        /// </summary>
        /// <param name="nodeid">Node id to search for.</param>
        /// <returns>True if node-id in worker list, false otherwise.</returns>
        public bool IsKnown(string nodeid)
        {
            for (int i = 1; i <= NumWorkers; i++)
                if (workers[i].nodeid.Equals(nodeid))
                    return true;
            return false;
        }

        #region GetLocalNodeInfo
        /// <summary>
        /// Get local node ip
        /// </summary>
        /// <returns>IP of local worker</returns>
        public string GetLocalNodeIp() => workers[1].address;

        /// <summary>
        /// Get local node port
        /// </summary>
        /// <returns>Port of local worker</returns>
        public int GetLocalNodePort() => workers[1].port;

        /// <summary>
        /// Get local node ID
        /// </summary>
        /// <returns>Node-id of local worker.</returns>
        public string GetLocalNodeId() => workers[1].nodeid;

        /// <summary>
        /// Get local node role
        /// </summary>
        /// <returns>Role of local node.</returns>
        public NodeRole GetLocalNodeRole() => workers[1].role;

        /// <summary>
        /// Get nodeid of primary.
        /// </summary>
        /// <returns>Primary-id of the node this node is replicating.</returns>
        public string GetLocalNodePrimaryId() => workers[1].replicaOfNodeId;

        /// <summary>
        /// Get config epoch for local worker.
        /// </summary>
        /// <returns>Config epoch of local node.</returns>
        public long GetLocalNodeConfigEpoch() => workers[1].configEpoch;

        /// <summary>
        /// Next valid config epoch which can be used as requestedEpoch for voting.
        /// </summary>
        /// <returns>Current config epoch of local node.</returns>
        public long GetLocalNodeCurrentConfigEpoch() => workers[1].currentConfigEpoch;

        /// <summary>
        /// Get last epoch this node has voted for
        /// </summary>
        /// <returns>Last voted config epoch of local node.</returns>
        public long GetLocalNodeLastVotedEpoch() => workers[1].lastVotedConfigEpoch;

        /// <summary>
        /// Return endpoint of primary if this node is a replica.
        /// </summary>
        /// <returns>Returns primary endpoints if this node is a replica, otherwise (null,-1)</returns>
        public (string address, int port) GetLocalNodePrimaryAddress() => GetWorkerAddressFromNodeId(workers[1].replicaOfNodeId);

        /// <summary>
        /// Get local node replicas
        /// </summary>
        /// <returns>Returns a list of node-ids representing the replicas that replicate this node.</returns>
        public List<string> GetLocalNodeReplicaIds() => GetReplicaIds(GetLocalNodeId());

        /// <summary>
        /// Get list of endpoints for all replicas of this node.
        /// </summary>
        /// <returns>List of (address,port) pairs.</returns>
        public List<(string, int)> GetLocalNodeReplicaEndpoints()
        {
            List<(string, int)> replicas = new();
            for (ushort i = 2; i < workers.Length; i++)
            {
                var replicaOf = workers[i].replicaOfNodeId;
                if (replicaOf != null && replicaOf.Equals(workers[1].nodeid))
                    replicas.Add((workers[i].address, workers[i].port));
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
            string myPrimaryId = includeMyPrimaryFirst ? GetLocalNodePrimaryId() : "";
            List<(string, int)> primaries = new();
            for (ushort i = 2; i < workers.Length; i++)
            {
                if (workers[i].role == NodeRole.PRIMARY && workers[i].nodeid != myPrimaryId)
                    primaries.Add((workers[i].address, workers[i].port));

                if (workers[i].nodeid == myPrimaryId)
                    primaries.Insert(0, (workers[i].address, workers[i].port));
            }
            return primaries;
        }

        /// <summary>
        /// Retrieve a list of slots served by this node's primary.
        /// </summary>
        /// <returns>List of slots.</returns>
        public List<int> GetLocalPrimarySlots()
        {
            var primaryId = GetLocalNodePrimaryId();
            List<int> result = new();
            for (int i = 0; i < MAX_HASH_SLOT_VALUE; i++)
            {
                if (workers[slotMap[i].workerId].nodeid == primaryId)
                    result.Add(i);
            }
            return result;
        }

        /// <summary>
        /// Find maximum config epoch from local config
        /// </summary>
        /// <returns>Integer representing max config epoch value.</returns>
        public long GetMaxConfigEpoch()
        {
            long mx = 0;
            for (int i = 1; i <= NumWorkers; i++)
                mx = Math.Max(workers[i].configEpoch, mx);
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
                remoteNodeIds.Add(workers[i].nodeid);
            return remoteNodeIds;
        }
        #endregion

        #region GetFromNodeId
        /// <summary>
        /// Get worker id from node id.
        /// </summary>
        /// <param name="nodeId"></param>
        /// <returns>Integer representing offset of worker in worker list.</returns>
        public int GetWorkerIdFromNodeId(string nodeId)
        {
            for (ushort i = 1; i <= NumWorkers; i++)
            {
                if (workers[i].nodeid.Equals(nodeId))
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
        public NodeRole GetNodeRoleFromNodeId(string nodeId) => workers[GetWorkerIdFromNodeId(nodeId)].role;

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
            return workerId == 0 ? (null, -1) : (workers[workerId].address, workers[workerId].port);
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

        /// <summary>
        /// Returns compressed representation of slots claimed by given node.
        /// </summary>
        /// <param name="nodeId"></param>
        /// <returns>Byte array representing bitmap of claimed slots.</returns>
        public byte[] GetClaimedSlotsFromNodeId(string nodeId)
        {
            byte[] claimedSlots = new byte[slotMap.Length / 8];
            for (int i = 0; i < slotMap.Length; i++)
            {
                if (workers[slotMap[i].workerId].nodeid == nodeId)
                    slotBitmapSetBit(ref claimedSlots, i);
            }
            return claimedSlots;
        }
        #endregion

        #region GetFromSlot

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
        public string GetNodeIdFromSlot(ushort slot) => workers[GetWorkerIdFromSlot(slot)].nodeid;

        /// <summary>
        /// Get endpoint of slot owner.
        /// </summary>
        /// <param name="slot">Slot number.</param>
        /// <returns>Pair of (string,integer) representing endpoint.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (string address, int port) GetEndpointFromSlot(ushort slot)
        {
            var workerId = GetWorkerIdFromSlot(slot);
            return (workers[workerId].address, workers[workerId].port);
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
            return (workers[workerId].address, workers[workerId].port);
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
            return (workers[workerId].address, workers[workerId].port);
        }

        /// <summary>
        /// Get config epoch from slot.
        /// </summary>
        /// <param name="slot">Slot number.</param>
        /// <returns>Long value representing config epoch.</returns>
        public long GetConfigEpochFromSlot(int slot)
        {
            if (slotMap[slot].workerId < 0)
                return 0;
            return workers[slotMap[slot].workerId].configEpoch;
        }
        #endregion

        /// <summary>
        /// Get formatted (using CLUSTER NODES format) cluster info.
        /// </summary>
        /// <returns>Formatted string.</returns>
        public string GetClusterInfo()
        {
            string nodes = "";
            for (ushort i = 1; i <= NumWorkers; i++)
                nodes += GetNodeInfo(i);
            return nodes;
        }

        /// <summary>
        /// Get formatted (using CLUSTER NODES format) worker info.
        /// </summary>
        /// <param name="workerId">Offset of worker in the worker list.</param>
        /// <returns>Formatted string.</returns>
        public string GetNodeInfo(ushort workerId)
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

            return $"{workers[workerId].nodeid} " +
                $"{workers[workerId].address}:{workers[workerId].port}@{workers[workerId].port + 10000},{workers[workerId].hostname} " +
                $"{(workerId == 1 ? "myself," : "")}{(workers[workerId].role == NodeRole.PRIMARY ? "master" : "slave")} " +
                $"{(workers[workerId].role == NodeRole.REPLICA ? workers[workerId].replicaOfNodeId : "-")} " +
                $"0 " +
                $"0 " +
                $"{workers[workerId].configEpoch} " +
                $"connected" +
                $"{GetSlotRange(workerId)}" +
                $"{GetSpecialStates(workerId)}\n";
        }

        private string GetSpecialStates(ushort workerId)
        {
            // Only print special states for local node
            if (workerId != 1) return "";
            string specialStates = "";
            for (int slot = 0; slot < slotMap.Length; slot++)
            {
                if (slotMap[slot]._state == SlotState.MIGRATING)
                {
                    // Get node-id of node that we are migrating to by using "transient" _workerId
                    specialStates += $" [{slot}->-{workers[slotMap[slot]._workerId].nodeid}]";
                }
                else if (slotMap[slot]._state == SlotState.IMPORTING)
                {
                    specialStates += $" [{slot}-<-{GetNodeIdFromSlot((ushort)slot)}]";
                }
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
            ushort startRange = ushort.MaxValue;
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
            var primaryId = workers[workerId].nodeid;
            List<int> replicaWorkerIds = new();
            for (ushort i = 1; i <= NumWorkers; i++)
            {
                string replicaOf = workers[i].replicaOfNodeId;
                if (replicaOf != null && replicaOf.Equals(primaryId))
                    replicaWorkerIds.Add(i);
            }
            return replicaWorkerIds;
        }

        private string CreateFormattedNodeInfo(int workerId)
        {
            var nodeInfo = "*12\r\n";
            nodeInfo += "$2\r\nid\r\n";
            nodeInfo += $"$40\r\n{workers[workerId].nodeid}\r\n";
            nodeInfo += "$4\r\nport\r\n";
            nodeInfo += $":{workers[workerId].port}\r\n";
            nodeInfo += "$7\r\naddress\r\n";
            nodeInfo += $"${workers[workerId].address.Length}\r\n{workers[workerId].address}\r\n";
            nodeInfo += "$4\r\nrole\r\n";
            nodeInfo += $"${workers[workerId].role.ToString().Length}\r\n{workers[workerId].role}\r\n";
            nodeInfo += "$18\r\nreplication-offset\r\n";
            nodeInfo += $":{workers[workerId].replicationOffset}\r\n";
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
                if (workers[i].role == NodeRole.PRIMARY)
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
                var address = workers[currSlotWorkerId].address;
                var port = workers[currSlotWorkerId].port;
                var nodeid = workers[currSlotWorkerId].nodeid;
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

        private List<int> GetSlotList(ushort workerId)
        {
            List<int> result = new();
            for (int i = 0; i < MAX_HASH_SLOT_VALUE; i++)
                if (slotMap[i].workerId == workerId) result.Add(i);
            return result;
        }

        /// <summary>
        /// Get Replicas for node-id.
        /// </summary>
        /// <param name="nodeid">Node-id string.</param>
        /// <returns></returns>
        public List<string> GetReplicas(string nodeid)
        {
            List<string> replicas = new();
            for (ushort i = 1; i < workers.Length; i++)
            {
                string replicaOf = workers[i].replicaOfNodeId;
                if (replicaOf != null && replicaOf.Equals(nodeid))
                    replicas.Add(GetNodeInfo(i));
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
            List<string> replicas = new();
            for (ushort i = 1; i < workers.Length; i++)
            {
                string replicaOf = workers[i].replicaOfNodeId;
                if (replicaOf != null && replicaOf.Equals(nodeid))
                    replicas.Add(workers[i].nodeid);
            }
            return replicas;
        }

        public List<(string, int)> GetReplicaEndpoints(string nodeid)
        {
            List<(string, int)> replicaEndpoints = new();
            for (ushort i = 1; i < workers.Length; i++)
            {
                string replicaOf = workers[i].replicaOfNodeId;
                if (replicaOf != null && replicaOf.Equals(nodeid))
                    replicaEndpoints.Add(new(workers[i].address, workers[i].port));
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
            return (w.address, w.port);
        }

        /// <summary>
        /// Return list of triples containing node-id,address,port.
        /// </summary>
        /// <returns>List of triplets.</returns>
        public List<(string, string, int)> GetWorkerInfoForGossip()
        {
            List<(string, string, int)> result = new();
            for (int i = 2; i < workers.Length; i++)
                result.Add((workers[i].nodeid, workers[i].address, workers[i].port));
            return result;
        }

        /// <summary>
        /// Return count of slots in given state.
        /// </summary>
        /// <param name="slotState">SlotState type.</param>
        /// <returns>Integer representing count of slots in given state.</returns>
        public int GetSlotCountForState(SlotState slotState)
        {
            int count = 0;
            for (int i = 0; i < slotMap.Length; i++)
                count += slotMap[i]._state == slotState ? 1 : 0;
            return count;
        }

        /// <summary>
        /// Return number of primary nodes.
        /// </summary>
        /// <returns>Integer representing number of primary nodes.</returns>
        public int GetPrimaryCount()
        {
            int count = 0;
            for (ushort i = 1; i <= NumWorkers; i++)
            {
                var w = workers[i];
                count += w.role == NodeRole.PRIMARY ? 1 : 0;
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
                if (w.address == address && w.port == port)
                    return w.nodeid;
            }
            return null;
        }

        /// <summary>
        /// Update replication offset lazily.
        /// </summary>
        /// <param name="newReplicationOffset">Long of new replication offset.</param>
        public void LazyUpdateLocalReplicationOffset(long newReplicationOffset)
            => workers[1].replicationOffset = newReplicationOffset;

        /// <summary>
        /// Merging incoming configuration from gossip with local configuration copy.
        /// </summary>
        /// <param name="other">Incoming config object.</param>
        /// <param name="workerBanList">Worker ban list used to prevent merging.</param>
        /// <returns>Cluster config object.</returns>
        public ClusterConfig Merge(ClusterConfig other, ConcurrentDictionary<string, long> workerBanList)
        {
            var localId = GetLocalNodeId();
            var newConfig = this;
            for (ushort i = 1; i < other.NumWorkers + 1; i++)
            {
                //Do not update local node config
                if (localId == other.workers[i].nodeid)
                    continue;
                //Skip any nodes scheduled for deletion
                if (workerBanList.ContainsKey(other.workers[i].nodeid))
                    continue;

                newConfig = newConfig.InPlaceUpdateWorker(
                    other.workers[i].nodeid,
                    other.workers[i].address,
                    other.workers[i].port,
                    other.workers[i].configEpoch,
                    other.workers[i].currentConfigEpoch,
                    other.workers[i].lastVotedConfigEpoch,
                    other.workers[i].role,
                    other.workers[i].replicaOfNodeId,
                    other.workers[i].hostname,
                    other.GetSlotList(i));
            }
            return newConfig;
        }

        private ClusterConfig InPlaceUpdateWorker(
            string nodeid,
            string address,
            int port,
            long configEpoch,
            long currentConfigEpoch,
            long lastVotedConfigEpoch,
            NodeRole role,
            string replicaOfNodeId,
            string hostname,
            List<int> slots)
        {
            ushort workerId = 0;
            for (int i = 1; i < workers.Length; i++)
            {
                if (workers[i].nodeid == nodeid)
                {
                    //Skip update if received config is smaller or equal than local worker epoch
                    //Update only if received config epoch is strictly greater
                    if (configEpoch <= workers[i].configEpoch) return this;
                    workerId = (ushort)i;
                    break;
                }
            }

            Worker[] newWorkers = this.workers;
            if (workerId == 0)
            {
                newWorkers = new Worker[workers.Length + 1];
                workerId = (ushort)workers.Length;
                Array.Copy(workers, newWorkers, workers.Length);
            }

            newWorkers[workerId].address = address;
            newWorkers[workerId].port = port;
            newWorkers[workerId].nodeid = nodeid;
            newWorkers[workerId].configEpoch = configEpoch;
            newWorkers[workerId].currentConfigEpoch = currentConfigEpoch;
            newWorkers[workerId].lastVotedConfigEpoch = lastVotedConfigEpoch;
            newWorkers[workerId].role = role;
            newWorkers[workerId].replicaOfNodeId = replicaOfNodeId;
            newWorkers[workerId].hostname = hostname;

            var newSlotMap = this.slotMap;
            if (slots != null)
            {
                foreach (int slot in slots)
                {
                    newSlotMap[slot]._workerId = workerId;
                    newSlotMap[slot]._state = SlotState.STABLE;
                }
            }

            return new ClusterConfig(newSlotMap, newWorkers);
        }

        /// <summary>
        /// Remove worker
        /// </summary>
        /// <param name="nodeid">Node-id string.</param>
        /// <returns>ClusterConfig object with updates.</returns>
        public ClusterConfig RemoveWorker(string nodeid)
        {
            ushort workerId = 0;
            for (int i = 1; i < workers.Length; i++)
            {
                if (workers[i].nodeid == nodeid)
                {
                    workerId = (ushort)i;
                    break;
                }
            }

            var newSlotMap = new HashSlot[MAX_HASH_SLOT_VALUE];
            Array.Copy(slotMap, newSlotMap, slotMap.Length);
            for (int i = 0; i < newSlotMap.Length; i++)
            {
                if (newSlotMap[i].workerId == workerId)
                {
                    newSlotMap[i]._workerId = 0;
                    newSlotMap[i]._state = SlotState.OFFLINE;
                }
                else if (newSlotMap[i].workerId > workerId)
                {
                    newSlotMap[i]._workerId--;
                }
            }

            Worker[] newWorkers = new Worker[workers.Length - 1];
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

            newWorkers[1].replicaOfNodeId = nodeid;
            newWorkers[1].role = NodeRole.REPLICA;
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

            newWorkers[1].role = role;
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
            newWorkers[1].role = NodeRole.PRIMARY;
            newWorkers[1].replicaOfNodeId = null;

            var slots = GetLocalPrimarySlots();
            var newSlotMap = new HashSlot[MAX_HASH_SLOT_VALUE];
            Array.Copy(slotMap, newSlotMap, slotMap.Length);
            foreach (int slot in slots)
            {
                newSlotMap[slot]._workerId = 1;
                newSlotMap[slot]._state = SlotState.STABLE;
            }

            return new ClusterConfig(newSlotMap, newWorkers);
        }

        /// <summary>
        /// Make local node owner of list of slots given. 
        /// Fails if slot already owned by someone else according to a message received from the gossip protocol.
        /// </summary>
        /// <param name="slots">Slots to assign.</param>
        /// <param name="slotAssigned">Slot already assigned if any during this bulk op.</param>
        /// <param name="state">SlotState type to be set.</param>
        /// <returns>ClusterConfig object with updates.</returns>
        public ClusterConfig AddSlots(List<int> slots, out int slotAssigned, SlotState state = SlotState.STABLE)
        {
            var newSlotMap = new HashSlot[16384];
            Array.Copy(slotMap, newSlotMap, slotMap.Length);
            slotAssigned = -1;

            if (slots != null)
            {
                foreach (int slot in slots)
                {
                    if (newSlotMap[slot].workerId != 0)
                    {
                        slotAssigned = slot;
                        return null;
                    }
                    newSlotMap[slot]._workerId = 1;
                    newSlotMap[slot]._state = state;
                }
            }

            return new ClusterConfig(newSlotMap, workers);
        }

        /// <summary>
        /// Remove slots from this local node.
        /// </summary>
        /// <param name="slots">Slots to be removed.</param>
        /// <param name="notLocalSlot">If a slot provided is not local.</param>
        /// <returns>ClusterConfig object with updates.</returns>
        public ClusterConfig RemoveSlots(List<int> slots, out int notLocalSlot)
        {
            var newSlotMap = new HashSlot[16384];
            Array.Copy(slotMap, newSlotMap, slotMap.Length);
            notLocalSlot = -1;

            if (slots != null)
            {
                foreach (int slot in slots)
                {
                    if (newSlotMap[slot].workerId == 0)
                    {
                        notLocalSlot = slot;
                        return null;
                    }
                    newSlotMap[slot]._workerId = 0;
                    newSlotMap[slot]._state = SlotState.OFFLINE;
                }
            }

            return new ClusterConfig(newSlotMap, workers);
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
            if (workers[1].configEpoch == 0 && workers[1].configEpoch < configEpoch)
            {
                newWorkers[1].configEpoch = configEpoch;
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
            long maxConfigEpoch = GetMaxConfigEpoch();
            var newWorkers = new Worker[workers.Length];
            Array.Copy(workers, newWorkers, workers.Length);
            newWorkers[1].configEpoch = maxConfigEpoch + 1;
            return new ClusterConfig(slotMap, newWorkers);
        }

        /// <summary>
        /// Bump current config epoch for voting
        /// </summary>
        /// <returns>ClusterConfig object with updates.</returns>
        public ClusterConfig BumpLocalNodeCurrentConfigEpoch()
        {
            long nextValidConfigEpoch = GetLocalNodeCurrentConfigEpoch();
            var newWorkers = new Worker[workers.Length];
            Array.Copy(workers, newWorkers, workers.Length);
            newWorkers[1].currentConfigEpoch = nextValidConfigEpoch == 0 ? GetMaxConfigEpoch() : nextValidConfigEpoch + 1;
            return new ClusterConfig(slotMap, newWorkers);
        }

        /// <summary>
        /// Check if sender has same local worker epoch as the receiver node and resolve collision.
        /// </summary>
        /// <param name="other">Incoming configuration object.</param>        
        /// <returns>ClusterConfig object with updates.</returns>
        public ClusterConfig HandleConfigEpochCollision(ClusterConfig other)
        {
            //if incoming config epoch different than local don't need to do anything
            if (GetLocalNodeConfigEpoch() != other.GetLocalNodeConfigEpoch())
                return this;

            var remoteNodeId = other.GetLocalNodeId();
            var localNodeId = GetLocalNodeId();

            //if localNodeId is smaller then do nothing
            if (localNodeId.CompareTo(remoteNodeId) <= 0) return this;

            var newWorkers = new Worker[workers.Length];
            Array.Copy(workers, newWorkers, workers.Length);
            newWorkers[1].configEpoch++;
            return new ClusterConfig(slotMap, newWorkers);
        }

        /// <summary>
        /// Updated last voted epoch to requested epoch.
        /// </summary>
        /// <param name="requestedEpoch">Requested epoch value.</param>
        /// <returns>ClusterConfig object with updates.</returns>
        public ClusterConfig SetLocalNodeLastVotedConfigEpoch(long requestedEpoch)
        {
            var newWorkers = new Worker[workers.Length];
            Array.Copy(workers, newWorkers, workers.Length);
            newWorkers[1].lastVotedConfigEpoch = requestedEpoch;
            return new ClusterConfig(slotMap, newWorkers);
        }
    }
}