// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable IDE0005 // Using directive is unnecessary.
using System;
using System.Collections.Generic;
using System.Linq;
#pragma warning restore IDE0005 // Using directive is unnecessary.
using System.Net;
using Garnet.cluster;
using Garnet.common;
using StackExchange.Redis;

namespace GarnetClusterManagement
{
    internal class ClientClusterNode
    {
        public ClientClusterNode(string raw)
        {
            Raw = raw;
            var parts = raw.Split(' ');

            var flags = parts[2].Split(',');

            // Add @... to the endpoint
            var ep = parts[1];
            int at = ep.IndexOf('@');
            if (at >= 0) ep = ep.Substring(0, at);

            if (Format.TryParseEndPoint(ep, out var epResult))
            {
                EndPoint = epResult;
            }
            if (flags.Contains("myself"))
            {
                IsMyself = true;
            }

            NodeId = parts[0];
            IsReplica = flags.Contains("slave") || flags.Contains("replica");
            IsNoAddr = flags.Contains("noaddr");
            ParentNodeId = string.IsNullOrWhiteSpace(parts[3]) ? null : parts[3];

#nullable enable
            List<SlotRange>? slots = null;
#nullable disable
            for (int i = 8; i < parts.Length; i++)
            {
                if (SlotRange.TryParse(parts[i], out SlotRange range))
                {
                    (slots ??= new List<SlotRange>(parts.Length - i)).Add(range);
                }
            }
            Slots = slots?.AsReadOnly() ?? (IList<SlotRange>)Array.Empty<SlotRange>();
            IsConnected = parts[7] == "connected"; // Can be "connected" or "disconnected"
        }

#nullable enable
        /// <summary>
        /// Gets the endpoint of the current node.
        /// </summary>
        /// 
        public EndPoint? EndPoint { get; }

        /// <summary>
        /// Gets whether this is the node which responded to the CLUSTER NODES request.
        /// </summary>
        public bool IsMyself { get; }

        /// <summary>
        /// Gets whether this node is a replica.
        /// </summary>
        public bool IsReplica { get; }

        /// <summary>
        /// Gets whether this node is flagged as noaddr.
        /// </summary>
        public bool IsNoAddr { get; }

        /// <summary>
        /// Gets the node's connection status.
        /// </summary>
        public bool IsConnected { get; }

        /// <summary>
        /// Gets the unique node-id of the current node.
        /// </summary>
        public string NodeId { get; }

        /// <summary>
        /// Gets the unique node-id of the parent of the current node.
        /// </summary>
        public string? ParentNodeId { get; }

        /// <summary>
        /// The configuration as reported by the server.
        /// </summary>
        public string Raw { get; }

        /// <summary>
        /// The slots owned by this server.
        /// </summary>
        public IList<SlotRange> Slots { get; }
#nullable disable
    }

    internal class ClientClusterConfig
    {
        /// <summary>
        /// Minimum hash slot value.
        /// </summary>
        public static readonly int MIN_HASH_SLOT_VALUE = 0;

        /// <summary>
        /// Maximum hash slot value.
        /// </summary>
        public static readonly int MAX_HASH_SLOT_VALUE = 16384;

        readonly HashSlot[] slotMap;
        readonly Worker[] workers;
        int workerCount = 1;
        List<(string, string)> orderedConfig = new();
        readonly Dictionary<string, string> config = new();

        public ClientClusterConfig(int node_count)
        {
            slotMap = new HashSlot[MAX_HASH_SLOT_VALUE];
            for (int i = 0; i < MAX_HASH_SLOT_VALUE; i++)
            {
                slotMap[i]._state = SlotState.OFFLINE;
                slotMap[i]._workerId = 0;
            }
            workers = new Worker[node_count + 1];
            workers[0].address = "unassigned";
            workers[0].port = 0;
            workers[0].nodeid = string.Empty;
            workers[0].configEpoch = 0;
            workers[0].role = NodeRole.UNASSIGNED;
            workers[0].replicaOfNodeId = string.Empty;
            workers[0].replicationOffset = 0;
            workers[0].hostname = null;
        }

        private List<int> GetSlotSequence(List<(int, int)> slotRanges)
        {
            var slots = new List<int>();
            foreach (var slotRange in slotRanges)
                for (int i = slotRange.Item1; i <= slotRange.Item2; i++)
                    slots.Add(i);
            return slots;
        }

        public void AddWorker(
            string nodeid,
            string address,
            int port,
            long configEpoch,
            NodeRole Role,
            string replicaOfNodeId,
            string hostname,
            List<(int, int)> slots)
        {
            int currentWorkerIndex = workerCount++;

            workers[currentWorkerIndex].nodeid = nodeid;
            workers[currentWorkerIndex].address = address;
            workers[currentWorkerIndex].port = port;
            workers[currentWorkerIndex].configEpoch = configEpoch;
            workers[currentWorkerIndex].role = Role;
            workers[currentWorkerIndex].replicaOfNodeId = replicaOfNodeId;
            workers[currentWorkerIndex].hostname = hostname;
            if (slots != null)
            {
                foreach (int slot in GetSlotSequence(slots))
                {
                    slotMap[slot]._state = SlotState.STABLE;
                    slotMap[slot]._workerId = (ushort)currentWorkerIndex;
                }
            }
            orderedConfig = GetOrderedClusterInfo();
        }

        public List<(string, string)> GetOrderedClusterInfo()
        {
            List<(string, string)> config = new List<(string, string)>();
            for (ushort i = 1; i < workerCount; i++)
            {
                config.Add((workers[i].nodeid, GetNodeInfo(workerId: i)));
            }

            config = config.OrderBy(x => x.Item1).ToList();
            return config;
        }

        private string GetNodeInfo(ushort workerId)
        {
            // <id> <ip:port@cport> <flags> <primary> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> <slot> ... <slot>
            //     1. <id> randomly generated 40 characters string. CLUSTER RESET HARD only.
            //     2. <ip:port@cport> node address and port throught which clients communicate.
            //     3. <flags> //TODO: hostname
            //     4. <primary> if node is replica, node Id of primary it replicates. If it is a primary -.
            //     5. <pint-sent> //TODO:
            //     6. <pong-recv> //TODO:
            //     7. <config-epoch> The configuration epoch (or version) of the current node (or of the current primary if the node is a replica).
            //     8. <link-state> //TODO:
            //     9. <slot> a hash slot number or a range of hash slots.
            //var hostname = Format.GetHostName(workers[workerId].address);
            //return $"{workers[workerId].nodeid} " +
            //    $"{workers[workerId].address}:{workers[workerId].port}@{workers[workerId].port + 10000} {hostname} " +
            //    $"{(workers[workerId].role == NodeRole.PRIMARY ? "primary" : "replica")} " +
            //    $"{(workers[workerId].role == NodeRole.REPLICA ? workers[workerId].replicaOfNodeId : "-")} " +
            //    $"0 " +
            //    $"0 " +
            //    $"{workers[workerId].configEpoch} " +
            //    $"connected" +
            //    $"{GetSlotRange(workerId)}\n";

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
                $"{GetSlotRange(workerId)}\n";
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

        private void PrintOrderedConfigList(List<(string, string)> orderedConfig)
        {
            foreach (var entry in orderedConfig)
                Console.Write(entry.Item2);
        }

        public void PrintConfig()
        {
            Console.WriteLine($"Cluster Configuration, Client view");
            PrintOrderedConfigList(orderedConfig);
            Console.WriteLine("");
        }

        public bool EqualsConfig(string config, bool replicasAssigned = false)
        {
            string _config = config.Replace("myself,", "");
            var lines = _config.ToString().Split('\n');
            List<(string, string[])> _orderedConfig = new();

            foreach (var line in lines)
            {
                if (line == "")
                    continue;
                var properties = line.ToString().Split(' ');
                var nodeId = properties[0].Trim();
                _orderedConfig.Add((nodeId, properties));
            }
            _orderedConfig = _orderedConfig.OrderBy(x => x.Item1).ToList();

            if (_orderedConfig.Count != orderedConfig.Count)
                return false;

            for (int i = 0; i < orderedConfig.Count; i++)
            {
                if (_orderedConfig[i].Item1 != orderedConfig[i].Item1)
                    throw new Exception($"Misaligned node list");

                var _properties = _orderedConfig[i].Item2;
                var properties = orderedConfig[i].Item2.ToString().Trim().Split(' ');
                if (properties.Length != _properties.Length)
                    return false;

                if (!properties[0].Equals(_properties[0])) return false; //nodeId
                if (!properties[1].Equals(_properties[1])) return false; //endpoint
                if (replicasAssigned && !properties[2].Equals(_properties[2])) return false; //role
                if (properties.Length > 8 && !properties[8].Equals(_properties[8])) return false; //slots
            }

            return true;
        }

        public Dictionary<string, string> GetConfigInfo()
        {
            Dictionary<string, string> config = new();
            for (ushort i = 1; i < workerCount; i++)
                config.Add(workers[i].nodeid, GetNodeInfo(workerId: i));

            return config;
        }

        public Dictionary<string, string> GetNodeConfig()
        {
            Dictionary<string, string> config = new();
            for (ushort i = 1; i < workerCount; i++)
            {
                config.Add(workers[i].nodeid, GetNodeInfo(workerId: i));
            }

            return config;
        }
    }
}