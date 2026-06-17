// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using StackExchange.Redis;

namespace Resp.benchmark
{
    /// <summary>
    /// Manages cluster topology discovery and provides APIs for cluster operations.
    /// Uses the existing --host and --port options to connect to any single cluster node.
    /// </summary>
    public class ClusterManager
    {
        readonly Options opts;

        public ClusterManager(Options opts)
        {
            this.opts = opts;
        }

        /// <summary>
        /// Connect to the configured node (--host/--port) and parse CLUSTER NODES to get primary shard info.
        /// Validates that all 16384 slots are assigned.
        /// </summary>
        public PrimaryInfo[] DiscoverPrimaryShards()
        {
            using var redis = ConnectionMultiplexer.Connect(
                BenchUtils.GetConfig(opts.Address, opts.Port, useTLS: opts.EnableTLS, tlsHost: opts.TlsHost, allowAdmin: true));

            var server = redis.GetServer(new IPEndPoint(IPAddress.Parse(opts.Address), opts.Port));
            var clusterConfig = server.ClusterNodes();

            var shards = ParseClusterConfig(clusterConfig);
            ValidateFullSlotCoverage(shards);

            return shards;
        }

        /// <summary>
        /// Convert SERedis ClusterConfiguration into PrimaryInfo array with replica mapping.
        /// </summary>
        private static PrimaryInfo[] ParseClusterConfig(ClusterConfiguration clusterConfig)
        {
            var shards = new List<PrimaryInfo>();
            var replicas = new List<ReplicaInfo>();

            // First pass: collect all primaries and replicas
            foreach (var node in clusterConfig.Nodes)
            {
                if (!node.IsConnected)
                {
                    Console.WriteLine($"Warning: Node {node.NodeId} ({(node.IsReplica ? "replica" : "primary")}) is not connected, skipping.");
                    continue;
                }

                if (node.IsReplica)
                {
                    // Collect replica information
                    var replica = new ReplicaInfo
                    {
                        NodeId = node.NodeId,
                        Address = node.EndPoint is IPEndPoint ipEp ? ipEp.Address.ToString() : node.EndPoint.ToString(),
                        Port = node.EndPoint is IPEndPoint ipEp2 ? ipEp2.Port : 0,
                        ParentId = node.ParentNodeId
                    };
                    replicas.Add(replica);
                }
                else
                {
                    // Collect primary shard
                    var shard = new PrimaryInfo
                    {
                        NodeId = node.NodeId,
                        Address = node.EndPoint is IPEndPoint ipEp ? ipEp.Address.ToString() : node.EndPoint.ToString(),
                        Port = node.EndPoint is IPEndPoint ipEp2 ? ipEp2.Port : 0,
                    };

                    // Collect slot ranges from this node's slots
                    foreach (var slot in node.Slots)
                        shard.SlotRanges.Add((slot.From, slot.To));

                    if (shard.SlotRanges.Count > 0)
                        shards.Add(shard);
                }
            }

            if (shards.Count == 0)
                throw new Exception("No primary shards with assigned slots found in CLUSTER NODES response.");

            // Second pass: map replicas to their primaries
            foreach (var replica in replicas)
            {
                var primary = shards.FirstOrDefault(s => s.NodeId == replica.ParentId);
                if (primary != null)
                {
                    primary.Replicas.Add(replica);
                }
                else
                {
                    Console.WriteLine($"Warning: Replica {replica.NodeId} references unknown primary {replica.ParentId}, skipping.");
                }
            }

            return shards.ToArray();
        }

        /// <summary>
        /// Validate that all 16384 slots (0-16383) are assigned across the discovered shards.
        /// </summary>
        private static void ValidateFullSlotCoverage(PrimaryInfo[] shards)
        {
            var covered = new bool[16384];

            foreach (var shard in shards)
            {
                foreach (var (start, end) in shard.SlotRanges)
                {
                    for (int s = start; s <= end; s++)
                        covered[s] = true;
                }
            }

            var missing = new List<int>();
            for (int i = 0; i < 16384; i++)
            {
                if (!covered[i])
                    missing.Add(i);
            }

            if (missing.Count > 0)
            {
                var ranges = CompactRanges(missing);
                var rangeStr = string.Join(", ", ranges.Take(5).Select(r => r.Start == r.End ? $"{r.Start}" : $"{r.Start}-{r.End}"));
                if (ranges.Count > 5)
                    rangeStr += $" ... ({missing.Count} total unassigned slots)";

                throw new Exception($"Cluster does not have full slot coverage. Missing slots: {rangeStr}");
            }
        }

        private static List<(int Start, int End)> CompactRanges(List<int> slots)
        {
            var ranges = new List<(int Start, int End)>();
            if (slots.Count == 0) return ranges;

            int start = slots[0], end = slots[0];
            for (int i = 1; i < slots.Count; i++)
            {
                if (slots[i] == end + 1)
                    end = slots[i];
                else
                {
                    ranges.Add((start, end));
                    start = end = slots[i];
                }
            }
            ranges.Add((start, end));
            return ranges;
        }
    }
}
