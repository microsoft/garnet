// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Resp.benchmark
{
    /// <summary>
    /// Represents a primary shard in the cluster with its endpoint and assigned slot ranges.
    /// </summary>
    public class PrimaryInfo
    {
        /// <summary>
        /// Node ID from CLUSTER NODES output.
        /// </summary>
        public string NodeId { get; set; }

        /// <summary>
        /// Host address of the shard.
        /// </summary>
        public string Address { get; set; }

        /// <summary>
        /// Port of the shard.
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// Slot ranges assigned to this shard. Each tuple is (startSlot, endSlot) inclusive.
        /// </summary>
        public List<(int Start, int End)> SlotRanges { get; set; } = new();

        /// <summary>
        /// Replicas of this primary shard. Empty list if no replicas exist.
        /// </summary>
        public List<ReplicaInfo> Replicas { get; set; } = new();

        /// <summary>
        /// Total number of slots assigned to this shard.
        /// </summary>
        public int TotalSlots
        {
            get
            {
                int count = 0;
                foreach (var (start, end) in SlotRanges)
                    count += end - start + 1;
                return count;
            }
        }

        /// <summary>
        /// Check if a given slot is assigned to this shard.
        /// </summary>
        public bool OwnsSlot(int slot)
        {
            foreach (var (start, end) in SlotRanges)
            {
                if (slot >= start && slot <= end)
                    return true;
            }
            return false;
        }

        public override string ToString()
            => $"{Address}:{Port} ({TotalSlots} slots)";
    }

    /// <summary>
    /// Represents a replica node in the cluster.
    /// </summary>
    public class ReplicaInfo
    {
        /// <summary>
        /// Node ID from CLUSTER NODES output.
        /// </summary>
        public string NodeId { get; set; }

        /// <summary>
        /// Host address of the replica.
        /// </summary>
        public string Address { get; set; }

        /// <summary>
        /// Port of the replica.
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// Parent primary node ID (extracted from ParentId field in CLUSTER NODES).
        /// </summary>
        public string ParentId { get; set; }

        public override string ToString()
            => $"{Address}:{Port}";
    }
}
