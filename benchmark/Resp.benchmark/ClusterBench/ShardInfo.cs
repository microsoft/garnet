// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Resp.benchmark
{
    /// <summary>
    /// Represents a primary shard in the cluster with its endpoint and assigned slot ranges.
    /// </summary>
    public class ShardInfo
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
}
