// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Shared sizing for the per-thread shard count of the device in-flight tracking
    /// (<c>NativeStorageDevice.NumShards</c>), which fans itself out across threads to remove the
    /// cache-line ping-pong a single global pending counter plus a shared free-slot queue create when
    /// dozens of submitter and completion threads touch them on every IO.
    /// <para>
    /// The <c>2 × ProcessorCount</c> term scales the count DOWN on small boxes
    /// (<see cref="Environment.ProcessorCount"/> honors process CPU affinity and cgroup limits); the cap
    /// bounds it on large ones, where each additional shard costs a slot-table block and another step in
    /// the O(shards) in-flight scan. Exceeding the cap is not needed: submitter threads beyond the shard
    /// count simply share a shard, and sharing is harmless because each shard's in-flight is already
    /// gated by the per-thread throttle, so a shared shard neither exhausts its slot free-list nor
    /// reintroduces global counter contention. The count need not be a power of two. An internal
    /// implementation detail, not a user knob.
    /// </para>
    /// </summary>
    internal static class ConcurrencySharding
    {
        /// <summary>Formula: two shards per logical processor, capped.</summary>
        internal static int Compute(int cap) => Math.Min(2 * Environment.ProcessorCount, cap);

        /// <summary>
        /// Device in-flight shard count. Cap 32 bounds the slot table and the O(shards) in-flight scan;
        /// submitters past that share shards, which the per-thread throttle keeps safe.
        /// </summary>
        internal static readonly int NumShardCount = Compute(32);
    }
}