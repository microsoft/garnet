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
    /// The shard count must stay at or above the peak concurrent submitter count (roughly
    /// <c>2 × ProcessorCount</c>) — below it, distinct concurrent submitters collide on a shard and the
    /// per-shard in-flight counters and slot free-lists re-contend — so it is capped at 32. The
    /// <c>2 × ProcessorCount</c> term scales the count DOWN on small boxes;
    /// <see cref="Environment.ProcessorCount"/> honors process CPU affinity and cgroup limits. The
    /// count need not be a power of two. An internal implementation detail, not a user knob.
    /// </para>
    /// </summary>
    internal static class ConcurrencySharding
    {
        /// <summary>Formula: two shards per logical processor, capped.</summary>
        internal static int Compute(int cap) => Math.Min(2 * Environment.ProcessorCount, cap);

        /// <summary>
        /// Device in-flight shard count. Cap 32: must stay above the peak concurrent submitter count
        /// or the per-shard in-flight counters and slot free-lists re-contend.
        /// </summary>
        internal static readonly int NumShardCount = Compute(32);
    }
}