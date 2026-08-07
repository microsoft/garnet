// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Shared sizing for the per-thread shard/stripe counts of the hot concurrent data structures
    /// that fan themselves out across threads to remove cache-line ping-pong: the device in-flight
    /// tracking (<c>NativeStorageDevice.NumShards</c>) and the sector-aligned buffer-pool free-lists
    /// (<c>SectorAlignedBufferPool.stripes</c>). Both derive their count from the same
    /// <see cref="Compute"/> formula so the sizing cannot diverge, but each carries its own cap
    /// because their contention floors differ in kind:
    /// <list type="bullet">
    /// <item><c>NumShards</c> must stay at or above the peak concurrent submitter count (roughly
    /// <c>2 × ProcessorCount</c>) — below it, distinct concurrent submitters collide on a shard and
    /// the per-shard in-flight counters and slot free-lists re-contend — so it is capped at 32.</item>
    /// <item><c>stripes</c> traffic is bounded by the device in-flight throttle rather than the
    /// submitter count, so it is thread-count-insensitive and holds peak at a smaller count; it is
    /// capped at 16.</item>
    /// </list>
    /// <para>
    /// The <c>2 × ProcessorCount</c> term scales the count DOWN on small boxes;
    /// <see cref="Environment.ProcessorCount"/> honors process CPU affinity and cgroup limits. The
    /// counts need not be powers of two. An internal implementation detail, not a user knob.
    /// </para>
    /// </summary>
    internal static class ConcurrencySharding
    {
        /// <summary>Shared formula: two shards per logical processor, capped.</summary>
        internal static int Compute(int cap) => Math.Min(2 * Environment.ProcessorCount, cap);

        /// <summary>
        /// Device in-flight shard count. Cap 32: must stay above the peak concurrent submitter count
        /// or the per-shard in-flight counters and slot free-lists re-contend.
        /// </summary>
        internal static readonly int NumShardCount = Compute(32);

        /// <summary>
        /// Buffer-pool free-list stripe count. Cap 16: thread-count-insensitive (its traffic is
        /// bounded by the device in-flight throttle, not the submitter count).
        /// </summary>
        internal static readonly int StripeCount = Compute(16);
    }
}