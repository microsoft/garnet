// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Numerics;

namespace Tsavorite.core
{
    /// <summary>
    /// Shared sizing for the structures that fan themselves out across threads to remove the cache-line
    /// ping-pong that a single shared counter, queue, or lock creates once dozens of threads touch it on
    /// every operation: the device in-flight tracking (<c>NativeStorageDevice.NumShards</c>) and the
    /// buffer pool's shared overflow depot.
    /// <para>
    /// The <c>2 × ProcessorCount</c> term scales the count DOWN on small boxes
    /// (<see cref="Environment.ProcessorCount"/> honors process CPU affinity and cgroup limits); the cap
    /// bounds it on large ones, where each additional shard costs memory and another step in whatever
    /// scan walks the shards. A fixed count cannot serve both ends: sized for a small box it serializes a
    /// large one, and sized for a large box it wastes memory on a small one. Internal implementation
    /// details, not user knobs.
    /// </para>
    /// </summary>
    internal static class ConcurrencySharding
    {
        /// <summary>Formula: two shards per logical processor, capped.</summary>
        internal static int Compute(int cap) => Math.Min(2 * Environment.ProcessorCount, cap);

        /// <summary>
        /// Same formula, clamped to <paramref name="min"/> and rounded up to a power of two, for callers
        /// that index shards with a mask rather than a modulo.
        /// </summary>
        internal static int ComputePow2(int min, int cap)
            => (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(Compute(cap), min));

        /// <summary>
        /// Device in-flight shard count. Cap 32 bounds the slot table and the O(shards) in-flight scan;
        /// submitters past that share shards, which the per-thread throttle keeps safe.
        /// </summary>
        internal static readonly int NumShardCount = Compute(32);

        /// <summary>
        /// Buffer pool depot stripe count (a power of two, so the depot can be indexed with a mask). Each
        /// stripe is an independently locked stack, so this is the number of threads that can touch the
        /// shared depot concurrently without serializing, and lock contention falls as threads per stripe.
        /// Floor 8 covers boxes whose thread counts are too low to contend. Cap 64 covers the common
        /// concurrency range while bounding both the per-class stripe array (~72 bytes per stripe per size
        /// class) and the scan a depot miss performs; workloads driving far more concurrent threads than
        /// that trade some throughput for those bounds.
        /// </summary>
        internal static readonly int DepotStripeCount = ComputePow2(min: 8, cap: 64);
    }
}