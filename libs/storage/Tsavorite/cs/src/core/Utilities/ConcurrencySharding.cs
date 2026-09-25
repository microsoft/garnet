// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Numerics;

namespace Tsavorite.core
{
    /// <summary>
    /// Shared sizing for the structures that shard themselves across threads so that a single counter,
    /// queue, or lock does not become a contended cache line once dozens of threads touch it on every
    /// operation: the device in-flight tracking (<c>NativeStorageDevice.NumShards</c>) and the buffer
    /// pool's shared overflow depot.
    /// <para>
    /// The <c>2 × ProcessorCount</c> term scales the count to the machine
    /// (<see cref="Environment.ProcessorCount"/> honors process CPU affinity and cgroup limits); the cap
    /// bounds the memory each shard costs and the length of whatever scan walks the shards. Internal
    /// implementation details, not user knobs.
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
        /// Buffer pool depot stripe count, a power of two so the depot can be indexed with a mask. Each
        /// stripe is an independently locked stack, so this is how many threads can touch the shared depot
        /// concurrently without serializing. Floor 8 keeps striping on low-processor boxes; cap 64 bounds
        /// the per-class stripe array (~72 bytes per stripe per size class) and the scan a depot miss
        /// performs.
        /// </summary>
        internal static readonly int DepotStripeCount = ComputePow2(min: 8, cap: 64);

        /// <summary>
        /// Expected number of threads concurrently renting from one buffer pool. Used to divide the pool's
        /// cacheable byte budget into equal per-thread slices, so that the slices of that many threads sum to
        /// the budget and no thread can retain enough to starve the others.
        /// </summary>
        internal static readonly int ExpectedConcurrentThreads = Compute(64);
    }
}