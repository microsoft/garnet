// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// Holds a set of RW-esque locks, optimized for reads.
    /// 
    /// This was originally created for Vector Sets, but is general enough for reuse.
    /// For Vector Sets, these are acquired and released as needed to prevent concurrent creation/deletion operations or deletion concurrent with read operations.
    /// 
    /// These are outside of Tsavorite for re-entrancy reasons reasons.
    /// </summary>
    /// <remarks>
    /// This is a counter based r/w lock scheme, with a bit of biasing for cache line awareness.
    /// 
    /// Each "key" acquires locks based on its hash.
    /// Each hash is mapped to a range of indexes, each range is lockShardCount in length.
    /// When acquiring a shared lock, we take one index out of the keys range and acquire a read lock.
    ///   This will block exclusive locks, but not impact other readers.
    /// When acquiring an exclusive lock, we acquire write locks for all indexes in the key's range IN INCREASING _LOGICAL_ ORDER.
    ///   The order is necessary to avoid deadlocks.
    ///   By ensuring all exclusive locks walk "up" we guarantee no two exclusive lock acquisitions end up waiting for each other.
    /// 
    /// Locks themselves are just ints, where a negative value indicates an exclusive lock and a positive value is the number of active readers.
    /// Read locks are acquired optimistically, so actual lock values will fluctate above int.MinValue when an exclusive lock is held.
    /// 
    /// The last set of optimizations is around cache lines coherency:
    ///   We assume cache lines of 64-bytes (the x86 default, which is also true for some [but not all] ARM processors) and size counters-per-core in multiples of that
    ///   We access array elements via reference, to avoid thrashing cache lines due to length checks
    ///   Each shard is placed, in so much as is possible, into a different cache line rather than grouping a hash's counts physically near each other
    ///     This will tend to allow a core to retain ownership of the same cache lines even as it moves between different hashes
    ///     
    /// Experimentally (using some rough microbenchmarks) various optimizations are worth (on either shared or exclusive acquisiton paths):
    ///   - Split shards across cache lines   : 7x (read path), 2.5x (write path)
    ///   - Fast math instead of mod and mult : 50% (read path), 20% (write path)
    ///   - Unsafe ref instead of array access:  0% (read path), 10% (write path)
    /// </remarks>
    public struct ReadOptimizedLock
    {
        // Beyond 4K bytes per core we're well past "this is worth the tradeoff", so cut off then.
        //
        // Must be a power of 2.
        private const int MaxPerCoreContexts = 1_024;

        /// <summary>
        /// Estimated size of cache lines on a processor.
        /// 
        /// Generally correct for x86-derived processors, sometimes correct for ARM-derived ones.
        /// </summary>
        public const int CacheLineSizeBytes = 64;

        [ThreadStatic]
        private static int ProcessorHint;

        private readonly int[] lockCounts;
        private readonly int coreSelectionMask;
        private readonly int perCoreCounts;
        private readonly ulong perCoreCountsFastMod;
        private readonly byte perCoreCountsMultShift;

        /// <summary>
        /// Create a new <see cref="ReadOptimizedLock"/>.
        /// 
        /// <paramref name="estimatedSimultaneousActiveLockers"/> accuracy impacts performance, not correctness.
        /// 
        /// Too low and unrelated locks will end up delaying each other.
        /// Too high and more memory than is necessary will be used.
        /// </summary>
        public ReadOptimizedLock(int estimatedSimultaneousActiveLockers)
        {
            Debug.Assert(estimatedSimultaneousActiveLockers > 0);

            // ~1 per core
            var coreCount = (int)BitOperations.RoundUpToPowerOf2((uint)Environment.ProcessorCount);
            coreSelectionMask = coreCount - 1;

            // Use estimatedSimultaneousActiveLockers to determine number of shards per lock.
            // 
            // We scale up to a whole multiple of CacheLineSizeBytes to reduce cache line thrashing.
            //
            // We scale to a power of 2 to avoid divisions (and some multiplies) in index calculation.
            perCoreCounts = estimatedSimultaneousActiveLockers;
            if (perCoreCounts % (CacheLineSizeBytes / sizeof(int)) != 0)
            {
                perCoreCounts += (CacheLineSizeBytes / sizeof(int)) - (perCoreCounts % (CacheLineSizeBytes / sizeof(int)));
            }
            Debug.Assert(perCoreCounts % (CacheLineSizeBytes / sizeof(int)) == 0, "Each core should be whole cache lines of data");

            perCoreCounts = (int)BitOperations.RoundUpToPowerOf2((uint)perCoreCounts);

            // Put an upper bound of ~1 page worth of locks per core (which is still quite high).
            //
            // For the largest realistic machines out there (384 cores) this will put us at around ~2M of lock data, max.
            if (perCoreCounts is <= 0 or > MaxPerCoreContexts)
            {
                perCoreCounts = MaxPerCoreContexts;
            }

            // Pre-calculate an alternative to %, as that division will be in the hot path
            perCoreCountsFastMod = (ulong.MaxValue / (uint)perCoreCounts) + 1;

            // Avoid two multiplies in the hot path
            perCoreCountsMultShift = (byte)BitOperations.Log2((uint)perCoreCounts);

            var numInts = coreCount * perCoreCounts;
            lockCounts = new int[numInts];
        }

        /// <summary>
        /// Take a hash and a _hint_ about the current processor and determine which count should be used.
        /// 
        /// Walking <paramref name="currentProcessorHint"/> from 0 to (<see cref="coreSelectionMask"/> + 1) [exclusive] will return
        /// all possible counts for a given hash.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly int CalculateIndex(long hashLong, int currentProcessorHint)
        {
            // Throw away half the top half of the hash
            //
            // This set of locks will be small enough that the extra bits shoulnd't matter
            var hash = (int)hashLong;

            // Hint might be out of range, so force it into the space we expect
            var currentProcessor = currentProcessorHint & coreSelectionMask;

            var startOfCoreCounts = currentProcessor << perCoreCountsMultShift;

            // Avoid doing a division in the hot path
            // Based on: https://github.com/dotnet/runtime/blob/3a95842304008b9ca84c14b4bec9ec99ed5802db/src/libraries/System.Private.CoreLib/src/System/Collections/HashHelpers.cs#L99
            var hashOffset = (uint)(((((perCoreCountsFastMod * (uint)hash) >> 32) + 1) << perCoreCountsMultShift) >> 32);

            Debug.Assert(hashOffset == ((uint)hash % perCoreCounts), "Replacing mod with multiplies failed");

            var ix = (int)(startOfCoreCounts + hashOffset);

            Debug.Assert(ix >= 0 && ix < lockCounts.Length, "About to do something out of bounds");

            return ix;
        }

        /// <summary>
        /// Attempt to acquire a shared lock for the given hash.
        /// 
        /// Will block exclusive locks until released.
        /// </summary>
        public readonly bool TryAcquireSharedLock(long hash, out int lockToken)
        {
            var ix = CalculateIndex(hash, GetProcessorHint());

            ref var acquireRef = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(lockCounts), ix);

            var res = Interlocked.Increment(ref acquireRef);
            if (res < 0)
            {
                // Exclusively locked
                _ = Interlocked.Decrement(ref acquireRef);
                Unsafe.SkipInit(out lockToken);
                return false;
            }

            lockToken = ix;
            return true;
        }

        /// <summary>
        /// Acquire a shared lock for the given hash, blocking until that succeeds.
        /// 
        /// Will block exclusive locks until released.
        /// </summary>
        public readonly void AcquireSharedLock(long hash, out int lockToken)
        {
            var ix = CalculateIndex(hash, GetProcessorHint());

            ref var acquireRef = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(lockCounts), ix);

            while (true)
            {
                var res = Interlocked.Increment(ref acquireRef);
                if (res < 0)
                {
                    // Exclusively locked
                    _ = Interlocked.Decrement(ref acquireRef);

                    // Spin until we can grab this one
                    _ = Thread.Yield();
                }
                else
                {
                    lockToken = ix;
                    return;
                }
            }
        }

        /// <summary>
        /// Release a lock previously acquired with <see cref="TryAcquireSharedLock(long, out int)"/> or <see cref="AcquireSharedLock(long, out int)"/>.
        /// </summary>
        public readonly void ReleaseSharedLock(int lockToken)
        {
            Debug.Assert(lockToken >= 0 && lockToken < lockCounts.Length, "Invalid lock token");

            ref var releaseRef = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(lockCounts), lockToken);

            _ = Interlocked.Decrement(ref releaseRef);
        }

        /// <summary>
        /// Attempt to acquire an exclusive lock for the given hash.
        /// 
        /// Will block all other locks until released.
        /// </summary>
        public readonly bool TryAcquireExclusiveLock(long hash, out int lockToken)
        {
            ref var countRef = ref MemoryMarshal.GetArrayDataReference(lockCounts);

            var coreCount = coreSelectionMask + 1;
            for (var i = 0; i < coreCount; i++)
            {
                var acquireIx = CalculateIndex(hash, i);
                ref var acquireRef = ref Unsafe.Add(ref countRef, acquireIx);

                if (Interlocked.CompareExchange(ref acquireRef, int.MinValue, 0) != 0)
                {
                    // Failed, release previously acquired
                    for (var j = 0; j < i; j++)
                    {
                        var releaseIx = CalculateIndex(hash, j);

                        ref var releaseRef = ref Unsafe.Add(ref countRef, releaseIx);
                        while (Interlocked.CompareExchange(ref releaseRef, 0, int.MinValue) != int.MinValue)
                        {
                            // Optimistic shared lock got us, back off and try again
                            _ = Thread.Yield();
                        }
                    }

                    Unsafe.SkipInit(out lockToken);
                    return false;
                }
            }

            // Successfully acquired all shards exclusively

            // Throwing away half the hash shouldn't affect correctness since we do the same thing when processing the full hash
            lockToken = (int)hash;

            return true;
        }


        /// <summary>
        /// Acquire an exclusive lock for the given hash, blocking until that succeeds.
        /// 
        /// Will block all other locks until released.
        /// </summary>
        public readonly void AcquireExclusiveLock(long hash, out int lockToken)
        {
            ref var countRef = ref MemoryMarshal.GetArrayDataReference(lockCounts);

            var coreCount = coreSelectionMask + 1;
            for (var i = 0; i < coreCount; i++)
            {
                var acquireIx = CalculateIndex(hash, i);

                ref var acquireRef = ref Unsafe.Add(ref countRef, acquireIx);
                while (Interlocked.CompareExchange(ref acquireRef, int.MinValue, 0) != 0)
                {
                    // Optimistic shared lock got us, or conflict with some other excluive lock acquisition
                    //
                    // Backoff and try again
                    _ = Thread.Yield();
                }
            }

            // Throwing away half the hash shouldn't affect correctness since we do the same thing when processing the full hash
            lockToken = (int)hash;
        }

        /// <summary>
        /// Release a lock previously acquired with <see cref="TryAcquireExclusiveLock(long, out int)"/>, <see cref="AcquireExclusiveLock(long, out int)"/>, or <see cref="TryPromoteSharedLock(long, int, out int)"/>.
        /// </summary>
        public readonly void ReleaseExclusiveLock(int lockToken)
        {
            // The lockToken is a hash, so no range check here

            ref var countRef = ref MemoryMarshal.GetArrayDataReference(lockCounts);

            var hash = lockToken;

            var coreCount = coreSelectionMask + 1;
            for (var i = 0; i < coreCount; i++)
            {
                var releaseIx = CalculateIndex(hash, i);

                ref var releaseRef = ref Unsafe.Add(ref countRef, releaseIx);
                while (Interlocked.CompareExchange(ref releaseRef, 0, int.MinValue) != int.MinValue)
                {
                    // Optimistic shared lock got us, back off and try again
                    _ = Thread.Yield();
                }
            }
        }

        /// <summary>
        /// Attempt to promote a shared lock previously acquired via <see cref="TryAcquireSharedLock(long, out int)"/> or <see cref="AcquireSharedLock(long, out int)"/> to an exclusive lock.
        /// 
        /// If successful, will block all other locks until released.
        /// 
        /// If successful, must be released with <see cref="ReleaseExclusiveLock(int)"/>.
        /// 
        /// If unsuccessful, shared lock will still be held and must be released with <see cref="ReleaseSharedLock(int)"/>.
        /// </summary>
        public readonly bool TryPromoteSharedLock(long hash, int lockToken, out int newLockToken)
        {
            Debug.Assert(Interlocked.CompareExchange(ref lockCounts[lockToken], 0, 0) > 0, "Illegal call when not holding shard lock");

            Debug.Assert(lockToken >= 0 && lockToken < lockCounts.Length, "Invalid lock token");

            ref var countRef = ref MemoryMarshal.GetArrayDataReference(lockCounts);

            var coreCount = coreSelectionMask + 1;
            for (var i = 0; i < coreCount; i++)
            {
                var acquireIx = CalculateIndex(hash, i);
                ref var acquireRef = ref Unsafe.Add(ref countRef, acquireIx);

                if (acquireIx == lockToken)
                {
                    // Do the promote
                    if (Interlocked.CompareExchange(ref acquireRef, int.MinValue, 1) != 1)
                    {
                        // Failed, release previously acquired all of which are exclusive locks
                        for (var j = 0; j < i; j++)
                        {
                            var releaseIx = CalculateIndex(hash, j);

                            ref var releaseRef = ref Unsafe.Add(ref countRef, releaseIx);
                            while (Interlocked.CompareExchange(ref releaseRef, 0, int.MinValue) != int.MinValue)
                            {
                                // Optimistic shared lock got us, back off and try again
                                _ = Thread.Yield();
                            }
                        }

                        // Note we're still holding the shared lock here
                        Unsafe.SkipInit(out newLockToken);
                        return false;
                    }
                }
                else
                {
                    // Otherwise attempt an exclusive acquire
                    if (Interlocked.CompareExchange(ref acquireRef, int.MinValue, 0) != 0)
                    {
                        // Failed, release previously acquired - one of which MIGHT be the shared lock
                        for (var j = 0; j < i; j++)
                        {
                            var releaseIx = CalculateIndex(hash, j);
                            var releaseTargetValue = releaseIx == lockToken ? 1 : 0;

                            ref var releaseRef = ref Unsafe.Add(ref countRef, releaseIx);
                            while (Interlocked.CompareExchange(ref releaseRef, releaseTargetValue, int.MinValue) != int.MinValue)
                            {
                                // Optimistic shared lock got us, back off and try again
                                _ = Thread.Yield();
                            }
                        }

                        // Note we're still holding the shared lock here
                        Unsafe.SkipInit(out newLockToken);
                        return false;
                    }
                }
            }

            // Throwing away half the hash shouldn't affect correctness since we do the same thing when processing the full hash
            newLockToken = (int)hash;
            return true;
        }

        /// <summary>
        /// Get a somewhat-correlated-to-processor value.
        /// 
        /// While we could use <see cref="Thread.GetCurrentProcessorId()"/>, that isn't fast on all platforms.
        /// 
        /// For our purposes, we just need something that will tend to keep different active processors
        /// from touching each other.  ManagedThreadId works well enough.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetProcessorHint()
        {
            var ret = ProcessorHint;
            if (ret == 0)
            {
                ProcessorHint = ret = Environment.CurrentManagedThreadId;
            }

            return ret;
        }
    }
}