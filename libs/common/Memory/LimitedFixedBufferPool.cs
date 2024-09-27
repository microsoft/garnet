// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Garnet.common
{
    /// <summary>
    /// LimitedFixedBufferPool is a pool of memory. 
    /// Internally, it is organized as an array of concurrent queues where each concurrent
    /// queue represents a memory of size in particular range. queue[i] contains memory 
    /// segments each of size (2^i * sectorSize).
    /// </summary>
    public sealed class LimitedFixedBufferPool : IDisposable
    {
        readonly PoolLevel[] pool;
        readonly int numLevels, minAllocationSize, maxEntriesPerLevel;
        /// <summary>
        /// This is the maximum allocated buffer size that the instance can support based on the number of pool levels.
        /// </summary>
        readonly int maxAllocationSize;
        readonly ILogger logger;

        /// <summary>
        /// Min allocation size
        /// </summary>
        public int MinAllocationSize => minAllocationSize;

        /// <summary>
        /// Total outstanding allocation references
        /// </summary>
        int totalReferences;

        /// <summary>
        /// Total out of bound allocation requests
        /// </summary>
        int totalOutOfBoundAllocations;

        /// <summary>
        /// Constructor
        /// </summary>
        public LimitedFixedBufferPool(int minAllocationSize, int maxEntriesPerLevel = 16, int numLevels = 4, ILogger logger = null)
        {
            this.minAllocationSize = minAllocationSize;
            this.maxAllocationSize = minAllocationSize << (numLevels - 1);
            this.maxEntriesPerLevel = maxEntriesPerLevel;
            this.numLevels = numLevels;
            this.logger = logger;
            pool = new PoolLevel[numLevels];
        }

        /// <summary>
        /// Validate if provided settings against the provided pool instance
        /// </summary>
        /// <param name="settings"></param>
        /// <returns></returns>
        public bool Validate(NetworkBufferSettings settings)
        {
            var sendBufferSize = settings.sendBufferSize;
            // Send buffer size should be inclusive of the max and min allocation sizes of this instance
            if (sendBufferSize > maxAllocationSize || sendBufferSize < minAllocationSize)
                return false;

            var initialReceiveSize = settings.initialReceiveBufferSize;
            // Initial received buffer size should be inclusive of the max and min allocation sizes of this instance
            if (initialReceiveSize > maxAllocationSize || initialReceiveSize < minAllocationSize)
                return false;

            var maxReceiveBufferSize = settings.maxReceiveBufferSize;
            // Maximum receive size should be inclusive of the max and min allocation sizes of this instance
            if (maxReceiveBufferSize > maxAllocationSize || maxReceiveBufferSize < minAllocationSize)
                return false;

            return true;
        }

        /// <summary>
        /// Return
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return(PoolEntry buffer)
        {
            var level = Position(buffer.entry.Length);
            if (level >= 0)
            {
                if (pool[level] != null)
                {
                    if (Interlocked.Increment(ref pool[level].size) <= maxEntriesPerLevel)
                    {
                        Array.Clear(buffer.entry, 0, buffer.entry.Length);
                        pool[level].items.Enqueue(buffer);
                    }
                    else
                        Interlocked.Decrement(ref pool[level].size);
                }
            }
            Debug.Assert(totalReferences > 0, $"Return with {totalReferences}");
            Interlocked.Decrement(ref totalReferences);
        }

        /// <summary>
        /// Get buffer
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe PoolEntry Get(int size)
        {
            if (Interlocked.Increment(ref totalReferences) < 0)
            {
                Interlocked.Decrement(ref totalReferences);
                logger?.LogError("Invalid Get on disposed pool");
                return null;
            }

            var level = Position(size);
            if (level == -1) Interlocked.Increment(ref totalOutOfBoundAllocations);

            if (level >= 0)
            {
                if (pool[level] == null)
                {
                    Interlocked.CompareExchange(ref pool[level], new PoolLevel(), null);
                }

                if (pool[level].items.TryDequeue(out var page))
                {
                    Interlocked.Decrement(ref pool[level].size);
                    page.Reuse();
                    return page;
                }
            }
            return new PoolEntry(size, this);
        }

        /// <summary>
        /// Purge pool entries from all levels
        /// NOTE:
        ///     This is used to reclaim any unused buffer pool entries that were previously allocated.
        ///     It does not wait for all referenced buffers to be returned.
        ///     Use Dispose of you want to destroy this instance.
        /// </summary>
        public void Purge()
        {
            for (var i = 0; i < numLevels; i++)
            {
                if (pool[i] == null) continue;
                // Keep trying Dequeuing until no items left to free
                while (pool[i].items.TryDequeue(out var _))
                    Interlocked.Decrement(ref pool[i].size);
            }
        }

        /// <summary>
        /// Dipose pool entries from all levels
        /// NOTE:
        ///     This is used to destroy the instance and reclaim all allocated buffer pool entries.
        ///     As a consequence it spin waits until totalReferences goes back down to 0 and blocks any future allocations.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
#if HANGDETECT
            int count = 0;
#endif
            while (totalReferences > int.MinValue &&
                Interlocked.CompareExchange(ref totalReferences, int.MinValue, 0) != 0)
            {
#if HANGDETECT
                    if (++count % 10000 == 0)
                        logger?.LogTrace("Dispose iteration {count}, {activeHandlerCount}", count, activeHandlerCount);
#endif
                Thread.Yield();
            }

            for (var i = 0; i < numLevels; i++)
            {
                if (pool[i] == null) continue;
                while (pool[i].size > 0)
                {
                    while (pool[i].items.TryDequeue(out var result))
                        Interlocked.Decrement(ref pool[i].size);
                    Thread.Yield();
                }
                pool[i] = null;
            }
        }

        /// <summary>
        /// Get statistics for this buffer pool
        /// </summary>
        /// <returns></returns>
        public string GetStats()
        {
            var stats = $"totalReferences={totalReferences}," +
                $"numLevels={numLevels}," +
                $"maxEntriesPerLevel={maxEntriesPerLevel}," +
                $"minAllocationSize={Format.MemoryBytes(minAllocationSize)}," +
                $"maxAllocationSize={Format.MemoryBytes(maxAllocationSize)}," +
                $"totalOutOfBoundAllocations={totalOutOfBoundAllocations}";

            var bufferStats = "";
            var totalBufferCount = 0;
            for (var i = 0; i < numLevels; i++)
            {
                if (pool[i] == null || pool[i].items.Count == 0) continue;
                totalBufferCount += pool[i].items.Count;
                bufferStats += $"<{pool[i].items.Count}:{Format.MemoryBytes(minAllocationSize << i)}>";
            }

            if (totalBufferCount > 0)
                stats += $",totalBufferCount={totalBufferCount},[" + bufferStats + "]";

            return stats;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int Position(int v)
        {
            if (v < minAllocationSize || !BitOperations.IsPow2(v))
                return -1;
            var level = GetLevel(minAllocationSize, v);
            return level >= numLevels ? -1 : level;
        }

        /// <summary>
        /// Calculate level from minAllocationSize and requestedSize
        /// </summary>
        /// <param name="minAllocationSize"></param>
        /// <param name="requestedSize"></param>
        /// <returns></returns>
        public static int GetLevel(int minAllocationSize, int requestedSize)
        {
            Debug.Assert(BitOperations.IsPow2(minAllocationSize));
            Debug.Assert(BitOperations.IsPow2(requestedSize));
            var level = requestedSize / minAllocationSize;

            return level == 1 ? 0 : BitOperations.Log2((uint)level - 1) + 1;
        }
    }
}