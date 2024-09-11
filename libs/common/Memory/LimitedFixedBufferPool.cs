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
        readonly ILogger logger;

        /// <summary>
        /// Min allocation size
        /// </summary>
        public int MinAllocationSize => minAllocationSize;

        int totalAllocations;

        /// <summary>
        /// Constructor
        /// </summary>
        public LimitedFixedBufferPool(int minAllocationSize, int maxEntriesPerLevel = 16, int numLevels = 4, ILogger logger = null)
        {
            this.minAllocationSize = minAllocationSize;
            this.maxEntriesPerLevel = maxEntriesPerLevel;
            this.numLevels = numLevels;
            this.logger = logger;
            pool = new PoolLevel[numLevels];
        }

        /// <summary>
        /// Return
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return(PoolEntry buffer)
        {
            int level = Position(buffer.entry.Length);
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
            Debug.Assert(totalAllocations > 0, $"Return with {totalAllocations}");
            Interlocked.Decrement(ref totalAllocations);
        }

        /// <summary>
        /// Get buffer
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe PoolEntry Get(int size)
        {
            if (Interlocked.Increment(ref totalAllocations) < 0)
            {
                Interlocked.Decrement(ref totalAllocations);
                logger?.LogError("Invalid Get on disposed pool");
                return null;
            }

            int level = Position(size);
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
        /// </summary>
        public void Purge()
        {
            for (var i = 0; i < numLevels; i++)
            {
                if (pool[i] == null) continue;
                // Keep trying Dequeuing until no items left to free
                while (pool[i].items.TryDequeue(out var entry))
                {
                    entry = null;
                    Interlocked.Decrement(ref pool[i].size);
                }
            }
        }

        /// <summary>
        /// Free buffer
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
#if HANGDETECT
            int count = 0;
#endif
            while (totalAllocations > int.MinValue &&
                Interlocked.CompareExchange(ref totalAllocations, int.MinValue, 0) != 0)
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
            var stats = $"totalAllocations = {totalAllocations}, " +
                $"numLevels = {numLevels}, " +
                $"maxEntriesPerLevel = {maxEntriesPerLevel}";

            var bufferStats = "";
            var totalBufferCount = 0;
            for (var i = 0; i < numLevels; i++)
            {
                if (pool[i] == null) continue;

                var count = pool[i].items.Count;

                if (count == 0) continue;

                totalBufferCount += count;
                bufferStats += $"<{count},{Format.KiloBytes(minAllocationSize * (i + 1))}KB>";

                // Keep trying Dequeuing until no items left to free
                while (pool[i].items.TryDequeue(out var entry))
                {
                    entry = null;
                    Interlocked.Decrement(ref pool[i].size);
                }
            }

            if (totalBufferCount > 0)
                stats += $", totalBufferCount:{totalBufferCount}[" + bufferStats + "]";

            return stats;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int Position(int v)
        {
            if (v < minAllocationSize || !BitOperations.IsPow2(v))
                return -1;
            return GetLevel(minAllocationSize, v);
        }

        public static int GetLevel(int minAllocationSize, int requestedSize)
        {
            Debug.Assert(BitOperations.IsPow2(minAllocationSize));
            Debug.Assert(BitOperations.IsPow2(requestedSize));
            requestedSize /= minAllocationSize;

            return requestedSize == 1 ? 0 : BitOperations.Log2((uint)requestedSize - 1) + 1;
        }
    }
}