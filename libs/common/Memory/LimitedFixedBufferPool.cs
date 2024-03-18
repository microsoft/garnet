// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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
        readonly bool useHandlesForPin;
        readonly ILogger logger;

        /// <summary>
        /// Min allocation size
        /// </summary>
        public int MinAllocationSize => minAllocationSize;

        int totalAllocations;

        /// <summary>
        /// Constructor
        /// </summary>
        public LimitedFixedBufferPool(int minAllocationSize, int maxEntriesPerLevel = 16, int numLevels = 4, bool useHandlesForPin = false, ILogger logger = null)
        {
            this.minAllocationSize = minAllocationSize;
            this.maxEntriesPerLevel = maxEntriesPerLevel;
            this.numLevels = numLevels;
            this.useHandlesForPin = useHandlesForPin;
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
                logger?.LogError($"Invalid Get on disposed pool");
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
            return new PoolEntry(size, this, useHandlesForPin);
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
            for (int i = 0; i < numLevels; i++)
            {
                if (pool[i] == null) continue;
                while (pool[i].size > 0)
                {
                    while (pool[i].items.TryDequeue(out var result))
                    {
                        Interlocked.Decrement(ref pool[i].size);
                    }
                    Thread.Yield();
                }
                pool[i] = null;
            }
        }

        /// <summary>
        /// Print pool contents
        /// </summary>
        public void Print()
        {
            for (int i = 0; i < numLevels; i++)
            {
                if (pool[i] == null) continue;
                foreach (var item in pool[i].items)
                {
                    Console.WriteLine("  " + item.entry.Length.ToString());
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int Position(int v)
        {
            if (v < minAllocationSize || !IsPowerOfTwo(v))
                return -1;

            v /= minAllocationSize;

            if (v == 1) return 0;
            v--;

            int r = 0; // r will be lg(v)
            while (true) // unroll for more speed...
            {
                v >>= 1;
                if (v == 0) break;
                r++;
            }
            if (r + 1 >= numLevels)
                return -1;
            return r + 1;
        }

        static bool IsPowerOfTwo(long x)
        {
            return (x > 0) && ((x & (x - 1)) == 0);
        }
    }
}