// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
#if DEBUG
using System.Collections.Concurrent;
#endif
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
        /// Process-wide budget this pool participates in. Never null; <see cref="NetworkBufferBudget.Disabled"/>
        /// for pools that are not connection-scaled, in which case every budget operation is inert.
        /// </summary>
        readonly NetworkBufferBudget budget;

        /// <summary>
        /// Ceiling on the total number of bytes retained on the idle free lists across all levels.
        /// Levels share this budget, so a burst on one size class can use the whole of it rather than
        /// being limited to <see cref="maxEntriesPerLevel"/> entries while other levels sit empty.
        /// </summary>
        readonly long maxPooledBytes;

        /// <summary>
        /// Bytes currently retained on the idle free lists.
        /// </summary>
        long pooledBytes;

        /// <summary>
        /// Bytes currently checked out by callers. This is the part of the footprint that scales with the
        /// number of connections, and it is not bounded by <see cref="maxPooledBytes"/>.
        /// </summary>
        long liveBytes;

        /// <summary>
        /// High-water mark of <see cref="liveBytes"/>.
        /// </summary>
        long peakLiveBytes;
        /// <summary>
        /// This is the maximum allocated buffer size that the instance can support based on the number of pool levels.
        /// </summary>
        readonly int maxAllocationSize;
        readonly ILogger logger;

        /// <summary>
        /// Pool owner type, packed into byte 1 of each <see cref="PoolEntry.source"/>.
        /// </summary>
        readonly int ownerByte;

        /// <summary>
        /// Min allocation size
        /// </summary>
        public int MinAllocationSize => minAllocationSize;

        /// <summary>
        /// Process-wide live-buffer budget this pool participates in. Never null; disabled for pools that
        /// are not connection-scaled.
        /// </summary>
        public NetworkBufferBudget Budget => budget;

        /// <summary>
        /// Bytes currently checked out of this pool by callers.
        /// </summary>
        public long LiveBytes => Interlocked.Read(ref liveBytes);

        /// <summary>
        /// Bytes currently retained on this pool's idle free lists.
        /// </summary>
        public long PooledBytes => Interlocked.Read(ref pooledBytes);

        /// <summary>
        /// Total outstanding allocation references
        /// </summary>
        int totalReferences;

        /// <summary>
        /// Total out of bound allocation requests
        /// </summary>
        int totalOutOfBoundAllocations;

#if DEBUG
        /// <summary>
        /// Tracks all outstanding (checked-out) pool entries for leak diagnosis.
        /// </summary>
        readonly ConcurrentDictionary<PoolEntry, byte> outstandingEntries = new();

        /// <summary>
        /// Timeout in milliseconds for Dispose to wait before logging outstanding entries.
        /// </summary>
        const int DisposeWaitDiagnosticMs = 5_000;
#endif

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="minAllocationSize">Smallest poolable allocation size; must be a power of two.</param>
        /// <param name="maxEntriesPerLevel">Per-level ceiling on retained idle entries.</param>
        /// <param name="numLevels">Number of size classes, each a doubling of <paramref name="minAllocationSize"/>.</param>
        /// <param name="ownerType">Subsystem that owns this pool, for diagnostics.</param>
        /// <param name="maxPooledBytes">Ceiling on total retained idle bytes across all levels. Zero derives it from <paramref name="maxEntriesPerLevel"/>.</param>
        /// <param name="budget">Process-wide live-buffer budget this pool participates in. Null means it participates in none.</param>
        /// <param name="logger">Logger.</param>
        public LimitedFixedBufferPool(int minAllocationSize, int maxEntriesPerLevel = 16, int numLevels = 4, PoolOwnerType ownerType = PoolOwnerType.Unknown, long maxPooledBytes = 0, NetworkBufferBudget budget = null, ILogger logger = null)
        {
            this.minAllocationSize = minAllocationSize;
            this.maxAllocationSize = minAllocationSize << (numLevels - 1);
            this.maxEntriesPerLevel = maxEntriesPerLevel;
            this.numLevels = numLevels;
            this.logger = logger;
            this.ownerByte = (int)ownerType << 8;
            this.budget = budget ?? NetworkBufferBudget.Disabled;
            pool = new PoolLevel[numLevels];

            if (maxPooledBytes > 0)
            {
                this.maxPooledBytes = maxPooledBytes;
            }
            else
            {
                // Preserve the historical bound: maxEntriesPerLevel entries on every level simultaneously.
                long derived = 0;
                for (var i = 0; i < numLevels; i++)
                    derived += (long)maxEntriesPerLevel * (minAllocationSize << i);
                this.maxPooledBytes = derived;
            }
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
#if DEBUG
            outstandingEntries.TryRemove(buffer, out _);
#endif
            var length = buffer.entry.Length;
            _ = Interlocked.Add(ref liveBytes, -length);
            budget.OnBufferReleased();

            var level = Position(length);
            // While the budget is binding, an over-sized idle buffer is pinned memory that the live
            // connections need, so drop it rather than holding it on the free list. Unpressured it is pooled
            // normally, so repeated short-lived connections handling moderately large payloads keep their
            // reuse. Same definition of pressure the receive shrink policy uses.
            if (level >= 0 && budget.IsUnderPressure && length > TargetSizeFor(buffer.source))
                level = -1;

            if (level >= 0)
            {
                if (pool[level] != null)
                {
                    if (Interlocked.Add(ref pooledBytes, length) <= maxPooledBytes)
                    {
                        if (Interlocked.Increment(ref pool[level].size) <= maxEntriesPerLevel)
                        {
                            Array.Clear(buffer.entry, 0, length);
                            pool[level].items.Enqueue(buffer);
                        }
                        else
                        {
                            Interlocked.Decrement(ref pool[level].size);
                            _ = Interlocked.Add(ref pooledBytes, -length);
                        }
                    }
                    else
                    {
                        _ = Interlocked.Add(ref pooledBytes, -length);
                    }
                }
            }
            Debug.Assert(totalReferences > 0, $"Return with {totalReferences}");
            Interlocked.Decrement(ref totalReferences);
        }

        /// <summary>
        /// The adapted size an entry of this kind should settle at. Send and receive have separate floors, so
        /// comparing a send buffer against the receive target would treat a correctly sized send buffer as
        /// over-sized and make send buffers un-poolable for as long as the budget binds.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int TargetSizeFor(int source)
            => (PoolEntryBufferType)(source & 0xFF) switch
            {
                PoolEntryBufferType.SaeaSendBuffer or PoolEntryBufferType.TransportSendBuffer => budget.TargetSendBufferSize,
                _ => budget.TargetBufferSize,
            };

        /// <summary>
        /// Get buffer
        /// </summary>
        /// <param name="size"></param>
        /// <param name="bufferType">Identifies the caller for leak diagnosis.</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe PoolEntry Get(int size, PoolEntryBufferType bufferType = PoolEntryBufferType.Unknown)
        {
            if (Interlocked.Increment(ref totalReferences) < 0)
            {
                Interlocked.Decrement(ref totalReferences);
                logger?.LogError("Invalid Get on disposed pool");
                return null;
            }

            var source = ownerByte | (int)bufferType;

            var live = Interlocked.Add(ref liveBytes, size);
            UpdatePeakLiveBytes(live);
            budget.OnBufferAcquired();

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
                    _ = Interlocked.Add(ref pooledBytes, -size);
                    page.Reuse();
                    page.source = source;
#if DEBUG
                    outstandingEntries[page] = 0;
#endif
                    return page;
                }
            }
            var entry = new PoolEntry(size, this);
            entry.source = source;
#if DEBUG
            outstandingEntries[entry] = 0;
#endif
            return entry;
        }

        void UpdatePeakLiveBytes(long live)
        {
            var peak = Interlocked.Read(ref peakLiveBytes);
            while (live > peak)
            {
                var seen = Interlocked.CompareExchange(ref peakLiveBytes, live, peak);
                if (seen == peak) break;
                peak = seen;
            }
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
                while (pool[i].items.TryDequeue(out var entry))
                {
                    Interlocked.Decrement(ref pool[i].size);
                    _ = Interlocked.Add(ref pooledBytes, -entry.entry.Length);
                }
            }
        }

        /// <summary>
        /// Dispose pool entries from all levels
        /// NOTE:
        ///     This is used to destroy the instance and reclaim all allocated buffer pool entries.
        ///     As a consequence it spin waits until totalReferences goes back down to 0 and blocks any future allocations.
        ///     In DEBUG builds, logs outstanding unreturned entries after a timeout for leak diagnosis.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
#if DEBUG
            var sw = Stopwatch.StartNew();
            var diagnosed = false;
#endif
            while (totalReferences > int.MinValue &&
                Interlocked.CompareExchange(ref totalReferences, int.MinValue, 0) != 0)
            {
#if DEBUG
                if (!diagnosed && sw.ElapsedMilliseconds > DisposeWaitDiagnosticMs)
                {
                    diagnosed = true;
                    var remaining = totalReferences;
                    var ownerType = (PoolOwnerType)(ownerByte >> 8);
                    logger?.LogError("LimitedFixedBufferPool.Dispose blocked with {remaining} unreturned references (poolOwner={ownerType}). Outstanding entries:", remaining, ownerType);
                    foreach (var kvp in outstandingEntries)
                    {
                        var entryBufferType = (PoolEntryBufferType)(kvp.Key.source & 0xFF);
                        var entryOwnerType = (PoolOwnerType)((kvp.Key.source >> 8) & 0xFF);
                        logger?.LogCritical("  Unreturned buffer: ownerType={ownerType}, bufferType={bufferType}, size={size}",
                            entryOwnerType, entryBufferType, kvp.Key.entry.Length);
                    }
                }
#endif
                Thread.Yield();
            }

            for (var i = 0; i < numLevels; i++)
            {
                if (pool[i] == null) continue;
                while (pool[i].size > 0)
                {
                    while (pool[i].items.TryDequeue(out var result))
                    {
                        Interlocked.Decrement(ref pool[i].size);
                        _ = Interlocked.Add(ref pooledBytes, -result.entry.Length);
                    }
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
                $"liveBytes={Format.MemoryBytes(LiveBytes)}," +
                $"peakLiveBytes={Format.MemoryBytes(Interlocked.Read(ref peakLiveBytes))}," +
                $"pooledBytes={Format.MemoryBytes(PooledBytes)}," +
                $"maxPooledBytes={Format.MemoryBytes(maxPooledBytes)}," +
                $"totalOutOfBoundAllocations={totalOutOfBoundAllocations}";

            var bufferStats = "";
            var totalBufferCount = 0;
            for (var i = 0; i < numLevels; i++)
            {
                var items = pool[i] == null || pool[i].items.IsEmpty ? 0 : pool[i].items.Count;
                totalBufferCount += items;
                bufferStats += $",{items}={Format.MemoryBytes(minAllocationSize << i)}";
            }

            stats += $",totalBufferCount={totalBufferCount}" + bufferStats;
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