// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
#define CHECK_FREE      // double-free / double-allocate detection; DEBUG only
#endif

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;

    /// <summary>
    /// A sector-aligned, pinned memory buffer handed out by <see cref="SectorAlignedBufferPool"/>.
    /// </summary>
    public sealed unsafe class SectorAlignedMemory
    {
        // Bit 31 of <see cref="level"/> flags a freed (returned) buffer under CHECK_FREE.
        const int kFreeBitMask = 1 << 31;

        /// <summary>Backing array, pinned for the lifetime of the buffer.</summary>
        public byte[] buffer;

        /// <summary>Offset from <see cref="buffer"/>[0] to <see cref="aligned_pointer"/> (internal alignment padding).</summary>
        public int aligned_offset;

        /// <summary>Sector-aligned start pointer into <see cref="buffer"/>.</summary>
        public byte* aligned_pointer;

        /// <summary>Caller offset above <see cref="aligned_pointer"/> (e.g. a read rounded down to its sector start).</summary>
        public int valid_offset;

        /// <summary>Requested (unaligned) byte count for the current operation.</summary>
        public int required_bytes;

        /// <summary>Bytes available after the operation (e.g. the aligned number of bytes read).</summary>
        public int available_bytes;

        // Size level within the pool; selects the free list and the buffer size.
        private int level;
        internal int Level => level
#if CHECK_FREE
            & ~kFreeBitMask
#endif
            ;

        // The owning pool (null for standalone buffers, whose Return is a no-op).
        internal SectorAlignedBufferPool pool;

#if CHECK_FREE
        internal bool Free
        {
            get => (level & kFreeBitMask) != 0;
            set
            {
                if (value)
                {
                    if (Free)
                        throw new TsavoriteException("Attempting to return an already-free block");
                    level |= kFreeBitMask;
                }
                else
                {
                    if (!Free)
                        throw new TsavoriteException("Attempting to allocate an already-allocated block");
                    level &= ~kFreeBitMask;
                }
            }
        }
#endif

        /// <summary>Constructor for pooled buffers (the pool sets the backing array).</summary>
        public SectorAlignedMemory(int level = default) => this.level = level;

        /// <summary>Create a standalone (non-pooled) sector-aligned buffer.</summary>
        public SectorAlignedMemory(int numRecords, int sectorSize)
        {
            required_bytes = numRecords;
            var requiredSize = sectorSize + RoundUp(required_bytes, sectorSize);    // extra sector for the aligned_offset
            buffer = GC.AllocateArray<byte>(requiredSize, pinned: true);
            var bufferAddr = (long)Unsafe.AsPointer(ref buffer[0]);
            aligned_pointer = (byte*)RoundUp(bufferAddr, sectorSize);
            aligned_offset = (int)((long)aligned_pointer - bufferAddr);
        }

        /// <summary>Abandon the buffer (releases the pinned array without recycling it).</summary>
        public void Dispose()
        {
            buffer = null;
#if CHECK_FREE
            Free = true;
#endif
        }

        /// <summary>Return this buffer to its pool (no-op for standalone buffers).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return() => pool?.Return(this);

        /// <summary>Total aligned capacity of the buffer.</summary>
        public int AlignedTotalCapacity => buffer.Length - aligned_offset;

        /// <summary>Total valid capacity (aligned capacity past <see cref="valid_offset"/>).</summary>
        public int ValidTotalCapacity => AlignedTotalCapacity - valid_offset;

        /// <summary>Requested capacity past <see cref="valid_offset"/>.</summary>
        public int RequiredCapacity => required_bytes - valid_offset;

        /// <summary>Valid pointer (aligned pointer plus <see cref="valid_offset"/>).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* GetValidPointer() => aligned_pointer + valid_offset;

        /// <summary>Span over the entire valid capacity past the valid pointer.</summary>
        public Span<byte> TotalValidSpan => new(GetValidPointer(), ValidTotalCapacity);

        /// <summary>Span over <see cref="available_bytes"/> past the aligned pointer.</summary>
        public Span<byte> AvailableSpan => new(aligned_pointer, available_bytes);

        /// <summary>Span over <see cref="available_bytes"/> past the valid pointer.</summary>
        public Span<byte> AvailableValidSpan => new(GetValidPointer(), available_bytes - valid_offset);

        /// <summary>Span over the requested bytes past the valid pointer.</summary>
        public Span<byte> RequiredValidSpan => new(GetValidPointer(), RequiredCapacity);

        /// <inheritdoc/>
        public override string ToString()
            => $"aligned: [offset {aligned_offset}, ptr 0x{(long)aligned_pointer:X}]; valid_offset {valid_offset};"
             + $" reqBytes {required_bytes}; availBytes {available_bytes}; cap {AlignedTotalCapacity}"
#if CHECK_FREE
             + $"; free {Free}"
#endif
             ;
    }

    /// <summary>Point-in-time memory accounting for the process-wide buffer pool.</summary>
    public readonly struct BufferPoolStatistics
    {
        /// <summary>Live pinned bytes across every tier and all in-flight (rented) buffers.</summary>
        public long AllocatedBytes { get; init; }

        /// <summary>Bytes currently parked in the shared (cross-thread) free lists.</summary>
        public long PooledBytes { get; init; }
    }

    /// <summary>
    /// Process-wide pool of sector-aligned, pinned buffers, accessed through <see cref="Shared"/>.
    /// All buffers are aligned to <see cref="SectorSize"/> (4096), which satisfies every device whose
    /// sector size divides it. Buffers are grouped into power-of-two size levels.
    /// <para>
    /// The same-thread rent/return hot path is served by a per-thread cache (Tier 1) with no locks or
    /// atomics; cross-thread returns and overflow fall back to per-level concurrent queues (Tier 2).
    /// </para>
    /// </summary>
    public sealed unsafe class SectorAlignedBufferPool
    {
        /// <summary>Fixed alignment of every buffer handed out by the pool.</summary>
        public const int SectorSize = 4096;

        /// <summary>The process-wide pool instance.</summary>
        public static SectorAlignedBufferPool Shared { get; } = new();

        const int Levels = 32;                          // power-of-two size levels

        // Tier-1 (per-thread) caching bounds. The cap is a safety ceiling on pathological retention;
        // realistic per-thread in-flight depth (bounded by the device throttle) stays well under it.
        const int Tier1MaxBufferBytes = 64 * 1024;      // do not cache buffers larger than this per-thread
        const int Tier1ListCapacity = 8192;             // max buffers per (thread, level)

        // Tier-2 retention bound: larger buffers are dropped to GC rather than parked.
        const int MaxPooledBufferBytes = 1 * 1024 * 1024;

        // Per-thread Tier-1 cache: a LIFO stack per size level. One object per thread keeps hot-path
        // TLS access to a single read; the same-thread rent/return cycle never leaves this tier.
        sealed class ThreadCache
        {
            public readonly Stack<SectorAlignedMemory>[] lists = new Stack<SectorAlignedMemory>[Levels];
        }

        [ThreadStatic] static ThreadCache t_cache;

        // Tier-2 (shared) free lists, lazily created per level.
        static readonly ConcurrentQueue<SectorAlignedMemory>[] s_tier2 = new ConcurrentQueue<SectorAlignedMemory>[Levels];
        static long s_allocatedBytes;                   // live pinned bytes (all tiers + in-flight)
        static long s_pooledBytes;                      // bytes parked in Tier-2

        private SectorAlignedBufferPool() { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int Position(int v)
        {
            if (v == 1) return 0;
            return BitOperations.Log2((uint)v - 1) + 1;
        }

        /// <summary>Rent a fully zeroed buffer of at least <paramref name="numBytes"/> bytes.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public SectorAlignedMemory Get(int numBytes) => GetInternal(numBytes, zero: true);

        /// <summary>
        /// Rent an uninitialized buffer for a device read that will fully overwrite the read region.
        /// Skips the per-rent zeroing; callers bound their access with valid_offset / available_bytes
        /// and MUST fully overwrite the region they read from disk.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public SectorAlignedMemory GetUninitializedForDeviceRead(int numBytes) => GetInternal(numBytes, zero: false);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private SectorAlignedMemory GetInternal(int numBytes, bool zero)
        {
            var level = Position(RoundUp(numBytes, SectorSize) / SectorSize);

            // Tier-1: per-thread cache (the same-thread rent/return hot path).
            var tc = t_cache;
            if (tc != null)
            {
                var s = tc.lists[level];
                if (s != null && s.TryPop(out var hit))
                    return Prepare(hit, numBytes, zero);
            }

            // Tier-2: cross-thread returns and overflow.
            var q = s_tier2[level];
            if (q != null && q.TryDequeue(out var pooled))
            {
                _ = Interlocked.Add(ref s_pooledBytes, -pooled.buffer.Length);
                return Prepare(pooled, numBytes, zero);
            }

            return Allocate(level, numBytes);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static SectorAlignedMemory Prepare(SectorAlignedMemory page, int numBytes, bool zero)
        {
#if CHECK_FREE
            page.Free = false;
#endif
            if (zero)
                Array.Clear(page.buffer, 0, page.buffer.Length);
            page.required_bytes = numBytes;
            return page;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private SectorAlignedMemory Allocate(int level, int numBytes)
        {
            var allocSize = SectorSize * ((1 << level) + 1);    // extra sector for the leading alignment round-up
            var page = new SectorAlignedMemory(level) { pool = this };
            page.buffer = GC.AllocateArray<byte>(allocSize, pinned: true);
            var addr = (long)Unsafe.AsPointer(ref page.buffer[0]);
            page.aligned_pointer = (byte*)RoundUp(addr, SectorSize);
            page.aligned_offset = (int)((long)page.aligned_pointer - addr);
            page.required_bytes = numBytes;
            _ = Interlocked.Add(ref s_allocatedBytes, allocSize);
            return page;                                          // freshly allocated arrays are already zeroed
        }

        /// <summary>Return a buffer to the pool.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return(SectorAlignedMemory page)
        {
#if CHECK_FREE
            page.Free = true;
#endif
            page.available_bytes = 0;
            page.required_bytes = 0;
            page.valid_offset = 0;

            var sz = page.buffer.Length;
            var level = page.Level;

            // Tier-1: park in the returning thread's cache when it is small enough.
            if (sz <= Tier1MaxBufferBytes)
            {
                var tc = t_cache ??= new ThreadCache();
                var s = tc.lists[level] ??= new Stack<SectorAlignedMemory>();
                if (s.Count < Tier1ListCapacity)
                {
                    s.Push(page);
                    return;
                }
            }

            // Tier-2: cross-thread / overflow. Drop oversized buffers to GC.
            if (sz <= MaxPooledBufferBytes)
            {
                var q = s_tier2[level];
                if (q == null)
                {
                    var created = new ConcurrentQueue<SectorAlignedMemory>();
                    q = Interlocked.CompareExchange(ref s_tier2[level], created, null) ?? created;
                }
                _ = Interlocked.Add(ref s_pooledBytes, sz);
                q.Enqueue(page);
                return;
            }

            _ = Interlocked.Add(ref s_allocatedBytes, -sz);
            page.buffer = null;
        }

        /// <summary>
        /// Legacy teardown hook. The pool is process-wide, so buffers stay available for reuse; use
        /// <see cref="TrimShared"/> to explicitly release pooled memory.
        /// </summary>
        public void Free() { }

        /// <summary>Snapshot of the process-wide pool's memory accounting.</summary>
        public static BufferPoolStatistics GetGlobalStatistics()
            => new() { AllocatedBytes = Interlocked.Read(ref s_allocatedBytes), PooledBytes = Interlocked.Read(ref s_pooledBytes) };

        /// <summary>
        /// Release all buffers parked in the shared (Tier-2) lists and the calling thread's cache.
        /// Other threads' caches are bounded and reclaimed as those threads recycle or exit.
        /// </summary>
        public static void TrimShared()
        {
            for (var i = 0; i < Levels; i++)
            {
                var q = s_tier2[i];
                if (q == null)
                    continue;
                while (q.TryDequeue(out var p))
                {
                    var sz = p.buffer.Length;
                    _ = Interlocked.Add(ref s_pooledBytes, -sz);
                    _ = Interlocked.Add(ref s_allocatedBytes, -sz);
                    p.buffer = null;
                }
            }

            var tc = t_cache;
            if (tc == null)
                return;
            for (var i = 0; i < Levels; i++)
            {
                var s = tc.lists[i];
                if (s == null)
                    continue;
                while (s.TryPop(out var p))
                {
                    _ = Interlocked.Add(ref s_allocatedBytes, -p.buffer.Length);
                    p.buffer = null;
                }
            }
        }
    }
}