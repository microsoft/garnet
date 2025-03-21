// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
#define CHECK_FREE      // disabled by default in Release due to overhead
#endif
// #define CHECK_FOR_LEAKS // disabled by default due to overhead

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Tsavorite.core.Utilities;

namespace Tsavorite.core
{
    /// <summary>
    /// Represents a sector-aligned unmanaged memory block, either owned by the <see cref="SectorAlignedMemoryPool"/> or allocated by the caller.
    /// </summary>
    public sealed unsafe class SectorAlignedMemory : IDisposable
    {
        // Byte #31 is used to denote free (1) or in-use (0) page
        private const int FreeBitMask = 1 << 31;

        private readonly SafeNativeMemoryHandle handle;

        /// <summary>
        /// Pointer to the memory.
        /// </summary>
        public byte* Pointer => handle.Pointer;

        /// <summary>
        /// Length of the memory.
        /// </summary>
        public int Length { get; set; }

        /// <summary>
        /// Valid offset
        /// </summary>
        public int ValidOffset { get; set; }

        /// <summary>
        /// Required bytes
        /// </summary>
        public int RequiredBytes { get; set; }

        /// <summary>
        /// Available bytes
        /// </summary>
        public int AvailableBytes { get; set; }

        private int levelAndFlags;

        /// <summary>
        /// The level of this memory in the buffer pool.
        /// <list type="bullet">
        /// <item>
        ///   If the memory is managed by a pool, this represents the level 
        ///   corresponds to a segment with size of <c>sectorSize * 2^level</c>
        /// </item>
        /// <item>
        ///   If the memory is not managed by a pool, this returns zero. 
        /// </item>
        /// </list>
        /// </summary>
        private int Level => levelAndFlags & ~FreeBitMask;

        /// <summary>
        /// If the memory is managed by a pool, the memory pool this buffer belongs to. Otherwise, <see langword="null"/>.
        /// </summary>
        internal SectorAlignedMemoryPool pool;

        /// <summary>
        /// Indicates whether a block is free or in-use by the pool.
        /// </summary>
        internal bool Free
        {
            get => (levelAndFlags & FreeBitMask) != 0;
            set
            {
                if (value)
                {
#if CHECK_FREE
                    if (Free)
                        throw new TsavoriteException("Attempting to return an already-free block");
#endif // CHECK_FREE
                    levelAndFlags |= FreeBitMask;
                }
                else
                {
#if CHECK_FREE
                    if (!Free)
                        throw new TsavoriteException("Attempting to allocate an already-allocated block");
#endif // CHECK_FREE
                    levelAndFlags &= ~FreeBitMask;
                }
            }
        }

        /// <summary>
        /// Create new instance of SectorAlignedMemory
        /// </summary>
        private SectorAlignedMemory(SafeNativeMemoryHandle handle, int length)
        {
            this.handle = handle;
            Length = length;
            // Assume ctor is called for allocation and leave Free unset
        }

        /// <inheritdoc cref="SectorAlignedMemory"/>
        private SectorAlignedMemory(SafeNativeMemoryHandle handle, int length, SectorAlignedMemoryPool pool, int level)
            : this(handle, length)
        {
            this.pool = pool;
            this.levelAndFlags = level;
            // Assume ctor is called for allocation and leave Free unset
        }

        /// <summary>
        /// Frees the underlying unmanaged memory
        /// </summary>
        public void Dispose() => handle?.Dispose();

        /// <summary>
        /// Clear the underlying buffer.
        /// </summary>
        public void Clear() => NativeMemory.Clear(Pointer, (nuint)Length);

        /// <summary>
        /// Return to the pool
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return() => pool?.Return(Level, this);

        /// <summary>
        /// Get valid pointer
        /// </summary>
        public byte* GetValidPointer() => Pointer + ValidOffset;

        /// <summary>
        /// Gets a span over the allocated memory.
        /// </summary>
        public Span<byte> AsSpan() => new(Pointer, Length);

        /// <summary>
        /// Gets a valid span which is calculated using <see cref="ValidOffset"/>
        /// </summary>
        public Span<byte> AsValidSpan() => new(Pointer + ValidOffset, Length - ValidOffset);

        /// <inheritdoc/>
        public override string ToString() => $"{(nuint)Pointer} {ValidOffset} {RequiredBytes} {AvailableBytes} {Free}";

        /// <summary>
        /// Allocates unmanaged aligned memory.
        /// </summary>
        /// <param name="byteCount">The total number of bytes to allocate.</param>
        /// <param name="alignment">The alignment.</param>
        /// <returns>Returns a <see cref="SectorAlignedMemory"/> instance wrapping the memory.</returns>
        public static SectorAlignedMemory Allocate(int byteCount, uint alignment)
        {
            var handle = SafeNativeMemoryHandle.Allocate(byteCount, alignment);
            return new SectorAlignedMemory(handle, byteCount);
        }

        /// <summary>
        /// Allocates unmanaged aligned memory segment for the pool.
        /// </summary>
        /// <param name="level">The level of the memory within the <paramref name="pool"/>.</param>
        /// <param name="alignment">The alignment size.</param>
        /// <param name="pool">The memory pool from which owns this memory.</param>
        /// <returns>Returns a pool owned <see cref="SectorAlignedMemory"/> instance wrapping the memory.</returns>
        internal static SectorAlignedMemory Allocate(int level, uint alignment, SectorAlignedMemoryPool pool)
        {
            var byteCount = checked((int)alignment * (1 << level));
            var handle = SafeNativeMemoryHandle.Allocate(byteCount, alignment);
            return new SectorAlignedMemory(handle, byteCount, pool, level);
        }
    }

    /// <summary>
    /// Represents a pool of memory.
    /// <para/>
    /// Internally, it is organized as an array of concurrent queues where each concurrent
    /// queue represents a memory of size in particular range. <c>queue[level]</c> contains memory 
    /// segments each of size <c>(sectorSize * 2^level)</c>.
    /// </summary>
    public sealed unsafe class SectorAlignedMemoryPool : IDisposable
    {
        private const int Levels = 32;

        private readonly int recordSize;
        private readonly int sectorSize;
        private readonly ConcurrentQueue<SectorAlignedMemory>[] queue;
#if CHECK_FOR_LEAKS
        static int totalGets, totalReturns;
#endif

        /// <summary>
        /// Initializes a new sector-aligned memory pool.
        /// </summary>
        /// <param name="recordSize">Record size. May be 1 if allocations of different lengths will be made</param>
        /// <param name="sectorSize">Sector size for memory alignment, e.g. from log device</param>
        public SectorAlignedMemoryPool(int recordSize, int sectorSize)
        {
            queue = new ConcurrentQueue<SectorAlignedMemory>[Levels];
            this.recordSize = recordSize;
            this.sectorSize = sectorSize;
        }

        /// <summary>
        /// Returns a memory to the pool.
        /// </summary>
        /// <param name="level">The level of the page in the pool.</param>
        /// <param name="memory">The memory instance.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return(int level, SectorAlignedMemory memory)
        {
#if CHECK_FOR_LEAKS
            Interlocked.Increment(ref totalReturns);
#endif

            memory.AvailableBytes = 0;
            memory.RequiredBytes = 0;
            memory.ValidOffset = 0;

            memory.Clear();
            memory.Free = true;

            Debug.Assert(queue[level] != null);
            Debug.Assert(memory.pool == this);
            queue[level].Enqueue(memory);
        }

        /// <summary>
        /// Calculates the level of memory based on required sectors.
        /// </summary>
        /// <param name="sectors">Number of sectors required.</param>
        /// <returns>The corresponding level for the memory.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int Level(int sectors)
        {
            return sectors == 1 ? 0 : BitOperations.Log2((uint)sectors - 1) + 1;
        }

        /// <summary>
        /// Allocates or retrieves a sector-aligned memory from the pool.
        /// </summary>
        /// <remarks>
        /// <paramref name="numRecords"/> is equivalent to requesting one record, returning smallest possible segment from the pool.
        /// </remarks>
        /// <param name="numRecords">The number of records required.</param>    
        /// <returns>
        /// A <see cref="SectorAlignedMemory"/> segment which length is the requested memory rounded up to nearest multiple of <c>sectorSize × 2^level</c> 
        /// for some <c>level</c> between 0 and 32.
        /// </returns>
        /// <example>
        /// <code>
        /// using var pool = new SectorAlignedMemory(recordSize: 1, sectorSize: 512);
        /// var buffer = pool.Get(1000);
        /// Debug.Assert(buffer.Length == 1024);
        /// buffer.Return();
        /// </code>
        /// </example>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe SectorAlignedMemory Get(int numRecords)
        {
#if CHECK_FOR_LEAKS
            Interlocked.Increment(ref totalGets);
#endif
            numRecords = Math.Max(numRecords, 1);

            // How many sectors do we need?
            var sectorsRequired = (numRecords * recordSize + (sectorSize - 1)) / sectorSize;
            var level = Level(sectorsRequired);
            if (queue[level] == null)
            {
                var localPool = new ConcurrentQueue<SectorAlignedMemory>();
                Interlocked.CompareExchange(ref queue[level], localPool, null);
            }

            if (queue[level].TryDequeue(out var page))
            {
                page.Free = false;
                return page;
            }

            return SectorAlignedMemory.Allocate(level, (uint)sectorSize, pool: this);
        }

        /// <summary>
        /// Free all the unmanaged memory owned by the pool.
        /// </summary>
        public void Dispose()
        {
#if CHECK_FOR_LEAKS
            Debug.Assert(totalGets == totalReturns);
#endif
            for (var i = 0; i < queue.Length; i++)
            {
                if (queue[i] == null) continue;
                while (queue[i].TryDequeue(out var result))
                {
                    result.Dispose();
                }
            }
        }
    }
}