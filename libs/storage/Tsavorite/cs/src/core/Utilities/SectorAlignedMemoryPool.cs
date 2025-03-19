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

namespace Tsavorite.core
{
    /// <summary>
    /// Represents a sector-aligned unmanaged memory block, either owned by the <see cref="SectorAlignedMemoryPool"/> or allocated by the caller.
    /// </summary>
    public sealed unsafe class SectorAlignedMemory : IDisposable
    {
        // Byte #31 is used to denote free (1) or in-use (0) page
        private const int FreeBitMask = 1 << 31;

        /// <summary>
        /// Pointer to the buffer
        /// </summary>
        public byte* BufferPtr { get; private set; }

        /// <summary>
        /// Length of the buffer.
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
        ///   If the memory is managed by a pool, this represents the in the memory pool, where 
        ///   level <c>n</c> corresponds to a memory size of <c>sectorSize * 2^n</c>
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
        /// Indicates whether a block is free or in-use.
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
        private SectorAlignedMemory(byte* buffer, int length)
        {
            BufferPtr = buffer;
            Length = length;
            // Assume ctor is called for allocation and leave Free unset
        }

        /// <summary>
        /// Create new instance of SectorAlignedMemory
        /// </summary>
        private SectorAlignedMemory(byte* buffer, int length, SectorAlignedMemoryPool pool, int level)
            : this(buffer, length)
        {
            this.pool = pool;
            this.levelAndFlags = level;
            // Assume ctor is called for allocation and leave Free unset
        }

        /// <summary>
        /// Frees the underlying unmanaged memory and sets <see cref="BufferPtr"/> to <see langword="null"/>
        /// </summary>
        public void Dispose()
        {
            if (BufferPtr is not null)
            {
                NativeMemory.AlignedFree(BufferPtr);
                GC.RemoveMemoryPressure(Length);
                BufferPtr = null;
            }
        }

        /// <summary>
        /// Clear the underlying buffer.
        /// </summary>
        public void Clear() => NativeMemory.Clear(BufferPtr, (nuint)Length);

        /// <summary>
        /// Return to the pool
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return() => pool?.Return(Level, this);

        /// <summary>
        /// Get valid pointer
        /// </summary>
        /// <returns></returns>
        public byte* GetValidPointer() => BufferPtr + ValidOffset;

        /// <summary>
        /// Gets a span over the allocated memory.
        /// </summary>
        public Span<byte> AsSpan() => new(BufferPtr, Length);

        /// <summary>
        /// Gets a valid span which is calculated using <see cref="ValidOffset"/>
        /// </summary>
        public Span<byte> AsValidSpan() => new(BufferPtr + ValidOffset, Length - ValidOffset);

        /// <inheritdoc/>
        public override string ToString() => $"{(nuint)BufferPtr} {ValidOffset} {RequiredBytes} {AvailableBytes} {Free}";

        /// <summary>
        /// Allocates memory aligned to a specified sector size and returns a new <see cref="SectorAlignedMemory"/> wrapping it.
        /// </summary>
        /// <param name="byteCount">Specifies the total number of bytes to allocate for the memory.</param>
        /// <param name="alignment">Defines the alignment size for the allocated memory.</param>
        /// <returns>Returns a SectorAlignedMemory instance wrapping the allocated memory.</returns>
        public static SectorAlignedMemory Allocate(int byteCount, uint alignment)
        {
            var memoryPtr = (byte*)NativeMemory.AlignedAlloc((uint)byteCount, alignment);
            GC.AddMemoryPressure(byteCount);
            return new SectorAlignedMemory(memoryPtr, byteCount);
        }

        /// <summary>
        /// Allocates memory aligned to a specified boundary and returns a new <see cref="SectorAlignedMemory"/> wrapping it.
        /// </summary>
        /// <param name="byteCount">Specifies the number of bytes to allocate for the memory.</param>
        /// <param name="alignment">Defines the alignment boundary for the allocated memory.</param>
        /// <param name="pool">Indicates the memory pool from which owns this memory.</param>
        /// <param name="level">Represents the level of allocation within the memory pool.</param>
        /// <returns>Returns a pool owned SectorAlignedMemory instance wrapping the allocated memory.</returns>
        internal static SectorAlignedMemory Allocate(int byteCount, uint alignment, SectorAlignedMemoryPool pool, int level)
        {
            var memoryPtr = (byte*)NativeMemory.AlignedAlloc((uint)byteCount, alignment);
            GC.AddMemoryPressure(byteCount);
            return new SectorAlignedMemory(memoryPtr, byteCount, pool, level);
        }
    }

    /// <summary>
    /// SectorAlignedBufferPool is a pool of memory.
    /// <para/>
    /// Internally, it is organized as an array of concurrent queues where each concurrent
    /// queue represents a memory of size in particular range. <c>queue[level]</c> contains memory 
    /// segments each of size <c>(2^level * sectorSize)</c>.
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
        /// Returns a memory buffer to the pool.
        /// </summary>
        /// <param name="page"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return(int level, SectorAlignedMemory page)
        {
#if CHECK_FOR_LEAKS
            Interlocked.Increment(ref totalReturns);
#endif

            page.AvailableBytes = 0;
            page.RequiredBytes = 0;
            page.ValidOffset = 0;

            page.Clear();
            page.Free = true;

            Debug.Assert(queue[level] != null);
            queue[level].Enqueue(page);
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
        /// <param name="numRecords">The number of records required.</param>    
        /// <returns>A <see cref="SectorAlignedMemory"/> that is aligned to the <see cref="sectorSize"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe SectorAlignedMemory Get(int numRecords)
        {
#if CHECK_FOR_LEAKS
            Interlocked.Increment(ref totalGets);
#endif
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

            return SectorAlignedMemory.Allocate(
                byteCount: sectorSize * (1 << level), (uint)sectorSize, pool: this, level);
        }

        /// <summary>
        /// Free all the pools unmanaged buffers
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