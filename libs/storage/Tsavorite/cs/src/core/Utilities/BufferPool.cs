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
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;

    /// <summary>
    /// Sector aligned memory allocator
    /// </summary>
    public sealed unsafe class SectorAlignedMemory
    {
        // Byte #31 is used to denote free (1) or in-use (0) page
        const int kFreeBitMask = 1 << 31;

        /// <summary>
        /// Actual buffer
        /// </summary>
        public byte[] buffer;

        /// <summary>
        /// Handle
        /// </summary>
        internal GCHandle handle;

        /// <summary>
        /// Offset for initial allocation alignment of the block; this is the offset from the first element of <see cref="buffer"/> to form <see cref="aligned_pointer"/>.
        /// This alignment is internal to <see cref="SectorAlignedMemory"/>, and ensures that callers see an aligned starting address.
        /// </summary>
        public int aligned_offset;

        /// <summary>
        /// Aligned pointer; initial allocation (the first element of <see cref="buffer"/>) plus <see cref="aligned_offset"/>
        /// This alignment is internal to <see cref="SectorAlignedMemory"/>, and ensures that callers see an aligned starting address.
        /// </summary>
        public byte* aligned_pointer;

        /// <summary>
        /// Valid offset for operations above <see cref="aligned_pointer"/>, to get their own desired alignment relative to our aligned starting address.
        /// This is set by the caller for operations such as file reading, which rounds down to the nearest sector size; this is the amount of that rounding down.
        /// Used by <see cref="GetValidPointer()"/>, which is <see cref="aligned_pointer"/> + <see cref="valid_offset"/>.
        /// </summary>
        public int valid_offset;

        /// <summary>
        /// Required (requested) bytes for the current operation: the unaligned number of bytes to read. There will always be at least this much usable space in the allocation.
        /// Use this when the original request size is needed.
        /// </summary>
        public int required_bytes;

        /// <summary>
        /// Available bytes after the operation is complete: the number of bytes actually read, e.g. aligned number of bytes requested. See <see cref="GetValidPointer()"/>.
        /// Use this to see if there are additional bytes over the original request (see <see cref="required_bytes"/>.
        /// </summary>
        public int available_bytes;

        private int level;
        internal int Level => level
#if CHECK_FREE
            & ~kFreeBitMask
#endif
            ;

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
                    this.level |= kFreeBitMask;
                }
                else
                {
                    if (!Free)
                        throw new TsavoriteException("Attempting to allocate an already-allocated block");
                    this.level &= ~kFreeBitMask;
                }
            }
        }
#endif // CHECK_FREE

        /// <summary>
        /// Default constructor
        /// </summary>
        public SectorAlignedMemory(int level = default)
        {
            this.level = level;
            // Assume ctor is called for allocation and leave Free unset
        }

        /// <summary>
        /// Create new instance of SectorAlignedMemory
        /// </summary>
        /// <param name="numRecords"></param>
        /// <param name="sectorSize"></param>
        public SectorAlignedMemory(int numRecords, int sectorSize)
        {
            const int recordSize = 1;
            required_bytes = numRecords * recordSize;
            int requiredSize = sectorSize + RoundUp(required_bytes, sectorSize);    // An additional sector size for the aligned_offset

            buffer = GC.AllocateArray<byte>(requiredSize, true);
            long bufferAddr = (long)Unsafe.AsPointer(ref buffer[0]);
            aligned_pointer = (byte*)((bufferAddr + (sectorSize - 1)) & ~((long)sectorSize - 1));
            aligned_offset = (int)((long)aligned_pointer - bufferAddr);
            // Assume ctor is called for allocation and leave Free unset
        }

        public unsafe (byte[] array, long offset) GetArrayAndUnalignedOffset(long alignedOffset)
        {
            long ptr = (long)Unsafe.AsPointer(ref buffer[0]);
            return (buffer, alignedOffset + ptr - (long)aligned_pointer);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            buffer = null;
#if CHECK_FREE
            this.Free = true;
#endif
        }

        /// <summary>
        /// Return
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return()
        {
            pool?.Return(this);
        }

        /// <summary>
        /// Get the total aligned memory capacity of the buffer
        /// </summary>
        public int AlignedTotalCapacity => buffer.Length - aligned_offset;

        /// <summary>
        /// Get the total valid memory capacity of the buffer
        /// </summary>
        public int ValidTotalCapacity => AlignedTotalCapacity - valid_offset;

        /// <summary>
        /// Get the total valid required (requested) capacity of the buffer
        /// </summary>
        public int RequiredCapacity => required_bytes - valid_offset;

        /// <summary>
        /// Get valid pointer (accounts for aligned padding plus any offset specified for the valid start of data)
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* GetValidPointer() => aligned_pointer + valid_offset;

        /// <summary>
        /// Get Span of entire allocated space after the valid pointer
        /// </summary>
        public Span<byte> TotalValidSpan => new(GetValidPointer(), ValidTotalCapacity);

        /// <summary>
        /// Get Span of entire allocated space after the aligned pointer (see <see cref="available_bytes"/>).
        /// </summary>
        public Span<byte> AvailableSpan => new(aligned_pointer, available_bytes);

        /// <summary>
        /// Get Span of entire allocated space after the valid pointer (see <see cref="valid_offset"/>).
        /// </summary>
        public Span<byte> AvailableValidSpan => new(GetValidPointer(), available_bytes - valid_offset);

        /// <summary>
        /// Returns the Span of requested space (see <see cref="required_bytes"/>).
        /// </summary>
        public Span<byte> RequiredValidSpan => new(GetValidPointer(), RequiredCapacity);

        /// <summary>
        /// ToString
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format($"aligned: [offset {aligned_offset}, ptr {(long)aligned_pointer} = 0x{(long)aligned_pointer:X}];" +
                $" valid: [offset {valid_offset} ptr {(long)GetValidPointer()} = 0x{(long)GetValidPointer():X}];" +
                $" reqBytes {required_bytes}; availBytes {available_bytes}; cap {AlignedTotalCapacity}"
#if CHECK_FREE
                + $"; free {Free}"
#endif
                );
        }
    }

    /// <summary>
    /// SectorAlignedBufferPool is a pool of memory. 
    /// Internally, it is organized as an array of concurrent queues where each concurrent
    /// queue represents a memory of size in particular range. queue[i] contains memory 
    /// segments each of size (2^i * sectorSize).
    /// </summary>
    public sealed class SectorAlignedBufferPool
    {
        /// <summary>
        /// Disable buffer pool.
        /// This static option should be enabled on program entry, and not modified once Tsavorite is instantiated.
        /// </summary>
        public static bool Disabled;

        /// <summary>
        /// Unpin objects when they are returned to the pool, so that we do not hold pinned objects long term.
        /// If set, we will unpin when objects are returned and re-pin when objects are returned from the pool.
        /// This static option should be enabled on program entry, and not modified once Tsavorite is instantiated.
        /// </summary>
        public static bool UnpinOnReturn;

        private const int levels = 32;
        private readonly int recordSize;
        private readonly int sectorSize;
        private readonly ConcurrentQueue<SectorAlignedMemory>[] queue;
#if CHECK_FOR_LEAKS
        static int totalGets, totalReturns;
#endif

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="recordSize">Record size. May be 1 if allocations of different lengths will be made</param>
        /// <param name="sectorSize">Sector size, e.g. from log device</param>
        public SectorAlignedBufferPool(int recordSize, int sectorSize)
        {
            queue = new ConcurrentQueue<SectorAlignedMemory>[levels];
            this.recordSize = recordSize;
            this.sectorSize = sectorSize;
        }

        public void EnsureSize(ref SectorAlignedMemory page, int size)
        {
            if (page is null)
            {
                page = Get(size);
                return;
            }
            if (page.AlignedTotalCapacity < size)
            {
                page.Return();
                page = Get(size);
                return;
            }

            // Reusing the page, so ensure this is set correctly.
            page.required_bytes = size;
        }

        /// <summary>
        /// Return
        /// </summary>
        /// <param name="page"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return(SectorAlignedMemory page)
        {
#if CHECK_FOR_LEAKS
            Interlocked.Increment(ref totalReturns);
#endif

#if CHECK_FREE
            page.Free = true;
#endif // CHECK_FREE

            Debug.Assert(queue[page.Level] != null);
            page.available_bytes = 0;
            page.required_bytes = 0;
            page.valid_offset = 0;
            Array.Clear(page.buffer, 0, page.buffer.Length);
            if (!Disabled)
            {
                if (UnpinOnReturn)
                {
                    page.handle.Free();
                    page.handle = default;
                }
                queue[page.Level].Enqueue(page);
            }
            else
            {
                if (UnpinOnReturn)
                    page.handle.Free();
                page.buffer = null;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int Position(int v)
        {
            if (v == 1) return 0;
            return BitOperations.Log2((uint)v - 1) + 1;
        }

        /// <summary>
        /// Get buffer
        /// </summary>
        /// <param name="numRecords"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe SectorAlignedMemory Get(int numRecords)
        {
#if CHECK_FOR_LEAKS
            Interlocked.Increment(ref totalGets);
#endif

            int required_bytes = numRecords * recordSize;
            int requiredSize = RoundUp(required_bytes, sectorSize);
            int index = Position(requiredSize / sectorSize);
            if (queue[index] == null)
            {
                var localPool = new ConcurrentQueue<SectorAlignedMemory>();
                Interlocked.CompareExchange(ref queue[index], localPool, null);
            }

            if (!Disabled && queue[index].TryDequeue(out SectorAlignedMemory page))
            {
#if CHECK_FREE
                page.Free = false;
#endif // CHECK_FREE
                if (UnpinOnReturn)
                {
                    page.handle = GCHandle.Alloc(page.buffer, GCHandleType.Pinned);
                    page.aligned_pointer = (byte*)RoundUp(page.handle.AddrOfPinnedObject(), sectorSize);
                    page.aligned_offset = (int)((long)page.aligned_pointer - page.handle.AddrOfPinnedObject());
                }
                page.required_bytes = required_bytes;
                return page;
            }

            page = new SectorAlignedMemory(level: index)
            {
                // Add an additional sector for the leading RoundUp of pageAddr to sectorSize.
                buffer = GC.AllocateArray<byte>(sectorSize * ((1 << index) + 1), !UnpinOnReturn)
            };
            if (UnpinOnReturn)
                page.handle = GCHandle.Alloc(page.buffer, GCHandleType.Pinned);
            long pageAddr = (long)Unsafe.AsPointer(ref page.buffer[0]);
            page.aligned_pointer = (byte*)RoundUp(pageAddr, sectorSize);
            page.aligned_offset = (int)((long)page.aligned_pointer - pageAddr);
            page.required_bytes = required_bytes;
            page.pool = this;
            return page;
        }

        /// <summary>
        /// Free buffer
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Free()
        {
#if CHECK_FOR_LEAKS
            Debug.Assert(totalGets == totalReturns);
#endif
            for (int i = 0; i < levels; i++)
            {
                if (queue[i] == null) continue;
                while (queue[i].TryDequeue(out SectorAlignedMemory result))
                    result.buffer = null;
            }
        }

        /// <summary>
        /// Print pool contents
        /// </summary>
        public void Print()
        {
            for (int i = 0; i < levels; i++)
            {
                if (queue[i] == null) continue;
                foreach (var item in queue[i])
                {
                    Console.WriteLine("  " + item.ToString());
                }
            }
        }
    }
}