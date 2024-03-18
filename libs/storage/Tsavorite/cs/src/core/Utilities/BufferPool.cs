// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
#define CHECK_FREE      // disabled by default in Release due to overhead
#endif
// #define CHECK_FOR_LEAKS // disabled by default due to overhead

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
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
        /// Offset
        /// </summary>
        public int offset;

        /// <summary>
        /// Aligned pointer
        /// </summary>
        public byte* aligned_pointer;

        /// <summary>
        /// Valid offset
        /// </summary>
        public int valid_offset;

        /// <summary>
        /// Required bytes
        /// </summary>
        public int required_bytes;

        /// <summary>
        /// Available bytes
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
            int recordSize = 1;
            int requiredSize = sectorSize + (((numRecords) * recordSize + (sectorSize - 1)) & ~(sectorSize - 1));

            buffer = GC.AllocateArray<byte>(requiredSize, true);
            long bufferAddr = (long)Unsafe.AsPointer(ref buffer[0]);
            aligned_pointer = (byte*)((bufferAddr + (sectorSize - 1)) & ~((long)sectorSize - 1));
            offset = (int)((long)aligned_pointer - bufferAddr);
            // Assume ctor is called for allocation and leave Free unset
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
        /// Get valid pointer
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* GetValidPointer()
        {
            return aligned_pointer + valid_offset;
        }

        /// <summary>
        /// ToString
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format($"{(long)aligned_pointer} {offset} {valid_offset} {required_bytes} {available_bytes}"
#if CHECK_FREE
                + $" {this.Free}"
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
            v--;

            int r = 0; // r will be lg(v)
            while (true) // unroll for more speed...
            {
                v >>= 1;
                if (v == 0) break;
                r++;
            }
            return r + 1;
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

            int requiredSize = sectorSize + (((numRecords) * recordSize + (sectorSize - 1)) & ~(sectorSize - 1));
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
                    page.aligned_pointer = (byte*)(((long)page.handle.AddrOfPinnedObject() + (sectorSize - 1)) & ~((long)sectorSize - 1));
                    page.offset = (int)((long)page.aligned_pointer - (long)page.handle.AddrOfPinnedObject());
                }
                return page;
            }

            page = new SectorAlignedMemory(level: index)
            {
                buffer = GC.AllocateArray<byte>(sectorSize * (1 << index), !UnpinOnReturn)
            };
            if (UnpinOnReturn)
                page.handle = GCHandle.Alloc(page.buffer, GCHandleType.Pinned);
            long pageAddr = (long)Unsafe.AsPointer(ref page.buffer[0]);
            page.aligned_pointer = (byte*)((pageAddr + (sectorSize - 1)) & ~((long)sectorSize - 1));
            page.offset = (int)((long)page.aligned_pointer - pageAddr);
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