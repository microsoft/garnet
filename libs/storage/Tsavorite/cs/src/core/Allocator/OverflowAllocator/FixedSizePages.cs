﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    internal unsafe partial class OverflowAllocator
    {
        /// <summary>
        /// These are the pages and freelist bins for the fixed-size sub-allocator of <see cref="OverflowAllocator"/>; this sub-allocator is limited
        /// to MaxAllocSize blocks (allocated items). Larger than that uses <see cref="OversizePages"/>. It uses a simple pointer-advancing scheme to
        /// get blocks of the requested size, and the freelist is a set of bins of power-of-2 sizes from 64 bytes to <see cref="MaxBlockSize"/>.
        /// </summary>
        /// <remarks>
        /// Allocations are done in exactly the size of the bin. Pages are much larger than <see cref="MaxBlockSize"/> and we advance a pointer
        /// through the pages on Allocate(). A freelist of pointers is maintained per bin; no deallocations are done from this region until the hlog
        /// deallocates the page. This scheme provides the fastest performance for smaller allocations, at the cost of some memory overhead, especially
        /// as block sizes become larger.
        /// </remarks>
        internal unsafe struct FixedSizePages
        {
            /// <summary>Size of the largest free-list bin</summary>
            internal const int MaxBlockSize = 64 * 1024;

            /// <summary>Size of the smallest free-list bin</summary>
            internal const int MinBlockSize = 1 << MinPowerOf2;

            private const int MinPowerOf2 = 6;

            /// <summary>The size of a page allocation; it is a power of 2, as in <see cref="LogSettings.OverflowFixedPageSizeBits"/></summary>
            internal int PageSize;

            /// <summary>The pages allocated by this allocator, all of size <see cref="PageSize"/>.</summary>
            internal NativePageVector PageVector;

            /// <summary>The heads of the fixed-size freelists, from 64 to <see cref="MaxBlockSize"/>.</summary>
            private long[] freeList;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal FixedSizePages(int pageSize)
            {
                PageSize = pageSize;
                var numBins = GetLogBase2(MaxBlockSize) - GetLogBase2(MinBlockSize) + 1;
                freeList = new long[numBins];
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal byte* Allocate(int size, bool zeroInit)
            {
                size = BlockHeader.PromoteSize(size);
                var ptr = PopFromFreeList(size, zeroInit);
                if (ptr != null)
                    return ptr;

                PageOffset localPageOffset = new() { PageAndOffset = PageVector.TailPageOffset.PageAndOffset };

                // We need to pointer-advance, and possibly allocate a new page.
                while (true)
                { 
                    // See if it fits on the current last page. This also applies to the "no pages allocated" case, because we initialize tailPageOffset.Offset to out-of-range
                    while (localPageOffset.Offset + size < PageSize)
                    {
                        // Fast path; simple offset pointer advance. If it fails, someone else advanced the pointer, so retry.
                        if (Interlocked.CompareExchange(ref PageVector.TailPageOffset.PageAndOffset, localPageOffset.PageAndOffset + size, localPageOffset.PageAndOffset)
                            == localPageOffset.PageAndOffset)
                        {
                            var blockPtr = (BlockHeader*)PageVector.Pages[localPageOffset.Page] + localPageOffset.Offset;
                            return ReturnBlockPointer(blockPtr, size, zeroInit);
                        }

                        // Re-get the PageVector's PageAndOffset and retry the loop.
                        localPageOffset.PageAndOffset = PageVector.TailPageOffset.PageAndOffset;
                    }

                    // If we are here, we advanced the pointer past end of page without returning an allocation. We need to allocate a new page. We ignore the return pointer
                    // here, because we obtain the return pointer in the pointer-advance on the next iteration of this loop.
                    // TODO: possibly handle the page-end fragment by "allocating" it to ourselves here and storing it the next-lowest freelist bin
                    if (!PageVector.TryAllocateNewPage(ref localPageOffset, PageSize, out _, out _))
                        continue;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static byte* ReturnBlockPointer(BlockHeader* blockPtr, int size, bool zeroInit)
            {
                size -= sizeof(BlockHeader);
                blockPtr->Size = size;
                ++blockPtr;
                if (zeroInit)
                    NativeMemory.Clear(blockPtr, (nuint)size);
                return (byte*)blockPtr;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private int FindBin(int size)
            {
                // The bin count is small enough we can just linear-scan both to get to the first big-enough slot (we are optimized for small keys,
                // so this is likely to be faster than Utility.GetLogBase2), and from there to end of list if we need to search higher bins.
                Debug.Assert(size <= MaxBlockSize && IsPowerOfTwo(size), $"Size is {size}; it should be a power of 2 <= MaxBlockSize {MaxBlockSize}");
                var bin = 0;
                for (; 1 << bin < size; ++bin)
                    ;
                return bin;
            }

            private byte* PopFromFreeList(int size, bool zeroInit)
            {
                // Size is assumed to have sizeof(BlockHeader) already added to the user request.
                var bin = FindBin(size);

                var initialBin = bin;
                do
                {
                    // Pop if there is anything there.
                    var head = freeList[bin];
                    if (head != default)
                    {
                        var next = *(long*)head;
                        if (Interlocked.CompareExchange(ref freeList[bin], next, head) == head)
                        { 
                            // If we had to search past the initial bin, we have to split the block in powers of two until we are down to the necessary size.
                            for (; bin > initialBin; --bin)
                            {
                                var halfCurrentBinSize = 1 << (bin + MinPowerOf2 - 1);
                                var splitPtr = head + halfCurrentBinSize;
                                PushToFreeList((BlockHeader*)splitPtr, bin - 1);
                            }
                            return ReturnBlockPointer((BlockHeader*)head, size, zeroInit);
                        }
                    }
                    ++bin;
                } while (bin < freeList.Length);

                // We did not find a free item.
                return default;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void PushToFreeList(BlockHeader* ptr, int bin)
            {
                long head;
                do
                {
                    head = freeList[bin];
                    *(long*)ptr = head;
                } while (Interlocked.CompareExchange(ref freeList[bin], (long)ptr, head) != head);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Free(byte* block)
            {
                // BlockHeader.Size has sizeof(BlockHeader) added to the user request.
                var blockHeader = (BlockHeader*)block - 1;
                PushToFreeList(blockHeader, FindBin(blockHeader->Size));
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Clear()
            {
                PageVector.Clear();
                System.Array.Clear(freeList);
            }
        }
    }
}
