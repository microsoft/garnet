// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    internal unsafe partial class OverflowAllocator
    {
        /// <summary>
        /// These are the pages and freelist bins for the fixed-size sub-allocator of <see cref="OverflowAllocator"/>; this sub-allocator is limited
        /// to MaxAllocSize blocks (allocated items). Larger than that uses <see cref="OversizePages"/>. It uses a simple pointer-advancing scheme to
        /// get blocks of the requested size, and the freelist is a set of bins of power-of-2 sizes from 64 bytes to <see cref="MaxExternalBlockSize"/>.
        /// TODO: The power-of-2 is probably too much waste so consider a smaller limit with more granular bins.
        /// </summary>
        /// <remarks>
        /// Allocations are done in exactly the size of the bin. Pages are much larger than <see cref="MaxExternalBlockSize"/> and we advance a pointer
        /// through the pages on Allocate(). A freelist of pointers is maintained per bin; no deallocations are done from this region until the hlog
        /// deallocates the page. This scheme provides the fastest performance for smaller allocations, at the cost of some memory overhead, especially
        /// as block sizes become larger.
        /// </remarks>
        internal unsafe class FixedSizePages
        {
            /// <summary>Size of the largest request we can service in this allocator</summary>
            internal const int MaxExternalBlockSize = MaxInternalBlockSize - BlockHeader.Size;

            /// <summary>Size of the largest free-list bin</summary>
            internal const int MaxInternalBlockSize = 16 * 1024;

            /// <summary>Size of the smallest free-list bin</summary>
            internal const int MinInternalBlockSize = 1 << MinPowerOf2;

            internal const int MinPowerOf2 = LogSettings.kLowestMaxInlineSizeBits;
            
            internal const int MinPageSize = MaxInternalBlockSize * 4;

            /// <summary>The size of a page allocation; it is a power of 2, as in <see cref="LogSettings.OverflowFixedPageSizeBits"/></summary>
            internal int PageSize;

            /// <summary>The pages allocated by this allocator, all of size <see cref="PageSize"/>.</summary>
            internal NativePageAllocator PageAllocator;

            /// <summary>
            /// The heads of the fixed-size freelists, from <see cref="MinInternalBlockSize"/> to <see cref="MaxInternalBlockSize"/>.
            /// Each bin element is a BlockHeader* followed by the BlockHeader* to the next free block.</summary>
            /// <remarks>
            /// This stores BlockHeader* rather than the "user pointer" which is returned from NativePageAllocator because we split the
            /// blocks returned from NativePageAllocator and thus keep it consistent by creating the BlockHeader* inside the upper half
            /// of the split.
            /// </remarks>
            internal long[] freeBins;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal FixedSizePages(int pageSize)
            {
                if (pageSize < MinPageSize)
                    throw new TsavoriteException($"pageSize {pageSize} must be >= {MinPageSize}");
                PageSize = pageSize;
                PageAllocator = new();
                var numBins = GetLogBase2(MaxInternalBlockSize) - GetLogBase2(MinInternalBlockSize) + 1;
                freeBins = new long[numBins];
            }

            /// <summary>Include blockHeader in the allocation size. For FixedSizeBlocks, this is included in the internal request size, so blocks remain power-of-2.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static int PromoteSize(int size) => (size <= MinInternalBlockSize - sizeof(BlockHeader)) ? MinInternalBlockSize  : (int)NextPowerOf2(size + sizeof(BlockHeader));

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal byte* Allocate(int userSize, bool zeroInit)
            {
                Debug.Assert(userSize <= MaxExternalBlockSize, $"userSize {userSize} must be < MaxExternalBlockSize {MaxExternalBlockSize}");

                var blockSize = PromoteSize(userSize);
                Debug.Assert(blockSize <= MaxInternalBlockSize, $"BlockSize {blockSize} must be < MaxInternalBlockSize {MaxInternalBlockSize}");
                if (TryPopFromFreeList(blockSize, userSize, zeroInit, out byte* ptr))
                    return ptr;

                PageOffset localPageOffset = new() { PageAndOffset = PageAllocator.TailPageOffset.PageAndOffset };

                if (!PageAllocator.IsInitialized && localPageOffset.Offset != NativePageAllocator.OffsetAsLatch)
                    PageAllocator.TryAllocateNewPage(ref localPageOffset, PageSize, out _, out _);

                // We need to pointer-advance, and possibly allocate a new page.
                while (true)
                { 
                    if (localPageOffset.Offset == NativePageAllocator.OffsetAsLatch)
                    {
                        _ = Thread.Yield();

                        // Re-get the PageVector's PageAndOffset and retry the loop.
                        localPageOffset.PageAndOffset = PageAllocator.TailPageOffset.PageAndOffset;
                        continue;
                    }

                    // See if it fits on the current last page. This also applies to the "no pages allocated" case, because we initialize tailPageOffset.Offset to out-of-range
                    if (localPageOffset.Offset + blockSize < PageSize)
                    {
                        // Fast path; simple offset pointer advance. If it fails, someone else advanced the pointer, so retry.
                        if (Interlocked.CompareExchange(ref PageAllocator.TailPageOffset.PageAndOffset, localPageOffset.PageAndOffset + blockSize, localPageOffset.PageAndOffset)
                            == localPageOffset.PageAndOffset)
                        {
                            var blockPtr = PageAllocator.Get(localPageOffset.Page, localPageOffset.Offset);
                            return BlockHeader.Initialize(blockPtr, blockSize, userSize, zeroInit);
                        }

                        // Re-get the PageVector's PageAndOffset and retry the loop. This may set the offset to OffsetAsLatch, which is handled in the next iteration.
                        localPageOffset.PageAndOffset = PageAllocator.TailPageOffset.PageAndOffset;
                    }
                    else
                    {

                        // If we are here, the pointer is past where adding blockSize would place it beyond the end of the page. We need to allocate a new page. We ignore the
                        // return pointer here, because we obtain the return pointer in the pointer-advance on the next iteration of this loop.
                        // TODO: possibly handle the page-end fragment by "allocating" it to ourselves here and storing it the next-lowest freelist bin
                        _ = PageAllocator.TryAllocateNewPage(ref localPageOffset, PageSize, out _, out _);
                    }
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private int FindBin(int blockSize)
            {
                Debug.Assert(blockSize >= MinInternalBlockSize && IsPowerOfTwo(blockSize), $"blockSize {blockSize} must be a power of 2 >= MinInternalBlockSize{MinInternalBlockSize}");
                var bin = GetLogBase2(blockSize) - MinPowerOf2;
                return IsPowerOfTwo(blockSize) ? bin : bin + 1;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static BlockHeader* GetNextPointer(BlockHeader* blockPtr) => *(BlockHeader**)(blockPtr + 1);
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static void SetNextPointer(BlockHeader* blockPtr, BlockHeader* next) => *(BlockHeader**)(blockPtr + 1) = next;

            internal bool TryPopFromFreeList(int binIndex, out byte* ptr)
            {
                var blockSize = 1 << (binIndex + MinPowerOf2);
                Debug.Assert(FindBin(blockSize) == binIndex, $"FindBin({blockSize}) is not binIndex {binIndex}");
                return TryPopFromFreeList(binIndex, blockSize, userSize: blockSize - sizeof(BlockHeader), zeroInit: false, out ptr);
            }

            private bool TryPopFromFreeList(int blockSize, int userSize, bool zeroInit, out byte* ptr)
            {
                // blockSize is assumed to have already been promoted to add sizeof(BlockHeader) to the user request.
                Debug.Assert(blockSize <= MaxInternalBlockSize && IsPowerOfTwo(blockSize), $"blockSize is {blockSize}; it should be a power of 2 <= MaxInternalBlockSize {MaxInternalBlockSize}");

                var bin = FindBin(blockSize);
                return TryPopFromFreeList(bin, blockSize, userSize, zeroInit, out ptr);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private bool TryPopFromFreeList(int binIndex, int blockSize, int userSize, bool zeroInit, out byte* ptr)
            {
                var initialBinIndex = binIndex;
                do
                {
                    // Pop if there is anything there.
                    var head = (BlockHeader*)freeBins[binIndex];
                    if (head != default)
                    {
                        // Non-empty bin so try to pop. TODO: Consider ABA issue here; we use pointers so there is no room for a version. ABA would be rare and the symptom is a leaked "next" pointer,
                        // and this allocator is used by the main allocators on a per-page basis; when those pages are evicted the allocator is reset. So a slow leak will not grow unbounded.
                        var next = GetNextPointer(head);
                        if (Interlocked.CompareExchange(ref freeBins[binIndex], (long)next, (long)head) == (long)head)
                        {
                            // Wait to do this assert until we've popped 'head'; another thread may have split it.
                            Debug.Assert(head->AllocatedSize == 1 << (binIndex + MinPowerOf2), $"bin size mismatch: expected {1 << (binIndex + MinPowerOf2)}, actual head->AllocatedSize {head->AllocatedSize}; initialBinIndex {initialBinIndex}, binIndex {binIndex}");

                            SetNextPointer(head, default);

                            // If we had to search past the initial bin, we have to split the block in powers of two until we are down to the requested size.
                            for (; binIndex > initialBinIndex; --binIndex)
                            {
                                var halfCurrentBinSize = 1 << (binIndex + MinPowerOf2 - 1);
                                var splitPtr = (BlockHeader*)((byte*)head + halfCurrentBinSize);
                                _ = BlockHeader.InitializeFixedSize(splitPtr, halfCurrentBinSize, halfCurrentBinSize - sizeof(BlockHeader), zeroInit: false);
                                PushToFreeList(splitPtr, binIndex - 1);
                            }
                            ptr = BlockHeader.InitializeFixedSize(head, blockSize, userSize, zeroInit);
                            return true;
                        }

                        // Non-empty bin but contention caused CAS failure (and it may be empty now); retry in the same bin.
                        continue;
                    }

                    // Empty bin; move on to the next bin.
                    ++binIndex;
                } while (binIndex < freeBins.Length);

                // We did not find a free item.
                ptr = default;
                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void PushToFreeList(BlockHeader* blockPtr, int binIndex)
            {
                Debug.Assert(blockPtr->AllocatedSize == 1 << (binIndex + MinPowerOf2), $"bin size mismatch: expected {1 << (binIndex + MinPowerOf2)}, actual blockPtr->AllocatedSize {blockPtr->AllocatedSize}");

                // We do not try to coalesce. TODO: See not in PopFromFreeList regarding ABA issue.
                BlockHeader* head;
                do
                {
                    head = (BlockHeader*)freeBins[binIndex];
                    SetNextPointer(blockPtr, head);     // Maintain the chain
                } while (Interlocked.CompareExchange(ref freeBins[binIndex], (long)blockPtr, (long)head) != (long)head);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Free(BlockHeader* blockPtr)
            {
                // BlockHeader.AllocatedSize has sizeof(BlockHeader) added to the user request and should be a power-of-2 bin size.
                PushToFreeList(blockPtr, FindBin(blockPtr->AllocatedSize));
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal bool TryRealloc(BlockHeader* blockPtr, int newUserSize, out byte* newPtr)
            {
                var blockSize = PromoteSize(newUserSize);

                // This can only be done in-place for FixedSizePages
                if (blockPtr->AllocatedSize < blockSize)
                {
                    newPtr = null;
                    return false;
                }
                blockPtr->UserSize = newUserSize;
                newPtr = (byte*)(blockPtr + 1);
                return true;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Clear()
            {
                PageAllocator.Clear();
                System.Array.Clear(freeBins);
            }
        }
    }
}
