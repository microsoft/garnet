// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// These are the pages and freelist indexes of freed pages for the oversize sub-allocator of <see cref="OverflowAllocator"/>; this 
    /// sub-allocator is used for requests larger than <see cref="FixedSizePages.MaxExternalBlockSize"/>.
    /// </summary>
    /// <remarks>
    /// Each allocation is on a separate page of the size requested and a page is freed immediately when its allocation is no longer in use. 
    /// The freelist is threaded through the page vector's free slots and starts with the most-recently-freed index.
    /// </remarks>
    internal unsafe partial class OverflowAllocator
    {
        internal class OversizePages
        {
            /// <summary>The pages allocated by this allocator.</summary>
            internal NativePageAllocator PageAllocator;

            private const int InvalidSlot = -1;

            /// <summary>The list of free oversize slots.</summary>
            internal SimpleConcurrentStack<int> freeSlots;

            public OversizePages()
            {
                PageAllocator = new();
                freeSlots = new();
            }

            /// <summary>Include blockHeader in the allocation size.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static int PromoteSize(int size) => size + sizeof(BlockHeader);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal byte* Allocate(int size, bool zeroInit)
            {
                size = PromoteSize(size);

                BlockHeader* blockPtr;
                int pageSlot;
                while (true)
                { 
                    if (freeSlots.TryPop(out pageSlot))
                    { 
                        try
                        {
                            blockPtr = PageAllocator.AllocatePage(pageSlot, size);
                            break;
                        }
                        catch
                        {
                            // Return the free slot on OOM
                            freeSlots.Push(pageSlot);
                            throw;
                        }
                    }

                    // No free slots, so go through the "extend the page vector" logic to allocate a page of the requested size.
                    PageOffset localPageOffset = new() { PageAndOffset = PageAllocator.TailPageOffset.PageAndOffset };
                    if (localPageOffset.Offset != NativePageAllocator.OffsetAsLatch 
                            && PageAllocator.TryAllocateNewPage(ref localPageOffset, size, out blockPtr, out pageSlot))
                        break;

                    // Re-get the PageVector's PageAndOffset and retry the loop. This is consistent with the loop control in FixedSizePages.
                    localPageOffset.PageAndOffset = PageAllocator.TailPageOffset.PageAndOffset;
                }
                return BlockHeader.Initialize((BlockHeader*)blockPtr, size, pageSlot, zeroInit);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Free(BlockHeader* blockPtr)
            {
                // This leaves the allocation in the allocated page array, but pushes it onto the freelist first, so we will null out that slot
                // in Clear(). To null it in the allocated page array would require taking another latch to ensure stability of the page array.
                var slot = blockPtr->Slot;
                freeSlots.Push(slot);
                PageAllocator.FreePage(blockPtr);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal bool TryRealloc(BlockHeader* blockPtr, int newUserSize, out byte* newPtr)
            {
                var blockSize = PromoteSize(newUserSize);

                // For Oversize, this is a reallocation of the single-item page. It throws OOM if unsuccessful.
                var slot = blockPtr->Slot;
                var newBlockPtr = PageAllocator.Realloc(blockPtr, blockSize);
                newBlockPtr->AllocatedSize = blockSize;
                newBlockPtr->Slot = slot;
                newPtr = (byte*)(newBlockPtr + 1);
                return true;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Clear()
            {
                // The PageVector has no concept of the FreeList, so we must set "free" slots to null before claring the PageVector.
                while (freeSlots.TryPop(out var slot))
                    PageAllocator.Set(slot, null);
                freeSlots = new();
                PageAllocator.Clear();
            }
        }
    }
}
