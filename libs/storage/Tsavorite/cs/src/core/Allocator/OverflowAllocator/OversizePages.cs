// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// These are the pages and freelist indexes of freed pages for the oversize sub-allocator of <see cref="OverflowAllocator"/>; this 
    /// sub-allocator is used for requests larger than <see cref="FixedSizePages.MaxBlockSize"/>.
    /// </summary>
    /// <remarks>
    /// Each allocation is on a separate page of the size requested and a page is freed immediately when its allocation is no longer in use. 
    /// The freelist is threaded through the page vector's free slots and starts with the most-recently-freed index.
    /// </remarks>
    internal unsafe partial class OverflowAllocator
    {
        internal struct OversizePages
        {
            /// <summary>The pages allocated by this allocator.</summary>
            internal NativePageVector PageVector;

            private const int InvalidSlot = -1;

            /// <summary>The head of the "linked list" of free oversize slots.</summary>
            private int freeList;

            public OversizePages()
            {
                PageVector = new();
                freeList = InvalidSlot;
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
                if (PopFreeSlot(size, out pageSlot))
                    blockPtr = PageVector.AllocatePage(pageSlot, size);
                else
                {
                    // No free slots, so go through the "extend the page vector" logic to allocate a page of the requested size.
                    PageOffset localPageOffset = new() { PageAndOffset = PageVector.TailPageOffset.PageAndOffset };
                    while (!PageVector.TryAllocateNewPage(ref localPageOffset, size, out blockPtr, out pageSlot))
                    {
                        // Re-get the PageVector's PageAndOffset and retry the loop. This is consistent with the loop control in FixedSizePages.
                        localPageOffset.PageAndOffset = PageVector.TailPageOffset.PageAndOffset;
                    }
                }
                return BlockHeader.Initialize((BlockHeader*)blockPtr, size, pageSlot, zeroInit);
            }

            private bool PopFreeSlot(int size, out int slot)
            {
                while(true)
                {
                    // Pop if there is anything there.
                    slot = freeList;
                    if (slot == InvalidSlot)
                        return false;

                    int next = (int)(long)PageVector.Pages[slot];

                    // Pages[slot] may have been reallocated by the time we try this CompareExchange, but in that case the freeList
                    // head will have changed and thus the CompareExchange will fail, so while 'next' may contain garbage (i.e. overwritten
                    // by the caller after receiving the allocation), we will not actually do the exchange or 'dereference' 'next'.
                    if (Interlocked.CompareExchange(ref freeList, next, slot) == slot)
                        return true;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void PushFreeSlot(int slot)
            {
                int head;
                do
                {
                    // This threads the free slot indexes through their freed slots in the page vector. This eliminates the need to grow the 
                    // page vector if there are a lot of frees, such as for revivification, failed CAS, and so on.
                    head = freeList;
                    PageVector.Pages[slot] = (byte*)(long)head;
                } while (Interlocked.CompareExchange(ref freeList, slot, head) != head);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Free(BlockHeader* blockPtr)
            {
                var slot = blockPtr->Slot;
                PageVector.FreePage(blockPtr);
                PushFreeSlot(slot);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal bool TryRealloc(BlockHeader* blockPtr, int newSize, out byte* newPtr)
            {
                // For Oversize, this is a reallocation of the single-item page. It thows OOM if unsuccessful.
                var slot = blockPtr->Slot;
                var newBlockPtr = PageVector.Realloc(blockPtr, newSize);
                newBlockPtr->AllocatedSize = newSize;
                newBlockPtr->Slot = slot;
                newPtr = (byte*)(newBlockPtr + 1);
                return true;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Clear()
            {
                // The PageVector has no concept of the FreeList, so we must set "free" slots to null before claring the PageVector.
                if (PageVector.Pages is not null)
                { 
                    var slot = freeList;
                    while (slot != InvalidSlot)
                    {
                        var next = (int)(long)PageVector.Pages[slot];
                        PageVector.Pages[slot] = null;
                        slot = next;
                    }
                }
                freeList = default;

                PageVector.Clear();
            }
        }
    }
}
