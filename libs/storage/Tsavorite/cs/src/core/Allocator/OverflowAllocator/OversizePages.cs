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
        internal partial struct OversizePages
        {
            /// <summary>The pages allocated by this allocator.</summary>
            private NativePageVector oversizePages;

            /// <summary>The pages allocated by this allocator.</summary>
            internal NativePageVector PageVector;

            private const int InvalidSlot = -1;

            /// <summary>The head of the "linked list" of free oversize slots.</summary>
            private int freeList = InvalidSlot;

            public OversizePages() { }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal byte* Allocate(int size, bool zeroInit)
            {
                size = OversizePageHeader.PromoteSize(size);

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
                return OversizePageHeader.Initialize(blockPtr, size, zeroInit, pageSlot);
            }

            private bool PopFreeSlot(int size, out int slot)
            {
                while(true)
                {
                    // Pop if there is anything there.
                    slot = freeList;
                    if (slot == InvalidSlot)
                        return false;

                    var next = ((OversizePageHeader*)PageVector.Pages[slot])->slot;

                    // Pages[slot] may have been reallocated by the time we try this CompareExchange, but in that case the freeList
                    // head will have changed and thus the CompareExchange will fail, so while 'next' may contain garbage (i.e. overwritten
                    // by the caller after receiving the allocation), we will not actually do the exchange or 'dereference' 'next'.
                    if (Interlocked.CompareExchange(ref freeList, next, slot) == slot)
                        return true;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void PushFreeSlot(OversizePageHeader* pageHeader)
            {
                var slot = pageHeader->slot;
                int head;
                do
                {
                    head = pageHeader->slot = freeList;
                } while (Interlocked.CompareExchange(ref freeList, slot, head) != head);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Free(byte* block)
            {
                // OversizePageHeader.BlockHeader.Size has sizeof(OversizePageHeader) added to the user request.
                var pageHeader = (OversizePageHeader*)block - 1;
                PushFreeSlot(pageHeader);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Clear()
            {
                PageVector.Clear();
                freeList = default;
            }
        }
    }
}
