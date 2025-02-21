// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
    internal unsafe partial class OverflowAllocator
    {
        internal unsafe class NativePageAllocator
        {
            // Initial number of pages in the allocator
            private const int InitialPageCount = 1024;

            // When the page vector is not yet initialized, or has been reset and this is the first use after the reset
            private const int UninitializedPage = -1;

            /// <summary><see cref="TailPageOffset"/> offset value to indicate a thread has the "lock" to resize the page array</summary> 
            internal const int OffsetAsLatch = 1 << LogSettings.kMaxPageSizeBits;

            private MultiLevelPageArray<IntPtr> pageArray = new();

            /// <summary>This increments the Page element when allocating new pages; for simple pointer-advance in <see cref="FixedSizePages"/>, Offset is also advanced.</summary>
            internal PageOffset TailPageOffset;

            public NativePageAllocator() => InitTailPageOffset();

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            void InitTailPageOffset() => TailPageOffset = new() { Page = -1, Offset = 0 };

            internal bool IsInitialized => pageArray.IsInitialized;

            /// <summary>
            /// Allocate a new page, possibly growing the page vector. This is called during Allocate(), so the 
            /// <paramref name="localPageOffset"/> is already set up by the caller so we can use it as a basis
            /// to function as a lock as well as the index of the next page to allocate.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal bool TryAllocateNewPage(ref PageOffset localPageOffset, int size, out BlockHeader* blockPtr, out int pageSlot)
            {
                // Increment the page index and set the offset to "take the latch". We need this additional latch layer over the pageArray because 
                // we need FixedSizePages to recognize it during pointer-advance, and spinwait if it sees it.
                PageOffset newPageOffset = new() { Page = localPageOffset.Page + 1, Offset = OffsetAsLatch };
                var tempPageAndOffset = new PageOffset { PageAndOffset = Interlocked.CompareExchange(ref TailPageOffset.PageAndOffset, newPageOffset.PageAndOffset, localPageOffset.PageAndOffset) };
                if (tempPageAndOffset.PageAndOffset != localPageOffset.PageAndOffset || tempPageAndOffset.Offset == OffsetAsLatch)
                {
                    // Someone else incremented the page (or maybe someone else sneaked in with a smaller request and allocated from the end of the page).
                    // Yield to give them a chance to do the actual page allocation, then return false to caller to retry the outer allocation logic.
                    _ = Thread.Yield();
                    blockPtr = default;
                    pageSlot = 0;
                    return false;
                }

                // First see if we need to grow the pages array.
                try
                {
                    var tail = pageArray.Allocate();
                    Debug.Assert(tail == newPageOffset.Page, "Tail should be the same as the incremented PageAndOffset.Page");
                    blockPtr = AllocatePage(tail, size);
                    pageSlot = tail;
                }
                catch
                {
                    // Restore on OOM
                    TailPageOffset.PageAndOffset = tempPageAndOffset.PageAndOffset;
                    blockPtr = default;
                    pageSlot = default;
                    return false;
                }

                // Update the caller's localPageOffset and return.
                newPageOffset.Offset = 0;   // clear the "latch" and set the offset to the beginning of the page
                localPageOffset.PageAndOffset = newPageOffset.PageAndOffset;
                TailPageOffset = newPageOffset;
                return true;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal BlockHeader* AllocatePage(int pageSlot, int size)
            {
                // This may throw; caller must handle "catch and restore" if needed
                var blockPtr = (BlockHeader*)NativeMemory.AlignedAlloc((nuint)size, Constants.kCacheLineBytes);
                pageArray.Set(pageSlot, (IntPtr)blockPtr);
                return blockPtr;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void FreePage(BlockHeader* page)
            {
                // This is only called from OversizePages; FixedSizePages don't free their pages during the lifetime of a page instance
                // in the Allocator. So we are safe to use the slot.
                var slot = page->Slot;
                var element = pageArray.Get(slot);
                NativeMemory.AlignedFree(page);
                pageArray.Set(slot, default);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal BlockHeader* Realloc(BlockHeader* page, int size)
            {
                // This is only called from OversizePages; FixedSizePages don't free their pages during the lifetime of a page instance
                // in the Allocator. So we are safe to use the slot.
                var slot = page->Slot;
                pageArray.Set(slot, default);
                var ptr = (byte*)NativeMemory.AlignedRealloc(page, (nuint)size, Constants.kCacheLineBytes);
                pageArray.Set(slot, (IntPtr)ptr);
                return (BlockHeader*)ptr;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal BlockHeader* Get(int pageSlot, int offset) => (BlockHeader*)(pageArray.Get(pageSlot) + offset);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Set(int pageSlot, byte* value) => pageArray.Set(pageSlot, (IntPtr)value);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Clear()
            {
                // The TailAndLatch tail is our page, as that is the unit we allocate in.
                if (pageArray is not null)
                    pageArray.Clear(intPtr => NativeMemory.AlignedFree((nuint*)intPtr));

                // Prep for reuse
                InitTailPageOffset();
            }
        }
    }
}