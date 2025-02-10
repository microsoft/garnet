// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
    internal unsafe partial class OverflowAllocator
    {
        internal unsafe struct NativePageVector
        {
            // Initial number of pages in the allocator
            private const int InitialPageCount = 1024;

            /// <summary><see cref="TailPageOffset"/> offset value to indicate another thread has the "lock" to resize the page array</summary> 
            internal const int OffsetAsLatch = LogSettings.kMaxPageSizeBits;

            internal byte*[] Pages;     // the vector of pages

            /// <summary>This increments the Page element when allocating new pages; for simple pointer-advance in <see cref="FixedSizePages"/>, Offset is also advanced.</summary>
            internal PageOffset TailPageOffset;

            public NativePageVector()
            {
                InitTailPageOffset();
            }

            void InitTailPageOffset()
            {
                TailPageOffset = new() { Page = -1, Offset = 0 };
            }

            /// <summary>
            /// Allocate a new page, possibly growing the page vector. This is called during Allocate(), so the 
            /// <paramref name="localPageOffset"/> is already set up by the caller so we can use it as a basis
            /// to function as a lock as well as the index of the next page to allocate.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal bool TryAllocateNewPage(ref PageOffset localPageOffset, int size, out BlockHeader* blockPtr, out int pageSlot)
            {
                // Increment the page index and set the offset to "take the latch".
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
                var pageCount = Pages?.Length ?? 0;
                if (newPageOffset.Page >= pageCount)
                {
                    var newPageCount = pageCount == 0 ? InitialPageCount : pageCount * 2;
                    var newPages = new byte*[newPageCount];
                    if (pageCount != 0)
                        Array.Copy(Pages, newPages, pageCount);
                    newPageOffset.Offset = 0;   // clear the "latch"
                    Pages = newPages;
                }

                blockPtr = AllocatePage(newPageOffset.Page, size);
                pageSlot = newPageOffset.Page;

                // Update the caller's localPageOffset and return.
                localPageOffset.PageAndOffset = newPageOffset.PageAndOffset;
                TailPageOffset = newPageOffset;
                return true;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal BlockHeader* AllocatePage(int pageSlot, int size)
            {
                var ptr = (byte*)NativeMemory.AlignedAlloc((nuint)size, Constants.kCacheLineBytes);
                Pages[pageSlot] = ptr;
                return (BlockHeader*)ptr;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void FreePage(BlockHeader* page)
            {
                Pages[page->Slot] = null;
                NativeMemory.AlignedFree(page);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal BlockHeader* Realloc(BlockHeader* page, int size)
            {
                var slot = page->Slot;
                Pages[slot] = null;
                var ptr = (byte*)NativeMemory.AlignedRealloc(page, (nuint)size, Constants.kCacheLineBytes);
                Pages[slot] = ptr;
                return (BlockHeader*)ptr;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Clear()
            {
                if (Pages is not null)
                {
                    for (var ii = 0; ii < TailPageOffset.Page; ++ii)
                    {
                        if (Pages[ii] == null)
                            continue;
                        NativeMemory.AlignedFree((nuint*)Pages[ii]);
                        Pages[ii] = null;
                    }
                }

                // Prep for reuse
                InitTailPageOffset();
            }
        }
    }
}