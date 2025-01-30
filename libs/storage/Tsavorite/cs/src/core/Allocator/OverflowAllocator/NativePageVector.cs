// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
    internal unsafe partial class OverflowAllocator
    {
        internal unsafe struct NativePageVector
        {
            private const int InitialPageCount = 1024;

            internal byte*[] Pages;     // the vector of pages

            /// <summary>This increments the Page element when allocating new pages; for simple pointer-advance in <see cref="FixedSizePages"/>, Offset is also advanced.</summary>
            internal PageOffset TailPageOffset = new() { Page = -1, Offset = int.MaxValue - 1 };

            public NativePageVector() { }

            /// <summary>
            /// Allocate a new page, possibly growing the page vector. This is called during Allocate(), so the 
            /// <paramref name="localPageOffset"/> is already set up by the caller so we can use it as a basis
            /// to function as a lock as well as the index of the next page to allocate.
            /// </summary>
            internal bool TryAllocateNewPage(ref PageOffset localPageOffset, int size, out BlockHeader* blockPtr, out int pageSlot)
            {
                // Increment the page index to "claim the lock".
                PageOffset newPageOffset = new() { Page = localPageOffset.Page + 1, Offset = 0 };
                TailPageOffset.PageAndOffset = Interlocked.CompareExchange(ref TailPageOffset.PageAndOffset, newPageOffset.PageAndOffset, localPageOffset.PageAndOffset);
                if (TailPageOffset.PageAndOffset != localPageOffset.PageAndOffset)
                {
                    // Someone else incremented the page (or maybe someone else sneaked in with a smaller request and allocated from the end of the page).
                    // Yield to give them a chance to do the actual page allocation, then retry.
                    // TODO: possibly handle the page-end fragment by "allocating" it to ourselves here and storing it in a binned freelist
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
                    localPageOffset.Offset = 0;
                    Pages = newPages;
                }

                blockPtr = AllocatePage(newPageOffset.Page, size);
                pageSlot = newPageOffset.Page;

                // Update tailPageOffset and return.
                TailPageOffset.PageAndOffset = newPageOffset.PageAndOffset;
                return true;
            }

            internal BlockHeader* AllocatePage(int pageSlot, int size)
            {
                var ptr = (byte*)NativeMemory.AlignedAlloc((nuint)size, Constants.kCacheLineBytes);
                Pages[pageSlot] = ptr;
                return (BlockHeader*)ptr;
            }
        }
    }
}