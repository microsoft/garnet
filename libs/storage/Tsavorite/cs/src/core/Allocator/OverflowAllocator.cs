﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// A class to manage Keys (and SpanByte values) that are too large to be inline in the main log page.
    /// </summary>
    /// <param name="pageSize">The size of a page in this allocator</param>
    /// <param name="oversizeLimit">If an allocation request is greater than this, an individual allocation is made for it. Must be less than or equal to pageSize.</param>
    public unsafe class OverflowAllocator(int pageSize, int oversizeLimit)
    {
        private const int InitialPageCount = 1024;

        private readonly int pageSize = pageSize;
        private readonly int oversizeLimit = oversizeLimit;

        // We use a separate array so we do not take cacheline space for a linked-list pointer (as oversizeList does).
        // The pages array itself does not need to be cache-aligned because only the intra-page pointers are accessed.
        private byte*[] pages;
        private int pageCount = 0;

        // The linked list of oversize allocations.
        private nuint oversizeList;

        // We advance the tail only; no deallocations until the main log page is deallocated.
        private PageOffset tailPageOffset = new() { Page = -1, Offset = pageSize + 1};

        public SpanByte Allocate(int size)
        {
            System.Diagnostics.Debug.Assert(size > 0, "Cannot have negative allocation size");
            System.Diagnostics.Debug.Assert(oversizeLimit > 0 && pageSize >= oversizeLimit, "OversizeLimit must be greater than zero and within pageSize");

            if (size > oversizeLimit)
                return AllocateOversize(size);

            while (true)
            {
                PageOffset localPageOffset = new() { PageAndOffset = tailPageOffset.PageAndOffset };

                // See if it fits on the current last page. This also applies to the "no pages allocated" case, because we initialize tailPageOffset.Offset to out-of-range
                while (localPageOffset.Offset + size < pageSize)
                {
                    // Fast path; simple offset pointer advance
                    if (Interlocked.CompareExchange(ref tailPageOffset.PageAndOffset, localPageOffset.PageAndOffset + size, localPageOffset.PageAndOffset) == localPageOffset.PageAndOffset)
                        return new(size, (IntPtr)pages[localPageOffset.Page][localPageOffset.Offset]);
                    localPageOffset.PageAndOffset = tailPageOffset.PageAndOffset;
                }

                // We need to allocate a new (possibly oversize) page. Increment the page index to "claim the lock".
                PageOffset newPageOffset = new() { Page = localPageOffset.Page + 1, Offset = 0 };
                tailPageOffset.PageAndOffset = Interlocked.CompareExchange(ref tailPageOffset.PageAndOffset, newPageOffset.PageAndOffset, localPageOffset.PageAndOffset);
                if (tailPageOffset.PageAndOffset != localPageOffset.PageAndOffset)
                {
                    // Someone else incremented the page (or maybe someone else sneaked in with a smaller request and allocated from the end of the page).
                    // Yield to give them a chance to do the actual page allocation, then retry.
                    // TODO: possibly handle the page-end fragment by "allocating" it to ourselves here and storing it in a binned freelist
                    _ = Thread.Yield();
                    continue;
                }

                // First see if we need to grow the pages array.
                if (newPageOffset.Page >= pageCount)
                {
                    var newPageCount = pageCount == 0 ? InitialPageCount : pageCount * 2;
                    var newPages = new byte*[newPageCount][];
                    if (pageCount != 0)
                        Array.Copy(pages, newPages, pageCount);
                    localPageOffset.Offset = 0;
                    pageCount = newPageCount;
                }

                // Allocate the new page.
                pages[newPageOffset.Page] = (byte*)NativeMemory.AlignedAlloc((nuint)pageSize, Constants.kCacheLineBytes);

                // Update tailPageOffset and retry.
                tailPageOffset.PageAndOffset = newPageOffset.PageAndOffset;
            }
        }

        public SpanByte AllocateOversize(int size)
        {
            var page = (byte*)NativeMemory.AlignedAlloc((nuint)(size + sizeof(nuint)), Constants.kCacheLineBytes);
            *(nuint*)page = Interlocked.Exchange(ref oversizeList, (nuint)page);
            NativeMemory.Clear(page + sizeof(nuint), (nuint)size);
            return new(size, (IntPtr)(page + sizeof(nuint)));
        }

        /// <summary>Clears and frees all allocations and prepares for reuse</summary>
        public void Clear()
        {
            // Free the oversize list.
            nuint nextPage = (nuint)null;
            for (nuint page = oversizeList; page != (nuint)null; page = nextPage)
            {
                nextPage = *(nuint*)page;
                NativeMemory.AlignedFree((nuint*)page);
            }
            oversizeList = (nuint)null;

            for (var ii = 0; ii < tailPageOffset.Page; ++ii)
            { 
                NativeMemory.AlignedFree((nuint*)pages[ii]);
                pages[ii] = null;
            }

            // Prep for reuse
            tailPageOffset = new() { Page = -1, Offset = pageSize + 1 };
        }
    }
}
