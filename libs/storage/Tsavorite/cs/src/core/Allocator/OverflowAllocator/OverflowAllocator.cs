// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// A class to manage Keys (and SpanByte values) that are too large to be inline in the hlog page. Each hlog page has its own instance.
    /// </summary>
    /// <remarks>This has two regions:
    ///     <list type="bullet">
    ///         <item>Fixed size: these are a set of bins in powers of 2 up to <see cref="FixedSizePages.PageSize"/>. See <see cref="FixedSizePages"/>.</item>
    ///         <item>Oversize: these are allocations greater than <see cref="FixedSizePages.MaxBlockSize"/>. Each allocation is a separate page. See <see cref="OversizePages"/></item>
    ///     </list>
    /// </remarks>
    internal unsafe partial class OverflowAllocator
    {
        private FixedSizePages fixedSizePages;
        private OversizePages oversizePages;

        /// <summary>
        /// Constructor for the allocator
        /// </summary>
        /// <param name="fixedPageSize">The size of a page for fixed-size allocations in this allocator</param>
        internal OverflowAllocator(int fixedPageSize)
        {
            System.Diagnostics.Debug.Assert(fixedPageSize >= FixedSizePages.MaxBlockSize && Utility.IsPowerOfTwo(fixedPageSize), "PageSize must be > FixedSizeLimit and a power of 2");
            fixedSizePages = new(fixedPageSize);
            oversizePages = new();
        }

        public byte* Allocate(int size, bool zeroInit)
        {
            System.Diagnostics.Debug.Assert(size > 0, "Cannot have negative allocation size");

            if (size > FixedSizePages.MaxBlockSize)
                return AllocateOversize(size, zeroInit);

            while (true)
            {
                PageOffset localPageOffset = new() { PageAndOffset = tailPageOffset.PageAndOffset };

                var sizeWithHeader = size + sizeof(BlockHeader);

                // See if it fits on the current last page. This also applies to the "no pages allocated" case, because we initialize tailPageOffset.Offset to out-of-range
                while (localPageOffset.Offset + sizeWithHeader < fixedPageSize)
                {
                    // Fast path; simple offset pointer advance
                    if (Interlocked.CompareExchange(ref tailPageOffset.PageAndOffset, localPageOffset.PageAndOffset + sizeWithHeader, localPageOffset.PageAndOffset) == localPageOffset.PageAndOffset)
                    {
                        var blockPtr = (BlockHeader*)pages[localPageOffset.Page] + localPageOffset.Offset;
                        blockPtr->Size = size;
                        var ptr = (byte*)(blockPtr + 1);
                        if (zeroInit)
                            NativeMemory.Clear(ptr, (nuint)size);
                        return ptr;
                    }
                    localPageOffset.PageAndOffset = tailPageOffset.PageAndOffset;
                }

                // We need to allocate a new page. Increment the page index to "claim the lock".
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
                    var newPages = new byte*[newPageCount];
                    if (pageCount != 0)
                        Array.Copy(pages, newPages, pageCount);
                    localPageOffset.Offset = 0;
                    pageCount = newPageCount;
                    pages = newPages;
                }

                // Allocate the new page. Defer zeroinit until the actual allocation.
                var pagePtr = (byte*)NativeMemory.AlignedAlloc((nuint)fixedPageSize, Constants.kCacheLineBytes);
                pages[newPageOffset.Page] = pagePtr;

                // Update tailPageOffset and retry.
                tailPageOffset.PageAndOffset = newPageOffset.PageAndOffset;
            }
        }

        /// <summary>Get the allocated size of this block. In-use size is tracked by caller.</summary>
        public int GetAllocatedSize(long address) => (*((BlockHeader*)address - 1)).Size;

        public byte* AllocateOversize(int size, bool zeroInit)
        {
            var page = (byte*)NativeMemory.AlignedAlloc((nuint)(size + sizeof(OversizePageHeader)), Constants.kCacheLineBytes);
            var pageHeader = *(OversizePageHeader*)page;
            pageHeader.BlockHeader.Size = size;

            while (true)
            {
                pageHeader.NextPage = oversizeList;
                if (Interlocked.CompareExchange(ref oversizeList, (nuint)page, pageHeader.NextPage) == pageHeader.NextPage)
                    break;
            }
            var ptr = page + sizeof(OversizePageHeader);
            if (zeroInit)
                NativeMemory.Clear(ptr, (nuint)size);
            return ptr;
        }

        internal void Free(byte* address)
        {
            // TODO
        }

        /// <summary>Clears and frees all allocations and prepares for reuse</summary>
        public void Clear()
        {
            // Free the oversize list.
            nuint nextPage = default;
            for (nuint page = oversizeList; page != default; page = nextPage)
            {
                nextPage = (*(OversizePageHeader*)page).NextPage;
                NativeMemory.AlignedFree((void*)page);
            }
            oversizeList = default;

            // Free the fixed-size list
            for (var ii = 0; ii < tailPageOffset.Page; ++ii)
            { 
                NativeMemory.AlignedFree((nuint*)pages[ii]);
                pages[ii] = null;
            }

            // Prep for reuse
            tailPageOffset = new() { Page = -1, Offset = fixedPageSize + 1 };
        }
    }
}
