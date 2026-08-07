// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.epoch.litmus
{
    /// <summary>
    /// A fixed pool of equally sized pages carved out of one mapping. Pages are handed out by
    /// index and never returned to the OS, so no round of the litmus allocates or enters the
    /// kernel; "freeing" a page is <see cref="Stamp"/>, which is why the mapping must stay live
    /// for as long as any thread can still dereference a page.
    /// </summary>
    internal sealed unsafe class PagePool : IDisposable
    {
        readonly byte* region;
        readonly nuint bytes;
        bool disposed;

        /// <summary>Bytes per page. A power of two, so <see cref="WordIndexMask"/> can wrap an index.</summary>
        internal nuint PageSize { get; }

        internal int PageCount { get; }

        /// <summary>8-byte words per page.</summary>
        internal int WordsPerPage { get; }

        /// <summary>Mask that folds any word index back into a page.</summary>
        internal int WordIndexMask => WordsPerPage - 1;

        internal PagePool(nuint pageSize, int pageCount)
        {
            if (pageSize < sizeof(long))
                throw new ArgumentOutOfRangeException(nameof(pageSize), pageSize, "a page must hold at least one word");
            if ((pageSize & (pageSize - 1)) != 0)
                throw new ArgumentOutOfRangeException(nameof(pageSize), pageSize, "page size must be a power of two so a word index can be masked into a page");
            if (pageCount <= 0)
                throw new ArgumentOutOfRangeException(nameof(pageCount), pageCount, "the pool needs at least one page");

            PageSize = pageSize;
            PageCount = pageCount;
            WordsPerPage = (int)(pageSize / sizeof(long));
            bytes = pageSize * (nuint)pageCount;

            if (bytes / pageSize != (nuint)pageCount)
                throw new ArgumentOutOfRangeException(nameof(pageCount), pageCount, "pool size overflows");

            region = Platform.MapPage(bytes);
        }

        /// <summary>The page at <paramref name="index"/>.</summary>
        internal long* Page(int index) => (long*)(region + ((nuint)index * PageSize));

        /// <summary>The page holding round <paramref name="round"/>, wrapping around the pool.</summary>
        internal long* PageForRound(long round) => Page((int)(round % PageCount));

        /// <summary>Fill a page with its own word indices, overwriting whatever <see cref="Stamp"/> left.</summary>
        internal void Fill(long* page)
        {
            for (var index = 0; index < WordsPerPage; index++)
                page[index] = index;
        }

        /// <summary>Stands in for the unmap: writing <paramref name="value"/> over every word destroys anything a still-protected reader could legitimately see.</summary>
        internal void Stamp(long address, long value)
        {
            var page = (long*)address;
            for (var index = 0; index < WordsPerPage; index++)
                page[index] = value;
        }

        public void Dispose()
        {
            if (disposed)
                return;

            disposed = true;
            Platform.Unmap(region, bytes);
        }
    }
}