// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Interface for hybrid log memory allocator struct wrapper callbacks for inlining performance-path callbacks from within
    /// <see cref="AllocatorBase{TStoreFunctions, TAllocatorCallbacks}"/> to the fully derived allocator, including both record accessors and Scan calls.
    /// </summary>
    /// <remarks>This interface does not currently appear in type constraints, but the organization may prove useful.</remarks>
    public interface IAllocatorCallbacks<TStoreFunctions>
        where TStoreFunctions : IStoreFunctions
    {
        /// <summary>Allocate the page in the circular buffer slot at <paramref name="pageIndex"/></summary>
        void AllocatePage(int pageIndex);

        /// <summary>
        /// Populate the page at <paramref name="destinationPageIndex"/> from the <paramref name="src"/> pointer, which has <paramref name="required_bytes"/> bytes.
        /// </summary>
        unsafe void PopulatePage(byte* src, int required_bytes, long destinationPageIndex);

        /// <summary>Free the page at <paramref name="pageIndex"/>, starting at <paramref name="offset"/></summary>
        void ClearPage(long pageIndex, int offset = 0);

        /// <summary>Free the page at <paramref name="pageIndex"/></summary>
        void FreePage(long pageIndex);

        /// <summary>Number of extra overflow pages allocated</summary>
        int OverflowPageCount { get; }
    }
}