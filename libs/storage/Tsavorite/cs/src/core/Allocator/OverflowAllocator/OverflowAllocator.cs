// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

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
    internal unsafe partial class OverflowAllocator : IDisposable
    {
        internal FixedSizePages fixedSizePages;
        internal OversizePages oversizePages;

        /// <summary>
        /// Constructor for the allocator
        /// </summary>
        /// <param name="fixedPageSize">The size of a page for fixed-size allocations in this allocator</param>
        internal OverflowAllocator(int fixedPageSize)
        {
            Debug.Assert(fixedPageSize >= FixedSizePages.MaxBlockSize && Utility.IsPowerOfTwo(fixedPageSize), "PageSize must be > FixedSizeLimit and a power of 2");
            fixedSizePages = new(fixedPageSize);
            oversizePages = new();
        }

        public byte* Allocate(int size, bool zeroInit)
        {
            Debug.Assert(size > 0, "Cannot have negative allocation size");

            if (size <= FixedSizePages.MaxBlockSize)
                return fixedSizePages.Allocate(size, zeroInit);
            return oversizePages.Allocate(size, zeroInit);
        }

        /// <summary>Get the allocated size of this block. In-use size is tracked by caller.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetAllocatedSize(long address) => BlockHeader.FromUserAddress(address)->AllocatedSize;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Free(long address)
        {
            var blockPtr = BlockHeader.FromUserAddress(address);
            if (blockPtr->AllocatedSize <= FixedSizePages.MaxBlockSize)
                fixedSizePages.Free(blockPtr);
            else
                oversizePages.Free(blockPtr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryRealloc(long address, int newSize, out byte* newPtr)
        {
            var blockPtr = BlockHeader.FromUserAddress(address);
            return blockPtr->AllocatedSize <= FixedSizePages.MaxBlockSize
                ? fixedSizePages.TryRealloc(blockPtr, newSize, out newPtr)
                : oversizePages.TryRealloc(blockPtr, newSize, out newPtr);
        }

        /// <summary>Clears and frees all allocations and prepares for reuse</summary>
        public void Clear()
        {
            fixedSizePages.Clear();
            oversizePages.Clear();
        }

        /// <summary>
        /// Dispose overflow allocator
        /// </summary>
        public void Dispose() => Clear();

        /// <summary>
        /// Finalizer to free native memory in case caller forgets Dispose().
        /// </summary>
        ~OverflowAllocator() => Dispose();
    }
}
