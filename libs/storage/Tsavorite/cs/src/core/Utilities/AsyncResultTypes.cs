// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#define CALLOC

namespace Tsavorite.core
{
    // Sealed class (was struct) so the device callback path can pass `this` as
    // <see cref="System.Object"/> without boxing the wrapper, and so the allocator can
    // pool instances across pending IOs. The pool drains on Reserve/Return via the
    // <see cref="AllocatorBase{TStoreFunctions,TAllocator}"/> get/return helpers.
    internal sealed class AsyncGetFromDiskResult<TContext>
    {
        public TContext context;
    }

    internal struct HashIndexPageAsyncFlushResult
    {
        public int chunkIndex;
        public SectorAlignedMemory mem;
    }

    internal struct HashIndexPageAsyncReadResult
    {
        public int chunkIndex;
    }

    internal struct OverflowPagesFlushAsyncResult
    {
        public SectorAlignedMemory mem;
    }

    internal struct OverflowPagesReadAsyncResult
    {
    }
}