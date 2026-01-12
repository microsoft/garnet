// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#define CALLOC

namespace Tsavorite.core
{
    internal struct AsyncGetFromDiskResult<TContext>
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