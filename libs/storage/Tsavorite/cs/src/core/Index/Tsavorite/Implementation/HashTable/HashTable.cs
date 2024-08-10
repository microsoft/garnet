// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// The Tsavorite hash table
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    internal unsafe struct HashTable
    {
        internal const int Size = HashTableSpine.Size + Constants.IntPtrSize * 3 + sizeof(long);

        [FieldOffset(0)]
        internal HashTableSpine spine;

        // Allocator for the hash buckets
        [FieldOffset(HashTableSpine.Size)]
        internal MallocFixedPageSize<HashBucket> overflowBucketsAllocator;
        [FieldOffset(HashTableSpine.Size + Constants.IntPtrSize)]
        internal MallocFixedPageSize<HashBucket> overflowBucketsAllocatorResize;

        // Array used to denote if a specific chunk is merged or not
        [FieldOffset(HashTableSpine.Size + Constants.IntPtrSize * 2)]
        internal long[] splitStatus;

        // Used as an atomic counter to check if resizing is complete
        [FieldOffset(HashTableSpine.Size + Constants.IntPtrSize * 3)]
        internal long numPendingChunksToBeSplit;

        public HashTable(long size, int sector_size, ILogger logger)
        {
            spine = new();
            overflowBucketsAllocator = new MallocFixedPageSize<HashBucket>(logger);

            spine.resizeInfo = default;
            spine.resizeInfo.status = ResizeOperationStatus.DONE;
            spine.resizeInfo.version = 0;
            Reinitialize(spine.resizeInfo.version, size, sector_size);
        }

        // This is used by both the ctor and by index resizing.
        internal unsafe void Reinitialize(int version, long size, int sector_size)
        {
            long size_bytes = size * sizeof(HashBucket);
            long aligned_size_bytes = sector_size +
                ((size_bytes + (sector_size - 1)) & ~(sector_size - 1));

            //Over-allocate and align the table to the cacheline
            spine.state[version].size = size;
            spine.state[version].size_mask = size - 1;
            spine.state[version].size_bits = Utility.GetLogBase2((int)size);

            spine.state[version].tableRaw = GC.AllocateArray<HashBucket>((int)(aligned_size_bytes / Constants.kCacheLineBytes), true);
            long sectorAlignedPointer = ((long)Unsafe.AsPointer(ref spine.state[version].tableRaw[0]) + (sector_size - 1)) & ~(sector_size - 1);
            spine.state[version].tableAligned = (HashBucket*)sectorAlignedPointer;
        }

        internal void Dispose()
        {
            overflowBucketsAllocator.Dispose();
        }
    }
}
