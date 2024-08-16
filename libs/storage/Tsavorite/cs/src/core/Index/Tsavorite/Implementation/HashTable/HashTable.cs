// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;
using System.Threading;

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


        internal bool SplitBuckets<TKey, TValue, TStoreFunctions, TAllocator>(long hash,
                in TStoreFunctions storeFunctions, TAllocator hlog, AllocatorBase<TKey, TValue, TStoreFunctions, TAllocator> hlogBase,
                TAllocator readcache, AllocatorBase<TKey, TValue, TStoreFunctions, TAllocator> readcacheBase)
            where TStoreFunctions : IStoreFunctions<TKey, TValue>
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
        {
            long masked_bucket_index = hash & spine.state[1 - spine.resizeInfo.version].size_mask;
            int chunkOffset = (int)(masked_bucket_index >> Constants.kSizeofChunkBits);

            // We have updated resizeInfo.version to the new version, so must use [1 - resizeInfo.version] to reference the old version.
            int numChunks = (int)(spine.state[1 - spine.resizeInfo.version].size / Constants.kSizeofChunk);
            if (numChunks == 0)
                numChunks = 1; // at least one chunk

            if (!Utility.IsPowerOfTwo(numChunks))
                throw new TsavoriteException("Invalid number of chunks: " + numChunks);

            // Process each chunk, wrapping around the end of the chunk list.
            for (int i = chunkOffset; i < chunkOffset + numChunks; i++)
            {
                // Try to gain exclusive access to this chunk's split state (1 means locked, 0 means unlocked, 2 means the chunk was already split);
                // another thread could also be running SplitChunks. TODO change these numbers to named consts.
                if (0 == Interlocked.CompareExchange(ref splitStatus[i & (numChunks - 1)], 1, 0))
                {
                    // "Chunks" are offsets into one contiguous allocation: tableAligned
                    long chunkSize = spine.state[1 - spine.resizeInfo.version].size / numChunks;
                    long ptr = chunkSize * (i & (numChunks - 1));

                    HashBucket* src_start = spine.state[1 - spine.resizeInfo.version].tableAligned + ptr;

                    // The start of the destination chunk
                    HashBucket* dest_start0 = spine.state[spine.resizeInfo.version].tableAligned + ptr;
                    // The midpoint of the destination chunk (old version size is half the new version size)
                    HashBucket* dest_start1 = spine.state[spine.resizeInfo.version].tableAligned + spine.state[1 - spine.resizeInfo.version].size + ptr;

                    SplitChunk(src_start, dest_start0, dest_start1, chunkSize, in storeFunctions, hlog, hlogBase, readcache, readcacheBase);

                    // The split for this chunk is done (2 means completed; no Interlock is needed here because we have exclusive access).
                    // splitStatus is re-created for each index size operation, so we never need to reset this to zero.
                    splitStatus[i & (numChunks - 1)] = 2;

                    if (Interlocked.Decrement(ref numPendingChunksToBeSplit) == 0)
                    {
                        // There are no more chunks to be split so GC the old version of the hash table
                        spine.state[1 - spine.resizeInfo.version] = default;
                        overflowBucketsAllocatorResize.Dispose();
                        overflowBucketsAllocatorResize = null;

                        // Caller will issue GlobalStateMachineStep(systemState);
                        return true;
                    }
                    break;
                }
            }

            while (Interlocked.Read(ref splitStatus[chunkOffset & (numChunks - 1)]) == 1)
            {
                Thread.Yield();
            }
            return false;
        }

        private void SplitChunk<TKey, TValue, TStoreFunctions, TAllocator>(HashBucket* _src_start, HashBucket* _dest_start0, HashBucket* _dest_start1, long chunkSize,
                in TStoreFunctions storeFunctions, TAllocator hlog, AllocatorBase<TKey, TValue, TStoreFunctions, TAllocator> hlogBase, 
                TAllocator readcache, AllocatorBase<TKey, TValue, TStoreFunctions, TAllocator> readcacheBase)
            where TStoreFunctions : IStoreFunctions<TKey, TValue>
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
        {
            for (int i = 0; i < chunkSize; i++)
            {
                var src_start = _src_start + i;

                long* left = (long*)(_dest_start0 + i);
                long* right = (long*)(_dest_start1 + i);

                // We'll step through a single bucket at a time
                long* left_end = left + Constants.kOverflowBucketIndex;
                long* right_end = right + Constants.kOverflowBucketIndex;

                HashBucketEntry entry = default;
                do
                {
                    for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                    {
                        entry.word = *(((long*)src_start) + index);
                        if (Constants.kInvalidEntry == entry.word)
                        {
                            continue;
                        }

                        var logicalAddress = entry.Address;
                        long physicalAddress = 0;

                        if (entry.ReadCache)
                        {
                            if (entry.AbsoluteAddress >= readcacheBase.HeadAddress)
                                physicalAddress = readcache.GetPhysicalAddress(entry.AbsoluteAddress);
                        }
                        else if (logicalAddress >= hlogBase.HeadAddress)
                            physicalAddress = hlog.GetPhysicalAddress(logicalAddress);

                        // It is safe to always use hlog instead of readcache for some calls such
                        // as GetKey and GetInfo
                        if (physicalAddress != 0)
                        {
                            var hash = storeFunctions.GetKeyHashCode64(ref hlog.GetKey(physicalAddress));
                            if ((hash & spine.state[spine.resizeInfo.version].size_mask) >> (spine.state[spine.resizeInfo.version].size_bits - 1) == 0)
                            {
                                // Insert in left
                                if (left == left_end)
                                {
                                    var new_bucket_logical = overflowBucketsAllocator.Allocate();
                                    var new_bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                    *left = new_bucket_logical;
                                    left = (long*)new_bucket;
                                    left_end = left + Constants.kOverflowBucketIndex;
                                }

                                *left = entry.word;
                                left++;

                                // Insert previous address in right
                                entry.Address = TraceBackForOtherChainStart(hlog.GetInfo(physicalAddress).PreviousAddress, 1, in storeFunctions, hlog, hlogBase, readcache, readcacheBase);
                                if ((entry.Address != Constants.kInvalidAddress) && (entry.Address != Constants.kTempInvalidAddress))
                                {
                                    if (right == right_end)
                                    {
                                        var new_bucket_logical = overflowBucketsAllocator.Allocate();
                                        var new_bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                        *right = new_bucket_logical;
                                        right = (long*)new_bucket;
                                        right_end = right + Constants.kOverflowBucketIndex;
                                    }

                                    *right = entry.word;
                                    right++;
                                }
                            }
                            else
                            {
                                // Insert in right
                                if (right == right_end)
                                {
                                    var new_bucket_logical = overflowBucketsAllocator.Allocate();
                                    var new_bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                    *right = new_bucket_logical;
                                    right = (long*)new_bucket;
                                    right_end = right + Constants.kOverflowBucketIndex;
                                }

                                *right = entry.word;
                                right++;

                                // Insert previous address in left
                                entry.Address = TraceBackForOtherChainStart(hlog.GetInfo(physicalAddress).PreviousAddress, 0, in storeFunctions, hlog, hlogBase, readcache, readcacheBase);
                                if ((entry.Address != Constants.kInvalidAddress) && (entry.Address != Constants.kTempInvalidAddress))
                                {
                                    if (left == left_end)
                                    {
                                        var new_bucket_logical = overflowBucketsAllocator.Allocate();
                                        var new_bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                        *left = new_bucket_logical;
                                        left = (long*)new_bucket;
                                        left_end = left + Constants.kOverflowBucketIndex;
                                    }

                                    *left = entry.word;
                                    left++;
                                }
                            }
                        }
                        else
                        {
                            // Insert in both new locations

                            // Insert in left
                            if (left == left_end)
                            {
                                var new_bucket_logical = overflowBucketsAllocator.Allocate();
                                var new_bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                *left = new_bucket_logical;
                                left = (long*)new_bucket;
                                left_end = left + Constants.kOverflowBucketIndex;
                            }

                            *left = entry.word;
                            left++;

                            // Insert in right
                            if (right == right_end)
                            {
                                var new_bucket_logical = overflowBucketsAllocator.Allocate();
                                var new_bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                *right = new_bucket_logical;
                                right = (long*)new_bucket;
                                right_end = right + Constants.kOverflowBucketIndex;
                            }

                            *right = entry.word;
                            right++;
                        }
                    }

                    if (*(((long*)src_start) + Constants.kOverflowBucketIndex) == 0) break;
                    src_start = (HashBucket*)overflowBucketsAllocatorResize.GetPhysicalAddress(*(((long*)src_start) + Constants.kOverflowBucketIndex));
                } while (true);
            }
        }

        private long TraceBackForOtherChainStart<TKey, TValue, TStoreFunctions, TAllocator>(long logicalAddress, int bit,
                in TStoreFunctions storeFunctions, TAllocator hlog, AllocatorBase<TKey, TValue, TStoreFunctions, TAllocator> hlogBase,
                TAllocator readcache, AllocatorBase<TKey, TValue, TStoreFunctions, TAllocator> readcacheBase)
            where TStoreFunctions : IStoreFunctions<TKey, TValue>
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
        {
            while (true)
            {
                HashBucketEntry entry = default;
                entry.Address = logicalAddress;
                if (entry.ReadCache)
                {
                    if (logicalAddress < readcacheBase.HeadAddress)
                        break;
                    var physicalAddress = readcache.GetPhysicalAddress(logicalAddress);
                    var hash = storeFunctions.GetKeyHashCode64(ref readcache.GetKey(physicalAddress));
                    if ((hash & spine.state[spine.resizeInfo.version].size_mask) >> (spine.state[spine.resizeInfo.version].size_bits - 1) == bit)
                    {
                        return logicalAddress;
                    }
                    logicalAddress = readcache.GetInfo(physicalAddress).PreviousAddress;
                }
                else
                {
                    if (logicalAddress < hlogBase.HeadAddress)
                        break;
                    var physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                    var hash = storeFunctions.GetKeyHashCode64(ref hlog.GetKey(physicalAddress));
                    if ((hash & spine.state[spine.resizeInfo.version].size_mask) >> (spine.state[spine.resizeInfo.version].size_bits - 1) == bit)
                    {
                        return logicalAddress;
                    }
                    logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                }
            }
            return logicalAddress;
        }

        internal void Dispose()
        {
            overflowBucketsAllocator.Dispose();
        }
    }
}
