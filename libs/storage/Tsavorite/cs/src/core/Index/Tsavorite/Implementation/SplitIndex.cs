// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        private void SplitBuckets(long hash)
        {
            long masked_bucket_index = hash & kernel.hashTable.spine.state[1 - kernel.hashTable.spine.resizeInfo.version].size_mask;
            int chunkOffset = (int)(masked_bucket_index >> Constants.kSizeofChunkBits);
            SplitBuckets(chunkOffset);
        }

        private void SplitBuckets(int chunkOffset)
        {
            // We have updated resizeInfo.version to the new version, so must use [1 - resizeInfo.version] to reference the old version.
            int numChunks = (int)(kernel.hashTable.spine.state[1 - kernel.hashTable.spine.resizeInfo.version].size / Constants.kSizeofChunk);
            if (numChunks == 0) numChunks = 1; // at least one chunk

            if (!Utility.IsPowerOfTwo(numChunks))
            {
                throw new TsavoriteException("Invalid number of chunks: " + numChunks);
            }

            // Process each chunk, wrapping around the end of the chunk list.
            for (int i = chunkOffset; i < chunkOffset + numChunks; i++)
            {
                // Try to gain exclusive access to this chunk's split state (1 means locked, 0 means unlocked, 2 means the chunk was already split);
                // another thread could also be running SplitChunks. TODO change these numbers to named consts.
                if (0 == Interlocked.CompareExchange(ref kernel.hashTable.splitStatus[i & (numChunks - 1)], 1, 0))
                {
                    // "Chunks" are offsets into one contiguous allocation: tableAligned
                    long chunkSize = kernel.hashTable.spine.state[1 - kernel.hashTable.spine.resizeInfo.version].size / numChunks;
                    long ptr = chunkSize * (i & (numChunks - 1));

                    HashBucket* src_start = kernel.hashTable.spine.state[1 - kernel.hashTable.spine.resizeInfo.version].tableAligned + ptr;

                    // The start of the destination chunk
                    HashBucket* dest_start0 = kernel.hashTable.spine.state[kernel.hashTable.spine.resizeInfo.version].tableAligned + ptr;
                    // The midpoint of the destination chunk (old version size is half the new version size)
                    HashBucket* dest_start1 = kernel.hashTable.spine.state[kernel.hashTable.spine.resizeInfo.version].tableAligned + kernel.hashTable.spine.state[1 - kernel.hashTable.spine.resizeInfo.version].size + ptr;

                    SplitChunk(src_start, dest_start0, dest_start1, chunkSize);

                    // The split for this chunk is done (2 means completed; no Interlock is needed here because we have exclusive access).
                    // splitStatus is re-created for each index size operation, so we never need to reset this to zero.
                    kernel.hashTable.splitStatus[i & (numChunks - 1)] = 2;

                    if (Interlocked.Decrement(ref kernel.hashTable.numPendingChunksToBeSplit) == 0)
                    {
                        // There are no more chunks to be split so GC the old version of the hash table
                        kernel.hashTable.spine.state[1 - kernel.hashTable.spine.resizeInfo.version] = default;
                        kernel.hashTable.overflowBucketsAllocatorResize.Dispose();
                        kernel.hashTable.overflowBucketsAllocatorResize = null;
                        GlobalStateMachineStep(systemState);
                        return;
                    }
                    break;
                }
            }

            while (Interlocked.Read(ref kernel.hashTable.splitStatus[chunkOffset & (numChunks - 1)]) == 1)
            {
                Thread.Yield();
            }
        }

        private void SplitChunk(
                    HashBucket* _src_start,
                    HashBucket* _dest_start0,
                    HashBucket* _dest_start1,
                    long chunkSize)
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
                            if (entry.AbsoluteAddress >= readCacheBase.HeadAddress)
                                physicalAddress = readcache.GetPhysicalAddress(entry.AbsoluteAddress);
                        }
                        else if (logicalAddress >= hlogBase.HeadAddress)
                            physicalAddress = hlog.GetPhysicalAddress(logicalAddress);

                        // It is safe to always use hlog instead of readcache for some calls such
                        // as GetKey and GetInfo
                        if (physicalAddress != 0)
                        {
                            var hash = storeFunctions.GetKeyHashCode64(ref hlog.GetKey(physicalAddress));
                            if ((hash & kernel.hashTable.spine.state[kernel.hashTable.spine.resizeInfo.version].size_mask) >> (kernel.hashTable.spine.state[kernel.hashTable.spine.resizeInfo.version].size_bits - 1) == 0)
                            {
                                // Insert in left
                                if (left == left_end)
                                {
                                    var new_bucket_logical = kernel.hashTable.overflowBucketsAllocator.Allocate();
                                    var new_bucket = (HashBucket*)kernel.hashTable.overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                    *left = new_bucket_logical;
                                    left = (long*)new_bucket;
                                    left_end = left + Constants.kOverflowBucketIndex;
                                }

                                *left = entry.word;
                                left++;

                                // Insert previous address in right
                                entry.Address = TraceBackForOtherChainStart(hlog.GetInfo(physicalAddress).PreviousAddress, 1);
                                if ((entry.Address != Constants.kInvalidAddress) && (entry.Address != Constants.kTempInvalidAddress))
                                {
                                    if (right == right_end)
                                    {
                                        var new_bucket_logical = kernel.hashTable.overflowBucketsAllocator.Allocate();
                                        var new_bucket = (HashBucket*)kernel.hashTable.overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
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
                                    var new_bucket_logical = kernel.hashTable.overflowBucketsAllocator.Allocate();
                                    var new_bucket = (HashBucket*)kernel.hashTable.overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                    *right = new_bucket_logical;
                                    right = (long*)new_bucket;
                                    right_end = right + Constants.kOverflowBucketIndex;
                                }

                                *right = entry.word;
                                right++;

                                // Insert previous address in left
                                entry.Address = TraceBackForOtherChainStart(hlog.GetInfo(physicalAddress).PreviousAddress, 0);
                                if ((entry.Address != Constants.kInvalidAddress) && (entry.Address != Constants.kTempInvalidAddress))
                                {
                                    if (left == left_end)
                                    {
                                        var new_bucket_logical = kernel.hashTable.overflowBucketsAllocator.Allocate();
                                        var new_bucket = (HashBucket*)kernel.hashTable.overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
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
                                var new_bucket_logical = kernel.hashTable.overflowBucketsAllocator.Allocate();
                                var new_bucket = (HashBucket*)kernel.hashTable.overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                *left = new_bucket_logical;
                                left = (long*)new_bucket;
                                left_end = left + Constants.kOverflowBucketIndex;
                            }

                            *left = entry.word;
                            left++;

                            // Insert in right
                            if (right == right_end)
                            {
                                var new_bucket_logical = kernel.hashTable.overflowBucketsAllocator.Allocate();
                                var new_bucket = (HashBucket*)kernel.hashTable.overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                *right = new_bucket_logical;
                                right = (long*)new_bucket;
                                right_end = right + Constants.kOverflowBucketIndex;
                            }

                            *right = entry.word;
                            right++;
                        }
                    }

                    if (*(((long*)src_start) + Constants.kOverflowBucketIndex) == 0) break;
                    src_start = (HashBucket*)kernel.hashTable.overflowBucketsAllocatorResize.GetPhysicalAddress(*(((long*)src_start) + Constants.kOverflowBucketIndex));
                } while (true);
            }
        }

        private long TraceBackForOtherChainStart(long logicalAddress, int bit)
        {
            while (true)
            {
                HashBucketEntry entry = default;
                entry.Address = logicalAddress;
                if (entry.ReadCache)
                {
                    if (logicalAddress < readCacheBase.HeadAddress)
                        break;
                    var physicalAddress = readcache.GetPhysicalAddress(logicalAddress);
                    var hash = storeFunctions.GetKeyHashCode64(ref readcache.GetKey(physicalAddress));
                    if ((hash & kernel.hashTable.spine.state[kernel.hashTable.spine.resizeInfo.version].size_mask) >> (kernel.hashTable.spine.state[kernel.hashTable.spine.resizeInfo.version].size_bits - 1) == bit)
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
                    if ((hash & kernel.hashTable.spine.state[kernel.hashTable.spine.resizeInfo.version].size_mask) >> (kernel.hashTable.spine.state[kernel.hashTable.spine.resizeInfo.version].size_bits - 1) == bit)
                    {
                        return logicalAddress;
                    }
                    logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                }
            }
            return logicalAddress;
        }
    }
}