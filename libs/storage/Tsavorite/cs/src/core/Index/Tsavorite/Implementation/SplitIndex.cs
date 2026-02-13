// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    using static LogAddress;

    public unsafe partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        internal void SplitAllBuckets()
        {
            var numChunks = (int)(state[1 - resizeInfo.version].size / Constants.kSizeofChunk);
            if (numChunks == 0) numChunks = 1; // at least one chunk
            for (var i = 0; i < numChunks; i++)
            {
                _ = SplitSingleBucket(i, numChunks);
            }

            // Wait for all chunks to be split
            while (numPendingChunksToBeSplit > 0)
                _ = Thread.Yield();

            // Splits done, GC the old version of the hash table
            Debug.Assert(numPendingChunksToBeSplit == 0);
            state[1 - resizeInfo.version] = default;
            overflowBucketsAllocatorResize.Dispose();
            overflowBucketsAllocatorResize = null;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        internal void SplitBuckets(long hash)
        {
            long masked_bucket_index = hash & state[1 - resizeInfo.version].size_mask;
            int chunkOffset = (int)(masked_bucket_index >> Constants.kSizeofChunkBits);
            SplitBuckets(chunkOffset);
        }

        private void SplitBuckets(int chunkOffset)
        {
            // We have updated resizeInfo.version to the new version, so must use [1 - resizeInfo.version] to reference the old version.
            int numChunks = (int)(state[1 - resizeInfo.version].size / Constants.kSizeofChunk);
            if (numChunks == 0) numChunks = 1; // at least one chunk

            if (!Utility.IsPowerOfTwo(numChunks))
            {
                throw new TsavoriteException("Invalid number of chunks: " + numChunks);
            }

            // Process each chunk, wrapping around the end of the chunk list.
            for (int i = chunkOffset; i < chunkOffset + numChunks; i++)
            {
                if (SplitSingleBucket(i, numChunks))
                {
                    break;
                }
            }

            while (Interlocked.Read(ref splitStatus[chunkOffset & (numChunks - 1)]) == 1)
                _ = Thread.Yield();
        }

        private bool SplitSingleBucket(int i, int numChunks)
        {
            // Try to gain exclusive access to this chunk's split state (1 means locked, 0 means unlocked, 2 means the chunk was already split);
            // another thread could also be running SplitChunks. TODO change these numbers to named consts.
            if (0 == Interlocked.CompareExchange(ref splitStatus[i & (numChunks - 1)], 1, 0))
            {
                // "Chunks" are offsets into one contiguous allocation: tableAligned
                long chunkSize = state[1 - resizeInfo.version].size / numChunks;
                long ptr = chunkSize * (i & (numChunks - 1));

                HashBucket* src_start = state[1 - resizeInfo.version].tableAligned + ptr;

                // The start of the destination chunk
                HashBucket* dest_start0 = state[resizeInfo.version].tableAligned + ptr;
                // The midpoint of the destination chunk (old version size is half the new version size)
                HashBucket* dest_start1 = state[resizeInfo.version].tableAligned + state[1 - resizeInfo.version].size + ptr;

                SplitChunk(src_start, dest_start0, dest_start1, chunkSize);

                // The split for this chunk is done (2 means completed; no Interlock is needed here because we have exclusive access).
                // splitStatus is re-created for each index size operation, so we never need to reset this to zero.
                splitStatus[i & (numChunks - 1)] = 2;

                _ = Interlocked.Decrement(ref numPendingChunksToBeSplit);
                return true;
            }
            return false;
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

                var systemState = stateMachineDriver.SystemState;
                // Verify that we are not moving latched buckets
                Debug.Assert(!HashBucket.IsLatched(src_start));

                HashBucketEntry entry = default;
                do
                {
                    for (int index = 0; index < Constants.kOverflowBucketIndex; index++)
                    {
                        entry.word = *(((long*)src_start) + index);
                        if (Constants.kInvalidEntry == entry.word)
                            continue;

                        LogRecord logRecord = default;
                        if (entry.IsReadCache)
                        {
                            if (entry.Address >= readcacheBase.HeadAddress)
                                logRecord = readcache.CreateLogRecord(entry.Address);
                        }
                        else if (entry.Address >= hlogBase.HeadAddress)
                            logRecord = hlog.CreateLogRecord(entry.Address);

                        if (logRecord.IsSet)
                        {
                            var physicalAddress = logRecord.physicalAddress;
                            var hash = storeFunctions.GetKeyHashCode64(logRecord.Key, logRecord.Namespace);
                            if ((hash & state[resizeInfo.version].size_mask) >> (state[resizeInfo.version].size_bits - 1) == 0)
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
                                entry.Address = TraceBackForOtherChainStart(LogRecord.GetInfo(physicalAddress).PreviousAddress, 1);
                                if ((entry.Address != kInvalidAddress) && (entry.Address != kTempInvalidAddress))
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
                                entry.Address = TraceBackForOtherChainStart(LogRecord.GetInfo(physicalAddress).PreviousAddress, 0);
                                if ((entry.Address != kInvalidAddress) && (entry.Address != kTempInvalidAddress))
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

        private long TraceBackForOtherChainStart(long logicalAddress, int bit)
        {
            while (true)
            {
                LogRecord logRecord;
                if (IsReadCache(logicalAddress))
                {
                    if (logicalAddress < readcacheBase.HeadAddress)
                        break;
                    logRecord = new LogRecord(readcacheBase.GetPhysicalAddress(logicalAddress));
                }
                else
                {
                    if (logicalAddress < hlogBase.HeadAddress)
                        break;
                    logRecord = new LogRecord(hlogBase.GetPhysicalAddress(logicalAddress));
                }

                var hash = storeFunctions.GetKeyHashCode64(logRecord.Key, logRecord.Namespace);
                if ((hash & state[resizeInfo.version].size_mask) >> (state[resizeInfo.version].size_bits - 1) == bit)
                    return logicalAddress;
                logicalAddress = logRecord.Info.PreviousAddress;
            }
            return logicalAddress;
        }
    }
}