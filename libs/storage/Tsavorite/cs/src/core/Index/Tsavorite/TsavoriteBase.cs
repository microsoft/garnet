// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    internal unsafe struct InternalHashTable
    {
        public long size;
        public long size_mask;
        public int size_bits;
        public HashBucket[] tableRaw;
        public HashBucket* tableAligned;
    }

    public unsafe partial class TsavoriteBase
    {
        // Initial size of the table
        internal long minTableSize = 16;

        // Allocator for the hash buckets
        internal MallocFixedPageSize<HashBucket> overflowBucketsAllocator;
        internal MallocFixedPageSize<HashBucket> overflowBucketsAllocatorResize;

        // An array of size two, that contains the old and new versions of the hash-table
        internal InternalHashTable[] state = new InternalHashTable[2];

        // Array used to denote if a specific chunk is merged or not
        internal long[] splitStatus;

        // Used as an atomic counter to check if resizing is complete
        internal long numPendingChunksToBeSplit;

        internal readonly LightEpoch epoch;
        readonly bool ownedEpoch;

        internal ResizeInfo resizeInfo;

        /// <summary>
        /// LoggerFactory
        /// </summary>
        protected ILoggerFactory loggerFactory;

        /// <summary>
        /// Logger
        /// </summary>
        protected ILogger logger;

        /// <summary>
        /// Constructor
        /// </summary>
        public TsavoriteBase(LightEpoch epoch = null, ILogger logger = null)
        {
            if (epoch == null)
            {
                this.epoch = new LightEpoch();
                ownedEpoch = true;
            }
            else
                this.epoch = epoch;
            overflowBucketsAllocator = new MallocFixedPageSize<HashBucket>(logger);
        }

        internal void Free()
        {
            Free(0);
            Free(1);
            if (ownedEpoch)
                epoch.Dispose();
            overflowBucketsAllocator.Dispose();
        }

        private static void Free(int version)
        {
        }

        /// <summary>
        /// Initialize
        /// </summary>
        /// <param name="size"></param>
        /// <param name="sector_size"></param>
        public void Initialize(long size, int sector_size)
        {
            if (!Utility.IsPowerOfTwo(size))
            {
                throw new ArgumentException("Size {0} is not a power of 2");
            }
            if (!Utility.Is32Bit(size))
            {
                throw new ArgumentException("Size {0} is not 32-bit");
            }

            minTableSize = size;
            resizeInfo = default;
            resizeInfo.status = ResizeOperationStatus.DONE;
            resizeInfo.version = 0;
            Initialize(resizeInfo.version, size, sector_size);
        }

        /// <summary>
        /// Initialize
        /// </summary>
        /// <param name="version"></param>
        /// <param name="size"></param>
        /// <param name="sector_size"></param>
        internal void Initialize(int version, long size, int sector_size)
        {
            long size_bytes = size * sizeof(HashBucket);
            long aligned_size_bytes = sector_size +
                ((size_bytes + (sector_size - 1)) & ~(sector_size - 1));

            logger?.LogTrace("KV Initialize size:{size}, sizeBytes:{sizeBytes} sectorSize:{sectorSize} alignedSizeBytes:{alignedSizeBytes}", size, size_bytes, sector_size, aligned_size_bytes);
            //Over-allocate and align the table to the cacheline
            state[version].size = size;
            state[version].size_mask = size - 1;
            state[version].size_bits = Utility.GetLogBase2((int)size);

            state[version].tableRaw = GC.AllocateArray<HashBucket>((int)(aligned_size_bytes / Constants.kCacheLineBytes), true);
            long sectorAlignedPointer = ((long)Unsafe.AsPointer(ref state[version].tableRaw[0]) + (sector_size - 1)) & ~(sector_size - 1);
            state[version].tableAligned = (HashBucket*)sectorAlignedPointer;
        }

        /// <summary>
        /// A helper function that is used to find the slot corresponding to a
        /// key in the specified version of the hash table
        /// </summary>
        /// <returns>true if such a slot exists, and populates <paramref name="hei"/>, else returns false</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool FindTag(ref HashEntryInfo hei)
        {
            var target_entry_word = default(long);
            var entry_slot_bucket = default(HashBucket*);
            var version = resizeInfo.version;
            var masked_entry_word = hei.hash & state[version].size_mask;
            hei.firstBucket = hei.bucket = state[version].tableAligned + masked_entry_word;
            hei.slot = Constants.kInvalidEntrySlot;
            hei.entry = default;
            hei.bucketIndex = masked_entry_word;

            do
            {
                // Search through the bucket looking for our key. Last entry is reserved for the overflow pointer.
                for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                {
                    target_entry_word = *(((long*)hei.bucket) + index);
                    if (0 == target_entry_word)
                        continue;

                    hei.entry.word = target_entry_word;
                    if (hei.tag == hei.entry.Tag && !hei.entry.Tentative)
                    {
                        hei.slot = index;
                        return true;
                    }
                }

                // Go to next bucket in the chain (if it is a nonzero overflow allocation)
                target_entry_word = *(((long*)hei.bucket) + Constants.kOverflowBucketIndex) & Constants.kAddressMask;
                if (target_entry_word == 0)
                {
                    // We lock the firstBucket, so it can't be cleared.
                    hei.bucket = default;
                    hei.entry = default;
                    return false;
                }
                hei.bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(target_entry_word);
            } while (true);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void FindOrCreateTag(ref HashEntryInfo hei, long BeginAddress)
        {
            var version = resizeInfo.version;
            var masked_entry_word = hei.hash & state[version].size_mask;
            hei.bucketIndex = masked_entry_word;

            while (true)
            {
                hei.firstBucket = hei.bucket = state[version].tableAligned + masked_entry_word;
                hei.slot = Constants.kInvalidEntrySlot;

                if (FindTagOrFreeInternal(ref hei, BeginAddress))
                    return;

                // Install tentative tag in free slot
                hei.entry = default;
                hei.entry.Tag = hei.tag;
                hei.entry.Address = Constants.kTempInvalidAddress;
                hei.entry.Tentative = true;

                // Insert the tag into this slot. Failure means another session inserted a key into that slot, so continue the loop to find another free slot.
                if (0 == Interlocked.CompareExchange(ref hei.bucket->bucket_entries[hei.slot], hei.entry.word, 0))
                {
                    // Make sure this tag isn't in a different slot already; if it is, make this slot 'available' and continue the search loop.
                    var orig_bucket = state[version].tableAligned + masked_entry_word;  // TODO local var not used; use or change to byval param
                    var orig_slot = Constants.kInvalidEntrySlot;                        // TODO local var not used; use or change to byval param

                    if (FindOtherSlotForThisTagMaybeTentativeInternal(hei.tag, ref orig_bucket, ref orig_slot, hei.bucket, hei.slot))
                    {
                        // We own the slot per CAS above, so it is OK to non-CAS the 0 back in
                        hei.bucket->bucket_entries[hei.slot] = 0;
                        // TODO: Why not return orig_bucket and orig_slot if it's not Tentative?
                    }
                    else
                    {
                        hei.entry.Tentative = false;
                        *((long*)hei.bucket + hei.slot) = hei.entry.word;
                        return;
                    }
                }
            }
        }

        /// <summary>
        /// Find existing entry (non-tentative) entry.
        /// </summary>
        /// <returns>If found, return the slot it is in, else return a pointer to some empty slot (which we may have allocated)</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindTagOrFreeInternal(ref HashEntryInfo hei, long BeginAddress = 0)
        {
            var target_entry_word = default(long);
            var entry_slot_bucket = default(HashBucket*);

            do
            {
                // Search through the bucket looking for our key. Last entry is reserved for the overflow pointer.
                for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                {
                    target_entry_word = *(((long*)hei.bucket) + index);
                    if (0 == target_entry_word)
                    {
                        if (hei.slot == Constants.kInvalidEntrySlot)
                        {
                            // Record the free slot and continue to search for the key
                            hei.slot = index;
                            entry_slot_bucket = hei.bucket;
                        }
                        continue;
                    }

                    // If the entry points to an address that has been truncated, it's free; try to reclaim it by setting its word to 0.
                    hei.entry.word = target_entry_word;
                    if (hei.entry.Address < BeginAddress && hei.entry.Address != Constants.kTempInvalidAddress)
                    {
                        if (hei.entry.word == Interlocked.CompareExchange(ref hei.bucket->bucket_entries[index], Constants.kInvalidAddress, target_entry_word))
                        {
                            if (hei.slot == Constants.kInvalidEntrySlot)
                            {
                                // Record the free slot and continue to search for the key
                                hei.slot = index;
                                entry_slot_bucket = hei.bucket;
                            }
                            continue;
                        }
                    }
                    if (hei.tag == hei.entry.Tag && !hei.entry.Tentative)
                    {
                        hei.slot = index;
                        return true;
                    }
                }

                // Go to next bucket in the chain (if it is a nonzero overflow allocation). Don't mask off the non-address bits here; they're needed for CAS.
                target_entry_word = *(((long*)hei.bucket) + Constants.kOverflowBucketIndex);
                while ((target_entry_word & Constants.kAddressMask) == 0)
                {
                    // There is no next bucket. If slot is Constants.kInvalidEntrySlot then we did not find an empty slot, so must allocate a new bucket.
                    if (hei.slot == Constants.kInvalidEntrySlot)
                    {
                        // Allocate new bucket
                        var logicalBucketAddress = overflowBucketsAllocator.Allocate();
                        var physicalBucketAddress = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(logicalBucketAddress);
                        long compare_word = target_entry_word;
                        target_entry_word = logicalBucketAddress;
                        target_entry_word |= compare_word & ~Constants.kAddressMask;

                        long result_word = Interlocked.CompareExchange(
                            ref hei.bucket->bucket_entries[Constants.kOverflowBucketIndex],
                            target_entry_word,
                            compare_word);

                        if (compare_word != result_word)
                        {
                            // Install of new bucket failed; free the allocation and and continue the search using the winner's entry
                            overflowBucketsAllocator.Free(logicalBucketAddress);
                            target_entry_word = result_word;
                            continue;
                        }

                        // Install of new overflow bucket succeeded; the tag was not found, so return the first slot of the new bucket
                        hei.bucket = physicalBucketAddress;
                        hei.slot = 0;
                        hei.entry = default;
                        return false;   // tag was not found
                    }

                    // Tag was not found and an empty slot was found, so return the empty slot
                    hei.bucket = entry_slot_bucket;
                    hei.entry = default;
                    return false;       // tag was not found
                }

                // The next bucket was there or was allocated. Move to it.
                hei.bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(target_entry_word & Constants.kAddressMask);
            } while (true);
        }


        /// <summary>
        /// Look for an existing entry (tentative or otherwise) for this hash/tag, other than the specified "except for this" bucket/slot.
        /// </summary>
        /// <returns>True if found, else false. Does not return a free slot.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindOtherSlotForThisTagMaybeTentativeInternal(ushort tag, ref HashBucket* bucket, ref int slot, HashBucket* except_bucket, int except_entry_slot)
        {
            var target_entry_word = default(long);
            var entry_slot_bucket = default(HashBucket*);

            do
            {
                // Search through the bucket looking for our key. Last entry is reserved for the overflow pointer.
                for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                {
                    target_entry_word = *(((long*)bucket) + index);
                    if (0 == target_entry_word)
                        continue;

                    HashBucketEntry entry = default;
                    entry.word = target_entry_word;
                    if (tag == entry.Tag)
                    {
                        if ((except_entry_slot == index) && (except_bucket == bucket))
                            continue;
                        slot = index;
                        return true;
                    }
                }

                // Go to next bucket in the chain (if it is a nonzero overflow allocation).
                target_entry_word = *(((long*)bucket) + Constants.kOverflowBucketIndex) & Constants.kAddressMask;
                if (target_entry_word == 0)
                    return false;
                bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(target_entry_word);
            } while (true);
        }

        /// <summary>
        /// Helper function used to update the slot atomically with the
        /// new offset value using the CAS operation
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="entrySlot"></param>
        /// <param name="expected"></param>
        /// <param name="desired"></param>
        /// <param name="found"></param>
        /// <returns>If atomic update was successful</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool UpdateSlot(HashBucket* bucket, int entrySlot, long expected, long desired, out long found)
        {
            found = Interlocked.CompareExchange(ref bucket->bucket_entries[entrySlot], desired, expected);
            return found == expected;
        }
    }
}