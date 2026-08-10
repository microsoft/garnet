// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    using static LogAddress;

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

        // Per-version direct-VM backing block for the hash index when the HashIndex native surface is enabled
        // ("full" mode). Kept separate from <see cref="state"/> (which is set to default on grow-completion) so
        // the pointer to free survives. A superseded block retired on a grow is munmap'd deterministically once no
        // index-checkpoint write references it (immediately when none is in flight, else when the write drains —
        // see RetireNativeIndexTable). The two LIVE tables at <see cref="Free"/> are instead handed to
        // <see cref="nativeTableRegistry"/> and freed at finalization, since a checkpoint write may reference them
        // and the device is disposed after this store.
        readonly DirectVmBlock[] tableBlocks = new DirectVmBlock[2];

        // Owns superseded/live direct-VM hash-index tables, freeing them at finalization (see tableBlocks).
        NativePageBlockRegistry nativeTableRegistry;

        // Count of in-flight index-checkpoint device writes (a producer sentinel held for the duration of issuance,
        // plus one unit per issued chunk write). A superseded table retired on a grow can be munmap'd immediately
        // only when this is zero; otherwise a (possibly canceled) index checkpoint's async write may still
        // reference it, so it is parked in deferredIndexFrees and freed when the last write completes. Zero and
        // unused for the managed backend. See BeginNativeIndexCheckpointIo / RetireNativeIndexTable.
        int nativeIndexIoOutstanding;

        // Direct-VM hash-index tables superseded by a grow while an index-checkpoint write was still outstanding;
        // freed deterministically once nativeIndexIoOutstanding drains to zero (or, if still parked at teardown,
        // handed to nativeTableRegistry for finalization). Guarded by nativeIndexFreeGate. Null for managed.
        System.Collections.Generic.List<DirectVmBlock> deferredIndexFrees;
        readonly object nativeIndexFreeGate = new();

        /// <summary>Diagnostic counter of superseded direct-VM hash-index tables freed deterministically (on grow,
        /// or when an index-checkpoint write drains); used by tests to assert the prompt-free path is exercised.</summary>
        internal static long NativeIndexTableFreeCount;

        /// <summary>Diagnostic counter of superseded direct-VM hash-index tables parked because an index-checkpoint
        /// write was in flight at grow time; used by tests to assert the deferral path is exercised.</summary>
        internal static long NativeIndexTableDeferredCount;

        // Captured once at construction: whether the main hash-index table uses the direct-VM backend. Used
        // instead of re-reading the process-global NativeAllocatorInitializer.EnabledSurfaces in InitializeMainIndex
        // (called at construction and on grow) so the index never switches backend mid-life if the global flag is
        // flipped after this store is built.
        readonly bool useNativeHashIndex = (NativeAllocatorInitializer.EnabledSurfaces & NativeAllocatorSurfaces.HashIndex) != 0;

        // Array used to denote if a specific chunk is merged or not
        internal long[] splitStatus;

        // Used as an atomic counter to check if resizing is complete
        internal long numPendingChunksToBeSplit;

        internal readonly LightEpoch epoch;
        readonly bool isEpochOwned;

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
                isEpochOwned = true;
            }
            else
                this.epoch = epoch;
            overflowBucketsAllocator = new MallocFixedPageSize<HashBucket>(logger);
        }

        internal void Free()
        {
            if (isEpochOwned)
                epoch.Dispose();
            overflowBucketsAllocator.Dispose();
            overflowBucketsAllocatorResize?.Dispose();

            // Hand the LIVE direct-VM index tables to the finalization-owned registry rather than munmap'ing here:
            // an index checkpoint's async device write may still reference a table (the device holds a raw pointer
            // and is disposed by the owner AFTER this store). No-op when managed (blocks are empty, Register skips them).
            if (!tableBlocks[0].IsEmpty || !tableBlocks[1].IsEmpty)
            {
                var registry = nativeTableRegistry ??= new NativePageBlockRegistry();
                registry.Register(tableBlocks[0]);
                registry.Register(tableBlocks[1]);
                tableBlocks[0] = default;
                tableBlocks[1] = default;
            }

            // Any superseded tables still parked awaiting an outstanding index-checkpoint write also go to the
            // registry (freed at finalization). Do NOT spin-wait on nativeIndexIoOutstanding here: the device is
            // disposed by the owner after this store, and a canceled/hung checkpoint write could otherwise wedge
            // teardown forever.
            lock (nativeIndexFreeGate)
            {
                if (deferredIndexFrees is { Count: > 0 })
                {
                    var registry = nativeTableRegistry ??= new NativePageBlockRegistry();
                    foreach (var block in deferredIndexFrees)
                        registry.Register(block);
                    deferredIndexFrees.Clear();
                }
            }
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

            if (useNativeHashIndex)
            {
                // Direct-VM (mmap/VirtualAlloc): demand-zero, first-touch-placed pages. A prior block may still
                // occupy this version slot (grow reuses the two versions alternately), and that superseded table is
                // dead once we overwrite the slot. Free it deterministically here when no index-checkpoint write is
                // outstanding; otherwise a canceled index checkpoint may have left an async device write referencing
                // it (StateMachineDriver releases the state machine on cancellation without draining the index-write
                // TCS), so park it and free it when the write drains (RetireNativeIndexTable).
                if (!tableBlocks[version].IsEmpty)
                {
                    RetireNativeIndexTable(tableBlocks[version]);
                    tableBlocks[version] = default;
                }
                var block = DirectVirtualMemory.Allocate(size_bytes, sector_size);
                tableBlocks[version] = block;
                state[version].tableRaw = null;
                state[version].tableAligned = (HashBucket*)block.AlignedPtr;
            }
            else
            {
                // Over-allocate and align the table to the cacheline
                state[version].tableRaw = GC.AllocateArray<HashBucket>((int)(aligned_size_bytes / Constants.kCacheLineBytes), true);
                var sectorAlignedPointer = ((long)Unsafe.AsPointer(ref state[version].tableRaw[0]) + (sector_size - 1)) & ~(sector_size - 1);
                state[version].tableAligned = (HashBucket*)sectorAlignedPointer;
            }

            // Successful (re-)allocation so update the state sizes.
            state[version].size = size;
            state[version].size_mask = size - 1;
            state[version].size_bits = Utility.GetLogBase2((int)size);
        }

        /// <summary>Count one unit of in-flight index-checkpoint device IO (a producer sentinel for the duration of
        /// issuance, or one issued chunk write). While non-zero, a superseded native table retired on a grow is
        /// parked rather than munmap'd, because a (possibly canceled) index-checkpoint write may still reference it.
        /// No-op for the managed backend.</summary>
        internal void BeginNativeIndexCheckpointIo()
        {
            if (useNativeHashIndex)
                _ = Interlocked.Increment(ref nativeIndexIoOutstanding);
        }

        /// <summary>Release one unit of in-flight index-checkpoint device IO (the issuance sentinel, or one chunk
        /// write's completion callback — including the error path). When the last unit is released, free any tables
        /// that were superseded by a grow while the write was outstanding. No-op for the managed backend.</summary>
        internal void EndNativeIndexCheckpointIo()
        {
            if (!useNativeHashIndex || Interlocked.Decrement(ref nativeIndexIoOutstanding) != 0)
                return;
            DirectVmBlock[] toFree = null;
            lock (nativeIndexFreeGate)
            {
                // Re-check under the lock: another checkpoint may have started (incremented) before we took it.
                if (Volatile.Read(ref nativeIndexIoOutstanding) == 0 && deferredIndexFrees is { Count: > 0 })
                {
                    toFree = deferredIndexFrees.ToArray();
                    deferredIndexFrees.Clear();
                }
            }
            if (toFree is not null)
                foreach (var block in toFree)
                {
                    _ = System.Threading.Interlocked.Increment(ref NativeIndexTableFreeCount);
                    DirectVirtualMemory.Free(block);
                }
        }

        /// <summary>Reclaim a direct-VM hash-index table superseded by a grow. Munmap it immediately when no
        /// index-checkpoint write is outstanding (the common case); otherwise park it (guarded by
        /// nativeIndexFreeGate) so a possibly-canceled index-checkpoint write still referencing it is never unmapped
        /// early — it is freed when the last write completes (<see cref="EndNativeIndexCheckpointIo"/>) or, if still
        /// parked at teardown, handed to the finalization-owned registry in <see cref="Free"/>. Cold path (grow).</summary>
        void RetireNativeIndexTable(in DirectVmBlock block)
        {
            if (block.IsEmpty)
                return;
            lock (nativeIndexFreeGate)
            {
                if (Volatile.Read(ref nativeIndexIoOutstanding) > 0)
                {
                    _ = Interlocked.Increment(ref NativeIndexTableDeferredCount);
                    (deferredIndexFrees ??= new()).Add(block);
                    return;
                }
            }
            _ = Interlocked.Increment(ref NativeIndexTableFreeCount);
            DirectVirtualMemory.Free(block);
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
                for (int index = 0; index < Constants.kOverflowBucketIndex; index++)
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
                target_entry_word = *(((long*)hei.bucket) + Constants.kOverflowBucketIndex) & kAddressBitMask;
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
                hei.entry.Address = kTempInvalidAddress;
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
                for (int index = 0; index < Constants.kOverflowBucketIndex; index++)
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
                    if (hei.entry.Address < BeginAddress && hei.entry.Address != kTempInvalidAddress)
                    {
                        if (hei.entry.word == Interlocked.CompareExchange(ref hei.bucket->bucket_entries[index], kInvalidAddress, target_entry_word))
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
                while ((target_entry_word & kAddressBitMask) == 0)
                {
                    // There is no next bucket. If slot is Constants.kInvalidEntrySlot then we did not find an empty slot, so must allocate a new bucket.
                    if (hei.slot == Constants.kInvalidEntrySlot)
                    {
                        // Allocate new bucket
                        var logicalBucketAddress = overflowBucketsAllocator.Allocate();
                        var physicalBucketAddress = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(logicalBucketAddress);
                        long compare_word = target_entry_word;
                        target_entry_word = logicalBucketAddress;
                        target_entry_word |= compare_word & ~kAddressBitMask;

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
                hei.bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(target_entry_word & kAddressBitMask);
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
                for (int index = 0; index < Constants.kOverflowBucketIndex; index++)
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
                target_entry_word = *(((long*)bucket) + Constants.kOverflowBucketIndex) & kAddressBitMask;
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