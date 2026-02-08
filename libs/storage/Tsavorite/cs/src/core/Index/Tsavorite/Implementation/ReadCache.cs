// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    // Partial file for readcache functions
    public unsafe partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal bool FindInReadCache(ref TKey key, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx, long minAddress = Constants.kInvalidAddress, bool alwaysFindLatestLA = true)
        {
            Debug.Assert(UseReadCache, "Should not call FindInReadCache if !UseReadCache");

            // minAddress, if present, comes from the pre-pendingIO entry.Address; there may have been no readcache entries then.
            minAddress = IsReadCache(minAddress) ? AbsoluteAddress(minAddress) : readCacheBase.HeadAddress;

        RestartChain:

            // 'recSrc' has already been initialized to the address in 'hei'.
            if (!stackCtx.hei.IsReadCache)
                return false;

            // This is also part of the initialization process for stackCtx.recSrc for each API/InternalXxx call.
            stackCtx.recSrc.LogicalAddress = Constants.kInvalidAddress;
            stackCtx.recSrc.PhysicalAddress = 0;

            // LatestLogicalAddress is the "leading" pointer and will end up as the highest logical address in the main log for this tag chain.
            stackCtx.recSrc.LatestLogicalAddress &= ~Constants.kReadCacheBitMask;

            while (true)
            {
                if (ReadCacheNeedToWaitForEviction(ref stackCtx))
                    goto RestartChain;

                // Increment the trailing "lowest read cache" address (for the splice point). We'll look ahead from this to examine the next record.
                stackCtx.recSrc.LowestReadCacheLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;
                stackCtx.recSrc.LowestReadCachePhysicalAddress = readcache.GetPhysicalAddress(stackCtx.recSrc.LowestReadCacheLogicalAddress);

                // Use a non-ref local, because we don't need to update.
                RecordInfo recordInfo = readcache.GetInfo(stackCtx.recSrc.LowestReadCachePhysicalAddress);

                // When traversing the readcache, we skip Invalid (Closed) records. We don't have Sealed records in the readcache because they cause
                // the operation to be retried, so we'd never get past them. Return true if we find a Valid read cache entry matching the key.
                if (!recordInfo.Invalid && stackCtx.recSrc.LatestLogicalAddress >= minAddress && !stackCtx.recSrc.HasReadCacheSrc
                    && storeFunctions.KeysEqual(ref key, ref readcache.GetKey(stackCtx.recSrc.LowestReadCachePhysicalAddress)))
                {
                    // Keep these at the current readcache location; they'll be the caller's source record.
                    stackCtx.recSrc.LogicalAddress = stackCtx.recSrc.LowestReadCacheLogicalAddress;
                    stackCtx.recSrc.PhysicalAddress = stackCtx.recSrc.LowestReadCachePhysicalAddress;
                    stackCtx.recSrc.SetHasReadCacheSrc();
                    stackCtx.recSrc.SetAllocator(readCacheBase);

                    // Read() does not need to continue past the found record; updaters need to continue to find latestLogicalAddress and lowestReadCache*Address.
                    if (!alwaysFindLatestLA)
                        return true;
                }

                // Update the leading LatestLogicalAddress to recordInfo.PreviousAddress, and if that is a main log record, break out.
                stackCtx.recSrc.LatestLogicalAddress = recordInfo.PreviousAddress & ~Constants.kReadCacheBitMask;
                if (!IsReadCache(recordInfo.PreviousAddress))
                    goto InMainLog;
            }

        InMainLog:
            if (stackCtx.recSrc.HasReadCacheSrc)
                return true;

            // We did not find the record in the readcache, so set these to the start of the main log entries, and the caller will call TracebackForKeyMatch
            Debug.Assert(ReferenceEquals(stackCtx.recSrc.AllocatorBase, hlogBase), "Expected recSrc.AllocatorBase == hlogBase");
            Debug.Assert(stackCtx.recSrc.LatestLogicalAddress > Constants.kTempInvalidAddress, "Must have a main-log address after readcache");
            stackCtx.recSrc.LogicalAddress = stackCtx.recSrc.LatestLogicalAddress;
            stackCtx.recSrc.PhysicalAddress = 0; // do *not* call hlog.GetPhysicalAddress(); LogicalAddress may be below hlog.HeadAddress. Let the caller decide when to do this.
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool ReadCacheNeedToWaitForEviction(ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
        {
            if (stackCtx.recSrc.LatestLogicalAddress < readCacheBase.HeadAddress)
            {
                SpinWaitUntilRecordIsClosed(stackCtx.recSrc.LatestLogicalAddress, readCacheBase);

                // Restore to hlog; we may have set readcache into Log and continued the loop, had to restart, and the matching readcache record was evicted.
                stackCtx.UpdateRecordSourceToCurrentHashEntry(hlogBase);
                return true;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool SpliceIntoHashChainAtReadCacheBoundary(ref TKey key, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx, long newLogicalAddress)
        {
            // Splice into the gap of the last readcache/first main log entries.
            Debug.Assert(stackCtx.recSrc.LowestReadCacheLogicalAddress >= readCacheBase.ClosedUntilAddress,
                        $"{nameof(VerifyInMemoryAddresses)} should have ensured LowestReadCacheLogicalAddress ({stackCtx.recSrc.LowestReadCacheLogicalAddress}) >= readcache.ClosedUntilAddress ({readCacheBase.ClosedUntilAddress})");

            // If the LockTable is enabled, then we either have an exclusive lock and thus cannot have a competing insert to the readcache, or we are doing a
            // Read() so we allow a momentary overlap of records because they're the same value (no update is being done).
            ref RecordInfo rcri = ref readcache.GetInfo(stackCtx.recSrc.LowestReadCachePhysicalAddress);
            return rcri.TryUpdateAddress(stackCtx.recSrc.LatestLogicalAddress, newLogicalAddress);
        }

        // Skip over all readcache records in this key's chain, advancing stackCtx.recSrc to the first non-readcache record we encounter.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SkipReadCache(ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx, out bool didRefresh)
        {
            Debug.Assert(UseReadCache, "Should not call SkipReadCache if !UseReadCache");
            didRefresh = false;

        RestartChain:
            // 'recSrc' has already been initialized to the address in 'hei'.
            if (!stackCtx.hei.IsReadCache)
                return;

            // This is FindInReadCache without the key comparison or untilAddress.
            stackCtx.recSrc.LogicalAddress = Constants.kInvalidAddress;
            stackCtx.recSrc.PhysicalAddress = 0;

            stackCtx.recSrc.LatestLogicalAddress = AbsoluteAddress(stackCtx.recSrc.LatestLogicalAddress);

            while (true)
            {
                if (ReadCacheNeedToWaitForEviction(ref stackCtx))
                {
                    didRefresh = true;
                    goto RestartChain;
                }

                // Increment the trailing "lowest read cache" address (for the splice point). We'll look ahead from this to examine the next record.
                stackCtx.recSrc.LowestReadCacheLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;
                stackCtx.recSrc.LowestReadCachePhysicalAddress = readcache.GetPhysicalAddress(stackCtx.recSrc.LowestReadCacheLogicalAddress);

                RecordInfo recordInfo = readcache.GetInfo(stackCtx.recSrc.LowestReadCachePhysicalAddress);
                if (!IsReadCache(recordInfo.PreviousAddress))
                {
                    stackCtx.recSrc.LatestLogicalAddress = recordInfo.PreviousAddress;
                    stackCtx.recSrc.LogicalAddress = stackCtx.recSrc.LatestLogicalAddress;
                    stackCtx.recSrc.PhysicalAddress = 0;
                    return;
                }
                stackCtx.recSrc.LatestLogicalAddress = AbsoluteAddress(recordInfo.PreviousAddress);
            }
        }

        // Skip over all readcache records in all key chains in this bucket, updating the bucket to point to the first main log record.
        // Called during checkpointing; we create a copy of the hash table page, eliminate read cache pointers from this copy, then write this copy to disk.
        private void SkipReadCacheBucket(HashBucket* bucket)
        {
            for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
            {
                HashBucketEntry* entry = (HashBucketEntry*)&bucket->bucket_entries[index];
                if (0 == entry->word)
                    continue;

                if (!entry->ReadCache) continue;
                var logicalAddress = entry->Address;
                var physicalAddress = readcache.GetPhysicalAddress(AbsoluteAddress(logicalAddress));

                while (true)
                {
                    logicalAddress = readcache.GetInfo(physicalAddress).PreviousAddress;
                    entry->Address = logicalAddress;
                    if (!entry->ReadCache)
                        break;
                    physicalAddress = readcache.GetPhysicalAddress(AbsoluteAddress(logicalAddress));
                }
            }
        }

        // Called after a readcache insert, to make sure there was no race with another session that added a main-log record at the same time.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool EnsureNoNewMainLogRecordWasSpliced(ref TKey key, RecordSource<TKey, TValue, TStoreFunctions, TAllocator> recSrc, long highestSearchedAddress, ref OperationStatus failStatus)
        {
            bool success = true;
            ref RecordInfo lowest_rcri = ref readcache.GetInfo(recSrc.LowestReadCachePhysicalAddress);
            Debug.Assert(!IsReadCache(lowest_rcri.PreviousAddress), "lowest-rcri.PreviousAddress should be a main-log address");
            if (lowest_rcri.PreviousAddress > highestSearchedAddress)
            {
                // Someone added a new record in the splice region. It won't be readcache; that would've been added at tail. See if it's our key.
                var minAddress = highestSearchedAddress > hlogBase.HeadAddress ? highestSearchedAddress : hlogBase.HeadAddress;
                if (TraceBackForKeyMatch(ref key, lowest_rcri.PreviousAddress, minAddress + 1, out long prevAddress, out _))
                    success = false;
                else if (prevAddress > highestSearchedAddress && prevAddress < hlogBase.HeadAddress)
                {
                    // One or more records were inserted and escaped to disk during the time of this Read/PENDING operation, untilLogicalAddress
                    // is below hlog.HeadAddress, and there are one or more inserted records between them:
                    //     hlog.HeadAddress -> [prevAddress is somewhere in here] -> untilLogicalAddress
                    // (If prevAddress is == untilLogicalAddress, we know there is nothing more recent, so the new readcache record should stay.)
                    // recSrc.HasLockTableLock may or may not be true. The new readcache record must be invalidated; then we return ON_DISK;
                    // this abandons the attempt to CopyToTail, and the caller proceeds with the possibly-stale value that was read.
                    success = false;
                    failStatus = OperationStatus.RECORD_ON_DISK;
                }
            }
            return success;
        }

        // Called to check if another session added a readcache entry from a pending read while we were inserting a record at the tail of the log.
        // If so, the new readcache record must be invalidated.
        // Note: The caller will do no epoch-refreshing operations after re-verifying the readcache chain following record allocation, so it is not
        // possible for the chain to be disrupted and the new insertion lost, even if readcache.HeadAddress is raised above hei.Address.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReadCacheCheckTailAfterSplice(ref TKey key, ref HashEntryInfo hei, long highestReadCacheAddressChecked)
        {
            Debug.Assert(UseReadCache, "Should not call ReadCacheCheckTailAfterSplice if !UseReadCache");

            // We already searched from hei.Address down; so now we search from hei.CurrentAddress down to just above hei.Address.
            HashBucketEntry entry = new() { word = hei.CurrentAddress };
            HashBucketEntry untilEntry = new() { word = highestReadCacheAddressChecked };

            // Traverse for the key above untilAddress (which may not be in the readcache if there were no readcache records when it was retrieved).
            while (entry.ReadCache && (entry.Address > untilEntry.Address || !untilEntry.ReadCache))
            {
                var physicalAddress = readcache.GetPhysicalAddress(entry.AbsoluteAddress);
                ref RecordInfo recordInfo = ref readcache.GetInfo(physicalAddress);
                if (!recordInfo.Invalid && storeFunctions.KeysEqual(ref key, ref readcache.GetKey(physicalAddress)))
                {
                    recordInfo.SetInvalidAtomic();
                    return;
                }
                entry.word = recordInfo.PreviousAddress;
            }

            // If we're here, no (valid) record for 'key' was found.
            return;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ReadCacheAbandonRecord(long physicalAddress)
        {
            // TODO: We currently don't save readcache allocations for retry, but we could
            ref var ri = ref readcache.GetInfo(physicalAddress);
            ri.SetInvalid();
            ri.PreviousAddress = Constants.kTempInvalidAddress;     // Necessary for ReadCacheEvict, but cannot be kInvalidAddress or we have recordInfo.IsNull
        }

        internal void ReadCacheEvict(long rcLogicalAddress, long rcToLogicalAddress)
        {
            // Iterate readcache entries in the range rcFrom/ToLogicalAddress, and remove them from the hash chain.
            while (rcLogicalAddress < rcToLogicalAddress)
            {
                var rcPhysicalAddress = readcache.GetPhysicalAddress(rcLogicalAddress);
                var (_, rcAllocatedSize) = readcache.GetRecordSize(rcPhysicalAddress);
                var rcRecordInfo = readcache.GetInfo(rcPhysicalAddress);

                // Check PreviousAddress for null to handle the info.IsNull() "partial record at end of page" case as well as readcache CAS failures
                // (such failed records are not in the hash chain, so we must not process them here). We do process other Invalid records here.
                if (rcRecordInfo.PreviousAddress <= Constants.kTempInvalidAddress)
                    goto NextRecord;

                // If there are any readcache entries for this key, the hash chain will always be of the form:
                //                   |----- readcache records -----|    |------ main log records ------|
                //      hashtable -> rcN -> ... -> rc3 -> rc2 -> rc1 -> mN -> ... -> m3 -> m2 -> m1 -> 0

                // This diagram shows that this readcache record's PreviousAddress (in 'entry') is always a lower-readcache or non-readcache logicalAddress,
                // and therefore this record and the entire sub-chain "to the right" should be evicted. The sequence of events is:
                //  1. Get the key from the readcache for this to-be-evicted record.
                //  2. Call FindTag on that key in the main store to get the start of the hash chain.
                //  3. Walk the hash chain's readcache entries, removing records in the "to be removed" range.
                //     Do not remove Invalid records outside this range; that leads to race conditions.
                Debug.Assert(!IsReadCache(rcRecordInfo.PreviousAddress) || AbsoluteAddress(rcRecordInfo.PreviousAddress) < rcLogicalAddress, "Invalid record ordering in readcache");

                // Find the hash index entry for the key in the store's hash table.
                ref TKey key = ref readcache.GetKey(rcPhysicalAddress);
                HashEntryInfo hei = new(storeFunctions.GetKeyHashCode64(ref key));
                if (!FindTag(ref hei))
                    goto NextRecord;

                ReadCacheEvictChain(rcToLogicalAddress, ref hei);

            NextRecord:
                if ((rcLogicalAddress & readCacheBase.PageSizeMask) + rcAllocatedSize > readCacheBase.PageSize)
                {
                    rcLogicalAddress = (1 + (rcLogicalAddress >> readCacheBase.LogPageSizeBits)) << readCacheBase.LogPageSizeBits;
                    continue;
                }
                rcLogicalAddress += rcAllocatedSize;
            }
        }

        private void ReadCacheEvictChain(long rcToLogicalAddress, ref HashEntryInfo hei)
        {
            // Traverse the chain of readcache entries for this key, looking "ahead" to .PreviousAddress to see if it is less than readcache.HeadAddress.
            // nextPhysicalAddress remains Constants.kInvalidAddress if hei.Address is < HeadAddress; othrwise, it is the lowest-address readcache record
            // remaining following this eviction, and its .PreviousAddress is updated to each lower record in turn until we hit a non-readcache record.
            long nextPhysicalAddress = Constants.kInvalidAddress;
            HashBucketEntry entry = new() { word = hei.entry.word };
            while (entry.ReadCache)
            {
                var la = entry.AbsoluteAddress;
                var pa = readcache.GetPhysicalAddress(la);
                ref RecordInfo ri = ref readcache.GetInfo(pa);

#if DEBUG
                // Due to collisions, we can compare the hash code *mask* (i.e. the hash bucket index), not the key
                var mask = state[resizeInfo.version].size_mask;
                var rc_mask = hei.hash & mask;
                var pa_mask = storeFunctions.GetKeyHashCode64(ref readcache.GetKey(pa)) & mask;
                Debug.Assert(rc_mask == pa_mask, "The keyHash mask of the hash-chain ReadCache entry does not match the one obtained from the initial readcache address");
#endif

                // If the record's address is above the eviction range, leave it there and track nextPhysicalAddress.
                if (la >= rcToLogicalAddress)
                {
                    nextPhysicalAddress = pa;
                    entry.word = ri.PreviousAddress;
                    continue;
                }

                // The record is being evicted. If we have a higher readcache record that is not being evicted, unlink 'la' by setting
                // (nextPhysicalAddress).PreviousAddress to (la).PreviousAddress.
                if (nextPhysicalAddress != Constants.kInvalidAddress)
                {
                    ref RecordInfo nextri = ref readcache.GetInfo(nextPhysicalAddress);
                    if (nextri.TryUpdateAddress(entry.Address, ri.PreviousAddress))
                        ri.PreviousAddress = Constants.kTempInvalidAddress;     // The record is no longer in the chain
                    entry.word = nextri.PreviousAddress;
                    continue;
                }

                // We are evicting the record whose address is in the hash bucket; unlink 'la' by setting the hash bucket to point to (la).PreviousAddress.
                if (hei.TryCAS(ri.PreviousAddress))
                    ri.PreviousAddress = Constants.kTempInvalidAddress;     // The record is no longer in the chain
                else
                    hei.SetToCurrent();
                entry.word = hei.entry.word;
            }
        }
    }
}