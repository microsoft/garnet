// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static LogAddress;

    // Partial file for readcache functions
    public unsafe partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal bool FindInReadCache(ReadOnlySpan<byte> key, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, long minAddress = kInvalidAddress, bool alwaysFindLatestLA = true)
        {
            Debug.Assert(UseReadCache, "Should not call FindInReadCache if !UseReadCache");

            // minAddress, if present, comes from the pre-pendingIO entry.Address; there may have been no readcache entries then.
            minAddress = IsReadCache(minAddress) ? AbsoluteAddress(minAddress) : readcacheBase.HeadAddress;

        RestartChain:

            // 'recSrc' has already been initialized to the address in 'hei'.
            if (!stackCtx.hei.IsReadCache)
                return false;

            // This is also part of the initialization process for stackCtx.recSrc for each API/InternalXxx call.
            stackCtx.recSrc.LogicalAddress = kInvalidAddress;
            stackCtx.recSrc.PhysicalAddress = 0;

            while (true)
            {
                if (ReadCacheNeedToWaitForEviction(ref stackCtx))
                    goto RestartChain;

                // LatestLogicalAddress is the "leading" pointer and will end up as the highest logical address in the main log for this tag chain.
                // Increment the trailing "lowest read cache" address (for the splice point). We'll look ahead from this to examine the next record.
                stackCtx.recSrc.LowestReadCacheLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;
                stackCtx.recSrc.LowestReadCachePhysicalAddress = readcacheBase.GetPhysicalAddress(stackCtx.recSrc.LowestReadCacheLogicalAddress);

                // Use a non-ref local, because we don't need to update.
                var recordInfo = LogRecord.GetInfo(stackCtx.recSrc.LowestReadCachePhysicalAddress);

                // When traversing the readcache, we skip Invalid (Closed) records. We don't have Sealed records in the readcache because they cause
                // the operation to be retried, so we'd never get past them. Return true if we find a Valid read cache entry matching the key.
                if (!recordInfo.Invalid && stackCtx.recSrc.LatestLogicalAddress >= minAddress && !stackCtx.recSrc.HasReadCacheSrc)
                {
                    var keySpan = recordInfo.KeyIsInline
                        ? LogRecord.GetInlineKey(stackCtx.recSrc.LowestReadCachePhysicalAddress)    // Most keys are inline and this is faster
                        : readcache.CreateLogRecord(stackCtx.recSrc.LowestReadCacheLogicalAddress).Key;
                    if (storeFunctions.KeysEqual(key, keySpan))
                    {
                        // Keep these at the current readcache location; they'll be the caller's source record.
                        stackCtx.recSrc.LogicalAddress = stackCtx.recSrc.LowestReadCacheLogicalAddress;
                        stackCtx.recSrc.PhysicalAddress = stackCtx.recSrc.LowestReadCachePhysicalAddress;
                        stackCtx.recSrc.SetAllocator(readcacheBase);
                        stackCtx.recSrc.SetHasReadCacheSrc();

                        // Read() does not need to continue past the found record; updaters need to continue to find latestLogicalAddress and lowestReadCache*Address.
                        if (!alwaysFindLatestLA)
                            return true;
                    }
                }

                // If a readcache record was evicted while we were processing it here, its .PreviousAddress will be kTempInvalidAddress.
                // This should not be the case otherwise; we should always find a valid main-log address after the readcache prefix chain.
                if (recordInfo.PreviousAddress <= kTempInvalidAddress)
                {
                    _ = ReadCacheNeedToWaitForEviction(ref stackCtx);
                    goto RestartChain;
                }

                // Update the leading LatestLogicalAddress to recordInfo.PreviousAddress, and if that is a main log record, break out.
                stackCtx.recSrc.LatestLogicalAddress = recordInfo.PreviousAddress;
                if (!IsReadCache(stackCtx.recSrc.LatestLogicalAddress))
                    goto InMainLog;
            }

        InMainLog:
            if (stackCtx.recSrc.HasReadCacheSrc)
                return true;

            // We did not find the record in the readcache, so set these to the start of the main log entries, and the caller will call TracebackForKeyMatch
            Debug.Assert(ReferenceEquals(stackCtx.recSrc.AllocatorBase, hlogBase), "Expected recSrc.AllocatorBase == hlogBase");
            Debug.Assert(stackCtx.recSrc.LatestLogicalAddress > kTempInvalidAddress, "Must have a main-log address after readcache");
            stackCtx.recSrc.LogicalAddress = stackCtx.recSrc.LatestLogicalAddress;
            stackCtx.recSrc.PhysicalAddress = 0; // do *not* call hlog.GetPhysicalAddress(); LogicalAddress may be below hlog.HeadAddress. Let the caller decide when to do this.
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool ReadCacheNeedToWaitForEviction(ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
        {
            Debug.Assert(stackCtx.hei.IsReadCache, "Can only call ReadCacheNeedToWaitForEviction when hei.IsReadCache is true");
            Debug.Assert(IsReadCache(stackCtx.recSrc.LatestLogicalAddress), "Can only call ReadCacheNeedToWaitForEviction when stackCtx.recSrc.LatestLogicalAddress is readcache");
            var logicalAddress = AbsoluteAddress(stackCtx.recSrc.LatestLogicalAddress);
            if (logicalAddress < readcacheBase.HeadAddress)
            {
                SpinWaitUntilRecordIsClosed(logicalAddress, readcacheBase);

                // Restore to hlog; we may have set readcache into Log and continued the loop, had to restart, and the matching readcache record was evicted.
                stackCtx.UpdateRecordSourceToCurrentHashEntry(hlogBase);
                return true;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool SpliceIntoHashChainAtReadCacheBoundary(ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, long newLogicalAddress)
        {
            // Splice into the gap of the last readcache/first main log entries.
            Debug.Assert(stackCtx.recSrc.LowestReadCacheLogicalAddress >= readcacheBase.ClosedUntilAddress,
                        $"{nameof(VerifyInMemoryAddresses)} should have ensured LowestReadCacheLogicalAddress ({stackCtx.recSrc.LowestReadCacheLogicalAddress}) >= readcache.ClosedUntilAddress ({readcacheBase.ClosedUntilAddress})");

            // If the LockTable is enabled, then we either have an exclusive lock and thus cannot have a competing insert to the readcache, or we are doing a
            // Read() so we allow a momentary overlap of records because they're the same value (no update is being done).
            ref var rcri = ref LogRecord.GetInfoRef(stackCtx.recSrc.LowestReadCachePhysicalAddress);
            return rcri.TryUpdateAddress(stackCtx.recSrc.LatestLogicalAddress, newLogicalAddress);
        }

        // Skip over all readcache records in this key's chain, advancing stackCtx.recSrc to the first non-readcache record we encounter.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SkipReadCache(ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, out bool didRefresh)
        {
            Debug.Assert(UseReadCache, "Should not call SkipReadCache if !UseReadCache");
            didRefresh = false;

        RestartChain:
            // 'recSrc' has already been initialized to the address in 'hei'.
            if (!stackCtx.hei.IsReadCache)
                return;

            // This is FindInReadCache without the key comparison or untilAddress.
            stackCtx.recSrc.LogicalAddress = kInvalidAddress;
            stackCtx.recSrc.PhysicalAddress = 0;

            while (true)
            {
                if (ReadCacheNeedToWaitForEviction(ref stackCtx))
                {
                    didRefresh = true;
                    goto RestartChain;
                }

                // Increment the trailing "lowest read cache" address (for the splice point). We'll look ahead from this to examine the next record.
                stackCtx.recSrc.LowestReadCacheLogicalAddress = AbsoluteAddress(stackCtx.recSrc.LatestLogicalAddress);
                stackCtx.recSrc.LowestReadCachePhysicalAddress = readcacheBase.GetPhysicalAddress(stackCtx.recSrc.LowestReadCacheLogicalAddress);

                var recordInfo = LogRecord.GetInfo(stackCtx.recSrc.LowestReadCachePhysicalAddress);
                if (!IsReadCache(recordInfo.PreviousAddress))
                {
                    stackCtx.recSrc.LatestLogicalAddress = recordInfo.PreviousAddress;
                    stackCtx.recSrc.LogicalAddress = stackCtx.recSrc.LatestLogicalAddress;
                    stackCtx.recSrc.PhysicalAddress = 0;
                    return;
                }
                stackCtx.recSrc.LatestLogicalAddress = recordInfo.PreviousAddress;
            }
        }

        // Skip over all readcache records in all key chains in this bucket, updating the bucket to point to the first main log record.
        // Called during checkpointing; we create a copy of the hash table page, eliminate read cache pointers from this copy, then write this copy to disk.
        private void SkipReadCacheBucket(HashBucket* bucket)
        {
            for (var index = 0; index < Constants.kOverflowBucketIndex; index++)
            {
                var entry = (HashBucketEntry*)&bucket->bucket_entries[index];
                if (0 == entry->word)
                    continue;

                if (!entry->IsReadCache) continue;
                var logicalAddress = entry->Address;
                var physicalAddress = readcacheBase.GetPhysicalAddress(logicalAddress);

                while (true)
                {
                    logicalAddress = LogRecord.GetInfo(physicalAddress).PreviousAddress;
                    entry->Address = logicalAddress;
                    if (!entry->IsReadCache)
                        break;
                    physicalAddress = readcacheBase.GetPhysicalAddress(logicalAddress);
                }
            }
        }

        // Called after a readcache insert, to make sure there was no race with another session that added a main-log record at the same time.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool EnsureNoNewMainLogRecordWasSpliced(ReadOnlySpan<byte> key, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, long highestSearchedAddress, ref OperationStatus failStatus)
        {
            Debug.Assert(!IsReadCache(highestSearchedAddress), "highestSearchedAddress should be a main-log address");
            var success = true;
            Debug.Assert(AbsoluteAddress(stackCtx.recSrc.LowestReadCacheLogicalAddress) >= readcacheBase.ClosedUntilAddress, "recSrc.LowestReadCachePhysicalAddress should be above ClosedUntilAddress");
            var lowest_rcri = LogRecord.GetInfo(stackCtx.recSrc.LowestReadCachePhysicalAddress);
            Debug.Assert(!IsReadCache(lowest_rcri.PreviousAddress), "lowest-rcri.PreviousAddress should be a main-log address");
            if (lowest_rcri.PreviousAddress > highestSearchedAddress)
            {
                // Someone added a new record in the splice region. It won't be readcache; that would've been added at tail. See if it's our key.
                var minAddress = highestSearchedAddress > hlogBase.HeadAddress ? highestSearchedAddress : hlogBase.HeadAddress;
                if (TraceBackForKeyMatch(key, lowest_rcri.PreviousAddress, minAddress + 1, out var prevAddress, out _))
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
        private void ReadCacheCheckTailAfterSplice(ReadOnlySpan<byte> key, ref HashEntryInfo hei, long highestReadCacheAddressChecked)
        {
            Debug.Assert(UseReadCache, "Should not call ReadCacheCheckTailAfterSplice if !UseReadCache");

            // We already searched from hei.Address down; so now we search from hei.CurrentAddress down to just above hei.Address.
            HashBucketEntry entry = new() { word = hei.CurrentAddress };
            HashBucketEntry untilEntry = new() { word = highestReadCacheAddressChecked };

            // Traverse for the key above untilAddress (which may not be in the readcache if there were no readcache records when it was retrieved).
            while (entry.IsReadCache && (!!untilEntry.IsReadCache || entry.Address > untilEntry.Address))
            {
                var logRecord = readcache.CreateLogRecord(entry.Address);
                ref var recordInfo = ref logRecord.InfoRef;
                if (!recordInfo.Invalid && storeFunctions.KeysEqual(key, logRecord.Key))
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
        static void ReadCacheAbandonRecord(long physicalAddress)
        {
            // TODO: We currently don't save readcache allocations for retry, but we could
            ref var ri = ref LogRecord.GetInfoRef(physicalAddress);
            ri.SetInvalid();
            ri.PreviousAddress = kTempInvalidAddress;     // Necessary for ReadCacheEvict, but cannot be kInvalidAddress or we have recordInfo.IsNull
        }

        internal void ReadCacheEvict(long rcLogicalAddress, long rcToLogicalAddress)
        {
            // Iterate readcache entries in the range rcFrom/ToLogicalAddress, and remove them from the hash chain.
            // First make sure we're not trying to process a logical address that's in a page header.
            var offsetOnPage = readcacheBase.GetOffsetOnPage(rcLogicalAddress);
            if (offsetOnPage < PageHeader.Size)
                rcLogicalAddress += PageHeader.Size - offsetOnPage;

            while (rcLogicalAddress < rcToLogicalAddress)
            {
                var logRecord = new LogRecord(readcacheBase.GetPhysicalAddress(rcLogicalAddress));
                var rcAllocatedSize = logRecord.AllocatedSize;
                var rcRecordInfo = logRecord.Info;

                // Check PreviousAddress for null to handle the info.IsNull() "partial record at end of page" case as well as readcache CAS failures
                // (such failed records are not in the hash chain, so we must not process them here). We do process other Invalid records here.
                if (rcRecordInfo.PreviousAddress <= kTempInvalidAddress)
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
                HashEntryInfo hei = new(storeFunctions.GetKeyHashCode64(logRecord.Key));
                if (!FindTag(ref hei))
                    goto NextRecord;

                ReadCacheEvictChain(rcToLogicalAddress, ref hei);

            NextRecord:
                if (readcacheBase.GetOffsetOnPage(rcLogicalAddress) + rcAllocatedSize >= readcacheBase.PageSize - RecordInfo.Size)
                {
                    rcLogicalAddress = readcacheBase.GetFirstValidLogicalAddressOnPage(1 + readcacheBase.GetPage(rcLogicalAddress));
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
            var nextPhysicalAddress = kInvalidAddress;
            HashBucketEntry entry = new() { word = hei.entry.word };
            while (entry.IsReadCache)
            {
                var logRecord = new LogRecord(readcacheBase.GetPhysicalAddress(entry.Address));
                ref var recordInfo = ref logRecord.InfoRef;

#if DEBUG
                // Due to collisions, we can compare the hash code *mask* (i.e. the hash bucket index), not the key
                var mask = state[resizeInfo.version].size_mask;
                var rc_mask = hei.hash & mask;
                var pa_mask = storeFunctions.GetKeyHashCode64(logRecord.Key) & mask;
                Debug.Assert(rc_mask == pa_mask, "The keyHash mask of the hash-chain ReadCache entry does not match the one obtained from the initial readcache address");
#endif

                // If the record's address is above the eviction range, leave it there and track nextPhysicalAddress.
                if (AbsoluteAddress(entry.Address) >= rcToLogicalAddress)
                {
                    Debug.Assert(!IsReadCache(recordInfo.PreviousAddress) || entry.Address > recordInfo.PreviousAddress, "Invalid ordering in readcache chain");

                    nextPhysicalAddress = logRecord.physicalAddress;
                    entry.word = recordInfo.PreviousAddress;
                    continue;
                }

                // The record is being evicted. If we have a higher readcache record that is not being evicted, unlink 'entry.Address' by setting
                // (nextPhysicalAddress).PreviousAddress to (entry.Address).PreviousAddress.
                if (nextPhysicalAddress != kInvalidAddress)
                {
                    ref var nextri = ref LogRecord.GetInfoRef(nextPhysicalAddress);
                    if (nextri.TryUpdateAddress(entry.Address, recordInfo.PreviousAddress))
                    {
                        recordInfo.PreviousAddress = kTempInvalidAddress;     // The record is no longer in the chain
                        entry.word = nextri.PreviousAddress;
                    }
                    else
                        Debug.Assert(entry.word == nextri.PreviousAddress, "We should be about to retry nextri.PreviousAddress");
                    continue;
                }

                // We are evicting the record whose address is in the hash bucket; unlink 'la' by setting the hash bucket to point to (la).PreviousAddress.
                if (hei.TryCAS(recordInfo.PreviousAddress))
                    recordInfo.PreviousAddress = kTempInvalidAddress;     // The record is no longer in the chain
                else
                    hei.SetToCurrent();
                entry.word = hei.entry.word;
            }
        }
    }
}