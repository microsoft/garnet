// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using static Tsavorite.core.LogAddress;

namespace Tsavorite.core
{
    public partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        private enum LatchDestination
        {
            CreateNewRecord,
            NormalProcessing,
            Retry
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static LogRecord WriteNewRecordInfo<TKey>(TKey key, AllocatorBase<TStoreFunctions, TAllocator> log, long logicalAddress, long physicalAddress,
            in RecordSizeInfo sizeInfo, bool inNewVersion, long previousAddress)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            var logRecord = log._wrapper.CreateLogRecord(logicalAddress, physicalAddress);
            logRecord.InitializeHeadersForNewRecord(inNewVersion, previousAddress);
            log._wrapper.InitializeRecord(key, logicalAddress, in sizeInfo, ref logRecord);
            Debug.Assert(logRecord.AllocatedSize == sizeInfo.AllocatedInlineRecordSize, $"Framed record length {logRecord.AllocatedSize} does not match the allocated size {sizeInfo.AllocatedInlineRecordSize}");
            return logRecord;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void OnDispose(ref LogRecord logRecord, DisposeReason disposeReason) => hlog.OnDispose(ref logRecord, disposeReason);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void OnDisposeDiskRecord(ref DiskLogRecord logRecord, DisposeReason disposeReason) => hlog.OnDisposeDiskRecord(ref logRecord, disposeReason);

        /// <summary>
        /// This is a wrapper for checking the record's version instead of just peeking at the latest record at the tail of the bucket.
        /// By calling with the address of the traced record, we can prevent a different key sharing the same bucket from deceiving 
        /// the operation to think that the version of the key has reached v+1 and thus to incorrectly update in place.
        /// </summary>
        /// <param name="logicalAddress">The logical address of the traced record for the key</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsRecordVersionNew(long logicalAddress)
        {
            HashBucketEntry entry = new() { word = logicalAddress };
            return IsEntryVersionNew(ref entry);
        }

        /// <summary>
        /// Check the version of the passed-in entry. 
        /// The semantics of this function are to check the tail of a bucket (indicated by entry), so we name it this way.
        /// </summary>
        /// <param name="entry">the last entry of a bucket</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.NoInlining)]  // Called only if in PREPARE, so don't inline for the usual case
        private bool IsEntryVersionNew(ref HashBucketEntry entry)
        {
            // A version shift can only happen in an address after the checkpoint starts, as v_new threads RCU entries to the tail.
            if (entry.Address < _hybridLogCheckpoint.info.fuzzyRegionStartAddress)
                return false;

            // Read cache entries are not in new version
            if (UseReadCache && entry.IsReadCache)
                return false;

            // If the record is in memory, check if it has the new version bit set
            if (entry.Address < hlogBase.HeadAddress)
                return false;
            return LogRecord.GetInfo(hlogBase.GetPhysicalAddress(entry.Address)).IsInNewVersion;
        }

        // Can only elide the record if it is the tail of the tag chain (i.e. is the record in the hash bucket entry) and its
        // PreviousAddress does not point to a valid record. Otherwise an earlier record for this key could be reachable again.
        // Also, it cannot be elided if it is frozen by a checkpoint or an in-flight flush.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CanElide<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, RecordInfo srcRecordInfo)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            Debug.Assert(!stackCtx.recSrc.HasReadCacheSrc, "Should not call CanElide() for readcache records");
            return stackCtx.hei.Address == stackCtx.recSrc.LogicalAddress && srcRecordInfo.PreviousAddress < hlogBase.BeginAddress
                        && !IsFrozen<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, srcRecordInfo);
        }

        // If the record is in a checkpoint range, it must not be modified. If it is in the fuzzy region, it can only be modified
        // if it is a new record. A record whose flush is committed but not yet durable is frozen for the same reason: an allocator
        // that flushes from the live page is reading the record image, so releasing its heap or rewriting its layout would persist
        // a torn or dangling record. Such a record is released at eviction instead.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsFrozen<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, RecordInfo srcRecordInfo)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            Debug.Assert(!stackCtx.recSrc.HasReadCacheSrc, "Should not call IsFrozen() for readcache records");
            if (sessionFunctions.Ctx.IsInV1
                        && (stackCtx.recSrc.LogicalAddress <= _hybridLogCheckpoint.info.fuzzyRegionStartAddress // In checkpoint range
                            || !srcRecordInfo.IsInNewVersion))                                                  // In fuzzy region and an old version
                return true;
            return hlog.IsFrozenForFlush(stackCtx.recSrc.LogicalAddress);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long GetMinRevivifiableAddress()
            => RevivificationManager.GetMinRevivifiableAddress(hlogBase.GetTailAddress(), hlogBase.ReadOnlyAddress);

        /// <summary>
        /// Whether an ongoing checkpoint still needs the <c>(v)</c> image of a CopyUpdate source, so its bytes must be
        /// cached before <c>PostCopyUpdater</c> can mutate the structures the new record shallow-copied from it.
        /// </summary>
        /// <remarks>
        /// This only ever *narrows* caching within a checkpoint; the caller still requires the destination to be in the new
        /// version before consulting it, so behaviour outside a checkpoint is unchanged.
        /// <para>
        /// A record an in-flight flush is reading is reported as needed regardless. Answering false releases the source value
        /// (<see cref="RMWInfo.ClearSourceValueObject"/>), and disposing a value whose record image is mid-flush would persist
        /// a torn or dangling record - the same hazard <c>OnDisposeSupersededSource</c> defers on the delete path.
        /// </para>
        /// <para>
        /// The fuzzy-region test mirrors <see cref="IsFrozen{TInput, TOutput, TContext, TSessionFunctionsWrapper}"/>:
        /// <see cref="RecordInfo.IsInNewVersion"/> is never cleared, so on its own it cannot distinguish a record written after
        /// *this* checkpoint's transaction start from one left over from an earlier checkpoint; the fuzzy-region start is what
        /// scopes it to the current one. The last test uses the Snapshot completion watermark: once the Snapshot has written
        /// the page holding the source, the <c>(v)</c> image is durable and caching it again preserves nothing.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CheckpointNeedsSourceImage(long srcLogicalAddress, RecordInfo srcRecordInfo)
        {
            if (hlog.IsFrozenForFlush(srcLogicalAddress))
                return true;

            if (srcLogicalAddress > _hybridLogCheckpoint.info.fuzzyRegionStartAddress && srcRecordInfo.IsInNewVersion)
                return false;

            return !hlogBase.SnapshotHasFlushedPageFor(srcLogicalAddress);
        }

        /// <summary>
        /// Dispose the resources of an in-memory source record that is being deleted. If an ongoing checkpoint or an
        /// in-flight flush has frozen it, mark it for deferred disposal instead.
        /// </summary>
        /// <remarks>
        /// This is the *deletion* path, not the general supersede path: callers are <c>InternalDelete</c>'s non-elide
        /// branch and the four <c>InternalRMW</c> expiration paths (<see cref="RMWAction.ExpireAndStop"/> and
        /// <see cref="RMWAction.ExpireAndResume"/>, both before and after the new record is allocated), whose net effect
        /// is a Delete. Two of those run before any CAS, so there need not be a new record at all. An ordinary
        /// CopyUpdate does *not* come here — it caches the source's bytes via <c>CacheSerializedObjectData</c> and
        /// defers clearing so <c>PostCopyUpdater</c> can still read the source.
        /// <para>
        /// Disposal clears the record's heap fields, which returns the value's <see cref="ObjectIdMap"/> slot to that page's
        /// free list for reuse by another record. A frozen record must keep its value until the flush that is reading it has
        /// captured it. Two flushes read a record the caller has already deleted: the snapshot flush reads object ids from
        /// its page copy but resolves them against the live map, and the object allocator's read-only flush serializes and
        /// writes the live page directly.
        /// </para>
        /// <para>
        /// Only a flush freeze is marked. A checkpoint freeze is deliberately left to eviction, because the drain's release
        /// condition would be wrong for it: the drain fires when the main log's FlushedUntilAddress passes the record, which
        /// says nothing about whether the snapshot has captured it, so disposing on that signal could free a value the
        /// snapshot flush has yet to write and persist a dangling object id.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void OnDisposeDeletedSource<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, ref LogRecord logRecord)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (IsFrozen<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, logRecord.Info))
            {
                // Mark for the flush-completion drain to dispose once the flush window closes. Setting a bit on a record that is
                // frozen for flush is safe, which is not obvious because the object allocator writes the LIVE PAGE and that of
                // course includes RecordInfo:
                //  - The flush writes RecordInfo to the device but never modifies it in memory, so it is not a competing writer.
                //    Whether the DMA captures this bit is therefore nondeterministic, and harmless: RecordInfo is a single
                //    8-byte aligned word so the device sees the old or the new value and never a tear, and
                //    RecordInfo.ClearBitsForDiskImages strips the bit on every read-from-disk path.
                //  - No CAS is needed. This runs on the operation path holding the record lock, so the set is exactly as safe
                //    as the Seal() that immediately follows it at the call sites.
                //  - The drain needs no record lock for a related reason: this call is always followed by Seal(), and
                //    SkipOnScan is IsClosedWord(word), i.e. (word & (Valid|Sealed)) != Valid, so a Sealed record is closed to
                //    operations (they take RETRY_LATER rather than touch it) and skipped by scans.
                // What is NOT safe, and is what the freeze exists to prevent, is destructive mutation: freeing heap, zeroing
                // the ObjectLogPosition the flush just stamped, or changing record layout.
                if (hlog.IsFrozenForFlush(stackCtx.recSrc.LogicalAddress))
                {
                    logRecord.InfoRef.DeferredDispose = true;
                    hlogBase.NoteDeferredDispose(stackCtx.recSrc.LogicalAddress);
                }
                return;
            }
            OnDispose(ref logRecord, DisposeReason.Deleted);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private (bool elided, bool added) TryElideAndTransferToFreeList<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, ref LogRecord logRecord)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Try to CAS out of the hashtable and if successful, add it to the free list.
            Debug.Assert(logRecord.Info.IsSealed, "Expected a Sealed record in TryElideAndTransferToFreeList");

            if (!stackCtx.hei.TryElide())
                return (false, false);

            return (true, TryTransferToFreeList<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, stackCtx.recSrc.LogicalAddress, ref logRecord));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryTransferToFreeList<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, long logicalAddress, ref LogRecord logRecord)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // The record has been CAS'd out of the hashtable or elided from the chain, so add it to the free list.
            Debug.Assert(logRecord.Info.IsClosed, "Expected a Closed record in TryTransferToFreeList");

            // If its address is not revivifiable, just leave it orphaned and invalid.
            if (logicalAddress < GetMinRevivifiableAddress())
                return false;

            // Application-level dispose (storeFunctions.OnDispose) was already called at the delete site.
            // Call hlog.OnDispose with RevivificationFreeList to clear the key for freelist reuse
            // (Deleted reason passes clearKey=false, but freelist reuse needs the key space cleared).
            OnDispose(ref logRecord, DisposeReason.RevivificationFreeList);

            return RevivificationManager.TryAdd(logicalAddress, ref logRecord, ref sessionFunctions.Ctx.RevivificationStats);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]      // Do not try to inline this, to keep TryAllocateRecord lean
        bool TryTakeFreeRecord<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, in RecordSizeInfo sizeInfo, long minRevivAddress,
                    out long logicalAddress, out long physicalAddress)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Caller checks for UseFreeRecordPool
            if (RevivificationManager.TryTake(sizeInfo.ActualInlineRecordSize, minRevivAddress, out logicalAddress, ref sessionFunctions.Ctx.RevivificationStats))
            {
                var logRecord = hlog.CreateLogRecord(logicalAddress);
                Debug.Assert(logRecord.Info.IsSealed, "TryTakeFreeRecord: recordInfo should still have the revivification Seal");

                // Preserve the Sealed bit due to checkpoint/recovery; see RecordInfo.WriteInfo.
                physicalAddress = logRecord.physicalAddress;
                return true;
            }

            // No free record available.
            logicalAddress = physicalAddress = default;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetRecordInvalid(long logicalAddress)
        {
            // This is called on exception recovery for a newly-inserted record.
            var localLog = IsReadCache(logicalAddress) ? readcacheBase : hlogBase;
            LogRecord.GetInfoRef(localLog.GetPhysicalAddress(logicalAddress)).SetInvalid();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CASRecordIntoChain(long newLogicalAddress, ref LogRecord newLogRecord, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
        {
            // Latch-free read cache: the only structural CAS is on the hash-table entry. The new record's PreviousAddress is
            // already the first main-log address (recSrc.LatestLogicalAddress), so CAS'ing it into the hash entry atomically
            // commits the record and *detaches* (drops) any read-cache prefix from the reachable chain. We never splice into
            // the read-cache/main-log boundary (no interior-record CAS). Dropped read-cache records are orphaned and reclaimed
            // by ReadCacheEvict when their page is closed. This keeps the head monotonic (it only advances to the new record,
            // never back to a superseded read-cache address), which is what makes the operation correct without a bucket latch.
            var result = stackCtx.hei.TryCAS(newLogicalAddress);
            if (result)
                newLogRecord.InfoRef.UnsealAndValidate();
            return result;
        }

        // Called after BlockAllocate or anything else that could shift HeadAddress, to return false for RETRY as needed.
        // The caller still rechecks that the BlockAllocated address is above the position it will insert at.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool VerifyInMemoryAddresses(ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
        {
            // If we have an in-memory source that fell below HeadAddress, return false and the caller will RETRY_LATER.
            // (We no longer need to guard the read-cache prefix boundary here: with the latch-free design nothing
            // consumes the boundary after FindInReadCache - the update detaches and the promotion head-inserts via the
            // hash-entry CAS, which itself fails if the prefix was concurrently evicted and the head changed.)
            // A readcache source keeps the readcache bit set in its LogicalAddress, so it must be compared to the
            // readcache HeadAddress in absolute form; AbsoluteAddress is a no-op for a main-log source.
            return !(stackCtx.recSrc.HasInMemorySrc && AbsoluteAddress(stackCtx.recSrc.LogicalAddress) < stackCtx.recSrc.AllocatorBase.HeadAddress);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindOrCreateTagAndTryEphemeralXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, out OperationStatus internalStatus)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Ephemeral must lock the bucket before traceback, to prevent revivification from yanking the record out from underneath us.
            // Manual locking already automatically locks the bucket. hei already has the key's hashcode.
            FindOrCreateTag(ref stackCtx.hei, hlogBase.BeginAddress);
            if (!TryEphemeralXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out internalStatus))
                return false;

            // Between the time we found the tag and the time we locked the bucket the record in hei.entry may have been elided, so make sure we don't have a stale address in hei.entry.
            stackCtx.hei.SetToCurrent();
            stackCtx.SetRecordSourceToHashEntry(hlogBase);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindTagAndTryEphemeralXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, out OperationStatus internalStatus)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Ephemeral must lock the bucket before traceback, to prevent revivification from yanking the record out from underneath us.
            // Manual locking already automatically locks the bucket. hei already has the key's hashcode.
            internalStatus = OperationStatus.NOTFOUND;
            if (!FindTag(ref stackCtx.hei) || !TryEphemeralXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out internalStatus))
                return false;

            // Between the time we found the tag and the time we locked the bucket the record in hei.entry may have been elided, so make sure we don't have a stale address in hei.entry.
            stackCtx.hei.SetToCurrent();
            stackCtx.SetRecordSourceToHashEntry(hlogBase);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindTagAndTryEphemeralSLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, out OperationStatus internalStatus)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Ephemeral must lock the bucket before traceback, to prevent revivification from yanking the record out from underneath us.
            // Manual locking already automatically locks the bucket. hei already has the key's hashcode.
            internalStatus = OperationStatus.NOTFOUND;
            if (!FindTag(ref stackCtx.hei) || !TryEphemeralSLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out internalStatus))
                return false;

            // Between the time we found the tag and the time we locked the bucket the record in hei.entry may have been elided, so make sure we don't have a stale address in hei.entry.
            stackCtx.hei.SetToCurrent();
            stackCtx.SetRecordSourceToHashEntry(hlogBase);
            return true;
        }


        // Note: We do not currently consider this reuse for mid-chain records (records past the HashBucket), because TracebackForKeyMatch would need
        //  to return the next-higher record whose .PreviousAddress points to this one, *and* we'd need to make sure that record was not revivified out.
        //  Also, we do not consider this in-chain reuse for records with different keys, because we don't get here if the keys don't match.
        [MethodImpl(MethodImplOptions.NoInlining)]  // Do not inline, to keep caller lean
        private void HandleRecordElision<TInput, TOutput, TContext, TSessionFunctionsWrapper>(
            TSessionFunctionsWrapper sessionFunctions, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, ref LogRecord srcLogRecord)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Record was already disposed at the delete site (OnDispose with DisposeReason.Deleted).
            // Heap fields and optionals are already cleared. This method only handles chain management.

            if (!RevivificationManager.IsEnabled)
            {
                // We are not doing revivification, so we just want to remove the record from the tag chain so we don't potentially do an IO later for key 
                // traceback. If we succeed, we need to SealAndInvalidate. It's fine if we don't succeed here; this is just tidying up the HashBucket. 
                if (stackCtx.hei.TryElide())
                    srcLogRecord.InfoRef.SealAndInvalidate();
                return;
            }

            if (RevivificationManager.UseFreeRecordPool)
            {
                // For non-FreeRecordPool revivification, we leave the record in as a normal tombstone so we can revivify it in the chain for the same key.
                // For FreeRecord Pool we must first Seal here, even if we're using the LockTable, because the Sealed state must survive this Delete() call.
                // We invalidate it also for checkpoint/recovery consistency (this removes Sealed bit so Scan would enumerate records that are not in any
                // tag chain--they would be in the freelist if the freelist survived Recovery), but we restore the Valid bit if it is returned to the chain,
                // which due to epoch protection is guaranteed to be done before the record can be written to disk and violate the "No Invalid records in
                // tag chain" invariant.
                srcLogRecord.InfoRef.SealAndInvalidate();

                // TODO: Reviv stats are added to SessionFunction's stats and not revivification manager - check why
                Debug.Assert(stackCtx.recSrc.LogicalAddress < hlogBase.ReadOnlyAddress || srcLogRecord.Info.Tombstone, $"Unexpected loss of Tombstone; Record should have been XLocked or SealInvalidated. RecordInfo: {srcLogRecord.Info.ToString()}");
                var (isElided, isAdded) = TryElideAndTransferToFreeList<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, ref srcLogRecord);

                if (!isElided)
                {
                    // Leave this in the chain as a normal Tombstone; we aren't going to add a new record so we can't leave this one sealed.
                    srcLogRecord.InfoRef.UnsealAndValidate();
                }
                else if (!isAdded && RevivificationManager.restoreDeletedRecordsIfBinIsFull)
                {
                    // The record was not added to the freelist, but was elided. See if we can put it back in as a normal Tombstone. Since we just
                    // elided it and the elision criteria is that it is the only above-BeginAddress record in the chain, and elision sets the
                    // HashBucketEntry.word to 0, it means we do not expect any records for this key's tag to exist after the elision. Therefore,
                    // we can re-insert the record iff the HashBucketEntry's address is <= kTempInvalidAddress.
                    // TODO: If the key was Overflow, it was cleared if isElided
                    stackCtx.hei = new(stackCtx.hei.hash);
                    FindOrCreateTag(ref stackCtx.hei, hlogBase.BeginAddress);

                    if (stackCtx.hei.entry.Address <= kTempInvalidAddress && stackCtx.hei.TryCAS(stackCtx.recSrc.LogicalAddress))
                        srcLogRecord.InfoRef.UnsealAndValidate();
                }
            }
        }
    }
}