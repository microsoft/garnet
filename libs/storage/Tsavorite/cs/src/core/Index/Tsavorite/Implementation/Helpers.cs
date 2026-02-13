// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    using static LogAddress;

    public unsafe partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
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
        static LogRecord WriteNewRecordInfo(ReadOnlySpan<byte> key, AllocatorBase<TStoreFunctions, TAllocator> log, long logicalAddress, long physicalAddress,
            in RecordSizeInfo sizeInfo, bool inNewVersion, long previousAddress)
        {
            var logRecord = log._wrapper.CreateLogRecord(logicalAddress, physicalAddress);
            logRecord.InfoRef.WriteInfo(inNewVersion, previousAddress);
            log._wrapper.InitializeRecord(key, logicalAddress, in sizeInfo, ref logRecord);
            return logRecord;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void DisposeRecord(ref LogRecord logRecord, DisposeReason disposeReason) => hlog.DisposeRecord(ref logRecord, disposeReason);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void DisposeRecord(ref DiskLogRecord logRecord, DisposeReason disposeReason) => hlog.DisposeRecord(ref logRecord, disposeReason);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void MarkPage<TInput, TOutput, TContext>(long logicalAddress, TsavoriteExecutionContext<TInput, TOutput, TContext> sessionCtx)
        {
            if (sessionCtx.phase == Phase.REST)
                hlog.MarkPage(logicalAddress, sessionCtx.version);
            else
                hlog.MarkPageAtomic(logicalAddress, sessionCtx.version);
        }

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
            if (entry.Address < _hybridLogCheckpoint.info.startLogicalAddress)
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
        // Also, it cannot be elided if it is frozen due to checkpointing.
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
        // if it is a new record.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsFrozen<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, RecordInfo srcRecordInfo)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            Debug.Assert(!stackCtx.recSrc.HasReadCacheSrc, "Should not call IsFrozen() for readcache records");
            return sessionFunctions.Ctx.IsInV1
                        && (stackCtx.recSrc.LogicalAddress <= _hybridLogCheckpoint.info.startLogicalAddress     // In checkpoint range
                            || !srcRecordInfo.IsInNewVersion);                                                  // In fuzzy region and an old version
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long GetMinRevivifiableAddress()
            => RevivificationManager.GetMinRevivifiableAddress(hlogBase.GetTailAddress(), hlogBase.ReadOnlyAddress);

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

            // If its address is not revivifiable, just leave it orphaned and invalid; the caller will Dispose with its own DisposeReason.
            if (logicalAddress < GetMinRevivifiableAddress())
                return false;

            // Dispose any existing key and value. We do this as soon as we have elided so objects are released for GC as early as possible.
            DisposeRecord(ref logRecord, DisposeReason.RevivificationFreeList);

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

        internal enum LatchOperation : byte
        {
            None,
            Shared,
            Exclusive
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
            var result = stackCtx.recSrc.LowestReadCachePhysicalAddress == kInvalidAddress
                ? stackCtx.hei.TryCAS(newLogicalAddress)
                : SpliceIntoHashChainAtReadCacheBoundary(newLogRecord.Key, ref stackCtx, newLogicalAddress);
            if (result)
                newLogRecord.InfoRef.UnsealAndValidate();
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PostCopyToTail<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
            where TSourceLogRecord : ISourceLogRecord
            => PostCopyToTail(in srcLogRecord, ref stackCtx, stackCtx.hei.Address);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PostCopyToTail<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, long highestReadCacheAddressChecked)
            where TSourceLogRecord : ISourceLogRecord
        {
            // Nothing required here if not using ReadCache
            if (!UseReadCache)
                return;

            // We're using the read cache, so any insertion must check that a readcache insertion wasn't done
            if (stackCtx.recSrc.HasReadCacheSrc)
            {
                // If we already have a readcache source, there will not be another inserted, so we can just invalidate the source directly.
                srcLogRecord.InfoRef.SetInvalidAtomic();
            }
            else if (stackCtx.recSrc.HasMainLogSrc)
            {
                // We did not have a readcache source, so while we spliced a new record into the readcache/mainlog gap a competing readcache record may have been inserted at the tail.
                // If so, invalidate it. highestReadCacheAddressChecked is hei.Address unless we are from ConditionalCopyToTail, which may have skipped the readcache before this.
                // See "Consistency Notes" in TryCopyToReadCache for a discussion of why there ie no "momentary inconsistency" possible here.
                ReadCacheCheckTailAfterSplice(srcLogRecord.Key, srcLogRecord.Namespace, ref stackCtx.hei, highestReadCacheAddressChecked);
            }
        }

        // Called after BlockAllocate or anything else that could shift HeadAddress, to adjust addresses or return false for RETRY as needed.
        // This refreshes the HashEntryInfo, so the caller needs to recheck to confirm the BlockAllocated address is still > hei.Address.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool VerifyInMemoryAddresses(ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
        {
            // If we have an in-memory source that fell below HeadAddress, return false and the caller will RETRY_LATER.
            if (stackCtx.recSrc.HasInMemorySrc && stackCtx.recSrc.LogicalAddress < stackCtx.recSrc.AllocatorBase.HeadAddress)
                return false;

            // If we're not using readcache or we don't have a splice point or it is still above readcache.HeadAddress, we're good.
            if (!UseReadCache || stackCtx.recSrc.LowestReadCacheLogicalAddress == kInvalidAddress || stackCtx.recSrc.LowestReadCacheLogicalAddress >= readcacheBase.HeadAddress)
                return true;

            // If the splice point went below readcache.HeadAddress, we would have to wait for the chain to be fixed up by eviction,
            // so just return RETRY_LATER and restart the operation.
            return false;
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
            if (!RevivificationManager.IsEnabled)
            {
                // We are not doing revivification, so we just want to remove the record from the tag chain so we don't potentially do an IO later for key 
                // traceback. If we succeed, we need to SealAndInvalidate. It's fine if we don't succeed here; this is just tidying up the HashBucket. 
                if (stackCtx.hei.TryElide())
                {
                    srcLogRecord.InfoRef.SealAndInvalidate();
                    DisposeRecord(ref srcLogRecord, DisposeReason.Elided);
                }
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
                    DisposeRecord(ref srcLogRecord, DisposeReason.Deleted);
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
                    DisposeRecord(ref srcLogRecord, DisposeReason.Deleted);
                }
                // Do not DisposeRecord if we fell through to here; that means TryElideAndTransferToFreeList elided and called DisposeRecord with DisposeReason.RevivificationFreeList.
            }
        }
    }
}