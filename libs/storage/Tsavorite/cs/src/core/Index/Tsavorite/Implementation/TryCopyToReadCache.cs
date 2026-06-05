// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static LogAddress;

    public partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>
        /// Copy a record from the disk to the read cache.
        /// </summary>
        /// <param name="pendingContext"></param>
        /// <param name="inputLogRecord">Input log record that was IO'd from disk</param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{TStoreFunctions, TAllocator}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        /// <param name="sessionFunctions"></param>
        /// <returns>True if copied to readcache, else false; readcache is "best effort", and we don't fail the read process, or slow it down by retrying.
        /// </returns>
        internal bool TryCopyToReadCache<TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(in TSourceLogRecord inputLogRecord, TSessionFunctionsWrapper sessionFunctions,
                                        ref PendingContext<TInput, TOutput, TContext> pendingContext, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            var sizeInfo = new RecordSizeInfo() { FieldInfo = inputLogRecord.GetRecordFieldInfo() };
            hlog.PopulateRecordSizeInfo(ref sizeInfo);

            if (!TryAllocateRecordReadCache(ref pendingContext, ref stackCtx, in sizeInfo, out var newLogicalAddress, out var newPhysicalAddress, out _ /*status*/))
                return false;
            var newLogRecord = WriteNewRecordInfo(inputLogRecord, readcacheBase, newLogicalAddress, newPhysicalAddress, in sizeInfo, inNewVersion: false, previousAddress: stackCtx.hei.Address);

            stackCtx.SetNewRecord(newLogicalAddress | RecordInfo.kIsReadCacheBitMask);
            _ = newLogRecord.TryCopyFrom(in inputLogRecord, in sizeInfo);

            // Insert the new record by CAS'ing directly into the hash entry (readcache records are always head-inserted
            // into the HashBucketEntry). It is possible that we will successfully CAS but subsequently fail because a
            // newer main-log record for this key was committed concurrently and must not be shadowed by this copy.
            var success = stackCtx.hei.TryCAS(newLogicalAddress | RecordInfo.kIsReadCacheBitMask);
            var casSuccess = success;

            var failStatus = OperationStatus.RETRY_NOW;     // Default to CAS-failed status, which does not require an epoch refresh

            // If lowestReadCacheLogicalAddress was previously set, verify no newer main-log record for this key was
            // committed below the read-cache boundary while we were inserting this copy. (If lowestReadCacheLogicalAddress
            // wasn't set, i.e. !hei.IsReadCache, then our head-insert CAS would have failed if there had been a
            // conflicting insert at the head.)
            if (success && stackCtx.recSrc.LowestReadCacheLogicalAddress != kInvalidAddress)
            {
                // We may have forced ReadCacheEvict by the TryAllocate above; in that case, stackCtx.recSrc.LowestReadCacheLogicalAddress, or even the entire
                // readcache chain, may have been evicted. Therefore, SkipReadCache here; reinitialize the recSrc to hei and the hlog (not readcache).
                if (AbsoluteAddress(stackCtx.recSrc.LowestReadCacheLogicalAddress) < readcacheBase.ClosedUntilAddress)
                {
                    stackCtx.SetRecordSourceToHashEntry(hlogBase);
                    SkipReadCache(ref stackCtx, out _); // No need to track didRefresh; we already know it was refreshed once.
                }

                if (stackCtx.hei.IsReadCache)
                {
                    // If a newer main-log record for this key was committed concurrently (e.g. from a value-changing
                    // update or a ConditionalCopyToTail), this read-cache copy is obsolete and must be Invalidated so it
                    // does not shadow the newer value. The companion check is ReadCacheCheckTailAfterSplice, run by the
                    // committing operation, which invalidates a competing read-cache copy added at the head.
                    // Consistency Notes:
                    //  - The value-changing update path publishes its new record by detaching the read-cache prefix with
                    //    a single hash-entry CAS, so a concurrent update that committed before our head-insert would have
                    //    changed hei.entry and failed our CAS; one that commits after is caught here / by escaping-to-disk
                    //    detection below (RECORD_ON_DISK).
                    //  - If there are two ReadCache inserts for the same key, one will fail the CAS because it will see the other's update which changed hei.entry.
                    success = EnsureNoNewMainLogRecordWasSpliced(inputLogRecord, ref stackCtx, pendingContext.initialLatestLogicalAddress, ref failStatus);
                }
            }

            if (success)
            {
                // We don't call PostInitialWriter here so we must do the size tracking separately.
                readcacheBase.logSizeTracker?.UpdateSize(in newLogRecord, add: true);

                newLogRecord.InfoRef.UnsealAndValidate();
                // Do not clear pendingContext.logicalAddress; we've already set it to the requested address, which is valid. We don't expose readcache
                // addresses, but here we found it in the main log address space, so retain that address.
                stackCtx.ClearNewRecord();
                return true;
            }

            // CAS failure, or another record was spliced in.
            stackCtx.SetNewRecordInvalid(ref newLogRecord.InfoRef);
            if (!casSuccess)
            {
                OnDispose(ref newLogRecord, DisposeReason.InitialWriterCASFailed);
                newLogRecord.InfoRef.PreviousAddress = kTempInvalidAddress;     // Necessary for ReadCacheEvict, but cannot be kInvalidAddress or we have recordInfo.IsNull
            }
            return false;
        }
    }
}