// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static LogAddress;

    public unsafe partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
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
            var newLogRecord = WriteNewRecordInfo(inputLogRecord.Key, readcacheBase, newLogicalAddress, newPhysicalAddress, in sizeInfo, inNewVersion: false, previousAddress: stackCtx.hei.Address);

            stackCtx.SetNewRecord(newLogicalAddress | RecordInfo.kIsReadCacheBitMask);
            _ = newLogRecord.TryCopyFrom(in inputLogRecord, in sizeInfo);

            // Insert the new record by CAS'ing directly into the hash entry (readcache records are always CAS'd into the HashBucketEntry, never spliced).
            // It is possible that we will successfully CAS but subsequently fail due to a main log entry having been spliced in.
            var success = stackCtx.hei.TryCAS(newLogicalAddress | RecordInfo.kIsReadCacheBitMask);
            var casSuccess = success;

            var failStatus = OperationStatus.RETRY_NOW;     // Default to CAS-failed status, which does not require an epoch refresh

            // If lowestReadCacheLogicalAddress was previously set, see if there was a conflicting splice into the readcache/mainlog gap.
            // (If lowestReadCacheLogicalAddress wasn't set, i.e. !hei.IsReadCache, then our CAS would have failed if there had been a conflicting insert at tail.)
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
                    // If someone added a main-log entry for this key from a CTT while we were inserting the new readcache record, then the new
                    // readcache record is obsolete and must be Invalidated. (If LowestReadCacheLogicalAddress == kInvalidAddress, then the CAS would have
                    // failed in this case.) If this was the first readcache record in the chain, then once we CAS'd it in someone could have spliced into
                    // it, but then that splice will call ReadCacheCheckTailAfterSplice and invalidate it if it's the same key.
                    // Consistency Notes:
                    //  - This is only a concern for CTT; an update would take an XLock which means the ReadCache insert could not be done until that XLock was released.
                    //    a. Therefore there is no "momentary inconsistency", because the value inserted at the splice would not be changed.
                    //    b. It is not possible for another thread to update the "at tail" value to introduce inconsistency until we have released the current SLock.
                    //  - If there are two ReadCache inserts for the same key, one will fail the CAS because it will see the other's update which changed hei.entry.
                    success = EnsureNoNewMainLogRecordWasSpliced(inputLogRecord.Key, ref stackCtx, pendingContext.initialLatestLogicalAddress, ref failStatus);
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
                DisposeRecord(ref newLogRecord, DisposeReason.InitialWriterCASFailed);
                newLogRecord.InfoRef.PreviousAddress = kTempInvalidAddress;     // Necessary for ReadCacheEvict, but cannot be kInvalidAddress or we have recordInfo.IsNull
            }
            return false;
        }
    }
}