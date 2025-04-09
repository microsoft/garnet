﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
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
        internal bool TryCopyToReadCache<TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(ref TSourceLogRecord inputLogRecord, TSessionFunctionsWrapper sessionFunctions,
                                        ref PendingContext<TInput, TOutput, TContext> pendingContext, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            var sizeInfo = new RecordSizeInfo() { FieldInfo = inputLogRecord.GetRecordFieldInfo() };
            hlog.PopulateRecordSizeInfo(ref sizeInfo);

            if (!TryAllocateRecordReadCache(ref pendingContext, ref stackCtx, ref sizeInfo, out var newLogicalAddress, out var newPhysicalAddress, out var allocatedSize, out _))
                return false;
            var newLogRecord = WriteNewRecordInfo(inputLogRecord.Key, readCacheBase, newLogicalAddress, newPhysicalAddress, inNewVersion: false, previousAddress: stackCtx.hei.Address);
            stackCtx.SetNewRecord(newLogicalAddress | Constants.kReadCacheBitMask);

            // Even though readcache records are immutable, we have to initialize the lengths
            readcache.InitializeValue(newPhysicalAddress, ref sizeInfo);
            newLogRecord.SetFillerLength(allocatedSize);
            newLogRecord.TryCopyFrom(ref inputLogRecord, ref sizeInfo);

            // Insert the new record by CAS'ing directly into the hash entry (readcache records are always CAS'd into the HashBucketEntry, never spliced).
            // It is possible that we will successfully CAS but subsequently fail due to a main log entry having been spliced in.
            var success = stackCtx.hei.TryCAS(newLogicalAddress | Constants.kReadCacheBitMask);
            var casSuccess = success;

            var failStatus = OperationStatus.RETRY_NOW;     // Default to CAS-failed status, which does not require an epoch refresh
            if (success && stackCtx.recSrc.LowestReadCacheLogicalAddress != Constants.kInvalidAddress)
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
                success = EnsureNoNewMainLogRecordWasSpliced(inputLogRecord.Key, stackCtx.recSrc, pendingContext.InitialLatestLogicalAddress, ref failStatus);
            }

            if (success)
            {
                if (success)
                    newLogRecord.InfoRef.UnsealAndValidate();
                pendingContext.logicalAddress = Constants.kInvalidAddress;  // We aren't doing anything with this; and we never expose readcache addresses
                stackCtx.ClearNewRecord();
                return true;
            }

            // CAS failure, or another record was spliced in.
            stackCtx.SetNewRecordInvalid(ref newLogRecord.InfoRef);
            if (!casSuccess)
            {
                DisposeRecord(ref newLogRecord, DisposeReason.SingleWriterCASFailed);
                newLogRecord.InfoRef.PreviousAddress = Constants.kTempInvalidAddress;     // Necessary for ReadCacheEvict, but cannot be kInvalidAddress or we have recordInfo.IsNull
            }
            return false;
        }
    }
}