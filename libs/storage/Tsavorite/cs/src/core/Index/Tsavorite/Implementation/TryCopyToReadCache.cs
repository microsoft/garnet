// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        /// <summary>
        /// Copy a record from the disk to the read cache.
        /// </summary>
        /// <param name="pendingContext"></param>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="recordValue"></param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{Key, Value, TStoreFunctions, TAllocator}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        /// <param name="sessionFunctions"></param>
        /// <returns>True if copied to readcache, else false; readcache is "best effort", and we don't fail the read process, or slow it down by retrying.
        /// </returns>
        internal bool TryCopyToReadCache<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref PendingContext<TInput, TOutput, TContext> pendingContext,
                                        ref TKey key, ref TInput input, ref TValue recordValue, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var (actualSize, allocatedSize, _) = hlog.GetRecordSize(ref key, ref recordValue);

            if (!TryAllocateRecordReadCache(ref pendingContext, ref stackCtx, allocatedSize, out long newLogicalAddress, out long newPhysicalAddress, out _))
                return false;
            ref var newRecordInfo = ref WriteNewRecordInfo(ref key, readCacheBase, newPhysicalAddress, inNewVersion: false, stackCtx.hei.Address);
            stackCtx.SetNewRecord(newLogicalAddress | Constants.kReadCacheBitMask);

            UpsertInfo upsertInfo = new()
            {
                Version = sessionFunctions.Ctx.version,
                SessionID = sessionFunctions.Ctx.sessionID,
                Address = Constants.kInvalidAddress,        // We do not expose readcache addresses
                KeyHash = stackCtx.hei.hash,
            };
            upsertInfo.SetRecordInfo(ref newRecordInfo);

            // Even though readcache records are immutable, we have to initialize the lengths
            ref TValue newRecordValue = ref readcache.GetAndInitializeValue(newPhysicalAddress, newPhysicalAddress + actualSize);
            (upsertInfo.UsedValueLength, upsertInfo.FullValueLength) = GetNewValueLengths(actualSize, allocatedSize, newPhysicalAddress, ref newRecordValue);

            TOutput output = default;
            if (!sessionFunctions.SingleWriter(ref key, ref input, ref recordValue, ref readcache.GetAndInitializeValue(newPhysicalAddress, newPhysicalAddress + actualSize),
                                            ref output, ref upsertInfo, WriteReason.CopyToReadCache, ref newRecordInfo))
            {
                stackCtx.SetNewRecordInvalid(ref newRecordInfo);
                return false;
            }

            // Insert the new record by CAS'ing directly into the hash entry (readcache records are always CAS'd into the HashBucketEntry, never spliced).
            // It is possible that we will successfully CAS but subsequently fail due to a main log entry having been spliced in.
            var success = stackCtx.hei.TryCAS(newLogicalAddress | Constants.kReadCacheBitMask);
            var casSuccess = success;

            OperationStatus failStatus = OperationStatus.RETRY_NOW;     // Default to CAS-failed status, which does not require an epoch refresh
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
                success = EnsureNoNewMainLogRecordWasSpliced(ref key, stackCtx.recSrc, pendingContext.InitialLatestLogicalAddress, ref failStatus);
            }

            if (success)
            {
                if (success)
                    newRecordInfo.UnsealAndValidate();
                pendingContext.recordInfo = newRecordInfo;
                pendingContext.logicalAddress = upsertInfo.Address;
                sessionFunctions.PostSingleWriter(ref key, ref input, ref recordValue, ref readcache.GetValue(newPhysicalAddress), ref output, ref upsertInfo, WriteReason.CopyToReadCache, ref newRecordInfo);
                stackCtx.ClearNewRecord();
                return true;
            }

            // CAS failure, or another record was spliced in.
            stackCtx.SetNewRecordInvalid(ref newRecordInfo);
            if (!casSuccess)
            {
                storeFunctions.DisposeRecord(ref readcache.GetKey(newPhysicalAddress), ref readcache.GetValue(newPhysicalAddress), DisposeReason.SingleWriterCASFailed);
                newRecordInfo.PreviousAddress = Constants.kTempInvalidAddress;     // Necessary for ReadCacheEvict, but cannot be kInvalidAddress or we have recordInfo.IsNull
            }
            return false;
        }
    }
}