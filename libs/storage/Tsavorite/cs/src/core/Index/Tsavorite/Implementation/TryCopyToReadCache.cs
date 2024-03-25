// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<Key, Value> : TsavoriteBase
    {
        /// <summary>
        /// Copy a record from the disk to the read cache.
        /// </summary>
        /// <param name="pendingContext"></param>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="recordValue"></param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{Key, Value}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        /// <param name="tsavoriteSession"></param>
        /// <returns>True if copied to readcache, else false; readcache is "best effort", and we don't fail the read process, or slow it down by retrying.
        /// </returns>
        internal bool TryCopyToReadCache<Input, Output, Context, TsavoriteSession>(TsavoriteSession tsavoriteSession, ref PendingContext<Input, Output, Context> pendingContext,
                                        ref Key key, ref Input input, ref Value recordValue, ref OperationStackContext<Key, Value> stackCtx)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            var (actualSize, allocatedSize, _) = hlog.GetRecordSize(ref key, ref recordValue);

            if (!TryAllocateRecordReadCache(ref pendingContext, ref stackCtx, allocatedSize, out long newLogicalAddress, out long newPhysicalAddress, out _))
                return false;
            ref var newRecordInfo = ref WriteNewRecordInfo(ref key, readcache, newPhysicalAddress, inNewVersion: false, tombstone: false, stackCtx.hei.Address);
            stackCtx.SetNewRecord(newLogicalAddress | Constants.kReadCacheBitMask);

            UpsertInfo upsertInfo = new()
            {
                Version = tsavoriteSession.Ctx.version,
                SessionID = tsavoriteSession.Ctx.sessionID,
                Address = Constants.kInvalidAddress,        // We do not expose readcache addresses
                KeyHash = stackCtx.hei.hash,
            };
            upsertInfo.SetRecordInfo(ref newRecordInfo);

            // Even though readcache records are immutable, we have to initialize the lengths
            ref Value newRecordValue = ref readcache.GetAndInitializeValue(newPhysicalAddress, newPhysicalAddress + actualSize);
            (upsertInfo.UsedValueLength, upsertInfo.FullValueLength) = GetNewValueLengths(actualSize, allocatedSize, newPhysicalAddress, ref newRecordValue);

            Output output = default;
            if (!tsavoriteSession.SingleWriter(ref key, ref input, ref recordValue, ref readcache.GetAndInitializeValue(newPhysicalAddress, newPhysicalAddress + actualSize),
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
                // If someone added a main-log entry for this key from an update or CTT while we were inserting the new readcache record, then the new
                // readcache record is obsolete and must be Invalidated. (If LowestReadCacheLogicalAddress == kInvalidAddress, then the CAS would have
                // failed in this case.) If this was the first readcache record in the chain, then once we CAS'd it in someone could have spliced into
                // it, but then that splice will call ReadCacheCheckTailAfterSplice and invalidate it if it's the same key.
                success = EnsureNoNewMainLogRecordWasSpliced(ref key, stackCtx.recSrc, pendingContext.InitialLatestLogicalAddress, ref failStatus);
            }

            if (success)
            {
                if (success)
                    newRecordInfo.UnsealAndValidate();
                pendingContext.recordInfo = newRecordInfo;
                pendingContext.logicalAddress = upsertInfo.Address;
                tsavoriteSession.PostSingleWriter(ref key, ref input, ref recordValue, ref readcache.GetValue(newPhysicalAddress), ref output, ref upsertInfo, WriteReason.CopyToReadCache, ref newRecordInfo);
                stackCtx.ClearNewRecord();
                return true;
            }

            // CAS failure, or another record was spliced in.
            stackCtx.SetNewRecordInvalid(ref newRecordInfo);
            if (!casSuccess)
            {
                tsavoriteSession.DisposeSingleWriter(ref readcache.GetKey(newPhysicalAddress), ref input, ref recordValue, ref readcache.GetValue(newPhysicalAddress),
                                                  ref output, ref upsertInfo, WriteReason.CopyToReadCache);
                newRecordInfo.PreviousAddress = Constants.kTempInvalidAddress;     // Necessary for ReadCacheEvict, but cannot be kInvalidAddress or we have recordInfo.IsNull
            }
            return false;
        }
    }
}