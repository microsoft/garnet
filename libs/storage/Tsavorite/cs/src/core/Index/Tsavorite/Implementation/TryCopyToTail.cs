// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<Key, Value> : TsavoriteBase
    {
        /// <summary>
        /// Copy a record from the immutable region of the log, from the disk, or from ConditionalCopyToTail to the tail of the log (or splice into the log/readcache boundary).
        /// </summary>
        /// <param name="pendingContext"></param>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="value"></param>
        /// <param name="output"></param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{Key, Value}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        /// <param name="srcRecordInfo">if <paramref name="stackCtx"/>.<see cref="RecordSource{Key, Value}.HasInMemorySrc"/>, the recordInfo to close, if transferring.</param>
        /// <param name="tsavoriteSession"></param>
        /// <param name="reason">The reason for this operation.</param>
        /// <returns>
        ///     <list type="bullet">
        ///     <item>RETRY_NOW: failed CAS, so no copy done. This routine deals entirely with new records, so will not encounter Sealed records</item>
        ///     <item>SUCCESS: copy was done</item>
        ///     </list>
        /// </returns>
        internal OperationStatus TryCopyToTail<Input, Output, Context, TsavoriteSession>(ref PendingContext<Input, Output, Context> pendingContext,
                                    ref Key key, ref Input input, ref Value value, ref Output output, ref OperationStackContext<Key, Value> stackCtx,
                                    ref RecordInfo srcRecordInfo, TsavoriteSession tsavoriteSession, WriteReason reason)
        where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            var (actualSize, allocatedSize, keySize) = hlog.GetRecordSize(ref key, ref value);
            if (!TryAllocateRecord(tsavoriteSession, ref pendingContext, ref stackCtx, actualSize, ref allocatedSize, keySize, new AllocateOptions() { Recycle = true },
                    out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status))
                return status;
            ref var newRecordInfo = ref WriteNewRecordInfo(ref key, hlog, newPhysicalAddress, inNewVersion: tsavoriteSession.Ctx.InNewVersion, tombstone: false, stackCtx.recSrc.LatestLogicalAddress);
            stackCtx.SetNewRecord(newLogicalAddress);

            UpsertInfo upsertInfo = new()
            {
                Version = tsavoriteSession.Ctx.version,
                SessionID = tsavoriteSession.Ctx.sessionID,
                Address = newLogicalAddress,
                KeyHash = stackCtx.hei.hash,
            };
            upsertInfo.SetRecordInfo(ref newRecordInfo);

            ref Value newRecordValue = ref hlog.GetAndInitializeValue(newPhysicalAddress, newPhysicalAddress + actualSize);
            (upsertInfo.UsedValueLength, upsertInfo.FullValueLength) = GetNewValueLengths(actualSize, allocatedSize, newPhysicalAddress, ref newRecordValue);

            if (!tsavoriteSession.SingleWriter(ref key, ref input, ref value, ref newRecordValue, ref output, ref upsertInfo, reason, ref newRecordInfo))
            {
                // Save allocation for revivification (not retry, because we won't retry here), or abandon it if that fails.
                if (RevivificationManager.UseFreeRecordPool && RevivificationManager.TryAdd(newLogicalAddress, newPhysicalAddress, allocatedSize, ref tsavoriteSession.Ctx.RevivificationStats))
                    stackCtx.ClearNewRecord();
                else
                    stackCtx.SetNewRecordInvalid(ref newRecordInfo);
                return (upsertInfo.Action == UpsertAction.CancelOperation) ? OperationStatus.CANCELED : OperationStatus.SUCCESS;
            }
            SetExtraValueLength(ref newRecordValue, ref srcRecordInfo, upsertInfo.UsedValueLength, upsertInfo.FullValueLength);

            // Insert the new record by CAS'ing either directly into the hash entry or splicing into the readcache/mainlog boundary.
            bool success = CASRecordIntoChain(ref key, ref stackCtx, newLogicalAddress, ref newRecordInfo);
            if (success)
            {
                newRecordInfo.UnsealAndValidate();
                PostCopyToTail(ref key, ref stackCtx, ref srcRecordInfo, pendingContext.InitialEntryAddress);

                pendingContext.recordInfo = newRecordInfo;
                pendingContext.logicalAddress = upsertInfo.Address;
                tsavoriteSession.PostSingleWriter(ref key, ref input, ref value, ref hlog.GetValue(newPhysicalAddress), ref output, ref upsertInfo, reason, ref newRecordInfo);
                stackCtx.ClearNewRecord();
                return OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.Found | StatusCode.CopiedRecord);
            }

            // CAS failed
            stackCtx.SetNewRecordInvalid(ref newRecordInfo);
            tsavoriteSession.DisposeSingleWriter(ref hlog.GetKey(newPhysicalAddress), ref input, ref value, ref hlog.GetValue(newPhysicalAddress), ref output, ref upsertInfo, reason);

            SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress, allocatedSize);
            return OperationStatus.RETRY_NOW;   // CAS failure does not require epoch refresh
        }
    }
}