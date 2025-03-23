// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>
        /// Copy a record from the immutable region of the log, from the disk, or from ConditionalCopyToTail to the tail of the log (or splice into the log/readcache boundary).
        /// </summary>
        /// <param name="pendingContext"></param>
        /// <param name="srcLogRecord"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{TStoreFunctions, TAllocator}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        /// <param name="srcRecordInfo">if <paramref name="stackCtx"/>.<see cref="RecordSource{TStoreFunctions, TAllocator}.HasInMemorySrc"/>, the recordInfo to close, if transferring.</param>
        /// <param name="sessionFunctions"></param>
        /// <param name="reason">The reason for this operation.</param>
        /// <returns>
        ///     <list type="bullet">
        ///     <item>RETRY_NOW: failed CAS, so no copy done. This routine deals entirely with new records, so will not encounter Sealed records</item>
        ///     <item>SUCCESS: copy was done</item>
        ///     </list>
        /// </returns>
        internal OperationStatus TryCopyToTail<TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(ref PendingContext<TInput, TOutput, TContext> pendingContext,
                                    ref TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx,
                                    ref RecordInfo srcRecordInfo, TSessionFunctionsWrapper sessionFunctions, WriteReason reason)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            var sizeInfo = new RecordSizeInfo() { FieldInfo = srcLogRecord.GetRecordFieldInfo() };
            hlog.PopulateRecordSizeInfo(ref sizeInfo);

            var allocOptions = new AllocateOptions() { recycle = true };
            if (!TryAllocateRecord(sessionFunctions, ref pendingContext, ref stackCtx, ref sizeInfo, allocOptions, out var newLogicalAddress, out var newPhysicalAddress, out var allocatedSize, out var status))
                return status;
            var newLogRecord = WriteNewRecordInfo(srcLogRecord.Key, hlogBase, newLogicalAddress, newPhysicalAddress, inNewVersion: sessionFunctions.Ctx.InNewVersion, previousAddress: stackCtx.recSrc.LatestLogicalAddress);
            stackCtx.SetNewRecord(newLogicalAddress);

            UpsertInfo upsertInfo = new()
            {
                Version = sessionFunctions.Ctx.version,
                SessionID = sessionFunctions.Ctx.sessionID,
                Address = newLogicalAddress,
                KeyHash = stackCtx.hei.hash,
            };

            hlog.InitializeValue(newPhysicalAddress, ref sizeInfo);
            newLogRecord.SetFillerLength(allocatedSize);

            if (!sessionFunctions.SingleCopyWriter(ref srcLogRecord, ref newLogRecord, ref sizeInfo, ref input, ref output, ref upsertInfo, reason))
            {
                // Save allocation for revivification (not retry, because we won't retry here), or abandon it if that fails.
                if (RevivificationManager.UseFreeRecordPool && RevivificationManager.TryAdd(newLogicalAddress, ref newLogRecord, ref sessionFunctions.Ctx.RevivificationStats))
                    stackCtx.ClearNewRecord();
                else
                    stackCtx.SetNewRecordInvalid(ref newLogRecord.InfoRef);
                return (upsertInfo.Action == UpsertAction.CancelOperation) ? OperationStatus.CANCELED : OperationStatus.SUCCESS;
            }

            // Insert the new record by CAS'ing either directly into the hash entry or splicing into the readcache/mainlog boundary.
            var success = CASRecordIntoChain(newLogicalAddress, ref newLogRecord, ref stackCtx);
            if (success)
            {
                newLogRecord.InfoRef.UnsealAndValidate();
                PostCopyToTail(ref srcLogRecord, ref stackCtx, pendingContext.InitialEntryAddress);

                pendingContext.logicalAddress = upsertInfo.Address;
                sessionFunctions.PostSingleWriter(ref newLogRecord, ref sizeInfo, ref input, srcLogRecord.GetReadOnlyValue(), ref output, ref upsertInfo, reason);
                stackCtx.ClearNewRecord();
                return OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.Found | StatusCode.CopiedRecord);
            }

            // CAS failed
            stackCtx.SetNewRecordInvalid(ref newLogRecord.InfoRef);
            DisposeRecord(ref newLogRecord, DisposeReason.SingleWriterCASFailed);

            SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress);
            return OperationStatus.RETRY_NOW;   // CAS failure does not require epoch refresh
        }
    }
}