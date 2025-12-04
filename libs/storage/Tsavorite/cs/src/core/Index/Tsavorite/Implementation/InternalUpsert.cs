// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;

namespace Tsavorite.core
{
    using static LogAddress;

    public unsafe partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>
        /// Upsert operation. Replaces the value corresponding to 'key' with provided 'value', if one exists, else inserts a new record with 'key' and 'value'.
        /// </summary>
        /// <param name="key">key of the record.</param>
        /// <param name="keyHash"></param>
        /// <param name="input">input used to update the value.</param>
        /// <param name="srcStringValue">String value to be updated to (or inserted if key does not exist); exclusive with <paramref name="srcObjectValue"/>.</param>
        /// <param name="srcObjectValue">String value to be updated to (or inserted if key does not exist); exclusive with <paramref name="srcStringValue"/>.</param>
        /// <param name="output">output where the result of the update can be placed</param>
        /// <param name="userContext">User context for the operation, in case it goes pending.</param>
        /// <param name="pendingContext">Pending context used internally to store the context of the operation.</param>
        /// <param name="sessionFunctions">Callback functions.</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully replaced(or inserted)</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_LATER</term>
        ///     <term>Cannot  be processed immediately due to system state. Add to pending list and retry later</term>
        ///     </item>
        ///     <item>
        ///     <term>CPR_SHIFT_DETECTED</term>
        ///     <term>A shift in version has been detected. Synchronize immediately to avoid violating CPR consistency.</term>
        ///     </item>
        /// </list>
        /// </returns>
        internal OperationStatus InternalUpsert<TValueSelector, TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(ReadOnlySpan<byte> key, long keyHash, ref TInput input,
                            ReadOnlySpan<byte> srcStringValue, IHeapObject srcObjectValue, in TSourceLogRecord inputLogRecord, ref TOutput output,
                            ref TContext userContext, ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions)
            where TValueSelector : IUpsertValueSelector
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            var latchOperation = LatchOperation.None;

            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(keyHash);
            pendingContext.keyHash = keyHash;
            pendingContext.logicalAddress = kInvalidAddress;
            pendingContext.eTag = LogRecord.NoETag;

            if (sessionFunctions.Ctx.phase == Phase.IN_PROGRESS_GROW)
                SplitBuckets(stackCtx.hei.hash);

            if (!FindOrCreateTagAndTryEphemeralXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out OperationStatus status))
                return status;

            LogRecord srcLogRecord = default;

            // We must use try/finally to ensure unlocking even in the presence of exceptions.
            try
            {
                // We blindly insert if the key isn't in the mutable region, so only check down to ReadOnlyAddress (minRevivifiableAddress is always >= ReadOnlyAddress).
                if (!TryFindRecordForUpdate(key, ref stackCtx, hlogBase.ReadOnlyAddress, out status))
                    return status;

                // Note: Upsert does not track pendingContext.InitialAddress because we don't have an InternalContinuePendingUpsert

                // If there is a readcache record, use it as the CopyUpdater source.
                if (stackCtx.recSrc.HasReadCacheSrc)
                {
                    srcLogRecord = stackCtx.recSrc.CreateLogRecord();
                    goto CreateNewRecord;
                }

                // Check for CPR consistency after checking if source is readcache.
                if (sessionFunctions.Ctx.phase != Phase.REST)
                {
                    var latchDestination = CheckCPRConsistencyUpsert(sessionFunctions.Ctx.phase, ref stackCtx, ref status, ref latchOperation);
                    switch (latchDestination)
                    {
                        case LatchDestination.Retry:
                            goto LatchRelease;
                        case LatchDestination.CreateNewRecord:
                            if (stackCtx.recSrc.HasMainLogSrc)
                                srcLogRecord = stackCtx.recSrc.CreateLogRecord();
                            goto CreateNewRecord;

                        default:
                            Debug.Assert(latchDestination == LatchDestination.NormalProcessing, "Unknown latchDestination value; expected NormalProcessing");
                            break;
                    }
                }

                if (stackCtx.recSrc.LogicalAddress >= hlogBase.ReadOnlyAddress)
                {
                    srcLogRecord = stackCtx.recSrc.CreateLogRecord();

                    // Mutable Region: Update the record in-place. We perform mutable updates only if we are in normal processing phase of checkpointing
                    UpsertInfo upsertInfo = new()
                    {
                        Version = sessionFunctions.Ctx.version,
                        SessionID = sessionFunctions.Ctx.sessionID,
                        Address = stackCtx.recSrc.LogicalAddress,
                        KeyHash = stackCtx.hei.hash
                    };

                    if (srcLogRecord.Info.Tombstone)
                    {
                        // If we're doing revivification and this is in the revivifiable range, try to revivify--otherwise we'll create a new record.
                        if (RevivificationManager.IsEnabled && stackCtx.recSrc.LogicalAddress >= GetMinRevivifiableAddress())
                        {
                            if (TryRevivifyInChain<TValueSelector, TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(
                                        ref srcLogRecord, ref input, srcStringValue, srcObjectValue, in inputLogRecord, ref output, ref pendingContext, sessionFunctions, ref stackCtx, ref upsertInfo, out status)
                                    || status != OperationStatus.SUCCESS)
                                goto LatchRelease;
                        }
                        goto CreateNewRecord;
                    }

                    var sizeInfo = TValueSelector.GetUpsertRecordSize(hlog, srcLogRecord.Key, srcStringValue, srcObjectValue, in inputLogRecord, ref input, sessionFunctions);

                    // Type arg specification is needed because we don't pass TContext
                    var ok = TValueSelector.InPlaceWriter<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper>(
                            ref srcLogRecord, in sizeInfo, ref input, srcStringValue, srcObjectValue, in inputLogRecord, ref output, ref upsertInfo, sessionFunctions);
                    if (ok)
                    {
                        MarkPage(stackCtx.recSrc.LogicalAddress, sessionFunctions.Ctx);
                        pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                        pendingContext.eTag = srcLogRecord.ETag;

                        status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord);
                        goto LatchRelease;
                    }
                    if (upsertInfo.Action == UpsertAction.CancelOperation)
                    {
                        status = OperationStatus.CANCELED;
                        goto LatchRelease;
                    }

                    // InPlaceWriter failed (e.g. insufficient space, another thread set Tombstone, etc). Write a new record, but track that we have to seal and unlock this one.
                    goto CreateNewRecord;
                }
                if (stackCtx.recSrc.HasMainLogSrc)
                {
                    // Safe Read-Only Region: Create a record in the mutable region, but set srcRecordInfo in case we are eliding.
                    srcLogRecord = stackCtx.recSrc.CreateLogRecord();
                    goto CreateNewRecord;
                }

                // No record exists, or readonly or below. Drop through to create new record.
                Debug.Assert(!sessionFunctions.IsTransactionalLocking || LockTable.IsLockedExclusive(ref stackCtx.hei), "A Transactional-session Upsert() of an on-disk or non-existent key requires a LockTable lock");

            CreateNewRecord:
                status = CreateNewRecordUpsert<TValueSelector, TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(
                        key, ref srcLogRecord, ref input, srcStringValue, srcObjectValue, in inputLogRecord, ref output, ref pendingContext, sessionFunctions, ref stackCtx);
                // We should never return "SUCCESS" for a new record operation: it returns NOTFOUND on success.
                Debug.Assert(OperationStatusUtils.IsAppend(status) || OperationStatusUtils.BasicOpCode(status) != OperationStatus.SUCCESS);
                goto LatchRelease;
            }
            finally
            {
                stackCtx.HandleNewRecordOnException(this);
                EphemeralXUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx);
            }

        LatchRelease:
            if (latchOperation != LatchOperation.None)
            {
                switch (latchOperation)
                {
                    case LatchOperation.Shared:
                        HashBucket.ReleaseSharedLatch(ref stackCtx.hei);
                        break;
                    case LatchOperation.Exclusive:
                        HashBucket.ReleaseExclusiveLatch(ref stackCtx.hei);
                        break;
                    default:
                        break;
                }
            }
            return status;
        }

        private bool TryRevivifyInChain<TValueSelector, TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(ref LogRecord logRecord, ref TInput input,
                ReadOnlySpan<byte> srcStringValue, IHeapObject srcObjectValue, in TSourceLogRecord inputLogRecord, ref TOutput output, ref PendingContext<TInput, TOutput, TContext> pendingContext,
                TSessionFunctionsWrapper sessionFunctions, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, ref UpsertInfo upsertInfo, out OperationStatus status)
            where TValueSelector : IUpsertValueSelector
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            if (IsFrozen<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, logRecord.Info))
                goto NeedNewRecord;

            // This record is safe to revivify even if its PreviousAddress points to a valid record, because it is revivified for the same key.
            var ok = false;
            try
            {
                logRecord.ClearOptionals();
                logRecord.InfoRef.ClearTombstone();

                var sizeInfo = TValueSelector.GetUpsertRecordSize(hlog, logRecord.Key, srcStringValue, srcObjectValue, in inputLogRecord, ref input, sessionFunctions);

                ref RevivificationStats stats = ref sessionFunctions.Ctx.RevivificationStats;

                // Type arg specification is needed because we don't pass TContext
                ok = TValueSelector.InitialWriter<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper>(
                        ref logRecord, in sizeInfo, ref input, srcStringValue, srcObjectValue, in inputLogRecord, ref output, ref upsertInfo, sessionFunctions);
                if (ok)
                {
                    TValueSelector.PostInitialWriter<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper>(
                            ref logRecord, in sizeInfo, ref input, srcStringValue, srcObjectValue, in inputLogRecord, ref output, ref upsertInfo, sessionFunctions);

                    // Success
                    MarkPage(stackCtx.recSrc.LogicalAddress, sessionFunctions.Ctx);
                    pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                    pendingContext.eTag = logRecord.ETag;

                    // Return NOTFOUND OperationStatus to indicate that the operation was successful but a previous record was not found.
                    status = OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.InPlaceUpdatedRecord);
                    stats.inChainSuccesses++;
                    return true;
                }
                // Did not revivify; restore the tombstone and leave the deleted record there.
                stats.inChainFailures++;
            }
            finally
            {
                if (!ok)
                    logRecord.InfoRef.SetTombstone();
            }

        NeedNewRecord:
            // Successful non-revivification; move to CreateNewRecord
            status = OperationStatus.SUCCESS;
            return false;
        }

        private LatchDestination CheckCPRConsistencyUpsert(Phase phase, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, ref OperationStatus status, ref LatchOperation latchOperation)
        {
            // See explanatory comments in CheckCPRConsistencyRMW.

            switch (phase)
            {
                case Phase.PREPARE: // Thread is in V
                    if (!IsEntryVersionNew(ref stackCtx.hei.entry))
                        break; // Normal Processing; thread is in V, record is in V

                    status = OperationStatus.CPR_SHIFT_DETECTED;
                    return LatchDestination.Retry;  // Pivot Thread for retry (do not operate on V+1 record when thread is in V)

                case Phase.IN_PROGRESS: // Thread is in V+1
                case Phase.WAIT_INDEX_CHECKPOINT:
                case Phase.WAIT_FLUSH:
                    if (IsRecordVersionNew(stackCtx.recSrc.LogicalAddress))
                        break;      // Normal Processing; V+1 thread encountered a record in V+1
                    return LatchDestination.CreateNewRecord;    // Upsert never goes pending; always force creation of a (V+1) record

                default: // Thread is in V
                    break;
            }
            return LatchDestination.NormalProcessing;
        }

        /// <summary>
        /// Create a new record for Upsert from a source Key, Value, and Input
        /// </summary>
        /// <param name="key">The record Key</param>
        /// <param name="srcLogRecord">The source record, if <paramref name="stackCtx"/>.<see cref="RecordSource{TStoreFunctions, TAllocator}.HasInMemorySrc"/> and
        /// it is either too small or is in readonly region, or is in readcache</param>
        /// <param name="input">Input to the operation</param>
        /// <param name="srcStringValue">String value to be set to; exclusive with <paramref name="srcObjectValue"/> and <paramref name="inputLogRecord"/>.</param>
        /// <param name="srcObjectValue">String value to be set to; exclusive with <paramref name="srcStringValue"/> and <paramref name="inputLogRecord"/>.</param>
        /// <param name="inputLogRecord">Log record to be copied from; exclusive with <paramref name="srcObjectValue"/> and <paramref name="srcStringValue"/>.</param>
        /// <param name="output">The result of ISessionFunctions operation</param>
        /// <param name="pendingContext">Information about the operation context</param>
        /// <param name="sessionFunctions">The current session</param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{TStoreFunctions, TAllocator}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        private OperationStatus CreateNewRecordUpsert<TValueSelector, TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(ReadOnlySpan<byte> key, ref LogRecord srcLogRecord, ref TInput input,
                ReadOnlySpan<byte> srcStringValue, IHeapObject srcObjectValue, in TSourceLogRecord inputLogRecord, ref TOutput output, ref PendingContext<TInput, TOutput, TContext> pendingContext,
                TSessionFunctionsWrapper sessionFunctions, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
            where TValueSelector : IUpsertValueSelector
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            var sizeInfo = TValueSelector.GetUpsertRecordSize(hlog, key, srcStringValue, srcObjectValue, in inputLogRecord, ref input, sessionFunctions);
            AllocateOptions allocOptions = new()
            {
                recycle = true,
                elideSourceRecord = stackCtx.recSrc.HasMainLogSrc && CanElide<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, srcLogRecord.Info)
            };

            if (!TryAllocateRecord(sessionFunctions, ref pendingContext, ref stackCtx, ref sizeInfo, allocOptions, out var newLogicalAddress, out var newPhysicalAddress, out var status))
                return status;

            var newLogRecord = WriteNewRecordInfo(key, hlogBase, newLogicalAddress, newPhysicalAddress, in sizeInfo, sessionFunctions.Ctx.InNewVersion, previousAddress: stackCtx.recSrc.LatestLogicalAddress);
            if (allocOptions.elideSourceRecord)
                newLogRecord.InfoRef.PreviousAddress = srcLogRecord.Info.PreviousAddress;
            stackCtx.SetNewRecord(newLogicalAddress);

            UpsertInfo upsertInfo = new()
            {
                Version = sessionFunctions.Ctx.version,
                SessionID = sessionFunctions.Ctx.sessionID,
                Address = newLogicalAddress,
                KeyHash = stackCtx.hei.hash,
            };

            // Type arg specification is needed because we don't pass TContext
            var success = TValueSelector.InitialWriter<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper>(
                    ref newLogRecord, in sizeInfo, ref input, srcStringValue, srcObjectValue, in inputLogRecord, ref output, ref upsertInfo, sessionFunctions);
            if (!success)
            {
                // Save allocation for revivification (not retry, because these aren't retry status codes), or abandon it if that fails.
                stackCtx.SetNewRecordInvalid(ref newLogRecord.InfoRef);
                if (!RevivificationManager.UseFreeRecordPool || !TryTransferToFreeList<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, newLogicalAddress, ref newLogRecord))
                    DisposeRecord(ref newLogRecord, DisposeReason.InsertAbandoned);

                if (upsertInfo.Action == UpsertAction.CancelOperation)
                    return OperationStatus.CANCELED;
                return OperationStatus.NOTFOUND;    // But not CreatedRecord
            }

            // Insert the new record by CAS'ing either directly into the hash entry or splicing into the readcache/mainlog boundary.
            // If the current record can be elided then we can freelist it; detach it by swapping its .PreviousAddress into newRecordInfo.
            success = CASRecordIntoChain(newLogicalAddress, ref newLogRecord, ref stackCtx);
            if (success)
            {
                PostCopyToTail(in srcLogRecord, ref stackCtx);

                // Type arg specification is needed because we don't pass TContext
                TValueSelector.PostInitialWriter<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper>(
                        ref newLogRecord, in sizeInfo, ref input, srcStringValue, srcObjectValue, in inputLogRecord, ref output, ref upsertInfo, sessionFunctions);

                // ElideSourceRecord means we have verified that the old source record is elidable and now that CAS has replaced it in the HashBucketEntry with
                // the new source record that does not point to the old source record, we have elided it, so try to transfer to freelist.
                if (allocOptions.elideSourceRecord)
                {
                    // Success should always Seal the old record. This may be readcache, readonly, or the temporary recordInfo, which is OK and saves the cost of an "if".
                    srcLogRecord.InfoRef.SealAndInvalidate();    // The record was elided, so Invalidate

                    if (stackCtx.recSrc.LogicalAddress >= GetMinRevivifiableAddress())
                        _ = TryTransferToFreeList<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, stackCtx.recSrc.LogicalAddress, ref srcLogRecord);
                    else
                        DisposeRecord(ref srcLogRecord, DisposeReason.Elided);
                }
                else if (stackCtx.recSrc.HasMainLogSrc)
                    srcLogRecord.InfoRef.Seal();              // The record was not elided, so do not Invalidate

                stackCtx.ClearNewRecord();
                pendingContext.logicalAddress = newLogicalAddress;
                pendingContext.eTag = newLogRecord.ETag;

                return OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.CreatedRecord);
            }

            // CAS failed
            stackCtx.SetNewRecordInvalid(ref newLogRecord.InfoRef);
            DisposeRecord(ref newLogRecord, DisposeReason.InitialWriterCASFailed);

            SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress);
            return OperationStatus.RETRY_NOW;   // CAS failure does not require epoch refresh
        }
    }
}