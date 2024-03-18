// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<Key, Value> : TsavoriteBase
    {
        /// <summary>
        /// Upsert operation. Replaces the value corresponding to 'key' with provided 'value', if one exists 
        /// else inserts a new record with 'key' and 'value'.
        /// </summary>
        /// <param name="key">key of the record.</param>
        /// <param name="keyHash"></param>
        /// <param name="input">input used to update the value.</param>
        /// <param name="value">value to be updated to (or inserted if key does not exist).</param>
        /// <param name="output">output where the result of the update can be placed</param>
        /// <param name="userContext">User context for the operation, in case it goes pending.</param>
        /// <param name="pendingContext">Pending context used internally to store the context of the operation.</param>
        /// <param name="tsavoriteSession">Callback functions.</param>
        /// <param name="lsn">Operation serial number</param>
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalUpsert<Input, Output, Context, TsavoriteSession>(ref Key key, long keyHash, ref Input input, ref Value value, ref Output output,
                            ref Context userContext, ref PendingContext<Input, Output, Context> pendingContext, TsavoriteSession tsavoriteSession, long lsn)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            var latchOperation = LatchOperation.None;

            OperationStackContext<Key, Value> stackCtx = new(keyHash);
            pendingContext.keyHash = keyHash;

            if (tsavoriteSession.Ctx.phase == Phase.IN_PROGRESS_GROW)
                SplitBuckets(stackCtx.hei.hash);

            if (!FindOrCreateTagAndTryTransientXLock<Input, Output, Context, TsavoriteSession>(tsavoriteSession, ref key, ref stackCtx, out OperationStatus status))
                return status;

            RecordInfo dummyRecordInfo = RecordInfo.InitialValid;
            ref RecordInfo srcRecordInfo = ref dummyRecordInfo;

            // We must use try/finally to ensure unlocking even in the presence of exceptions.
            try
            {
                // We blindly insert if the key isn't in the mutable region, so only check down to ReadOnlyAddress (minRevivifiableAddress is always >= ReadOnlyAddress).
                if (!TryFindRecordForUpdate(ref key, ref stackCtx, hlog.ReadOnlyAddress, out status, wantLock: DoRecordIsolation))
                    return status;

                // Need the extra 'if' to set this here as there are several places it would have to be set otherwise if it has a RecordIsolation lock.
                if (stackCtx.recSrc.HasInMemorySrc)
                    srcRecordInfo = ref stackCtx.recSrc.GetInfo();

                // Note: Upsert does not track pendingContext.InitialAddress because we don't have an InternalContinuePendingUpsert

                // If there is a readcache record, use it as the CopyUpdater source.
                if (stackCtx.recSrc.HasReadCacheSrc)
                    goto CreateNewRecord;

                // Check for CPR consistency after checking if source is readcache.
                if (tsavoriteSession.Ctx.phase != Phase.REST)
                {
                    var latchDestination = CheckCPRConsistencyUpsert(tsavoriteSession.Ctx.phase, ref stackCtx, ref status, ref latchOperation);
                    switch (latchDestination)
                    {
                        case LatchDestination.Retry:
                            goto LatchRelease;
                        case LatchDestination.CreateNewRecord:
                            goto CreateNewRecord;
                        case LatchDestination.CreatePendingContext:
                            CreatePendingUpsertContext(ref key, ref input, ref value, output, userContext, ref pendingContext, tsavoriteSession, lsn, ref stackCtx);
                            goto LatchRelease;
                        default:
                            Debug.Assert(latchDestination == LatchDestination.NormalProcessing, "Unknown latchDestination value; expected NormalProcessing");
                            break;
                    }
                }

                if (stackCtx.recSrc.LogicalAddress >= hlog.ReadOnlyAddress)
                {
                    // Mutable Region: Update the record in-place. We perform mutable updates only if we are in normal processing phase of checkpointing
                    UpsertInfo upsertInfo = new()
                    {
                        Version = tsavoriteSession.Ctx.version,
                        SessionID = tsavoriteSession.Ctx.sessionID,
                        Address = stackCtx.recSrc.LogicalAddress,
                        KeyHash = stackCtx.hei.hash
                    };

                    upsertInfo.SetRecordInfo(ref srcRecordInfo);
                    ref Value recordValue = ref stackCtx.recSrc.GetValue();

                    if (srcRecordInfo.Tombstone)
                    {
                        // If we're doing revivification and this is in the revivifiable range, try to revivify--otherwise we'll create a new record.
                        if (RevivificationManager.IsEnabled && stackCtx.recSrc.LogicalAddress >= GetMinRevivifiableAddress())
                        {
                            if (TryRevivifyInChain(ref key, ref input, ref value, ref output, ref pendingContext, tsavoriteSession, ref stackCtx, ref srcRecordInfo, ref upsertInfo, out status, ref recordValue)
                                    || status != OperationStatus.SUCCESS)
                                goto LatchRelease;
                        }
                        goto CreateNewRecord;
                    }

                    // upsertInfo's lengths are filled in and GetValueLengths and SetLength are called inside ConcurrentWriter, in RecordIsolation if needed.
                    if (tsavoriteSession.ConcurrentWriter(stackCtx.recSrc.PhysicalAddress, ref key, ref input, ref value, ref recordValue, ref output, ref upsertInfo, ref srcRecordInfo))
                    {
                        MarkPage(stackCtx.recSrc.LogicalAddress, tsavoriteSession.Ctx);
                        pendingContext.recordInfo = srcRecordInfo;
                        pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                        status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord);
                        goto LatchRelease;
                    }
                    if (upsertInfo.Action == UpsertAction.CancelOperation)
                    {
                        status = OperationStatus.CANCELED;
                        goto LatchRelease;
                    }

                    // ConcurrentWriter failed (e.g. insufficient space, another thread set Tombstone, etc). Write a new record, but track that we have to seal and unlock this one.
                    goto CreateNewRecord;
                }

                // No record exists, or readonly or below. Drop through to create new record. RecordIsolation does not lock in ReadOnly as we cannot modify the record in-place.
                Debug.Assert(!tsavoriteSession.IsManualLocking || LockTable.IsLockedExclusive(ref key, ref stackCtx.hei), "A Lockable-session Upsert() of an on-disk or non-existent key requires a LockTable lock");

            CreateNewRecord:
                status = CreateNewRecordUpsert(ref key, ref input, ref value, ref output, ref pendingContext, tsavoriteSession, ref stackCtx, ref srcRecordInfo);
                if (!OperationStatusUtils.IsAppend(status))
                {
                    // We should never return "SUCCESS" for a new record operation: it returns NOTFOUND on success.
                    Debug.Assert(OperationStatusUtils.BasicOpCode(status) != OperationStatus.SUCCESS);
                    if (status == OperationStatus.ALLOCATE_FAILED && pendingContext.IsAsync)
                        CreatePendingUpsertContext(ref key, ref input, ref value, output, userContext, ref pendingContext, tsavoriteSession, lsn, ref stackCtx);
                }
                goto LatchRelease;
            }
            finally
            {
                stackCtx.HandleNewRecordOnException(this);
                if (!TransientXUnlock<Input, Output, Context, TsavoriteSession>(tsavoriteSession, ref key, ref stackCtx))
                    stackCtx.recSrc.UnlockExclusive(ref srcRecordInfo, hlog.HeadAddress);
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

        // No AggressiveInlining; this is a less-common function and it may imnprove inlining of InternalUpsert to have this be a virtcall.
        private void CreatePendingUpsertContext<Input, Output, Context, TsavoriteSession>(ref Key key, ref Input input, ref Value value, Output output, Context userContext,
                ref PendingContext<Input, Output, Context> pendingContext, TsavoriteSession tsavoriteSession, long lsn, ref OperationStackContext<Key, Value> stackCtx)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            pendingContext.type = OperationType.UPSERT;
            if (pendingContext.key == default)
                pendingContext.key = hlog.GetKeyContainer(ref key);
            if (pendingContext.input == default)
                pendingContext.input = tsavoriteSession.GetHeapContainer(ref input);
            if (pendingContext.value == default)
                pendingContext.value = hlog.GetValueContainer(ref value);

            pendingContext.output = output;
            if (pendingContext.output is IHeapConvertible heapConvertible)
                heapConvertible.ConvertToHeap();

            pendingContext.userContext = userContext;
            pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
            pendingContext.version = tsavoriteSession.Ctx.version;
            pendingContext.serialNum = lsn;
        }

        private bool TryRevivifyInChain<Input, Output, Context, TsavoriteSession>(ref Key key, ref Input input, ref Value value, ref Output output, ref PendingContext<Input, Output, Context> pendingContext,
                TsavoriteSession tsavoriteSession, ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo, ref UpsertInfo upsertInfo, out OperationStatus status, ref Value recordValue)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            if (IsFrozen<Input, Output, Context, TsavoriteSession>(tsavoriteSession, ref stackCtx, ref srcRecordInfo))
                goto NeedNewRecord;

            // This record is safe to revivify even if its PreviousAddress points to a valid record, because it is revivified for the same key.
            bool ok = true;
            try
            {
                if (srcRecordInfo.Tombstone)
                {
                    srcRecordInfo.Tombstone = false;

                    if (RevivificationManager.IsFixedLength)
                        upsertInfo.UsedValueLength = upsertInfo.FullValueLength = RevivificationManager.FixedValueLength;
                    else
                    {
                        var recordLengths = GetRecordLengths(stackCtx.recSrc.PhysicalAddress, ref recordValue, ref srcRecordInfo);
                        upsertInfo.FullValueLength = recordLengths.fullValueLength;

                        // Input is not included in record-length calculations for Upsert
                        var (requiredSize, _, _) = hlog.GetRecordSize(ref key, ref value);
                        (ok, upsertInfo.UsedValueLength) = TryReinitializeTombstonedValue<Input, Output, Context, TsavoriteSession>(tsavoriteSession,
                                ref srcRecordInfo, ref key, ref recordValue, requiredSize, recordLengths);
                    }

                    if (ok && tsavoriteSession.SingleWriter(ref key, ref input, ref value, ref recordValue, ref output, ref upsertInfo, WriteReason.Upsert, ref srcRecordInfo))
                    {
                        // Success
                        MarkPage(stackCtx.recSrc.LogicalAddress, tsavoriteSession.Ctx);
                        pendingContext.recordInfo = srcRecordInfo;
                        pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                        status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord);
                        return true;
                    }

                    // Did not revivify; restore the tombstone and leave the deleted record there.
                    srcRecordInfo.Tombstone = true;
                }
            }
            finally
            {
                if (ok)
                    SetExtraValueLength(ref recordValue, ref srcRecordInfo, upsertInfo.UsedValueLength, upsertInfo.FullValueLength);
                else
                    SetTombstoneAndExtraValueLength(ref recordValue, ref srcRecordInfo, upsertInfo.UsedValueLength, upsertInfo.FullValueLength);    // Restore tombstone and ensure default value on inability to update in place
            }

        NeedNewRecord:
            // Successful non-revivification; move to CreateNewRecord
            status = OperationStatus.SUCCESS;
            return false;
        }

        private LatchDestination CheckCPRConsistencyUpsert(Phase phase, ref OperationStackContext<Key, Value> stackCtx, ref OperationStatus status, ref LatchOperation latchOperation)
        {
            if (!DoTransientLocking)
                return AcquireCPRLatchUpsert(phase, ref stackCtx, ref status, ref latchOperation);

            // This is AcquireCPRLatchUpsert without the bucket latching, since we already have a latch on either the bucket or the recordInfo.
            // See additional comments in AcquireCPRLatchRMW.

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

                default:
                    break;
            }
            return LatchDestination.NormalProcessing;
        }

        private LatchDestination AcquireCPRLatchUpsert(Phase phase, ref OperationStackContext<Key, Value> stackCtx, ref OperationStatus status, ref LatchOperation latchOperation)
        {
            // See additional comments in AcquireCPRLatchRMW.

            switch (phase)
            {
                case Phase.PREPARE: // Thread is in V
                    if (HashBucket.TryAcquireSharedLatch(ref stackCtx.hei))
                    {
                        // Set to release shared latch (default)
                        latchOperation = LatchOperation.Shared;
                        if (IsEntryVersionNew(ref stackCtx.hei.entry))
                        {
                            status = OperationStatus.CPR_SHIFT_DETECTED;
                            return LatchDestination.Retry;  // Pivot Thread for retry (do not operate on V+1 record when thread is in V)
                        }
                        break; // Normal Processing; thread is in V, record is in V
                    }

                    // Could not acquire Shared latch; system must be in V+1 (or we have too many shared latches).
                    status = OperationStatus.CPR_SHIFT_DETECTED;
                    return LatchDestination.Retry;  // Pivot Thread for retry

                case Phase.IN_PROGRESS: // Thread is in V+1
                    if (IsRecordVersionNew(stackCtx.recSrc.LogicalAddress))
                        break;      // Normal Processing; V+1 thread encountered a record in V+1

                    if (HashBucket.TryAcquireExclusiveLatch(ref stackCtx.hei))
                    {
                        // Set to release exclusive latch (default)
                        latchOperation = LatchOperation.Exclusive;
                        return LatchDestination.CreateNewRecord; // Upsert never goes pending; always force creation of a (v+1) record
                    }

                    // Could not acquire exclusive latch; likely a conflict on the bucket.
                    status = OperationStatus.RETRY_LATER;
                    return LatchDestination.Retry;  // Retry after refresh

                case Phase.WAIT_INDEX_CHECKPOINT:   // Thread is in v+1
                case Phase.WAIT_FLUSH:
                    if (IsRecordVersionNew(stackCtx.recSrc.LogicalAddress))
                        break;      // Normal Processing; V+1 thread encountered a record in V+1
                    return LatchDestination.CreateNewRecord;    // Upsert never goes pending; always force creation of a (V+1) record

                default:
                    break;
            }
            return LatchDestination.NormalProcessing;
        }

        /// <summary>
        /// Create a new record for Upsert
        /// </summary>
        /// <param name="key">The record Key</param>
        /// <param name="input">Input to the operation</param>
        /// <param name="value">The value to insert</param>
        /// <param name="output">The result of IFunctions.SingleWriter</param>
        /// <param name="pendingContext">Information about the operation context</param>
        /// <param name="tsavoriteSession">The current session</param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{Key, Value}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        /// <param name="srcRecordInfo">If <paramref name="stackCtx"/>.<see cref="RecordSource{Key, Value}.HasInMemorySrc"/>,
        ///     this is the <see cref="RecordInfo"/> for <see cref="RecordSource{Key, Value}.LogicalAddress"/></param>
        private OperationStatus CreateNewRecordUpsert<Input, Output, Context, TsavoriteSession>(ref Key key, ref Input input, ref Value value, ref Output output,
                                                                                             ref PendingContext<Input, Output, Context> pendingContext, TsavoriteSession tsavoriteSession,
                                                                                             ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            var (actualSize, allocatedSize, keySize) = hlog.GetRecordSize(ref key, ref value);   // Input is not included in record-length calculations for Upsert
            AllocateOptions allocOptions = new()
            {
                Recycle = true,

                // If the source record is elidable we can try to elide from the chain and transfer it to the FreeList if we're doing Revivification
                IgnoreHeiAddress = stackCtx.recSrc.HasMainLogSrc && CanElide<Input, Output, Context, TsavoriteSession>(tsavoriteSession, ref stackCtx, ref srcRecordInfo)
            };

            if (!TryAllocateRecord(tsavoriteSession, ref pendingContext, ref stackCtx, actualSize, ref allocatedSize, keySize, allocOptions,
                    out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status))
                return status;

            ref RecordInfo newRecordInfo = ref WriteNewRecordInfo(ref key, hlog, newPhysicalAddress, inNewVersion: tsavoriteSession.Ctx.InNewVersion, tombstone: false, stackCtx.recSrc.LatestLogicalAddress);
            if (allocOptions.IgnoreHeiAddress)
                newRecordInfo.PreviousAddress = srcRecordInfo.PreviousAddress;
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

            if (!tsavoriteSession.SingleWriter(ref key, ref input, ref value, ref newRecordValue, ref output, ref upsertInfo, WriteReason.Upsert, ref newRecordInfo))
            {
                // Save allocation for revivification (not retry, because these aren't retry status codes), or abandon it if that fails.
                if (RevivificationManager.UseFreeRecordPool && RevivificationManager.TryAdd(newLogicalAddress, newPhysicalAddress, allocatedSize, ref tsavoriteSession.Ctx.RevivificationStats))
                    stackCtx.ClearNewRecord();
                else
                    stackCtx.SetNewRecordInvalid(ref newRecordInfo);

                if (upsertInfo.Action == UpsertAction.CancelOperation)
                    return OperationStatus.CANCELED;
                return OperationStatus.NOTFOUND;    // But not CreatedRecord
            }

            SetExtraValueLength(ref newRecordValue, ref newRecordInfo, upsertInfo.UsedValueLength, upsertInfo.FullValueLength);

            // Insert the new record by CAS'ing either directly into the hash entry or splicing into the readcache/mainlog boundary.
            // If the current record can be elided then we can freelist it; detach it by swapping its .PreviousAddress into newRecordInfo.
            bool success = CASRecordIntoChain(ref key, ref stackCtx, newLogicalAddress, ref newRecordInfo);
            if (success)
            {
                PostCopyToTail(ref key, ref stackCtx, ref srcRecordInfo);

                tsavoriteSession.PostSingleWriter(ref key, ref input, ref value, ref newRecordValue, ref output, ref upsertInfo, WriteReason.Upsert, ref newRecordInfo);

                // IgnoreHeiAddress means we have verified that the old source record is elidable and now that CAS has replaced it in the HashBucketEntry with
                // the new source record that does not point to the old source record, we have elided it, so try to transfer to freelist.
                if (allocOptions.IgnoreHeiAddress)
                {
                    // Success should always Seal the old record. This may be readcache, readonly, or the temporary recordInfo, which is OK and saves the cost of an "if".
                    stackCtx.recSrc.UnlockExclusiveAndSealInvalidate(ref srcRecordInfo);    // The record was elided, so Invalidate

                    if (stackCtx.recSrc.LogicalAddress >= GetMinRevivifiableAddress())
                    {
                        // We need to re-get the old record's length because upsertInfo has the new record's info. If freelist-add fails, it remains Sealed/Invalidated.
                        var oldRecordLengths = GetRecordLengths(stackCtx.recSrc.PhysicalAddress, ref hlog.GetValue(stackCtx.recSrc.PhysicalAddress), ref srcRecordInfo);
                        TryTransferToFreeList<Input, Output, Context, TsavoriteSession>(tsavoriteSession, ref stackCtx, ref srcRecordInfo, oldRecordLengths);
                    }
                }
                else
                    stackCtx.recSrc.UnlockExclusiveAndSeal(ref srcRecordInfo);              // The record was not elided, so do not Invalidate

                stackCtx.ClearNewRecord();
                pendingContext.recordInfo = newRecordInfo;
                pendingContext.logicalAddress = newLogicalAddress;
                return OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.CreatedRecord);
            }

            // CAS failed
            stackCtx.SetNewRecordInvalid(ref newRecordInfo);
            tsavoriteSession.DisposeSingleWriter(ref hlog.GetKey(newPhysicalAddress), ref input, ref value, ref hlog.GetValue(newPhysicalAddress), ref output, ref upsertInfo, WriteReason.Upsert);

            SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress, allocatedSize);
            return OperationStatus.RETRY_NOW;   // CAS failure does not require epoch refresh
        }
    }
}