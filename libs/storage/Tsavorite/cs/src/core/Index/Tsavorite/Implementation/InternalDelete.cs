// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        /// <summary>
        /// Delete operation. Replaces the value corresponding to 'key' with tombstone.
        /// If at head, tries to remove item from hash chain
        /// </summary>
        /// <param name="key">Key of the record to be deleted.</param>
        /// <param name="keyHash"></param>
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
        ///     <term>The value has been successfully deleted</term>
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
        internal OperationStatus InternalDelete<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref TKey key, long keyHash, ref TContext userContext,
                            ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var latchOperation = LatchOperation.None;

            OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(keyHash);
            pendingContext.keyHash = keyHash;

            if (sessionFunctions.Ctx.phase == Phase.IN_PROGRESS_GROW)
                SplitBuckets(stackCtx.hei.hash);

            if (!FindTagAndTryTransientXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref key, ref stackCtx, out OperationStatus status))
                return status;

            RecordInfo dummyRecordInfo = RecordInfo.InitialValid;
            ref RecordInfo srcRecordInfo = ref dummyRecordInfo;

            DeleteInfo deleteInfo = new()
            {
                Version = sessionFunctions.Ctx.version,
                SessionID = sessionFunctions.Ctx.sessionID
            };

            // We must use try/finally to ensure unlocking even in the presence of exceptions.
            try
            {
                // Search the entire in-memory region; this lets us find a tombstoned record in the immutable region, avoiding unnecessarily adding one.
                if (!TryFindRecordForUpdate(ref key, ref stackCtx, hlogBase.HeadAddress, out status))
                    return status;

                // Note: Delete does not track pendingContext.InitialAddress because we don't have an InternalContinuePendingDelete

                deleteInfo.Address = stackCtx.recSrc.LogicalAddress;
                deleteInfo.KeyHash = stackCtx.hei.hash;

                if (stackCtx.recSrc.HasReadCacheSrc)
                {
                    // Use the readcache record as the CopyUpdater source.
                    srcRecordInfo = ref stackCtx.recSrc.GetInfo();
                    goto CreateNewRecord;
                }

                // Check for CPR consistency after checking if source is readcache.
                if (sessionFunctions.Ctx.phase != Phase.REST)
                {
                    var latchDestination = CheckCPRConsistencyDelete(sessionFunctions.Ctx.phase, ref stackCtx, ref status, ref latchOperation);
                    switch (latchDestination)
                    {
                        case LatchDestination.Retry:
                            goto LatchRelease;
                        case LatchDestination.CreateNewRecord:
                            if (stackCtx.recSrc.HasMainLogSrc)
                                srcRecordInfo = ref stackCtx.recSrc.GetInfo();
                            goto CreateNewRecord;
                        default:
                            Debug.Assert(latchDestination == LatchDestination.NormalProcessing, "Unknown latchDestination value; expected NormalProcessing");
                            break;
                    }
                }

                if (stackCtx.recSrc.LogicalAddress >= hlogBase.ReadOnlyAddress)
                {
                    srcRecordInfo = ref stackCtx.recSrc.GetInfo();

                    // If we already have a deleted record, there's nothing to do.
                    if (srcRecordInfo.Tombstone)
                        return OperationStatus.NOTFOUND;

                    // Mutable Region: Update the record in-place
                    deleteInfo.SetRecordInfo(ref srcRecordInfo);
                    ref TValue recordValue = ref stackCtx.recSrc.GetValue();

                    // DeleteInfo's lengths are filled in and GetRecordLengths and SetDeletedValueLength are called inside ConcurrentDeleter.
                    if (sessionFunctions.ConcurrentDeleter(stackCtx.recSrc.PhysicalAddress, ref stackCtx.recSrc.GetKey(), ref recordValue, ref deleteInfo, ref srcRecordInfo, out int fullRecordLength))
                    {
                        MarkPage(stackCtx.recSrc.LogicalAddress, sessionFunctions.Ctx);

                        // Try to transfer the record from the tag chain to the free record pool iff previous address points to invalid address.
                        // Otherwise an earlier record for this key could be reachable again.
                        if (CanElide<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, ref srcRecordInfo))
                        {
                            HandleRecordElision<TInput, TOutput, TContext, TSessionFunctionsWrapper>(
                                sessionFunctions, ref stackCtx, ref srcRecordInfo, deleteInfo.UsedValueLength, deleteInfo.FullValueLength, fullRecordLength);
                        }

                        status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord);
                        goto LatchRelease;
                    }
                    if (deleteInfo.Action == DeleteAction.CancelOperation)
                    {
                        status = OperationStatus.CANCELED;
                        goto LatchRelease;
                    }

                    // Could not delete in place for some reason - create new record.
                    goto CreateNewRecord;
                }
                else if (stackCtx.recSrc.HasMainLogSrc)
                {
                    // If we already have a deleted record, there's nothing to do.
                    srcRecordInfo = ref stackCtx.recSrc.GetInfo();
                    if (srcRecordInfo.Tombstone)
                        return OperationStatus.NOTFOUND;
                    goto CreateNewRecord;
                }

                // Either on-disk or no record exists - drop through to create new record.
                Debug.Assert(!sessionFunctions.IsManualLocking || LockTable.IsLockedExclusive(ref stackCtx.hei), "A Lockable-session Delete() of an on-disk or non-existent key requires a LockTable lock");

            CreateNewRecord:
                // Immutable region or new record
                status = CreateNewRecordDelete(ref key, ref pendingContext, sessionFunctions, ref stackCtx, ref srcRecordInfo, ref deleteInfo);
                if (!OperationStatusUtils.IsAppend(status))
                {
                    // We should never return "SUCCESS" for a new record operation: it returns NOTFOUND on success.
                    Debug.Assert(OperationStatusUtils.BasicOpCode(status) != OperationStatus.SUCCESS);
                    if (status == OperationStatus.ALLOCATE_FAILED && pendingContext.IsAsync)
                        CreatePendingDeleteContext(ref key, userContext, ref pendingContext, sessionFunctions, ref stackCtx);
                }
            }
            finally
            {
                stackCtx.HandleNewRecordOnException(this);
                sessionFunctions.PostDeleteOperation(ref key, ref deleteInfo, epoch);
                TransientXUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref key, ref stackCtx);
            }

        LatchRelease:
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

        // No AggressiveInlining; this is a less-common function and it may improve inlining of InternalDelete if the compiler decides not to inline this.
        private void CreatePendingDeleteContext<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref TKey key, TContext userContext,
                ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            pendingContext.type = OperationType.DELETE;
            if (pendingContext.key == default) pendingContext.key = hlog.GetKeyContainer(ref key);
            pendingContext.userContext = userContext;
            pendingContext.InitialLatestLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;
            pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
        }

        private LatchDestination CheckCPRConsistencyDelete(Phase phase, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx, ref OperationStatus status, ref LatchOperation latchOperation)
        {
            // This is the same logic as Upsert; neither goes pending.
            return CheckCPRConsistencyUpsert(phase, ref stackCtx, ref status, ref latchOperation);
        }

        /// <summary>
        /// Create a new tombstoned record for Delete
        /// </summary>
        /// <param name="key">The record Key</param>
        /// <param name="pendingContext">Information about the operation context</param>
        /// <param name="sessionFunctions">The current session</param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{Key, Value, TStoreFunctions, TAllocator}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        /// <param name="srcRecordInfo">If <paramref name="stackCtx"/>.<see cref="RecordSource{Key, Value, TStoreFunctions, TAllocator}.HasInMemorySrc"/>,
        ///     this is the <see cref="RecordInfo"/> for <see cref="RecordSource{Key, Value, TStoreFunctions, TAllocator}.LogicalAddress"/></param>
        /// <param name="deleteInfo">DeleteInfo</param>
        private OperationStatus CreateNewRecordDelete<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref TKey key, ref PendingContext<TInput, TOutput, TContext> pendingContext,
                TSessionFunctionsWrapper sessionFunctions, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx, ref RecordInfo srcRecordInfo, ref DeleteInfo deleteInfo)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var value = default(TValue);
            var (actualSize, allocatedSize, keySize) = hlog.GetRecordSize(ref key, ref value);

            // We know the existing record cannot be elided; it must point to a valid record; otherwise InternalDelete would have returned NOTFOUND.
            if (!TryAllocateRecord(sessionFunctions, ref pendingContext, ref stackCtx, actualSize, ref allocatedSize, keySize, new AllocateOptions() { Recycle = true },
                    out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status))
                return status;

            ref RecordInfo newRecordInfo = ref WriteNewRecordInfo(ref key, hlogBase, newPhysicalAddress, inNewVersion: sessionFunctions.Ctx.InNewVersion, stackCtx.recSrc.LatestLogicalAddress);
            newRecordInfo.SetTombstone();
            stackCtx.SetNewRecord(newLogicalAddress);

            deleteInfo.Address = newLogicalAddress;
            deleteInfo.KeyHash = stackCtx.hei.hash;
            deleteInfo.SetRecordInfo(ref newRecordInfo);

            ref TValue newRecordValue = ref hlog.GetAndInitializeValue(newPhysicalAddress, newPhysicalAddress + actualSize);
            (deleteInfo.UsedValueLength, deleteInfo.FullValueLength) = GetNewValueLengths(actualSize, allocatedSize, newPhysicalAddress, ref newRecordValue);

            if (!sessionFunctions.SingleDeleter(ref key, ref newRecordValue, ref deleteInfo, ref newRecordInfo))
            {
                // This record was allocated with a minimal Value size (unless it was a revivified larger record) so there's no room for a Filler,
                // but we may want it for a later Delete, or for insert with a smaller Key.
                if (RevivificationManager.UseFreeRecordPool && RevivificationManager.TryAdd(newLogicalAddress, newPhysicalAddress, allocatedSize, ref sessionFunctions.Ctx.RevivificationStats))
                    stackCtx.ClearNewRecord();
                else
                    stackCtx.SetNewRecordInvalid(ref newRecordInfo);

                if (deleteInfo.Action == DeleteAction.CancelOperation)
                    return OperationStatus.CANCELED;
                return OperationStatus.NOTFOUND;    // But not CreatedRecord
            }

            SetTombstoneAndExtraValueLength(ref newRecordValue, ref newRecordInfo, deleteInfo.UsedValueLength, deleteInfo.FullValueLength);

            // Insert the new record by CAS'ing either directly into the hash entry or splicing into the readcache/mainlog boundary.
            bool success = CASRecordIntoChain(ref key, ref stackCtx, newLogicalAddress, ref newRecordInfo);
            if (success)
            {
                PostCopyToTail(ref key, ref stackCtx, ref srcRecordInfo);

                // Note that this is the new logicalAddress; we have not retrieved the old one if it was below HeadAddress, and thus
                // we do not know whether 'logicalAddress' belongs to 'key' or is a collision.
                sessionFunctions.PostSingleDeleter(ref key, ref deleteInfo, ref newRecordInfo);

                // Success should always Seal the old record. This may be readcache, readonly, or the temporary recordInfo, which is OK and saves the cost of an "if".
                srcRecordInfo.Seal();    // Not elided so Seal without invalidate

                stackCtx.ClearNewRecord();
                pendingContext.recordInfo = newRecordInfo;
                pendingContext.logicalAddress = newLogicalAddress;
                return OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.CreatedRecord);
            }

            // CAS failed
            stackCtx.SetNewRecordInvalid(ref newRecordInfo);
            ref TValue insertedValue = ref hlog.GetValue(newPhysicalAddress);
            ref TKey insertedKey = ref hlog.GetKey(newPhysicalAddress);
            storeFunctions.DisposeRecord(ref insertedKey, ref insertedValue, DisposeReason.SingleDeleterCASFailed);

            SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress, allocatedSize);
            return OperationStatus.RETRY_NOW;   // CAS failure does not require epoch refresh
        }
    }
}