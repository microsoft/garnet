// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TValue>
        where TAllocator : IAllocator<TValue, TStoreFunctions>
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
        internal OperationStatus InternalDelete<TInput, TOutput, TContext, TSessionFunctionsWrapper>(SpanByte key, long keyHash, ref TContext userContext,
                            ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var latchOperation = LatchOperation.None;

            OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx = new(keyHash);
            pendingContext.keyHash = keyHash;

            if (sessionFunctions.Ctx.phase == Phase.IN_PROGRESS_GROW)
                SplitBuckets(stackCtx.hei.hash);

            if (!FindTagAndTryEphemeralXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out OperationStatus status))
                return status;

            LogRecord<TValue> srcLogRecord = default;

            // We must use try/finally to ensure unlocking even in the presence of exceptions.
            try
            {
                // Search the entire in-memory region; this lets us find a tombstoned record in the immutable region, avoiding unnecessarily adding one.
                if (!TryFindRecordForUpdate(key, ref stackCtx, hlogBase.HeadAddress, out status))
                    return status;

                // Note: Delete does not track pendingContext.InitialAddress because we don't have an InternalContinuePendingDelete

                DeleteInfo deleteInfo = new()
                {
                    Version = sessionFunctions.Ctx.version,
                    SessionID = sessionFunctions.Ctx.sessionID,
                    Address = stackCtx.recSrc.LogicalAddress,
                    KeyHash = stackCtx.hei.hash
                };

                if (stackCtx.recSrc.HasReadCacheSrc)
                {
                    // Use the readcache record as the CopyUpdater source.
                    srcLogRecord = stackCtx.recSrc.CreateLogRecord();
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
                            goto CreateNewRecord;
                        default:
                            Debug.Assert(latchDestination == LatchDestination.NormalProcessing, "Unknown latchDestination value; expected NormalProcessing");
                            break;
                    }
                }

                if (stackCtx.recSrc.LogicalAddress >= hlogBase.ReadOnlyAddress)
                {
                    srcLogRecord = stackCtx.recSrc.CreateLogRecord();

                    // If we already have a deleted record, there's nothing to do.
                    if (srcLogRecord.Info.Tombstone)
                        return OperationStatus.NOTFOUND;

                    // Mutable Region: Update the record in-place

                    // DeleteInfo's lengths are filled in and GetRecordLengths and SetDeletedValueLength are called inside ConcurrentDeleter.
                    if (sessionFunctions.ConcurrentDeleter(ref srcLogRecord, ref deleteInfo))
                    {
                        MarkPage(stackCtx.recSrc.LogicalAddress, sessionFunctions.Ctx);

                        // Try to transfer the record from the tag chain to the free record pool iff previous address points to invalid address.
                        // Otherwise an earlier record for this key could be reachable again.
                        // Note: We do not currently consider this reuse for mid-chain records (records past the HashBucket), because TracebackForKeyMatch would need
                        //  to return the next-higher record whose .PreviousAddress points to this one, *and* we'd need to make sure that record was not revivified out.
                        //  Also, we do not consider this in-chain reuse for records with different keys, because we don't get here if the keys don't match.
                        if (CanElide<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, srcLogRecord.Info))
                        {
                            if (!RevivificationManager.IsEnabled)
                            {
                                // We are not doing revivification, so we just want to remove the record from the tag chain so we don't potentially do an IO later for key 
                                // traceback. If we succeed, we need to SealAndInvalidate. It's fine if we don't succeed here; this is just tidying up the HashBucket. 
                                if (stackCtx.hei.TryElide())
                                    srcLogRecord.InfoRef.SealAndInvalidate();
                            }
                            else if (RevivificationManager.UseFreeRecordPool)
                            {
                                // For non-FreeRecordPool revivification, we leave the record in as a normal tombstone so we can revivify it in the chain for the same key.
                                // For FreeRecord Pool we must first Seal here, even if we're using the LockTable, because the Sealed state must survive this Delete() call.
                                // We invalidate it also for checkpoint/recovery consistency (this removes Sealed bit so Scan would enumerate records that are not in any
                                // tag chain--they would be in the freelist if the freelist survived Recovery), but we restore the Valid bit if it is returned to the chain,
                                // which due to epoch protection is guaranteed to be done before the record can be written to disk and violate the "No Invalid records in
                                // tag chain" invariant.
                                srcLogRecord.InfoRef.SealAndInvalidate();

                                bool isElided = false, isAdded = false;
                                Debug.Assert(srcLogRecord.Info.Tombstone, $"Unexpected loss of Tombstone; Record should have been XLocked or SealInvalidated. RecordInfo: {srcLogRecord.Info.ToString()}");
                                (isElided, isAdded) = TryElideAndTransferToFreeList<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, ref srcLogRecord);

                                if (!isElided)
                                {
                                    // Leave this in the chain as a normal Tombstone; we aren't going to add a new record so we can't leave this one sealed.
                                    srcLogRecord.InfoRef.UnsealAndValidate();
                                }
                                else if (!isAdded && RevivificationManager.restoreDeletedRecordsIfBinIsFull)
                                {
                                    // The record was not added to the freelist, but was elided. See if we can put it back in as a normal Tombstone. Since we just
                                    // elided it and the elision criteria is that it is the only above-BeginAddress record in the chain, and elision sets the
                                    // HashBucketEntry.word to 0, it means we do not expect any records for this key's tag to exist after the elision. Therefore,
                                    // we can re-insert the record iff the HashBucketEntry's address is <= kTempInvalidAddress.
                                    stackCtx.hei = new(stackCtx.hei.hash);
                                    FindOrCreateTag(ref stackCtx.hei, hlogBase.BeginAddress);

                                    if (stackCtx.hei.entry.Address <= Constants.kTempInvalidAddress && stackCtx.hei.TryCAS(stackCtx.recSrc.LogicalAddress))
                                        srcLogRecord.InfoRef.UnsealAndValidate();
                                }
                            }
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
                else if (stackCtx.recSrc.LogicalAddress >= hlogBase.HeadAddress)
                {
                    // If we already have a deleted record, there's nothing to do.
                    srcLogRecord = stackCtx.recSrc.CreateLogRecord();
                    if (srcLogRecord.Info.Tombstone)
                        return OperationStatus.NOTFOUND;
                    goto CreateNewRecord;
                }

                // Either on-disk or no record exists - drop through to create new record.
                Debug.Assert(!sessionFunctions.IsTransactionalLocking || LockTable.IsLockedExclusive(ref stackCtx.hei), "A Transactional-session Delete() of an on-disk or non-existent key requires a LockTable lock");

            CreateNewRecord:
                // Immutable region or new record
                status = CreateNewRecordDelete(key, ref srcLogRecord, ref pendingContext, sessionFunctions, ref stackCtx);
                if (!OperationStatusUtils.IsAppend(status))
                {
                    // We should never return "SUCCESS" for a new record operation: it returns NOTFOUND on success.
                    Debug.Assert(OperationStatusUtils.BasicOpCode(status) != OperationStatus.SUCCESS);
                    if (status == OperationStatus.ALLOCATE_FAILED && pendingContext.IsAsync)
                        CreatePendingDeleteContext(key, userContext, ref pendingContext, sessionFunctions, ref stackCtx);
                }
            }
            finally
            {
                stackCtx.HandleNewRecordOnException(this);
                EphemeralXUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx);
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
        private void CreatePendingDeleteContext<TInput, TOutput, TContext, TSessionFunctionsWrapper>(SpanByte key, TContext userContext,
                ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions, ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            pendingContext.type = OperationType.DELETE;
            if (pendingContext.key == default) pendingContext.key = hlog.GetKeyContainer(key);
            pendingContext.userContext = userContext;
            pendingContext.InitialLatestLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;
            pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
        }

        private LatchDestination CheckCPRConsistencyDelete(Phase phase, ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx, ref OperationStatus status, ref LatchOperation latchOperation)
        {
            // This is the same logic as Upsert; neither goes pending.
            return CheckCPRConsistencyUpsert(phase, ref stackCtx, ref status, ref latchOperation);
        }

        /// <summary>
        /// Create a new tombstoned record for Delete
        /// </summary>
        /// <param name="key">The record Key</param>
        /// <param name="srcLogRecord">The source record, if <paramref name="stackCtx"/>.<see cref="RecordSource{TValue, TStoreFunctions, TAllocator}.HasInMemorySrc"/> and
        /// it is either too small or is in readonly region, or is in raadcache</param>
        /// <param name="pendingContext">Information about the operation context</param>
        /// <param name="sessionFunctions">The current session</param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{TValue, TStoreFunctions, TAllocator}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        private OperationStatus CreateNewRecordDelete<TInput, TOutput, TContext, TSessionFunctionsWrapper>(SpanByte key, ref LogRecord<TValue> srcLogRecord, ref PendingContext<TInput, TOutput, TContext> pendingContext,
                TSessionFunctionsWrapper sessionFunctions, ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var sizeInfo = hlog.GetDeleteRecordSize(key);
            AllocateOptions allocOptions = new()
            {
                recycle = true,
                elideSourceRecord = stackCtx.recSrc.HasMainLogSrc && CanElide<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, srcLogRecord.Info)
            };

            // We know the existing record cannot be elided; it must point to a valid record; otherwise InternalDelete would have returned NOTFOUND.
            if (!TryAllocateRecord(sessionFunctions, ref pendingContext, ref stackCtx, ref sizeInfo, allocOptions, out var newLogicalAddress, out var newPhysicalAddress, out var allocatedSize, out var status))
                return status;

            var newLogRecord = WriteNewRecordInfo(key, hlogBase, newLogicalAddress, newPhysicalAddress, sessionFunctions.Ctx.InNewVersion, previousAddress: stackCtx.recSrc.LatestLogicalAddress);
            newLogRecord.InfoRef.SetTombstone();
            stackCtx.SetNewRecord(newLogicalAddress);

            DeleteInfo deleteInfo = new()
            {
                Version = sessionFunctions.Ctx.version,
                SessionID = sessionFunctions.Ctx.sessionID,
                Address = newLogicalAddress,
                KeyHash = stackCtx.hei.hash,
            };

            hlog.InitializeValue(newPhysicalAddress, sizeInfo.FieldInfo.ValueSize);
            newLogRecord.SetFillerLength(allocatedSize);

            if (!sessionFunctions.SingleDeleter(ref newLogRecord, ref deleteInfo))
            {
                // This record was allocated with a minimal Value size (unless it was a revivified larger record) so there's no room for a Filler,
                // but we may want it for a later Delete, or for insert with a smaller Key.
                if (RevivificationManager.UseFreeRecordPool && RevivificationManager.TryAdd(newLogicalAddress, ref newLogRecord, ref sessionFunctions.Ctx.RevivificationStats))
                    stackCtx.ClearNewRecord();
                else
                    stackCtx.SetNewRecordInvalid(ref newLogRecord.InfoRef);

                if (deleteInfo.Action == DeleteAction.CancelOperation)
                    return OperationStatus.CANCELED;
                return OperationStatus.NOTFOUND;    // But not CreatedRecord
            }

            newLogRecord.InfoRef.SetTombstone();

            // Insert the new record by CAS'ing either directly into the hash entry or splicing into the readcache/mainlog boundary.
            var success = CASRecordIntoChain(newLogicalAddress, ref newLogRecord, ref stackCtx);
            if (success)
            {
                PostCopyToTail(ref srcLogRecord, ref stackCtx);

                // Note that this is the new logicalAddress; we have not retrieved the old one if it was below HeadAddress, and thus
                // we do not know whether 'logicalAddress' belongs to 'key' or is a collision.
                sessionFunctions.PostSingleDeleter(ref newLogRecord, ref deleteInfo);

                // Success should always Seal the old record. This may be readcache, readonly, or the temporary recordInfo, which is OK and saves the cost of an "if".
                srcLogRecord.InfoRef.Seal();    // Not elided so Seal without invalidate

                stackCtx.ClearNewRecord();
                pendingContext.logicalAddress = newLogicalAddress;
                return OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.CreatedRecord);
            }

            // CAS failed
            stackCtx.SetNewRecordInvalid(ref newLogRecord.InfoRef);
            DisposeRecord(ref newLogRecord, DisposeReason.SingleDeleterCASFailed);

            SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress);
            return OperationStatus.RETRY_NOW;   // CAS failure does not require epoch refresh
        }
    }
}