// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    using static LogAddress;

    public unsafe partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>
        /// Read-Modify-Write Operation. Updates value of 'key' using 'input' and current value.
        /// Pending operations are processed either using InternalRetryPendingRMW or 
        /// InternalContinuePendingRMW.
        /// </summary>
        /// <param name="key">key of the record.</param>
        /// <param name="keyHash">the hash of <parameref name="key"/></param>
        /// <param name="input">input used to update the value.</param>
        /// <param name="output">Location to store output computed from input and value.</param>
        /// <param name="userContext">user context corresponding to operation used during completion callback.</param>
        /// <param name="pendingContext">pending context created when the operation goes pending.</param>
        /// <param name="sessionFunctions">Callback functions.</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully updated (or inserted).</term>
        ///     </item>
        ///     <item>
        ///     <term>RECORD_ON_DISK</term>
        ///     <term>The record corresponding to 'key' is on disk. Issue async IO to retrieve record and retry later.</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_LATER</term>
        ///     <term>Cannot  be processed immediately due to system state. Add to pending list and retry later.</term>
        ///     </item>
        ///     <item>
        ///     <term>CPR_SHIFT_DETECTED</term>
        ///     <term>A shift in version has been detected. Synchronize immediately to avoid violating CPR consistency.</term>
        ///     </item>
        /// </list>
        /// </returns>
        internal OperationStatus InternalRMW<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ReadOnlySpan<byte> key, long keyHash, ref TInput input, ref TOutput output, ref TContext userContext,
                                    ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var latchOperation = LatchOperation.None;

            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(keyHash);
            pendingContext.keyHash = keyHash;
            pendingContext.logicalAddress = kInvalidAddress;
            pendingContext.eTag = LogRecord.NoETag;

            if (sessionFunctions.Ctx.phase == Phase.IN_PROGRESS_GROW)
                SplitBuckets(stackCtx.hei.hash);

            if (!FindOrCreateTagAndTryEphemeralXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out var status))
                return status;

            LogRecord srcLogRecord = default;

            // We must use try/finally to ensure unlocking even in the presence of exceptions.
            try
            {
                // Search the entire in-memory region.
                if (!TryFindRecordForUpdate(key, ref stackCtx, hlogBase.HeadAddress, out status))
                    return status;

                // These track the latest main-log address in the tag chain; InternalContinuePendingRMW uses them to check for new inserts.
                pendingContext.initialEntryAddress = stackCtx.hei.Address;
                pendingContext.initialLatestLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;

                // If there is a readcache record, use it as the CopyUpdater source.
                if (stackCtx.recSrc.HasReadCacheSrc)
                {
                    srcLogRecord = stackCtx.recSrc.CreateLogRecord();
                    goto CreateNewRecord;
                }

                // Check for CPR consistency after checking if source is readcache.
                if (sessionFunctions.Ctx.phase != Phase.REST)
                {
                    var latchDestination = CheckCPRConsistencyRMW(sessionFunctions.Ctx.phase, ref stackCtx, ref status, ref latchOperation);
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
                    RMWInfo rmwInfo = new()
                    {
                        Version = sessionFunctions.Ctx.version,
                        SessionID = sessionFunctions.Ctx.sessionID,
                        Address = stackCtx.recSrc.LogicalAddress,
                        KeyHash = stackCtx.hei.hash,
                    };

                    if (srcLogRecord.Info.Tombstone)
                    {
                        // If we're doing revivification and this is in the revivifiable range, try to revivify--otherwise we'll create a new record.
                        if (RevivificationManager.IsEnabled && stackCtx.recSrc.LogicalAddress >= GetMinRevivifiableAddress())
                        {
                            if (!sessionFunctions.NeedInitialUpdate(key, ref input, ref output, ref rmwInfo))
                            {
                                status = OperationStatus.NOTFOUND;
                                goto LatchRelease;
                            }

                            if (TryRevivifyInChain(ref srcLogRecord, ref input, ref output, ref pendingContext, sessionFunctions, ref stackCtx, ref rmwInfo, out status)
                                    || status != OperationStatus.SUCCESS)
                                goto LatchRelease;
                        }
                        goto CreateNewRecord;
                    }

                    var sizeInfo = hlog.GetRMWCopyRecordSize(in srcLogRecord, ref input, sessionFunctions);
                    if (sessionFunctions.InPlaceUpdater(ref srcLogRecord, in sizeInfo, ref input, ref output, ref rmwInfo, out status))
                    {
                        MarkPage(stackCtx.recSrc.LogicalAddress, sessionFunctions.Ctx);

                        // status has been set by InPlaceUpdater
                        pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                        pendingContext.eTag = srcLogRecord.ETag;
                        goto LatchRelease;
                    }

                    if (rmwInfo.Action == RMWAction.ExpireAndStop)
                    {
                        MarkPage(stackCtx.recSrc.LogicalAddress, sessionFunctions.Ctx);
                        // Tombstone has been set by SessionFunctionsWrapper.InPlaceUpdater
                        srcLogRecord.InfoRef.SetDirtyAndModified();

                        pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                        pendingContext.eTag = srcLogRecord.ETag;

                        // ExpireAndStop means to override default Delete handling (which is to go to InitialUpdater) by leaving the tombstoned record as current.
                        // Our SessionFunctionsWrapper.InPlaceUpdater implementation has already reinitialized-in-place or set Tombstone as appropriate and marked the record.

                        // Try to transfer the record from the tag chain to the free record pool iff previous address points to invalid address.
                        // Otherwise an earlier record for this key could be reachable again.
                        if (CanElide<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, srcLogRecord.Info))
                            HandleRecordElision<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, ref srcLogRecord);
                        else
                            DisposeRecord(ref srcLogRecord, DisposeReason.Deleted);

                        goto LatchRelease;
                    }
                    else if (rmwInfo.Action == RMWAction.WrongType)
                    {
                        // status has been set by InPlaceUpdater, and no modification should have been made to the record.
                        pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                        pendingContext.eTag = srcLogRecord.ETag;
                        status = OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.WrongType);
                        goto LatchRelease;
                    }

                    if (OperationStatusUtils.BasicOpCode(status) != OperationStatus.SUCCESS)
                        goto LatchRelease;

                    // InPlaceUpdater failed (e.g. insufficient space, another thread set Tombstone, etc). Use this record as the CopyUpdater source.
                    goto CreateNewRecord;
                }
                if (stackCtx.recSrc.LogicalAddress >= hlogBase.SafeReadOnlyAddress && !stackCtx.recSrc.GetInfo().Tombstone)
                {
                    // Fuzzy Region: Must retry after epoch refresh, due to lost-update anomaly
                    status = OperationStatus.RETRY_LATER;
                    goto LatchRelease;
                }
                if (stackCtx.recSrc.HasMainLogSrc)
                {
                    // Safe Read-Only Region: CopyUpdate to create a record in the mutable region.
                    srcLogRecord = stackCtx.recSrc.CreateLogRecord();
                    goto CreateNewRecord;
                }
                if (stackCtx.recSrc.LogicalAddress >= hlogBase.BeginAddress)
                {
                    if (hlogBase.IsNullDevice)
                        goto CreateNewRecord;

                    // Disk Region: Need to issue async io requests. Locking will be checked on pending completion.
                    status = OperationStatus.RECORD_ON_DISK;
                    CreatePendingRMWContext(key, ref input, ref output, userContext, ref pendingContext, sessionFunctions, ref stackCtx);
                    goto LatchRelease;
                }

                // No record exists - drop through to create new record.
                Debug.Assert(!sessionFunctions.IsTransactionalLocking || LockTable.IsLockedExclusive(ref stackCtx.hei), "A Transactional-session RMW() of an on-disk or non-existent key requires a LockTable lock");

            CreateNewRecord:
                {
                    // Here, the input* data for 'doingCU' is the same as recSrc.
                    status = CreateNewRecordRMW(key, in srcLogRecord, ref input, ref output, ref pendingContext, sessionFunctions, ref stackCtx,
                                                doingCU: stackCtx.recSrc.HasInMemorySrc && !srcLogRecord.Info.Tombstone);

                    // OperationStatus.SUCCESS is OK here even if !OperationStatusUtils.IsAppend(status); it means NeedCopyUpdate or NeedInitialUpdate returned false
                    goto LatchRelease;
                }
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

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void CreatePendingRMWContext<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, TContext userContext,
                ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            pendingContext.type = OperationType.RMW;
            pendingContext.CopyInputsForReadOrRMW(key, ref input, ref output, userContext, sessionFunctions, hlogBase.bufferPool);
            pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
        }

        private bool TryRevivifyInChain<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref LogRecord logRecord, ref TInput input, ref TOutput output, ref PendingContext<TInput, TOutput, TContext> pendingContext,
                        TSessionFunctionsWrapper sessionFunctions, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, ref RMWInfo rmwInfo, out OperationStatus status)
                    where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (IsFrozen<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, logRecord.Info))
                goto NeedNewRecord;

            // This record is safe to revivify even if its PreviousAddress points to a valid record, because it is revivified for the same key.
            var ok = false;
            try
            {
                var sizeInfo = hlog.GetRMWInitialRecordSize(logRecord.Key, ref input, sessionFunctions);
                ref RevivificationStats stats = ref sessionFunctions.Ctx.RevivificationStats;
                if (logRecord.TrySetContentLengths(in sizeInfo))
                {
                    logRecord.InfoRef.ClearTombstone();
                    logRecord.ClearOptionals();

                    if (sessionFunctions.InitialUpdater(ref logRecord, in sizeInfo, ref input, ref output, ref rmwInfo))
                    {
                        sessionFunctions.PostInitialUpdater(ref logRecord, in sizeInfo, ref input, ref output, ref rmwInfo);

                        // Success
                        MarkPage(stackCtx.recSrc.LogicalAddress, sessionFunctions.Ctx);
                        pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                        pendingContext.eTag = logRecord.ETag;

                        // We "IPU'd" because we reused a tombstone, but since the record we have reused did not logically exist, we must also bubble up that the original key was not found (logically).
                        // OperationStatus.NOTFOUND bubbles up success but also indicates that the record was not found in the database.
                        status = OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.InPlaceUpdatedRecord);
                        stats.inChainSuccesses++;
                        ok = true;
                        return true;
                    }
                }
                // Did not revivify; restore the tombstone in 'finally' and leave the deleted record there.
                stats.inChainFailures++;
            }
            finally
            {
                if (!ok)
                    logRecord.InfoRef.SetTombstone();
            }

        NeedNewRecord:
            // Successful non-revivification; move to CreateNewRecord.
            status = OperationStatus.SUCCESS;
            return false;
        }

        private LatchDestination CheckCPRConsistencyRMW(Phase phase, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, ref OperationStatus status, ref LatchOperation latchOperation)
        {
            // The idea of CPR is that if a thread in version V tries to perform an operation and notices a record in V+1, it needs to back off and run CPR_SHIFT_DETECTED.
            // Similarly, a V+1 thread cannot update a V record; it needs to do a read-copy-update (or upsert at tail) instead of an in-place update.
            // For background info: Prior to HashBucket-based locking, we had to lock the bucket in the following way:
            //  1. V threads take shared lock on bucket
            //  2. V+1 threads take exclusive lock on bucket, refreshing until they can
            //  3. If V thread cannot take shared lock, that means the system is in V+1 so we can immediately refresh and go to V+1 (do CPR_SHIFT_DETECTED)
            //  4. If V thread manages to get shared lock, but encounters a V+1 record, it knows the system is in V+1 so it will do CPR_SHIFT_DETECTED
            // Now we no longer need to do the bucket latching, since we already have a latch on the bucket.

            switch (phase)
            {
                case Phase.PREPARE: // Thread is in V
                    if (!IsEntryVersionNew(ref stackCtx.hei.entry))
                        break; // Normal Processing; thread is in V, record is in V

                    status = OperationStatus.CPR_SHIFT_DETECTED;
                    return LatchDestination.Retry;  // Pivot Thread for retry (do not operate on v+1 record when thread is in V)

                case Phase.IN_PROGRESS: // Thread is in v+1
                case Phase.WAIT_INDEX_CHECKPOINT:
                case Phase.WAIT_FLUSH:
                    if (IsRecordVersionNew(stackCtx.recSrc.LogicalAddress))
                        break;      // Normal Processing; V+1 thread encountered a record in V+1

                    if (stackCtx.recSrc.HasMainLogSrc)
                        return LatchDestination.CreateNewRecord;    // Record is in memory so force creation of a (V+1) record
                    break;  // Normal Processing; the record is below HeadAddress so the operation will go pending

                default:  // Thread is in V
                    break;
            }
            return LatchDestination.NormalProcessing;
        }

        /// <summary>
        /// Create a new record for RMW
        /// </summary>
        /// <param name="key">Key, if inserting a new record.</param>
        /// <param name="srcLogRecord">The source record. If <paramref name="stackCtx"/>.<see cref="RecordSource{TStoreFunctions, TAllocator}.HasInMemorySrc"/>
        /// it is in-memory (either too small or is in readonly region, or is in readcache); otherwise it is from disk IO</param>
        /// <param name="input">Input to the ISessionFunctions operation</param>
        /// <param name="output">The result of ISessionFunctions operation</param>
        /// <param name="pendingContext">Information about the operation context</param>
        /// <param name="sessionFunctions">The current session</param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{TStoreFunctions, TAllocator}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions. If called from pending IO,
        ///     this is populated from the data read from disk.</param>
        /// <param name="doingCU">Whether we are doing a CopyUpdate, either from in-memory or pending IO.</param>
        /// <returns></returns>
        private OperationStatus CreateNewRecordRMW<TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(ReadOnlySpan<byte> key, in TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output,
                                                                                          ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions,
                                                                                          ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, bool doingCU)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            var forExpiration = false;
            var addTombstone = false;

        RetryNow:

            RMWInfo rmwInfo = new()
            {
                Version = sessionFunctions.Ctx.version,
                SessionID = sessionFunctions.Ctx.sessionID,
                Address = doingCU && !stackCtx.recSrc.HasReadCacheSrc ? stackCtx.recSrc.LogicalAddress : kInvalidAddress,
                KeyHash = stackCtx.hei.hash,
            };

            AllocateOptions allocOptions = new()
            {
                recycle = true,
                elideSourceRecord = stackCtx.recSrc.HasMainLogSrc && CanElide<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, srcLogRecord.Info)
            };

            // Perform Need*
            if (doingCU)
            {
                if (!sessionFunctions.NeedCopyUpdate(in srcLogRecord, ref input, ref output, ref rmwInfo))
                {
                    if (rmwInfo.Action == RMWAction.CancelOperation)
                        return OperationStatus.CANCELED;
                    else if (rmwInfo.Action == RMWAction.ExpireAndResume)
                    {
                        doingCU = false;
                        forExpiration = true;
                    }
                    else if (rmwInfo.Action == RMWAction.ExpireAndStop)
                    {
                        if (allocOptions.elideSourceRecord)
                        {
                            srcLogRecord.InfoRef.SetTombstone();
                            srcLogRecord.InfoRef.SetDirtyAndModified();

                            // Elide from hei, and try to either do in-chain tombstoning or free list transfer. srcLogRecord is elidable so must be a memory LogRecord.
                            ref var inMemoryLogRecord = ref srcLogRecord.AsMemoryLogRecordRef();
                            HandleRecordElision<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, ref inMemoryLogRecord);
                            // no new record created and hash entry is empty now
                            return OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.Expired);
                        }
                        // otherwise we shall continue down the tombstoning path
                        addTombstone = true;
                    }
                    else if (rmwInfo.Action == RMWAction.WrongType)
                        return OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.WrongType);
                    else
                        return OperationStatus.SUCCESS;
                }
            }

            if (!doingCU)
            {
                if (!sessionFunctions.NeedInitialUpdate(key, ref input, ref output, ref rmwInfo))
                    return rmwInfo.Action == RMWAction.CancelOperation ? OperationStatus.CANCELED : OperationStatus.NOTFOUND;
            }

            // Allocate and initialize the new record
            RecordSizeInfo sizeInfo;
            if (!addTombstone)
            {
                sizeInfo = doingCU
                    ? hlog.GetRMWCopyRecordSize(in srcLogRecord, ref input, sessionFunctions)
                    : hlog.GetRMWInitialRecordSize(key, ref input, sessionFunctions);
            }
            else
            {
                Debug.Assert(!allocOptions.elideSourceRecord, "Elidable records going down the deletion via RMW path from NCU should have already been handled." +
                    "This block only handles NCU requested deletion for unelidable src records.");
                sizeInfo = hlog.GetDeleteRecordSize(key);
            }

            if (!TryAllocateRecord(sessionFunctions, ref pendingContext, ref stackCtx, ref sizeInfo, allocOptions, out var newLogicalAddress, out var newPhysicalAddress, out var status))
                return status;

            var newLogRecord = WriteNewRecordInfo(key, hlogBase, newLogicalAddress, newPhysicalAddress, in sizeInfo, sessionFunctions.Ctx.InNewVersion, previousAddress: stackCtx.recSrc.LatestLogicalAddress);
            if (allocOptions.elideSourceRecord)
                newLogRecord.InfoRef.PreviousAddress = srcLogRecord.Info.PreviousAddress;
            stackCtx.SetNewRecord(newLogicalAddress);

            rmwInfo.Address = newLogicalAddress;

            if (!doingCU)
            {
                if (sessionFunctions.InitialUpdater(ref newLogRecord, in sizeInfo, ref input, ref output, ref rmwInfo))
                {
                    status = forExpiration
                        ? OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.CreatedRecord | StatusCode.Expired)
                        : OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.CreatedRecord);
                }
                else
                {
                    if (rmwInfo.Action == RMWAction.CancelOperation)
                        return OperationStatus.CANCELED;
                    return OperationStatus.NOTFOUND | (forExpiration ? OperationStatus.EXPIRED : OperationStatus.NOTFOUND);
                }
            }
            else if (!addTombstone)
            {
                if (sessionFunctions.CopyUpdater(in srcLogRecord, ref newLogRecord, in sizeInfo, ref input, ref output, ref rmwInfo))
                {
                    status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.CopyUpdatedRecord);

                    // Do not elide (restore newRecordInfo.PreviousAddress to its original WriteNewRecordInfo state) if requested to preserve the source record.
                    if (rmwInfo.PreserveCopyUpdaterSourceRecord)
                    {
                        allocOptions.elideSourceRecord = false;
                        newLogRecord.InfoRef.PreviousAddress = stackCtx.recSrc.LatestLogicalAddress;
                    }

                    if (rmwInfo.ClearSourceValueObject)
                        srcLogRecord.ClearValueIfHeap(obj => storeFunctions.DisposeValueObject(obj, DisposeReason.CopyUpdated));
                    goto DoCAS;
                }
                if (rmwInfo.Action == RMWAction.CancelOperation)
                {
                    // Save allocation for revivification (not retry, because this is canceling of the current operation), or abandon it if that fails.
                    if (RevivificationManager.UseFreeRecordPool && RevivificationManager.TryAdd(newLogicalAddress, ref newLogRecord, ref sessionFunctions.Ctx.RevivificationStats))
                        stackCtx.ClearNewRecord();
                    else
                        stackCtx.SetNewRecordInvalid(ref newLogRecord.InfoRef);
                    return OperationStatus.CANCELED;
                }
                if (rmwInfo.Action == RMWAction.ExpireAndStop)
                {
                    Debug.Assert(!addTombstone, "Should not have gone down RCU if NCU had already requested tombstoning." +
                        "This block should only handle expiration/tombstoning via RCU.");
                    addTombstone = true;
                    newLogRecord.InfoRef.SetTombstone();
                    newLogRecord.InfoRef.SetDirtyAndModified();
                    status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.CreatedRecord | StatusCode.Expired);
                    goto DoCAS;
                }
                else if (rmwInfo.Action == RMWAction.ExpireAndResume)
                {
                    doingCU = false;
                    forExpiration = true;

                    if (!ReinitializeExpiredRecord<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref newLogRecord, ref input, ref output, ref rmwInfo, newLogicalAddress, sessionFunctions, isIpu: false, out status))
                    {
                        // An IPU was not (or could not) be done. Cancel if requested, else invalidate the allocated record and retry.
                        if (status == OperationStatus.CANCELED)
                            return status;

                        // Save allocation for revivification (not retry, because this may have been false because the record was too small), or abandon it if that fails.
                        stackCtx.SetNewRecordInvalid(ref newLogRecord.InfoRef);
                        if (!RevivificationManager.UseFreeRecordPool || !TryTransferToFreeList<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, newLogicalAddress, ref newLogRecord))
                            DisposeRecord(ref newLogRecord, DisposeReason.InsertAbandoned);
                        goto RetryNow;
                    }
                    addTombstone = newLogRecord.Info.Tombstone;
                    goto DoCAS;
                }
                else if (rmwInfo.Action == RMWAction.WrongType)
                {
                    status = OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.CreatedRecord | StatusCode.Expired);
                    return OperationStatus.NOTFOUND;
                }
                else
                    return OperationStatus.SUCCESS | (forExpiration ? OperationStatus.EXPIRED : OperationStatus.SUCCESS);
            }
            else
            {
                Debug.Assert(!addTombstone, "This block should only be handling tombstoning requests by NCU where the previous record was not elidable.");
                newLogRecord.InfoRef.SetDirtyAndModified();
                newLogRecord.InfoRef.SetTombstone();
                status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.CreatedRecord | StatusCode.Expired);
            }

        DoCAS:
            // The record being cas'd below is going to be the tombstone record in the case of RCU requested tombstone, and NCU tombstoning.
            // For all other cases this is the new computed record after an RMW.
            // Insert the new record by CAS'ing either directly into the hash entry or splicing into the readcache/mainlog boundary.
            var success = CASRecordIntoChain(newLogicalAddress, ref newLogRecord, ref stackCtx);
            if (success)
            {
                PostCopyToTail(in srcLogRecord, ref stackCtx);

                // If IU, status will be NOTFOUND; return that.
                if (!doingCU)
                {
                    // If IU, status will be NOTFOUND. ReinitializeExpiredRecord has many paths but is straightforward so no need to assert here.
                    Debug.Assert(forExpiration || OperationStatus.NOTFOUND == OperationStatusUtils.BasicOpCode(status), $"Expected NOTFOUND but was {status}");
                    if (!addTombstone)
                        sessionFunctions.PostInitialUpdater(ref newLogRecord, in sizeInfo, ref input, ref output, ref rmwInfo);
                }
                else
                {
                    // Else it was a CopyUpdater so call PCU if tombstoning has not been requested by NCU or CU
                    if (!addTombstone)
                    {
                        // If we are not currently taking a checkpoint, we can just clear the old Object (or Overflow) because the new version of the object is
                        // already created in the new record, which has just been successfully CAS'd. Delay actually doing the clearing so PostCopyUpdater can modify
                        // the object (and track sizes) before it is cleared. (If we are called from Pending IO then srcLogRecord will be a DiskLogRecord and we
                        // do not need to serialize data as this is not involved in checkpointing, and the DiskLogRecord is Disposed after we return up the Pending chain.)
                        var isMemoryLogRecord = srcLogRecord.IsMemoryLogRecord;
                        if (srcLogRecord.Info.ValueIsObject && isMemoryLogRecord)
                            srcLogRecord.ValueObject.CacheSerializedObjectData(ref srcLogRecord.AsMemoryLogRecordRef(), ref newLogRecord, ref rmwInfo);
                        var pcuSuccess = sessionFunctions.PostCopyUpdater(in srcLogRecord, ref newLogRecord, in sizeInfo, ref input, ref output, ref rmwInfo);
                        if (pcuSuccess)
                        {
                            if (!newLogRecord.Info.IsInNewVersion && isMemoryLogRecord)
                                srcLogRecord.AsMemoryLogRecordRef().ClearValueIfHeap(obj => storeFunctions.DisposeValueObject(obj, DisposeReason.CopyUpdated));
                        }
                        else if (rmwInfo.Action == RMWAction.ExpireAndStop)
                        {
                            newLogRecord.InfoRef.SetTombstone();
                            status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.CopyUpdatedRecord | StatusCode.Expired);
                        }
                        else if (rmwInfo.Action == RMWAction.WrongType)
                        {
                            status = OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.CopyUpdatedRecord | StatusCode.Expired);
                            goto Done;
                        }
                    }

                    // ElideSourceRecord means we have verified that the old source record is elidable and now that CAS has replaced it in the HashBucketEntry with
                    // the new source record that does not point to the old source record, we have elided it, so try to transfer to freelist.
                    if (allocOptions.elideSourceRecord)
                    {
                        // Success should always Seal the old record. This may be readcache, readonly, or the temporary recordInfo, which is OK and saves the cost of an "if".
                        srcLogRecord.InfoRef.SealAndInvalidate();    // The record was elided, so Invalidate

                        // If we're here we have MainLogSrc so AsLogRecord is a simple reassignment guaranteed to succeed.
                        ref var inMemoryLogRecord = ref srcLogRecord.AsMemoryLogRecordRef();
                        if (stackCtx.recSrc.LogicalAddress >= GetMinRevivifiableAddress())
                            _ = TryTransferToFreeList<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, stackCtx.recSrc.LogicalAddress, ref inMemoryLogRecord);
                        else
                            DisposeRecord(ref inMemoryLogRecord, DisposeReason.Elided);
                    }
                    else if (stackCtx.recSrc.HasMainLogSrc)
                        srcLogRecord.InfoRef.Seal();              // The record was not elided, so do not Invalidate
                }

            Done:
                stackCtx.ClearNewRecord();
                pendingContext.logicalAddress = newLogicalAddress;
                pendingContext.eTag = newLogRecord.ETag;
                return status;
            }

            // CAS failed
            stackCtx.SetNewRecordInvalid(ref newLogRecord.InfoRef);
            DisposeRecord(ref newLogRecord, doingCU ? DisposeReason.CopyUpdaterCASFailed : DisposeReason.InitialUpdaterCASFailed);

            SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress);
            return OperationStatus.RETRY_NOW;   // CAS failure does not require epoch refresh
        }

        internal bool ReinitializeExpiredRecord<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref LogRecord logRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo,
                                                                                       long logicalAddress, TSessionFunctionsWrapper sessionFunctions, bool isIpu, out OperationStatus status)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // This is called for InPlaceUpdater or CopyUpdater only; CopyUpdater however does not copy an expired record, so we return CreatedRecord.
            var advancedStatusCode = isIpu ? StatusCode.InPlaceUpdatedRecord : StatusCode.CreatedRecord;
            advancedStatusCode |= StatusCode.Expired;
            if (!sessionFunctions.NeedInitialUpdate(logRecord.Key, ref input, ref output, ref rmwInfo))
            {
                if (rmwInfo.Action == RMWAction.CancelOperation)
                {
                    status = OperationStatus.CANCELED;
                    return false;
                }

                // Expiration with no insertion.
                logRecord.InfoRef.SetTombstone();
                status = OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, advancedStatusCode);
                return true;
            }

            // Try to reinitialize in place
            var sizeInfo = hlog.GetRMWInitialRecordSize(logRecord.Key, ref input, sessionFunctions);

            if (logRecord.TryReinitializeValueLength(in sizeInfo))
            {
                if (sessionFunctions.InitialUpdater(ref logRecord, in sizeInfo, ref input, ref output, ref rmwInfo))
                {
                    // If IPU path, we need to complete PostInitialUpdater as well
                    if (isIpu)
                        sessionFunctions.PostInitialUpdater(ref logRecord, in sizeInfo, ref input, ref output, ref rmwInfo);

                    status = OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, advancedStatusCode);
                    return true;
                }
                else
                {
                    if (rmwInfo.Action == RMWAction.CancelOperation)
                    {
                        status = OperationStatus.CANCELED;
                        return false;
                    }
                    else
                    {
                        // Expiration with no insertion.
                        logRecord.InfoRef.SetTombstone();
                        status = OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, advancedStatusCode);
                        return true;
                    }
                }
            }

            // Reinitialization in place was not possible. InternalRMW will do the following based on who called this:
            //  IPU: move to the NIU->allocate->IU path
            //  CU: caller invalidates allocation, retries operation as NIU->allocate->IU
            status = OperationStatus.SUCCESS;
            return false;
        }
    }
}