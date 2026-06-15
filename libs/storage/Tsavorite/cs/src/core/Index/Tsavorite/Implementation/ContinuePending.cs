// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>
        /// Continue a pending read operation. Computes 'output' from 'input' and value corresponding to 'key'
        /// obtained from disk. Optionally, it copies the value to tail to serve future read/write requests quickly.
        /// </summary>
        /// <param name="request">Async response from disk.</param>
        /// <param name="pendingContext">Pending context corresponding to operation.</param>
        /// <param name="sessionFunctions">Callback functions.</param>
        /// <returns>
        /// <list type = "table" >
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The output has been computed and stored in 'output'.</term>
        ///     </item>
        /// </list>
        /// </returns>
        internal OperationStatus ContinuePendingRead<TInput, TOutput, TContext, TSessionFunctionsWrapper>(AsyncIOContext request,
                                                        ref PendingContext<TInput, TOutput, TContext> pendingContext,
                                                        ref PendingIoSlot<TInput, TOutput, TContext> slot,
                                                        TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (request.logicalAddress < hlogBase.BeginAddress || request.logicalAddress < slot.minAddress)
                goto NotFound;

            if (pendingContext.IsReadAtAddress && !pendingContext.IsNoKey && !storeFunctions.KeysEqual(slot.DiskLogRecord, slot.requestKey))
                goto NotFound;

            SpinWaitUntilClosed(request.logicalAddress);
            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(storeFunctions.GetKeyHashCode64(slot.DiskLogRecord));

            while (true)
            {
                if (!FindTagAndTryEphemeralSLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out var status))
                {
                    Debug.Assert(status != OperationStatus.NOTFOUND, "Expected to FindTag in InternalContinuePendingRead");
                    if (HandleImmediateRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(status, sessionFunctions, ref pendingContext))
                        continue;
                    return status;
                }

                stackCtx.SetRecordSourceToHashEntry(hlogBase);

                try
                {
                    // During the pending operation, a record for the key may have been added to the log or readcache. Don't look for this if we are reading at address (rather than key).
                    LogRecord memoryRecord = default;
                    if (!pendingContext.IsReadAtAddress)
                    {
                        if (TryFindRecordInMemory(slot.DiskLogRecord, ref stackCtx, ref pendingContext))
                        {
                            memoryRecord = stackCtx.recSrc.CreateLogRecord();
                            if (memoryRecord.Info.Tombstone)
                                goto NotFound;

                            // V threads cannot access V+1 records. Use the latest logical address rather than the traced address (logicalAddress) per comments in AcquireCPRLatchRMW.
                            if (sessionFunctions.Ctx.phase == Phase.PREPARE && IsEntryVersionNew(ref stackCtx.hei.entry))
                                return OperationStatus.CPR_SHIFT_DETECTED; // Pivot thread; retry
                        }
                        else
                        {
                            // We didn't find a record for the key in memory, but if recSrc.LogicalAddress (which is the .PreviousAddress of the lowest record
                            // above InitialLatestLogicalAddress we could reach) is > InitialLatestLogicalAddress, then it means InitialLatestLogicalAddress is
                            // now below HeadAddress and there is at least one record below HeadAddress but above InitialLatestLogicalAddress. Reissue the Read(),
                            // using the LogicalAddress we just found as minAddress. We will either find an in-memory version of the key that was added after the
                            // TryFindRecordInMemory we just did, or do IO and find the record we just found or one above it. Read() updates InitialLatestLogicalAddress,
                            // so if we do IO, the next time we come to CompletePendingRead we will only search for a newer version of the key in any records added
                            // after our just-completed TryFindRecordInMemory.
                            if (stackCtx.recSrc.LogicalAddress > pendingContext.initialLatestLogicalAddress
                                && (!slot.HasMinAddress || stackCtx.recSrc.LogicalAddress >= slot.minAddress))
                            {
                                // Re-issue InternalRead. If it goes pending again, CreatePendingReadContext needs a fresh op
                                // to hold the slot (the current op is being drained). Rent a fresh op and MOVE the populated
                                // slot to it so the heap-owning fields (requestKey, input, diskLogRecord) transfer without
                                // re-allocation. Clear this slot so the drain does not double-dispose.
                                //
                                // We set newOp.slot but NOT newOp.basePendingContext here: the slot must be in place BEFORE
                                // the InternalRead call so CreatePendingReadContext finds a populated slot (and the call can
                                // read newOp.slot.input/etc. by ref). The basePendingContext, by contrast, is snapshotted by
                                // HandleOperationStatus's RECORD_ON_DISK branch AFTER the Internal call has mutated
                                // pendingContext (logicalAddress, initialEntryAddress, initialLatestLogicalAddress).
                                // Setting it here would just be overwritten with a stale value.
                                var newOp = sessionFunctions.Ctx.RentAsyncIOContext();
                                newOp.slot = slot;
                                slot = default;
                                pendingContext.pendingOp = newOp;

                                OperationStatus internalStatus;
                                do
                                {
                                    internalStatus = InternalRead(newOp.slot.DiskLogRecord, newOp.slot.keyHash, ref newOp.slot.input.Get(), ref newOp.slot.output,
                                        newOp.slot.userContext, ref pendingContext, sessionFunctions, isFromPending: true);
                                }
                                while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pendingContext));

                                // If the re-issued read did NOT go pending (it succeeded in-memory or returned NOTFOUND), move the
                                // slot back to the caller's slot so the completion callback / completedOutputs.TransferFrom can read
                                // its key/input/output. Return the unused fresh op to the pool. For RECORD_ON_DISK (which
                                // HandleOperationStatus already consumed via newOp), newOp's slot now lives on a different in-flight
                                // op, so there's nothing to move back.
                                if (internalStatus != OperationStatus.RECORD_ON_DISK)
                                {
                                    slot = newOp.slot;
                                    newOp.slot = default;
                                    sessionFunctions.Ctx.ReturnAsyncIOContext(newOp);
                                }
                                return internalStatus;
                            }
                        }
                    }

                    ReadInfo readInfo = new()
                    {
                        Version = sessionFunctions.Ctx.version,
                        Address = request.logicalAddress,
                        IsFromPending = true,
                    };
                    pendingContext.logicalAddress = request.logicalAddress;

                    if (!memoryRecord.IsSet && slot.diskLogRecord.Info.Tombstone)
                    {
                        if (pendingContext.IsReadAtAddress)
                        {
                            // Be consistent with InternalReadAtAddress and return the tombstoned record we retrieved from disk.
                            _ = sessionFunctions.Reader(in slot.diskLogRecord, ref slot.input.Get(), ref slot.output, ref readInfo);
                        }
                        goto NotFound;
                    }

                    var success = false;
                    if (stackCtx.recSrc.HasMainLogSrc && stackCtx.recSrc.LogicalAddress >= hlogBase.ReadOnlyAddress)
                    {
                        // If this succeeds, we don't need to copy to tail or readcache, so return success.
                        if (sessionFunctions.Reader(in memoryRecord, ref slot.input.Get(), ref slot.output, ref readInfo))
                        {
                            pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                            return OperationStatus.SUCCESS;
                        }
                    }
                    else if (memoryRecord.IsSet)
                    {
                        // This may be in the immutable region, which means it may be an updated version of the record.
                        success = sessionFunctions.Reader(in memoryRecord, ref slot.input.Get(), ref slot.output, ref readInfo);
                    }
                    else // Not found in memory so return the disk copy.
                        success = sessionFunctions.Reader(in slot.diskLogRecord, ref slot.input.Get(), ref slot.output, ref readInfo);

                    if (!success)
                    {
                        if (readInfo.Action == ReadAction.CancelOperation)
                            return OperationStatus.CANCELED;
                        if (readInfo.Action == ReadAction.Expire)
                            return OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.Expired);
                        if (readInfo.Action == ReadAction.WrongType)
                            return OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.WrongType);
                        goto NotFound;
                    }

                    // See if we are copying to read cache or tail of log. If we already found the record in memory, we're done.
                    if (pendingContext.readCopyOptions.CopyFrom != ReadCopyFrom.None && !memoryRecord.IsSet)
                    {
                        if (pendingContext.readCopyOptions.CopyTo == ReadCopyTo.MainLog)
                        {
                            // Plumb source logical address so PostCopyToTail can name per-flush snapshot files.
                            pendingContext.originalAddress = request.logicalAddress;
                            status = ConditionalCopyToTail(sessionFunctions, ref pendingContext, slot.keyHash, in slot.diskLogRecord, ref stackCtx);
                        }
                        else if (pendingContext.readCopyOptions.CopyTo == ReadCopyTo.ReadCache && !stackCtx.recSrc.HasReadCacheSrc
                                && TryCopyToReadCache(in slot.diskLogRecord, sessionFunctions, ref pendingContext, ref stackCtx))
                            status |= OperationStatus.COPIED_RECORD_TO_READ_CACHE;
                    }
                    else
                    {
                        return OperationStatus.SUCCESS;
                    }
                }
                finally
                {
                    stackCtx.HandleNewRecordOnException(this);
                    EphemeralSUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx);
                }

                // Must do this *after* Unlocking. Status was set by InternalTryCopyToTail.
                if (!HandleImmediateRetryStatus(status, sessionFunctions, ref pendingContext))
                    return status;
            } // end while (true)

        NotFound:
            return OperationStatus.NOTFOUND;
        }

        /// <summary>
        /// Continue a pending RMW operation with the record retrieved from disk.
        /// </summary>
        /// <param name="request">record read from the disk.</param>
        /// <param name="pendingContext">internal context for the pending RMW operation</param>
        /// <param name="sessionFunctions">Callback functions.</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully updated(or inserted).</term>
        ///     </item>
        ///     <item>
        ///     <term>RECORD_ON_DISK</term>
        ///     <term>We need to issue an IO to continue.</term>
        ///     </item>
        /// </list>
        /// </returns>
        internal OperationStatus ContinuePendingRMW<TInput, TOutput, TContext, TSessionFunctionsWrapper>(AsyncIOContext request,
                                                ref PendingContext<TInput, TOutput, TContext> pendingContext,
                                                ref PendingIoSlot<TInput, TOutput, TContext> slot,
                                                TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var keyFound = request.logicalAddress >= hlogBase.BeginAddress && request.logicalAddress >= slot.minAddress;
            if (keyFound)
                SpinWaitUntilClosed(request.logicalAddress);

            RMWInfo rmwInfo = new()
            {
                Version = sessionFunctions.Ctx.version,
                SessionID = sessionFunctions.Ctx.sessionID
            };

            OperationStatus status;

            while (true)
            {
                OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(slot.keyHash);
                if (!FindOrCreateTagAndTryEphemeralXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out status))
                    goto CheckRetry;

                try
                {
                    // During the pending operation a record for the key may have been added to the log. If so, break and go through the full InternalRMW sequence;
                    // the record in 'request' is stale. We only lock for tag-chain stability during search.
                    if (TryFindRecordForPendingOperation(slot.requestKey, ref stackCtx, out status, ref pendingContext))
                    {
                        if (status != OperationStatus.SUCCESS)
                            goto CheckRetry;
                        break;
                    }

                    // We didn't find a record for the key in memory, but if recSrc.LogicalAddress (which is the .PreviousAddress of the lowest record
                    // above InitialLatestLogicalAddress we could reach) is > InitialLatestLogicalAddress, then it means InitialLatestLogicalAddress is
                    // now below HeadAddress and there is at least one record below HeadAddress but above InitialLatestLogicalAddress. We must do InternalRMW.
                    if (stackCtx.recSrc.LogicalAddress > pendingContext.initialLatestLogicalAddress)
                    {
                        Debug.Assert(pendingContext.initialLatestLogicalAddress < hlogBase.HeadAddress, "Failed to search all in-memory records");
                        break;
                    }

                    // Here, the input data for 'doingCU' comes from the request, so populate the RecordSource copy from that.
                    stackCtx.recSrc.LogicalAddress = request.logicalAddress;
                    status = CreateNewRecordRMW(slot.requestKey, in slot.diskLogRecord, ref slot.input.Get(), ref slot.output,
                                                ref pendingContext, sessionFunctions, ref stackCtx, doingCU: keyFound && !slot.diskLogRecord.Info.Tombstone, ref rmwInfo);
                }
                finally
                {
                    stackCtx.HandleNewRecordOnException(this);
                    try
                    {
                        sessionFunctions.PostRMWOperation(slot.requestKey, ref slot.input.Get(), ref rmwInfo, epoch);
                    }
                    finally
                    {
                        EphemeralXUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx);
                    }
                }

                // Must do this *after* Unlocking.
            CheckRetry:
                if (!HandleImmediateRetryStatus(status, sessionFunctions, ref pendingContext))
                    return status;
            } // end while (true)

            // Unfortunately, InternalRMW will go through the lookup process again. But we're only here in the case another record was added or we went below
            // HeadAddress, and this should be rare. If the re-issued InternalRMW goes pending again, we move the slot to a fresh op so the heap-owning fields
            // transfer cleanly (the current op is mid-drain; clearing this slot avoids double-dispose).
            //
            // We set newOp.slot but NOT newOp.basePendingContext here: the slot must be in place BEFORE the InternalRMW call
            // so CreatePendingRMWContext finds a populated slot (and the call can read newOp.slot.input/etc. by ref). The
            // basePendingContext, by contrast, is snapshotted by HandleOperationStatus's RECORD_ON_DISK branch AFTER the
            // Internal call has mutated pendingContext (logicalAddress, initialEntryAddress, initialLatestLogicalAddress).
            // Setting it here would just be overwritten with a stale value.
            var newOp = sessionFunctions.Ctx.RentAsyncIOContext();
            newOp.slot = slot;
            slot = default;
            pendingContext.pendingOp = newOp;
            do
                status = InternalRMW(newOp.slot.requestKey, newOp.slot.keyHash, ref newOp.slot.input.Get(), ref newOp.slot.output, ref newOp.slot.userContext, ref pendingContext, sessionFunctions);
            while (HandleImmediateRetryStatus(status, sessionFunctions, ref pendingContext));

            // If the re-issued RMW did NOT go pending again (it succeeded in-memory), move the slot back to the caller's slot so
            // the completion callback / completedOutputs.TransferFrom can read its key/input/output. Return the unused fresh op
            // to the pool. For RECORD_ON_DISK (which HandleOperationStatus already consumed via newOp), newOp's slot now lives
            // on a different in-flight op, so there's nothing to move back.
            if (status != OperationStatus.RECORD_ON_DISK)
            {
                slot = newOp.slot;
                newOp.slot = default;
                sessionFunctions.Ctx.ReturnAsyncIOContext(newOp);
            }
            return status;
        }

        /// <summary>
        /// Continue a pending CONDITIONAL_INSERT operation with the record retrieved from disk, checking whether a record for this key was
        /// added since we went pending; in that case this operation must be adjusted to use current data.
        /// </summary>
        /// <param name="request">record read from the disk.</param>
        /// <param name="pendingContext">internal context for the pending RMW operation</param>
        /// <param name="sessionFunctions">Callback functions.</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully inserted, or was found above the specified address.</term>
        ///     </item>
        ///     <item>
        ///     <term>RECORD_ON_DISK</term>
        ///     <term>We need to issue an IO to continue.</term>
        ///     </item>
        /// </list>
        /// </returns>
        internal OperationStatus ContinuePendingConditionalCopyToTail<TInput, TOutput, TContext, TSessionFunctionsWrapper>(AsyncIOContext request,
                                                ref PendingContext<TInput, TOutput, TContext> pendingContext,
                                                ref PendingIoSlot<TInput, TOutput, TContext> slot,
                                                TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // If the key was found at or above minAddress, do nothing.
            // If we're here we know the key matches because AllocatorBase.AsyncGetFromDiskCallback skips colliding keys by following the .PreviousAddress chain.
            if (request.logicalAddress >= slot.minAddress)
                return OperationStatus.SUCCESS;

            // Prepare to copy to tail. Use data from slot, not request; we're only made it to this line if the key was not found, and thus the request was not populated.
            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(slot.keyHash);

            // See if the record was added above the highest address we checked before issuing the IO.
            var minAddress = pendingContext.initialLatestLogicalAddress + 1;
            OperationStatus internalStatus;
            do
            {
                if (TryFindRecordInMainLogForConditionalOperation<ConditionallyHoistedKey, TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, slot.requestKey, ref stackCtx,
                        currentAddress: request.logicalAddress, minAddress, slot.maxAddress, out internalStatus, out var needIO))
                    return OperationStatus.SUCCESS;
                if (!OperationStatusUtils.IsRetry(internalStatus))
                {
                    // Plumb source logical address so PostCopyToTail can name per-flush snapshot files.
                    // request.logicalAddress is the actual disk-resolved source (AsyncGetFromDiskCallback
                    // walks the chain to skip colliding keys).
                    pendingContext.originalAddress = request.logicalAddress;
                    if (needIO)
                    {
                        // We need a fresh op to carry the slot for the next IO (this op is mid-drain). Move the slot.
                        //
                        // We set newOp.slot but NOT newOp.basePendingContext here: the slot must be in place BEFORE the
                        // PrepareIOForConditionalOperation call so it finds a populated slot (and reads its requestKey /
                        // diskLogRecord). The basePendingContext, by contrast, is snapshotted by HandleOperationStatus's
                        // RECORD_ON_DISK branch AFTER the prepare call has mutated pendingContext (originalAddress,
                        // initialLatestLogicalAddress, logicalAddress). Setting it here would just be overwritten with a stale value.
                        var newOp = sessionFunctions.Ctx.RentAsyncIOContext();
                        newOp.slot = slot;
                        slot = default;
                        pendingContext.pendingOp = newOp;
                        // HeadAddress may have risen above minAddress; if so, we need IO.
                        internalStatus = PrepareIOForConditionalOperation(sessionFunctions, ref pendingContext, newOp.slot.keyHash, in newOp.slot.diskLogRecord, ref stackCtx, minAddress, newOp.slot.maxAddress);
                    }
                    else
                    {
                        internalStatus = ConditionalCopyToTail(sessionFunctions, ref pendingContext, slot.keyHash, in slot.diskLogRecord, ref stackCtx);
                    }
                }
            }
            while (sessionFunctions.Store.HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(internalStatus, sessionFunctions));
            return internalStatus;
        }

        /// <summary>
        /// Continue a pending CONDITIONAL_SCAN_PUSH operation with the record retrieved from disk, checking whether a record for this key was
        /// added since we went pending; in that case this operation "fails" as it finds the record.
        /// </summary>
        /// <param name="request">record read from the disk.</param>
        /// <param name="pendingContext">internal context for the pending RMW operation</param>
        /// <param name="sessionFunctions">Callback functions.</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully inserted, or was found above the specified address.</term>
        ///     </item>
        ///     <item>
        ///     <term>RECORD_ON_DISK</term>
        ///     <term>We need to issue an IO to continue.</term>
        ///     </item>
        /// </list>
        /// </returns>
        internal OperationStatus ContinuePendingConditionalScanPush<TInput, TOutput, TContext, TSessionFunctionsWrapper>(AsyncIOContext request,
                                                ref PendingContext<TInput, TOutput, TContext> pendingContext,
                                                ref PendingIoSlot<TInput, TOutput, TContext> slot,
                                                TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // If the key was found at or above minAddress, do nothing; we'll push it when we get to it. If we flagged the iteration to stop, do nothing.
            // If we're here we know the key matches because AllocatorBase.AsyncGetFromDiskCallback skips colliding keys by following the .PreviousAddress chain.
            if (request.logicalAddress >= slot.minAddress || slot.scanCursorState.stop)
                return OperationStatus.SUCCESS;

            // Prepare to push to caller's iterator functions. Use data from slot, not request; we're only made it to this line if the key was not found,
            // and thus the request was not populated. The new minAddress should be the highest logicalAddress we previously saw, because we need to make sure the
            // record was not added to the log after we initialized the pending IO.
            _ = hlogBase.ConditionalScanPush<TInput, TOutput, TContext, TSessionFunctionsWrapper, DiskLogRecord>(sessionFunctions, slot.scanCursorState,
                in slot.diskLogRecord, originalAddress: pendingContext.originalAddress, currentAddress: request.logicalAddress,
                minAddress: pendingContext.initialLatestLogicalAddress + 1, maxAddress: slot.maxAddress);

            // ConditionalScanPush has already called HandleOperationStatus, so return SUCCESS here.
            return OperationStatus.SUCCESS;
        }
    }
}