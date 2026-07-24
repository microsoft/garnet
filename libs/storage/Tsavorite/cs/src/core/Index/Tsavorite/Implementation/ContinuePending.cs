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
        /// <param name="operationState">Pending context corresponding to operation.</param>
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
                                                        ref OperationState<TInput, TOutput, TContext> operationState,
                                                        ref PendingState<TInput, TOutput, TContext> pendingState,
                                                        TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (request.logicalAddress < hlogBase.BeginAddress || request.logicalAddress < pendingState.minAddress)
                goto NotFound;

            if (operationState.IsReadAtAddress && !operationState.IsNoKey && !storeFunctions.KeysEqual(pendingState.DiskLogRecord, pendingState.requestKey))
                goto NotFound;

            SpinWaitUntilClosed(request.logicalAddress);
            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(storeFunctions.GetKeyHashCode64(pendingState.DiskLogRecord));

            while (true)
            {
                if (!FindTagAndTryEphemeralSLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out var status))
                {
                    Debug.Assert(status != OperationStatus.NOTFOUND, "Expected to FindTag in InternalContinuePendingRead");
                    if (HandleImmediateRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(status, sessionFunctions, ref operationState))
                        continue;
                    return status;
                }

                stackCtx.SetRecordSourceToHashEntry(hlogBase);

                try
                {
                    // During the pending operation, a record for the key may have been added to the log or readcache. Don't look for this if we are reading at address (rather than key).
                    LogRecord memoryRecord = default;
                    if (!operationState.IsReadAtAddress)
                    {
                        if (TryFindRecordInMemory(pendingState.DiskLogRecord, ref stackCtx, ref operationState))
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
                            // TryFindRecordInMemory we just did, or do IO and find the record we just found or one above it. Read() updates InitialLatestLogicalAddress
                            // if we do IO, in which case the next time we come to CompletePendingRead we will only search for a newer version of the key in any records
                            // added after our just-completed TryFindRecordInMemory.
                            if (stackCtx.recSrc.LogicalAddress > operationState.initialLatestLogicalAddress
                                && (!pendingState.HasMinAddress || stackCtx.recSrc.LogicalAddress >= pendingState.minAddress))
                            {
                                // Re-issue InternalRead. If it goes pending again, CreatePendingReadContext needs a fresh op
                                // to hold the pendingState (the current op is being drained). Rent a fresh op and MOVE the populated
                                // pendingState to it so the heap-owning fields (requestKey, input, diskLogRecord) transfer without
                                // re-allocation. Clear this pendingState so the drain does not double-dispose.
                                //
                                // We set newOp.pendingState but NOT newOp.baseOperationState here: the pendingState must be in place BEFORE
                                // the InternalRead call so CreatePendingReadContext finds a populated pendingState (and the call can
                                // read newOp.pendingState.input/etc. by ref). The baseOperationState, by contrast, is snapshotted by
                                // HandleOperationStatus's RECORD_ON_DISK branch AFTER the Internal call has mutated
                                // operationState (logicalAddress, initialEntryAddress, initialLatestLogicalAddress).
                                // Setting it here would just be overwritten with a stale value.
                                var newOp = sessionFunctions.Ctx.RentPendingIoContext();
                                newOp.pendingState = pendingState;
                                pendingState = default;
                                operationState.pendingOp = newOp;

                                OperationStatus internalStatus;
                                do
                                {
                                    internalStatus = InternalRead(newOp.pendingState.DiskLogRecord, newOp.pendingState.keyHash, ref newOp.pendingState.input.Get(), ref newOp.pendingState.output,
                                        newOp.pendingState.userContext, ref operationState, sessionFunctions, isFromPending: true);
                                }
                                while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref operationState));

                                // If the re-issued read did NOT go pending (it succeeded in-memory or returned NOTFOUND), move the
                                // pendingState back to the caller's pendingState so the completion callback / completedOutputs.TransferFrom can read
                                // its key/input/output. Return the unused fresh op to the pool. For RECORD_ON_DISK (which
                                // HandleOperationStatus already consumed via newOp), newOp's pendingState now lives on a different in-flight
                                // op, so there's nothing to move back.
                                if (internalStatus != OperationStatus.RECORD_ON_DISK)
                                {
                                    pendingState = newOp.pendingState;
                                    newOp.pendingState = default;
                                    operationState.pendingOp = null;
                                    sessionFunctions.Ctx.ReturnPendingIoContext(newOp);
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
                    operationState.logicalAddress = request.logicalAddress;

                    if (!memoryRecord.IsSet && pendingState.diskLogRecord.Info.Tombstone)
                    {
                        if (operationState.IsReadAtAddress)
                        {
                            // Be consistent with InternalReadAtAddress and return the tombstoned record we retrieved from disk.
                            _ = sessionFunctions.Reader(in pendingState.diskLogRecord, ref pendingState.input.Get(), ref pendingState.output, ref readInfo);
                        }
                        goto NotFound;
                    }

                    var success = false;
                    if (stackCtx.recSrc.HasMainLogSrc && stackCtx.recSrc.LogicalAddress >= hlogBase.ReadOnlyAddress)
                    {
                        // If this succeeds, we don't need to copy to tail or readcache, so return success.
                        if (sessionFunctions.Reader(in memoryRecord, ref pendingState.input.Get(), ref pendingState.output, ref readInfo))
                        {
                            operationState.logicalAddress = stackCtx.recSrc.LogicalAddress;
                            return OperationStatus.SUCCESS;
                        }
                    }
                    else if (memoryRecord.IsSet)
                    {
                        // This may be in the immutable region, which means it may be an updated version of the record.
                        success = sessionFunctions.Reader(in memoryRecord, ref pendingState.input.Get(), ref pendingState.output, ref readInfo);
                    }
                    else // Not found in memory so return the disk copy.
                        success = sessionFunctions.Reader(in pendingState.diskLogRecord, ref pendingState.input.Get(), ref pendingState.output, ref readInfo);

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
                    if (operationState.readCopyOptions.CopyFrom != ReadCopyFrom.None && !memoryRecord.IsSet)
                    {
                        // status is already OperationStatus.SUCCESS here: FindTagAndTryEphemeralSLock (at the top of the loop) returns true
                        // only after TryEphemeralSLock has set status to SUCCESS, and nothing below changes it before this point.
                        if (operationState.readCopyOptions.CopyTo == ReadCopyTo.MainLog)
                        {
                            // Plumb source logical address so PostCopyToTail can name per-flush snapshot files.
                            operationState.originalAddress = request.logicalAddress;
                            status = ConditionalCopyToTail(sessionFunctions, ref operationState, pendingState.keyHash, in pendingState.diskLogRecord, ref stackCtx);
                        }
                        else if (operationState.readCopyOptions.CopyTo == ReadCopyTo.ReadCache && !stackCtx.recSrc.HasReadCacheSrc
                                && TryCopyToReadCache(in pendingState.diskLogRecord, sessionFunctions, ref operationState, ref stackCtx))
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
                if (!HandleImmediateRetryStatus(status, sessionFunctions, ref operationState))
                    return status;
            } // end while (true)

        NotFound:
            return OperationStatus.NOTFOUND;
        }

        /// <summary>
        /// Continue a pending RMW operation with the record retrieved from disk.
        /// </summary>
        /// <param name="request">record read from the disk.</param>
        /// <param name="operationState">internal context for the pending RMW operation</param>
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
                                                ref OperationState<TInput, TOutput, TContext> operationState,
                                                ref PendingState<TInput, TOutput, TContext> pendingState,
                                                TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var keyFound = request.logicalAddress >= hlogBase.BeginAddress && request.logicalAddress >= pendingState.minAddress;
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
                OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(pendingState.keyHash);
                if (!FindOrCreateTagAndTryEphemeralXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out status))
                    goto CheckRetry;

                try
                {
                    // During the pending operation a record for the key may have been added to the log. If so, break and go through the full InternalRMW sequence;
                    // the record in 'request' is stale. We only lock for tag-chain stability during search.
                    if (TryFindRecordForPendingOperation(pendingState.requestKey, ref stackCtx, out status, ref operationState))
                    {
                        if (status != OperationStatus.SUCCESS)
                            goto CheckRetry;
                        break;
                    }

                    // We didn't find a record for the key in memory, but if recSrc.LogicalAddress (which is the .PreviousAddress of the lowest record
                    // above InitialLatestLogicalAddress we could reach) is > InitialLatestLogicalAddress, then it means InitialLatestLogicalAddress is
                    // now below HeadAddress and there is at least one record below HeadAddress but above InitialLatestLogicalAddress. We must do InternalRMW.
                    if (stackCtx.recSrc.LogicalAddress > operationState.initialLatestLogicalAddress)
                    {
                        Debug.Assert(operationState.initialLatestLogicalAddress < hlogBase.HeadAddress, "Failed to search all in-memory records");
                        break;
                    }

                    // Here, the input data for 'doingCU' comes from the request, so populate the RecordSource copy from that.
                    stackCtx.recSrc.LogicalAddress = request.logicalAddress;
                    status = CreateNewRecordRMW(pendingState.requestKey, in pendingState.diskLogRecord, ref pendingState.input.Get(), ref pendingState.output,
                                                ref operationState, sessionFunctions, ref stackCtx, doingCU: keyFound && !pendingState.diskLogRecord.Info.Tombstone, ref rmwInfo);
                }
                finally
                {
                    stackCtx.HandleNewRecordOnException(this);
                    try
                    {
                        sessionFunctions.PostRMWOperation(pendingState.requestKey, ref pendingState.input.Get(), ref rmwInfo, epoch);
                    }
                    finally
                    {
                        EphemeralXUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx);
                    }
                }

                // Must do this *after* Unlocking.
            CheckRetry:
                if (!HandleImmediateRetryStatus(status, sessionFunctions, ref operationState))
                    return status;
            } // end while (true)

            // Unfortunately, InternalRMW will go through the lookup process again. But we're only here in the case another record was added or we went below
            // HeadAddress, and this should be rare. If the re-issued InternalRMW goes pending again, we move the pendingState to a fresh op so the heap-owning fields
            // transfer cleanly (the current op is mid-drain; clearing this pendingState avoids double-dispose).
            //
            // We set newOp.pendingState but NOT newOp.baseOperationState here: the pendingState must be in place BEFORE the InternalRMW call
            // so CreatePendingRMWContext finds a populated pendingState (and the call can read newOp.pendingState.input/etc. by ref). The
            // baseOperationState, by contrast, is snapshotted by HandleOperationStatus's RECORD_ON_DISK branch AFTER the
            // Internal call has mutated operationState (logicalAddress, initialEntryAddress, initialLatestLogicalAddress).
            // Setting it here would just be overwritten with a stale value.
            var newOp = sessionFunctions.Ctx.RentPendingIoContext();
            newOp.pendingState = pendingState;
            pendingState = default;
            operationState.pendingOp = newOp;
            do
                status = InternalRMW(newOp.pendingState.requestKey, newOp.pendingState.keyHash, ref newOp.pendingState.input.Get(), ref newOp.pendingState.output, ref newOp.pendingState.userContext, ref operationState, sessionFunctions);
            while (HandleImmediateRetryStatus(status, sessionFunctions, ref operationState));

            // If the re-issued RMW did NOT go pending again (it succeeded in-memory), move the pendingState back to the caller's pendingState so
            // the completion callback / completedOutputs.TransferFrom can read its key/input/output. Return the unused fresh op
            // to the pool. For RECORD_ON_DISK (which HandleOperationStatus already consumed via newOp), newOp's pendingState now lives
            // on a different in-flight op, so there's nothing to move back.
            if (status != OperationStatus.RECORD_ON_DISK)
            {
                pendingState = newOp.pendingState;
                newOp.pendingState = default;
                operationState.pendingOp = null;
                sessionFunctions.Ctx.ReturnPendingIoContext(newOp);
            }
            return status;
        }

        /// <summary>
        /// Continue a pending CONDITIONAL_INSERT operation with the record retrieved from disk, checking whether a record for this key was
        /// added since we went pending; in that case this operation must be adjusted to use current data.
        /// </summary>
        /// <param name="request">record read from the disk.</param>
        /// <param name="operationState">internal context for the pending RMW operation</param>
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
                                                ref OperationState<TInput, TOutput, TContext> operationState,
                                                ref PendingState<TInput, TOutput, TContext> pendingState,
                                                TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // If the key was found at or above minAddress, do nothing.
            // If we're here we know the key matches because AllocatorBase.AsyncGetFromDiskCallback skips colliding keys by following the .PreviousAddress chain.
            if (request.logicalAddress >= pendingState.minAddress)
                return OperationStatus.SUCCESS;

            // Prepare to copy to tail. Use data from pendingState, not request; we're only made it to this line if the key was not found, and thus the request was not populated.
            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(pendingState.keyHash);

            // See if the record was added above the highest address we checked before issuing the IO.
            var minAddress = operationState.initialLatestLogicalAddress + 1;
            OperationStatus internalStatus;
            do
            {
                if (TryFindRecordInMainLogForConditionalOperation<ConditionallyHoistedKey, TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, pendingState.requestKey, ref stackCtx,
                        currentAddress: request.logicalAddress, minAddress, pendingState.maxAddress, out internalStatus, out var needIO))
                    return OperationStatus.SUCCESS;
                if (!OperationStatusUtils.IsRetry(internalStatus))
                {
                    // Plumb source logical address so PostCopyToTail can name per-flush snapshot files.
                    // request.logicalAddress is the actual disk-resolved source (AsyncGetFromDiskCallback
                    // walks the chain to skip colliding keys).
                    operationState.originalAddress = request.logicalAddress;
                    if (needIO)
                    {
                        // We need a fresh op to carry the pendingState for the next IO (this op is mid-drain). Move the pendingState.
                        //
                        // We set newOp.pendingState but NOT newOp.baseOperationState here: the pendingState must be in place BEFORE the
                        // PrepareIOForConditionalOperation call so it finds a populated pendingState (and reads its requestKey /
                        // diskLogRecord). The baseOperationState, by contrast, is snapshotted by HandleOperationStatus's
                        // RECORD_ON_DISK branch AFTER the prepare call has mutated operationState (originalAddress,
                        // initialLatestLogicalAddress, logicalAddress). Setting it here would just be overwritten with a stale value.
                        var newOp = sessionFunctions.Ctx.RentPendingIoContext();
                        newOp.pendingState = pendingState;
                        pendingState = default;
                        operationState.pendingOp = newOp;
                        // HeadAddress may have risen above minAddress; if so, we need IO.
                        internalStatus = PrepareIOForConditionalOperation(sessionFunctions, ref operationState, newOp.pendingState.keyHash, in newOp.pendingState.diskLogRecord, ref stackCtx, minAddress, newOp.pendingState.maxAddress);
                    }
                    else
                    {
                        internalStatus = ConditionalCopyToTail(sessionFunctions, ref operationState, pendingState.keyHash, in pendingState.diskLogRecord, ref stackCtx);
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
        /// <param name="operationState">internal context for the pending RMW operation</param>
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
                                                ref OperationState<TInput, TOutput, TContext> operationState,
                                                ref PendingState<TInput, TOutput, TContext> pendingState,
                                                TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // If the key was found at or above minAddress, do nothing; we'll push it when we get to it. If we flagged the iteration to stop, do nothing.
            // If we're here we know the key matches because AllocatorBase.AsyncGetFromDiskCallback skips colliding keys by following the .PreviousAddress chain.
            if (request.logicalAddress >= pendingState.minAddress || pendingState.scanCursorState.stop)
                return OperationStatus.SUCCESS;

            // Prepare to push to caller's iterator functions. Use data from pendingState, not request; we're only made it to this line if the key was not found,
            // and thus the request was not populated. The new minAddress should be the highest logicalAddress we previously saw, because we need to make sure the
            // record was not added to the log after we initialized the pending IO.
            _ = hlogBase.ConditionalScanPush<TInput, TOutput, TContext, TSessionFunctionsWrapper, DiskLogRecord>(sessionFunctions, pendingState.scanCursorState,
                in pendingState.diskLogRecord, originalAddress: operationState.originalAddress, currentAddress: request.logicalAddress,
                minAddress: operationState.initialLatestLogicalAddress + 1, maxAddress: pendingState.maxAddress);

            // ConditionalScanPush has already called HandleOperationStatus, so return SUCCESS here.
            return OperationStatus.SUCCESS;
        }
    }
}