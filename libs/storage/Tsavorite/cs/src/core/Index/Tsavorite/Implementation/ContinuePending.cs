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
                                                        ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (request.logicalAddress < hlogBase.BeginAddress || request.logicalAddress < pendingContext.minAddress)
                goto NotFound;

            if (pendingContext.IsReadAtAddress && !pendingContext.IsNoKey && !storeFunctions.KeysEqual(pendingContext.Key, pendingContext.requestKey.Get()))
                goto NotFound;

            SpinWaitUntilClosed(request.logicalAddress);
            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(storeFunctions.GetKeyHashCode64(pendingContext.Key));

            while (true)
            {
                if (!FindTagAndTryEphemeralSLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out var status))
                {
                    Debug.Assert(status != OperationStatus.NOTFOUND, "Expected to FindTag in InternalContinuePendingRead");
                    if (HandleImmediateRetryStatus(status, sessionFunctions, ref pendingContext))
                        continue;
                    return status;
                }

                stackCtx.SetRecordSourceToHashEntry(hlogBase);
                pendingContext.eTag = pendingContext.diskLogRecord.ETag;

                try
                {
                    // During the pending operation, a record for the key may have been added to the log or readcache. Don't look for this if we are reading at address (rather than key).
                    LogRecord memoryRecord = default;
                    if (!pendingContext.IsReadAtAddress)
                    {
                        if (TryFindRecordInMemory(pendingContext.Key, ref stackCtx, ref pendingContext))
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
                                && (!pendingContext.HasMinAddress || stackCtx.recSrc.LogicalAddress >= pendingContext.minAddress))
                            {
                                OperationStatus internalStatus;
                                do
                                {
                                    internalStatus = InternalRead(pendingContext.Key, pendingContext.keyHash, ref pendingContext.input.Get(), ref pendingContext.output,
                                        pendingContext.userContext, ref pendingContext, sessionFunctions);
                                }
                                while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pendingContext));
                                return internalStatus;
                            }
                        }
                    }

                    if (!memoryRecord.IsSet && pendingContext.diskLogRecord.Info.Tombstone)
                        goto NotFound;

                    ReadInfo readInfo = new()
                    {
                        Version = sessionFunctions.Ctx.version,
                        Address = request.logicalAddress,
                        IsFromPending = pendingContext.type != OperationType.NONE,
                    };
                    pendingContext.logicalAddress = request.logicalAddress;

                    var success = false;
                    if (stackCtx.recSrc.HasMainLogSrc && stackCtx.recSrc.LogicalAddress >= hlogBase.ReadOnlyAddress)
                    {
                        // If this succeeds, we don't need to copy to tail or readcache, so return success.
                        if (sessionFunctions.Reader(in memoryRecord, ref pendingContext.input.Get(), ref pendingContext.output, ref readInfo))
                        {
                            pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                            return OperationStatus.SUCCESS;
                        }
                    }
                    else if (memoryRecord.IsSet)
                    {
                        // This may be in the immutable region, which means it may be an updated version of the record.
                        success = sessionFunctions.Reader(in memoryRecord, ref pendingContext.input.Get(), ref pendingContext.output, ref readInfo);
                    }
                    else
                        success = sessionFunctions.Reader(in pendingContext.diskLogRecord, ref pendingContext.input.Get(), ref pendingContext.output, ref readInfo);

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
                            status = ConditionalCopyToTail(sessionFunctions, ref pendingContext, in pendingContext.diskLogRecord, ref stackCtx);
                        else if (pendingContext.readCopyOptions.CopyTo == ReadCopyTo.ReadCache && !stackCtx.recSrc.HasReadCacheSrc
                                && TryCopyToReadCache(in pendingContext.diskLogRecord, sessionFunctions, ref pendingContext, ref stackCtx))
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
                                                ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var keyFound = request.logicalAddress >= hlogBase.BeginAddress && request.logicalAddress >= pendingContext.minAddress;
            if (keyFound)
                SpinWaitUntilClosed(request.logicalAddress);

            OperationStatus status;

            while (true)
            {
                OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(pendingContext.keyHash);
                if (!FindOrCreateTagAndTryEphemeralXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out status))
                    goto CheckRetry;

                try
                {
                    // During the pending operation a record for the key may have been added to the log. If so, break and go through the full InternalRMW sequence;
                    // the record in 'request' is stale. We only lock for tag-chain stability during search.
                    if (TryFindRecordForPendingOperation(pendingContext.requestKey.Get(), ref stackCtx, out status, ref pendingContext))
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

                    // Here, the input data for 'doingCU' is the from the request, so populate the RecordSource copy from that, preserving LowestReadCache*.
                    stackCtx.recSrc.LogicalAddress = request.logicalAddress;
                    status = CreateNewRecordRMW(pendingContext.requestKey.Get(), in pendingContext.diskLogRecord, ref pendingContext.input.Get(), ref pendingContext.output,
                                                ref pendingContext, sessionFunctions, ref stackCtx, doingCU: keyFound && !pendingContext.diskLogRecord.Info.Tombstone);
                }
                finally
                {
                    stackCtx.HandleNewRecordOnException(this);
                    EphemeralXUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx);
                }

            // Must do this *after* Unlocking.
            CheckRetry:
                if (!HandleImmediateRetryStatus(status, sessionFunctions, ref pendingContext))
                    return status;
            } // end while (true)

            // Unfortunately, InternalRMW will go through the lookup process again. But we're only here in the case another record was added or we went below
            // HeadAddress, and this should be rare.
            do
                status = InternalRMW(pendingContext.requestKey.Get(), pendingContext.keyHash, ref pendingContext.input.Get(), ref pendingContext.output, ref pendingContext.userContext, ref pendingContext, sessionFunctions);
            while (HandleImmediateRetryStatus(status, sessionFunctions, ref pendingContext));
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
                                                ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // If the key was found at or above minAddress, do nothing.
            // If we're here we know the key matches because AllocatorBase.AsyncGetFromDiskCallback skips colliding keys by following the .PreviousAddress chain.
            if (request.logicalAddress >= pendingContext.minAddress)
                return OperationStatus.SUCCESS;

            // Prepare to copy to tail. Use data from pendingContext, not request; we're only made it to this line if the key was not found, and thus the request was not populated.
            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(pendingContext.keyHash);

            // See if the record was added above the highest address we checked before issuing the IO.
            var minAddress = pendingContext.initialLatestLogicalAddress + 1;
            OperationStatus internalStatus;
            do
            {
                if (TryFindRecordInMainLogForConditionalOperation<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, pendingContext.requestKey.Get(), ref stackCtx,
                        currentAddress: request.logicalAddress, minAddress, pendingContext.maxAddress, out internalStatus, out var needIO))
                    return OperationStatus.SUCCESS;
                if (!OperationStatusUtils.IsRetry(internalStatus))
                {
                    // HeadAddress may have risen above minAddress; if so, we need IO.
                    internalStatus = needIO
                        ? PrepareIOForConditionalOperation(ref pendingContext, in pendingContext.diskLogRecord, ref stackCtx, minAddress, pendingContext.maxAddress)
                        : ConditionalCopyToTail(sessionFunctions, ref pendingContext, in pendingContext.diskLogRecord, ref stackCtx);
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
                                                ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // If the key was found at or above minAddress, do nothing; we'll push it when we get to it. If we flagged the iteration to stop, do nothing.
            // If we're here we know the key matches because AllocatorBase.AsyncGetFromDiskCallback skips colliding keys by following the .PreviousAddress chain.
            if (request.logicalAddress >= pendingContext.minAddress || pendingContext.scanCursorState.stop)
                return OperationStatus.SUCCESS;

            // Prepare to push to caller's iterator functions. Use data from pendingContext, not request; we're only made it to this line if the key was not found,
            // and thus the request was not populated. The new minAddress should be the highest logicalAddress we previously saw, because we need to make sure the
            // record was not added to the log after we initialized the pending IO.
            _ = hlogBase.ConditionalScanPush<TInput, TOutput, TContext, TSessionFunctionsWrapper, DiskLogRecord>(sessionFunctions,
                pendingContext.scanCursorState, in pendingContext.diskLogRecord, currentAddress: request.logicalAddress, minAddress: pendingContext.initialLatestLogicalAddress + 1, maxAddress: pendingContext.maxAddress);

            // ConditionalScanPush has already called HandleOperationStatus, so return SUCCESS here.
            return OperationStatus.SUCCESS;
        }
    }
}