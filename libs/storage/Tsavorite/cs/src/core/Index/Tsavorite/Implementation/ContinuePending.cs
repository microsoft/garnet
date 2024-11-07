// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        /// <summary>
        /// Continue a pending read operation. Computes 'output' from 'input' and value corresponding to 'key'
        /// obtained from disk. Optionally, it copies the value to tail to serve future read/write requests quickly.
        /// </summary>
        /// <param name="request">Async response from disk.</param>
        /// <param name="pendingContext">Pending context corresponding to operation.</param>
        /// <param name="sessionFunctions">Callback functions.</param>
        /// <param name="kernelSession">The kernel session (basic or dual)</param>
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
        internal OperationStatus ContinuePendingRead<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKernelSession, TKeyLocker>(AsyncIOContext<TKey, TValue> request,
                                                        ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions, ref TKernelSession kernelSession)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, IKeyLocker
        {
            ref var srcRecordInfo = ref hlog.GetInfoFromBytePointer(request.record.GetValidPointer());
            srcRecordInfo.ClearBitsForDiskImages();

            if (request.logicalAddress >= hlogBase.BeginAddress && request.logicalAddress >= pendingContext.minAddress)
            {
                SpinWaitUntilClosed(request.logicalAddress);

                // If NoKey, we do not have the key in the initial call and must use the key from the satisfied request.
                ref var key = ref pendingContext.NoKey ? ref hlog.GetContextRecordKey(ref request) : ref pendingContext.key.Get();

                while (true)
                {
                    HashEntryInfo hei = default;
                    OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(ref hei);
                    OperationStatus status = default;

                    try
                    {
                        var kstatus = Kernel.UnsafeEnterForRead<TKernelSession, TKeyLocker>(ref kernelSession, storeFunctions.GetKeyHashCode64(ref key), partitionId, out hei);
                        Debug.Assert(kstatus.Found, "Expected to FindTag in InternalContinuePendingRead");
                        stackCtx.SetRecordSourceToHashEntry(hlogBase);

                        ref var value = ref hlog.GetContextRecordValue(ref request);

                        // During the pending operation, a record for the key may have been added to the log or readcache. Don't look for this if we are reading at address (rather than key).
                        if (!pendingContext.IsReadAtAddress)
                        {
                            if (TryFindRecordInMemory(ref key, ref stackCtx, ref pendingContext))
                            {
                                srcRecordInfo = ref stackCtx.recSrc.GetInfo();

                                // V threads cannot access V+1 records. Use the latest logical address rather than the traced address (logicalAddress) per comments in AcquireCPRLatchRMW.
                                if (sessionFunctions.ExecutionCtx.phase == Phase.PREPARE && IsEntryVersionNew(ref stackCtx.hei.entry))
                                    return OperationStatus.CPR_SHIFT_DETECTED; // Pivot thread; retry
                                value = ref stackCtx.recSrc.GetValue();
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
                                if (stackCtx.recSrc.LogicalAddress > pendingContext.InitialLatestLogicalAddress
                                    && (!pendingContext.HasMinAddress || stackCtx.recSrc.LogicalAddress >= pendingContext.minAddress))
                                {
                                    OperationStatus internalStatus;
                                    do
                                    {
                                        internalStatus = InternalRead<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(ref stackCtx, ref key, ref pendingContext.input.Get(), ref pendingContext.output,
                                            pendingContext.userContext, ref pendingContext, sessionFunctions);
                                    }
                                    while (HandleImmediateRetryStatus<TInput, TOutput, TContext, TKeyLocker>(ref stackCtx.hei, internalStatus, sessionFunctions.ExecutionCtx, ref pendingContext));
                                    return internalStatus;
                                }
                            }
                        }

                        if (srcRecordInfo.Tombstone)
                            goto NotFound;

                        ReadInfo readInfo = new()
                        {
                            Version = sessionFunctions.ExecutionCtx.version,
                            Address = request.logicalAddress,
                            IsFromPending = pendingContext.type != OperationType.NONE,
                        };
                        readInfo.SetRecordInfo(ref srcRecordInfo);

                        var success = false;
                        if (stackCtx.recSrc.HasMainLogSrc && stackCtx.recSrc.LogicalAddress >= hlogBase.ReadOnlyAddress)
                        {
                            // If this succeeds, we don't need to copy to tail or readcache, so return success.
                            if (sessionFunctions.ConcurrentReader(ref key, ref pendingContext.input.Get(), ref value, ref pendingContext.output, ref readInfo, ref srcRecordInfo))
                                return OperationStatus.SUCCESS;
                        }
                        else
                        {
                            // This may be in the immutable region, which means it may be an updated version of the record.
                            success = sessionFunctions.SingleReader(ref key, ref pendingContext.input.Get(), ref value, ref pendingContext.output, ref readInfo);
                        }

                        if (!success)
                        {
                            pendingContext.recordInfo = srcRecordInfo;
                            if (readInfo.Action == ReadAction.CancelOperation)
                                return OperationStatus.CANCELED;
                            if (readInfo.Action == ReadAction.Expire)
                                return OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.Expired);
                            goto NotFound;
                        }

                        // See if we are copying to read cache or tail of log. If we are copying to readcache but already found the record in the readcache, we're done.
                        if (pendingContext.readCopyOptions.CopyFrom != ReadCopyFrom.None)
                        {
                            if (pendingContext.readCopyOptions.CopyTo == ReadCopyTo.MainLog)
                                status = ConditionalCopyToTail<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(sessionFunctions, ref pendingContext, ref key, ref pendingContext.input.Get(), ref value, ref pendingContext.output,
                                                               pendingContext.userContext, ref stackCtx, WriteReason.CopyToTail);
                            else if (pendingContext.readCopyOptions.CopyTo == ReadCopyTo.ReadCache && !stackCtx.recSrc.HasReadCacheSrc
                                    && TryCopyToReadCache(sessionFunctions, ref pendingContext, ref key, ref pendingContext.input.Get(), ref value, ref stackCtx))
                                status |= OperationStatus.COPIED_RECORD_TO_READ_CACHE;
                        }
                        else
                        {
                            pendingContext.recordInfo = srcRecordInfo;
                            return OperationStatus.SUCCESS;
                        }
                    }
                    finally
                    {
                        stackCtx.HandleNewRecordOnException(this);
                        TKeyLocker.UnlockTransientExclusive(Kernel, ref hei);       // Epoch management is done above this
                    }

                    // Must do this *after* Unlocking. Status was set by InternalTryCopyToTail.
                    if (!HandleImmediateRetryStatus<TInput, TOutput, TContext, TKeyLocker>(ref hei, status, sessionFunctions.ExecutionCtx, ref pendingContext))
                        return status;
                } // end while (true)
            }

        NotFound:
            pendingContext.recordInfo = srcRecordInfo;
            return OperationStatus.NOTFOUND;
        }

        /// <summary>
        /// Continue a pending RMW operation with the record retrieved from disk.
        /// </summary>
        /// <param name="request">record read from the disk.</param>
        /// <param name="pendingContext">internal context for the pending RMW operation</param>
        /// <param name="sessionFunctions">Callback functions.</param>
        /// <param name="kernelSession">The kernel session (basic or dual)</param>
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
        internal OperationStatus ContinuePendingRMW<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKernelSession, TKeyLocker>(AsyncIOContext<TKey, TValue> request,
                                                ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions, ref TKernelSession kernelSession)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, IKeyLocker
        {
            ref var key = ref pendingContext.key.Get();

            SpinWaitUntilClosed(request.logicalAddress);

            var recordPointer = request.record.GetValidPointer();
            var requestRecordInfo = hlog.GetInfoFromBytePointer(recordPointer); // Not ref, as we don't want to write into request.record
            ref var srcRecordInfo = ref requestRecordInfo;
            srcRecordInfo.ClearBitsForDiskImages();

            OperationStatus status = default;
            HashEntryInfo hei = default;
            OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(ref hei);

            while (true)
            {

                try
                {
                    var kstatus = Kernel.UnsafeEnterForUpdate<TKernelSession, TKeyLocker>(ref kernelSession, storeFunctions.GetKeyHashCode64(ref key), partitionId, hlogBase.BeginAddress, out hei);
                    Debug.Assert(kstatus.Found, "Expected to FindTag in InternalContinuePendingRead");
                    stackCtx.SetRecordSourceToHashEntry(hlogBase);

                    // During the pending operation a record for the key may have been added to the log. If so, break and go through the full InternalRMW sequence;
                    // the record in 'request' is stale. We only lock for tag-chain stability during search.
                    if (TryFindRecordForPendingOperation(ref key, ref stackCtx, hlogBase.HeadAddress, out status, ref pendingContext))
                    {
                        if (status != OperationStatus.SUCCESS)
                            goto CheckRetry;
                        break;
                    }

                    // We didn't find a record for the key in memory, but if recSrc.LogicalAddress (which is the .PreviousAddress of the lowest record
                    // above InitialLatestLogicalAddress we could reach) is > InitialLatestLogicalAddress, then it means InitialLatestLogicalAddress is
                    // now below HeadAddress and there is at least one record below HeadAddress but above InitialLatestLogicalAddress. We must do InternalRMW.
                    if (stackCtx.recSrc.LogicalAddress > pendingContext.InitialLatestLogicalAddress)
                    {
                        Debug.Assert(pendingContext.InitialLatestLogicalAddress < hlogBase.HeadAddress, "Failed to search all in-memory records");
                        break;
                    }

                    // Here, the input data for 'doingCU' is from the request, so populate the RecordSource copy from that, preserving LowestReadCache*.
                    stackCtx.recSrc.LogicalAddress = request.logicalAddress;
                    stackCtx.recSrc.PhysicalAddress = (long)recordPointer;

                    status = CreateNewRecordRMW(ref key, ref pendingContext.input.Get(), ref hlog.GetContextRecordValue(ref request), ref pendingContext.output,
                                                ref pendingContext, sessionFunctions, ref stackCtx, ref srcRecordInfo,
                                                doingCU: request.logicalAddress >= hlogBase.BeginAddress && !srcRecordInfo.Tombstone);
                }
                finally
                {
                    stackCtx.HandleNewRecordOnException(this);
                    TKeyLocker.UnlockTransientExclusive(Kernel, ref hei);
                }

            // Must do this *after* Unlocking.
            CheckRetry:
                if (!HandleImmediateRetryStatus<TInput, TOutput, TContext, TKeyLocker>(ref hei, status, sessionFunctions.ExecutionCtx, ref pendingContext))
                    return status;
            } // end while (true)

            // Unfortunately, InternalRMW will go through the lookup process again. But we're only here in the case another record was added or we went below
            // HeadAddress, and this should be rare.
            do
                status = InternalRMW<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(ref stackCtx, ref key, ref pendingContext.input.Get(), ref pendingContext.output,
                        pendingContext.userContext, ref pendingContext, sessionFunctions);
            while (HandleImmediateRetryStatus<TInput, TOutput, TContext, TKeyLocker>(ref hei, status, sessionFunctions.ExecutionCtx, ref pendingContext));
            return status;
        }

        /// <summary>
        /// Continue a pending CONDITIONAL_INSERT operation with the record retrieved from disk, checking whether a record for this key was
        /// added since we went pending; in that case this operation must be adjusted to use current data.
        /// </summary>
        /// <param name="request">record read from the disk.</param>
        /// <param name="pendingContext">internal context for the pending RMW operation</param>
        /// <param name="sessionFunctions">Callback functions.</param>
        /// <param name="kernelSession">The kernel session (basic or dual)</param>
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
        internal OperationStatus ContinuePendingConditionalCopyToTail<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKernelSession, TKeyLocker>(AsyncIOContext<TKey, TValue> request,
                                                ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions, ref TKernelSession kernelSession)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, IKeyLocker
        {
            // If the key was found at or above minAddress, do nothing.
            if (request.logicalAddress >= pendingContext.minAddress)
                return OperationStatus.SUCCESS;

            // Prepare to copy to tail. Use data from pendingContext, not request; we're only made it to this line if the key was not found, and thus the request was not populated.
            // Locking is done, if necessary, in ConditionalCopyToTail
            ref var key = ref pendingContext.key.Get();
            HashEntryInfo hei = new(storeFunctions.GetKeyHashCode64(ref key), partitionId);
            OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(ref hei);

            // See if the record was added above the highest address we checked before issuing the IO.
            var minAddress = pendingContext.InitialLatestLogicalAddress + 1;
            OperationStatus internalStatus;
            do
            {
                if (TryFindRecordInMainLogForConditionalOperation<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref key, ref stackCtx, currentAddress: request.logicalAddress, minAddress, out internalStatus, out var needIO))
                    return OperationStatus.SUCCESS;
                if (!OperationStatusUtils.IsRetry(internalStatus))
                {
                    // HeadAddress may have risen above minAddress; if so, we need IO.
                    internalStatus = needIO
                        ? PrepareIOForConditionalOperation(sessionFunctions, ref pendingContext, ref key, ref pendingContext.input.Get(), ref pendingContext.value.Get(),
                                                            ref pendingContext.output, pendingContext.userContext, ref stackCtx, minAddress, WriteReason.Compaction)
                        : ConditionalCopyToTail<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(sessionFunctions, ref pendingContext, ref key, ref pendingContext.input.Get(), ref pendingContext.value.Get(),
                                                            ref pendingContext.output, pendingContext.userContext, ref stackCtx, pendingContext.writeReason);
                }
            }
            while (sessionFunctions.Store.HandleImmediateNonPendingRetryStatus(ref hei, internalStatus, sessionFunctions.ExecutionCtx));
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
        internal OperationStatus ContinuePendingConditionalScanPush<TInput, TOutput, TContext, TSessionFunctionsWrapper>(AsyncIOContext<TKey, TValue> request,
                                                ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // If the key was found at or above minAddress, do nothing; we'll push it when we get to it. If we flagged the iteration to stop, do nothing.
            if (request.logicalAddress >= pendingContext.minAddress || pendingContext.scanCursorState.stop)
                return OperationStatus.SUCCESS;

            // Prepare to push to caller's iterator functions. Use data from pendingContext, not request; we're only made it to this line if the key was not found,
            // and thus the request was not populated. The new minAddress should be the highest logicalAddress we previously saw, because we need to make sure the
            // record was not added to the log after we initialized the pending IO.
            hlogBase.ConditionalScanPush<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, pendingContext.scanCursorState, pendingContext.recordInfo, ref pendingContext.key.Get(), ref pendingContext.value.Get(),
                currentAddress: request.logicalAddress, minAddress: pendingContext.InitialLatestLogicalAddress + 1);

            // ConditionalScanPush has already called HandleOperationStatus, so return SUCCESS here.
            return OperationStatus.SUCCESS;
        }
    }
}