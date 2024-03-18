// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<Key, Value> : TsavoriteBase
    {
        /// <summary>
        /// Continue a pending read operation. Computes 'output' from 'input' and value corresponding to 'key'
        /// obtained from disk. Optionally, it copies the value to tail to serve future read/write requests quickly.
        /// </summary>
        /// <param name="request">Async response from disk.</param>
        /// <param name="pendingContext">Pending context corresponding to operation.</param>
        /// <param name="tsavoriteSession">Callback functions.</param>
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
        internal OperationStatus ContinuePendingRead<Input, Output, Context, TsavoriteSession>(AsyncIOContext<Key, Value> request,
                                                        ref PendingContext<Input, Output, Context> pendingContext, TsavoriteSession tsavoriteSession)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            ref RecordInfo srcRecordInfo = ref hlog.GetInfoFromBytePointer(request.record.GetValidPointer());
            srcRecordInfo.ClearBitsForDiskImages();

            if (request.logicalAddress >= hlog.BeginAddress && request.logicalAddress >= pendingContext.minAddress)
            {
                SpinWaitUntilClosed(request.logicalAddress);

                // If NoKey, we do not have the key in the initial call and must use the key from the satisfied request.
                ref Key key = ref pendingContext.NoKey ? ref hlog.GetContextRecordKey(ref request) : ref pendingContext.key.Get();
                OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));

                while (true)
                {
                    if (!FindTagAndTryTransientSLock<Input, Output, Context, TsavoriteSession>(tsavoriteSession, ref key, ref stackCtx, out var status))
                    {
                        if (HandleImmediateRetryStatus(status, tsavoriteSession, ref pendingContext))
                            continue;
                        return status;
                    }

                    if (!FindTag(ref stackCtx.hei))
                        Debug.Fail("Expected to FindTag in InternalContinuePendingRead");
                    stackCtx.SetRecordSourceToHashEntry(hlog);

                    try
                    {
                        // During the pending operation, a record for the key may have been added to the log or readcache.
                        ref var value = ref hlog.GetContextRecordValue(ref request);
                        bool found, needsRevivCheck = TracebackNeedsRevivCheck(ref stackCtx);
                        if (needsRevivCheck)
                        {
                            var minAddress = pendingContext.InitialLatestLogicalAddress < hlog.HeadAddress ? hlog.HeadAddress : pendingContext.InitialLatestLogicalAddress + 1;
                            found = TryFindRecordForRead(ref key, ref stackCtx, minAddress, out status);
                            if (!found && status != OperationStatus.SUCCESS)
                            {
                                if (HandleImmediateRetryStatus(status, tsavoriteSession, ref pendingContext))
                                    continue;
                                return status;
                            }
                        }
                        else
                        {
                            found = TryFindRecordInMemory(ref key, ref stackCtx, ref pendingContext);
                        }

                        if (found)
                        {
                            srcRecordInfo = ref stackCtx.recSrc.GetInfo();
                            if (!needsRevivCheck && DoRecordIsolation && stackCtx.recSrc.HasMainLogSrc && !stackCtx.recSrc.TryLockShared(ref srcRecordInfo))
                            {
                                HandleImmediateRetryStatus(OperationStatus.RETRY_LATER, tsavoriteSession, ref pendingContext);
                                continue;
                            }

                            // V threads cannot access V+1 records. Use the latest logical address rather than the traced address (logicalAddress) per comments in AcquireCPRLatchRMW.
                            if (tsavoriteSession.Ctx.phase == Phase.PREPARE && IsEntryVersionNew(ref stackCtx.hei.entry))
                                return OperationStatus.CPR_SHIFT_DETECTED; // Pivot thread; retry
                            value = ref stackCtx.recSrc.GetValue();
                        }
                        if (srcRecordInfo.Tombstone)
                            goto NotFound;

                        ReadInfo readInfo = new()
                        {
                            Version = tsavoriteSession.Ctx.version,
                            Address = request.logicalAddress,
                        };
                        readInfo.SetRecordInfo(ref srcRecordInfo);

                        bool success = false;
                        if (stackCtx.recSrc.HasMainLogSrc && stackCtx.recSrc.LogicalAddress >= hlog.ReadOnlyAddress)
                        {
                            // If this succeeds, we don't need to copy to tail or readcache, so return success.
                            if (tsavoriteSession.ConcurrentReader(ref key, ref pendingContext.input.Get(), ref value, ref pendingContext.output, ref readInfo, ref srcRecordInfo))
                                return OperationStatus.SUCCESS;
                        }
                        else
                        {
                            // This may be in the immutable region, which means it may be an updated version of the record.
                            success = tsavoriteSession.SingleReader(ref key, ref pendingContext.input.Get(), ref value, ref pendingContext.output, ref readInfo);
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
                                status = ConditionalCopyToTail(tsavoriteSession, ref pendingContext, ref key, ref pendingContext.input.Get(), ref value, ref pendingContext.output,
                                                               pendingContext.userContext, pendingContext.serialNum, ref stackCtx, WriteReason.CopyToTail);
                            else if (pendingContext.readCopyOptions.CopyTo == ReadCopyTo.ReadCache && !stackCtx.recSrc.HasReadCacheSrc
                                    && TryCopyToReadCache(tsavoriteSession, ref pendingContext, ref key, ref pendingContext.input.Get(), ref value, ref stackCtx))
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
                        if (!TransientSUnlock<Input, Output, Context, TsavoriteSession>(tsavoriteSession, ref key, ref stackCtx))
                            stackCtx.recSrc.UnlockShared(ref srcRecordInfo, hlog.HeadAddress);
                    }

                    // Must do this *after* Unlocking. Status was set by InternalTryCopyToTail.
                    if (!HandleImmediateRetryStatus(status, tsavoriteSession, ref pendingContext))
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
        /// <param name="tsavoriteSession">Callback functions.</param>
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
        internal OperationStatus ContinuePendingRMW<Input, Output, Context, TsavoriteSession>(AsyncIOContext<Key, Value> request,
                                                ref PendingContext<Input, Output, Context> pendingContext, TsavoriteSession tsavoriteSession)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            ref Key key = ref pendingContext.key.Get();

            SpinWaitUntilClosed(request.logicalAddress);

            byte* recordPointer = request.record.GetValidPointer();
            var requestRecordInfo = hlog.GetInfoFromBytePointer(recordPointer); // Not ref, as we don't want to write into request.record
            ref var srcRecordInfo = ref requestRecordInfo;
            srcRecordInfo.ClearBitsForDiskImages();

            OperationStatus status;

            while (true)
            {
                OperationStackContext<Key, Value> stackCtx = new(pendingContext.keyHash);
                if (!FindOrCreateTagAndTryTransientXLock<Input, Output, Context, TsavoriteSession>(tsavoriteSession, ref key, ref stackCtx, out status))
                    goto CheckRetry;

                try
                {
                    // During the pending operation a record for the key may have been added to the log. If so, break and go through the full InternalRMW sequence;
                    // the record in 'request' is stale. RecordIsolation locking is not done here; if we find a source record we don't lock--we go to InternalRMW.
                    // So we only do LockTable-based locking for tag-chain stability during search.
                    if (TryFindRecordForPendingOperation(ref key, ref stackCtx, hlog.HeadAddress, out status, ref pendingContext))
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
                        Debug.Assert(pendingContext.InitialLatestLogicalAddress < hlog.HeadAddress, "Failed to search all in-memory records");
                        break;
                    }

                    // Here, the input data for 'doingCU' is the from the request, so populate the RecordSource copy from that, preserving LowestReadCache*.
                    stackCtx.recSrc.LogicalAddress = request.logicalAddress;
                    stackCtx.recSrc.PhysicalAddress = (long)recordPointer;

                    status = CreateNewRecordRMW(ref key, ref pendingContext.input.Get(), ref hlog.GetContextRecordValue(ref request), ref pendingContext.output,
                                                ref pendingContext, tsavoriteSession, ref stackCtx, ref srcRecordInfo,
                                                doingCU: request.logicalAddress >= hlog.BeginAddress && !srcRecordInfo.Tombstone);
                }
                finally
                {
                    stackCtx.HandleNewRecordOnException(this);
                    TransientXUnlock<Input, Output, Context, TsavoriteSession>(tsavoriteSession, ref key, ref stackCtx);
                    // Do not do RecordIsolation unlock here, as we did not lock it above:  stackCtx.recSrc.UnlockExclusive(ref srcRecordInfo, hlog.HeadAddress);
                }

            // Must do this *after* Unlocking.
            CheckRetry:
                if (!HandleImmediateRetryStatus(status, tsavoriteSession, ref pendingContext))
                    return status;
            } // end while (true)

            // Unfortunately, InternalRMW will go through the lookup process again. But we're only here in the case another record was added or we went below
            // HeadAddress, and this should be rare.
            do
                status = InternalRMW(ref key, pendingContext.keyHash, ref pendingContext.input.Get(), ref pendingContext.output, ref pendingContext.userContext, ref pendingContext, tsavoriteSession, pendingContext.serialNum);
            while (HandleImmediateRetryStatus(status, tsavoriteSession, ref pendingContext));
            return status;
        }

        /// <summary>
        /// Continue a pending CONDITIONAL_INSERT operation with the record retrieved from disk, checking whether a record for this key was
        /// added since we went pending; in that case this operation must be adjusted to use current data.
        /// </summary>
        /// <param name="request">record read from the disk.</param>
        /// <param name="pendingContext">internal context for the pending RMW operation</param>
        /// <param name="tsavoriteSession">Callback functions.</param>
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
        internal OperationStatus ContinuePendingConditionalCopyToTail<Input, Output, Context, TsavoriteSession>(AsyncIOContext<Key, Value> request,
                                                ref PendingContext<Input, Output, Context> pendingContext, TsavoriteSession tsavoriteSession)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            // If the key was found at or above minAddress, do nothing.
            if (request.logicalAddress >= pendingContext.minAddress)
                return OperationStatus.SUCCESS;

            // Prepare to copy to tail. Use data from pendingContext, not request; we're only made it to this line if the key was not found, and thus the request was not populated.
            ref Key key = ref pendingContext.key.Get();
            OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));

            // See if the record was added above the highest address we checked before issuing the IO.
            var minAddress = pendingContext.InitialLatestLogicalAddress + 1;
            OperationStatus internalStatus;
            do
            {
                if (TryFindRecordInMainLogForConditionalOperation<Input, Output, Context, TsavoriteSession>(tsavoriteSession, ref key, ref stackCtx, minAddress, out internalStatus, out bool needIO))
                    return OperationStatus.SUCCESS;
                if (!OperationStatusUtils.IsRetry(internalStatus))
                {
                    // HeadAddress may have risen above minAddress; if so, we need IO.
                    internalStatus = needIO
                        ? PrepareIOForConditionalOperation(tsavoriteSession, ref pendingContext, ref key, ref pendingContext.input.Get(), ref pendingContext.value.Get(),
                                                            ref pendingContext.output, pendingContext.userContext, pendingContext.serialNum, ref stackCtx, minAddress, WriteReason.Compaction)
                        : ConditionalCopyToTail(tsavoriteSession, ref pendingContext, ref key, ref pendingContext.input.Get(), ref pendingContext.value.Get(),
                                                            ref pendingContext.output, pendingContext.userContext, pendingContext.serialNum, ref stackCtx, pendingContext.writeReason);
                }
            }
            while (tsavoriteSession.Store.HandleImmediateNonPendingRetryStatus<Input, Output, Context, TsavoriteSession>(internalStatus, tsavoriteSession));
            return internalStatus;
        }

        /// <summary>
        /// Continue a pending CONDITIONAL_SCAN_PUSH operation with the record retrieved from disk, checking whether a record for this key was
        /// added since we went pending; in that case this operation "fails" as it finds the record.
        /// </summary>
        /// <param name="request">record read from the disk.</param>
        /// <param name="pendingContext">internal context for the pending RMW operation</param>
        /// <param name="tsavoriteSession">Callback functions.</param>
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
        internal OperationStatus ContinuePendingConditionalScanPush<Input, Output, Context, TsavoriteSession>(AsyncIOContext<Key, Value> request,
                                                ref PendingContext<Input, Output, Context> pendingContext, TsavoriteSession tsavoriteSession)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            // If the key was found at or above minAddress, do nothing; we'll push it when we get to it. If we flagged the iteration to stop, do nothing.
            if (request.logicalAddress >= pendingContext.minAddress || pendingContext.scanCursorState.stop)
                return OperationStatus.SUCCESS;

            // Prepare to push to caller's iterator functions. Use data from pendingContext, not request; we're only made it to this line if the key was not found,
            // and thus the request was not populated. The new minAddress should be the highest logicalAddress we previously saw, because we need to make sure the
            // record was not added to the log after we initialized the pending IO.
            hlog.ConditionalScanPush<Input, Output, Context, TsavoriteSession>(tsavoriteSession, pendingContext.scanCursorState, pendingContext.recordInfo, ref pendingContext.key.Get(), ref pendingContext.value.Get(),
                minAddress: pendingContext.InitialLatestLogicalAddress + 1);

            // ConditionalScanPush has already called HandleOperationStatus, so return SUCCESS here.
            return OperationStatus.SUCCESS;
        }
    }
}