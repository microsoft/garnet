// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool HandleImmediateRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(
            OperationStatus internalStatus,
            TSessionFunctionsWrapper sessionFunctions,
            ref PendingContext<TInput, TOutput, TContext> pendingContext)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            => (internalStatus & OperationStatus.BASIC_MASK) > OperationStatus.MAX_MAP_TO_COMPLETED_STATUSCODE
                && HandleRetryStatus(internalStatus, sessionFunctions, ref pendingContext);

        /// <summary>
        /// Handle retry for operations that will not go pending (e.g., InternalLock)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(OperationStatus internalStatus, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            Debug.Assert(epoch.ThisInstanceProtected());
            switch (internalStatus)
            {
                case OperationStatus.RETRY_NOW:
                    _ = Thread.Yield();
                    return true;
                case OperationStatus.RETRY_LATER:
                    InternalRefresh<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions);
                    _ = Thread.Yield();
                    return true;
                default:
                    return false;
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private bool HandleRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(
            OperationStatus internalStatus,
            TSessionFunctionsWrapper sessionFunctions,
            ref PendingContext<TInput, TOutput, TContext> pendingContext)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            Debug.Assert(epoch.ThisInstanceProtected());
            switch (internalStatus)
            {
                case OperationStatus.RETRY_NOW:
                    _ = Thread.Yield();
                    return true;
                case OperationStatus.RETRY_LATER:
                    InternalRefresh<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions);
                    _ = Thread.Yield();
                    return true;
                case OperationStatus.CPR_SHIFT_DETECTED:
                    // Retry as (v+1) Operation
                    SynchronizeEpoch(sessionFunctions.Ctx, ref pendingContext, sessionFunctions);
                    return true;
                case OperationStatus.ALLOCATE_FAILED:
                    // Async handles this in its own way, as part of the *AsyncResult.Complete*() sequence.
                    Debug.Assert(!pendingContext.flushEvent.IsDefault(), "flushEvent is required for ALLOCATE_FAILED");
                    try
                    {
                        epoch.Suspend();
                        pendingContext.flushEvent.Wait();
                    }
                    finally
                    {
                        pendingContext.flushEvent = default;
                        epoch.Resume();
                    }
                    return true;
                default:
                    return false;
            }
        }

        /// <summary>
        /// Performs appropriate handling based on the internal failure status of the trial.
        /// </summary>
        /// <param name="sessionCtx">Thread (or session) context under which operation was tried to execute.</param>
        /// <param name="pendingContext">Internal context of the operation.</param>
        /// <param name="operationStatus">Internal status of the trial.</param>
        /// <returns>Operation status</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status HandleOperationStatus<TInput, TOutput, TContext>(TsavoriteExecutionContext<TInput, TOutput, TContext> sessionCtx,
            ref PendingContext<TInput, TOutput, TContext> pendingContext, OperationStatus operationStatus)
            => OperationStatusUtils.TryConvertToCompletedStatusCode(operationStatus, out var status)
                ? status
                : HandleOperationStatus(sessionCtx, ref pendingContext, operationStatus, out _);

        /// <summary>
        /// Performs appropriate handling based on the internal failure status of the trial.
        /// </summary>
        /// <param name="sessionCtx">Thread (or session) context under which operation was tried to execute.</param>
        /// <param name="pendingContext">Internal context of the operation.</param>
        /// <param name="operationStatus">Internal status of the trial.</param>
        /// <param name="request">IO request, if operation went pending</param>
        /// <param name="holder">On a re-pend (continued operation), the existing dictionary holder whose <c>value</c> field IS
        ///     <paramref name="pendingContext"/>'s backing storage, so it is re-added to the dictionary as an 8-byte ref. For a
        ///     first-time pending (the common case) this is null and a holder is rented here and the struct copied into it once.</param>
        /// <returns>Operation status</returns>
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal Status HandleOperationStatus<TInput, TOutput, TContext>(
            TsavoriteExecutionContext<TInput, TOutput, TContext> sessionCtx,
            ref PendingContext<TInput, TOutput, TContext> pendingContext,
            OperationStatus operationStatus,
            out AsyncIOContext request,
            PendingContextHolder<TInput, TOutput, TContext> holder = null)
        {
            Debug.Assert(operationStatus != OperationStatus.RETRY_NOW, "OperationStatus.RETRY_NOW should have been handled before HandleOperationStatus");
            Debug.Assert(operationStatus != OperationStatus.RETRY_LATER, "OperationStatus.RETRY_LATER should have been handled before HandleOperationStatus");
            Debug.Assert(operationStatus != OperationStatus.CPR_SHIFT_DETECTED, "OperationStatus.CPR_SHIFT_DETECTED should have been handled before HandleOperationStatus");

            // AsyncIOContext is now a class; default = null. Only allocate when we actually need an IO request.
            request = null;

            if (OperationStatusUtils.TryConvertToCompletedStatusCode(operationStatus, out var status))
                return status;

            if (operationStatus == OperationStatus.ALLOCATE_FAILED)
            {
                Debug.Assert(!pendingContext.flushEvent.IsDefault(), "Expected flushEvent for ALLOCATE_FAILED");
                Debug.Fail("Should have handled ALLOCATE_FAILED before HandleOperationStatus");
                return new(StatusCode.Pending);
            }
            else if (operationStatus == OperationStatus.RECORD_ON_DISK)
            {
                Debug.Assert(pendingContext.flushEvent.IsDefault(), "Cannot have flushEvent with RECORD_ON_DISK");
                pendingContext.id = sessionCtx.totalPending++;

                // Store the pending op as an 8-byte holder reference in the dictionary (vs a ~200-byte struct copy +
                // Buffer.BulkMoveWithWriteBarrier on every dictionary op). A first-time pending (holder == null) rents a
                // holder and copies the stack PendingContext into it once. A re-pend already has its holder (the dict
                // entry == pendingContext's backing storage), so it is re-added directly with no copy.
                if (holder is not null)
                {
                    sessionCtx.ioPendingRequests.Add(pendingContext.id, holder);
                    // On a re-pend (continued read), the record just read is stale — the re-issued IO reads the
                    // next chain record. holder.value IS the dictionary entry and the sole owner, so dispose it
                    // (not just drop the reference) to return its buffer, then leave it unset for the next
                    // completion's TransferFrom. Conditional ops keep the record as their copy/push source.
                    if (!pendingContext.IsConditionalOp && holder.value.diskLogRecord.IsSet)
                    {
                        OnDisposeDiskRecord(ref holder.value.diskLogRecord, DisposeReason.DeserializedFromDisk);
                        holder.value.diskLogRecord.Dispose();
                    }
                }
                else
                {
                    // First-time pending: rent a holder and copy the stack PendingContext in. The caller retains its own
                    // pendingContext.diskLogRecord for disposal, so the dict copy must only drop its reference (disposing
                    // here would double-free the caller's record).
                    var fallbackHolder = sessionCtx.RentPendingContextHolder();
                    fallbackHolder.value = pendingContext;
                    sessionCtx.ioPendingRequests.Add(pendingContext.id, fallbackHolder);
                    if (!pendingContext.IsConditionalOp)
                        fallbackHolder.value.diskLogRecord = default;
                }

                // Issue asynchronous I/O request. AsyncIOContext is a class — rent or allocate one instance,
                // fill its fields directly (each field-set is a single store, no Buffer.BulkMoveWithWriteBarrier).
                request = sessionCtx.RentAsyncIOContext();
                request.id = pendingContext.id;

                // Copying the key is stable; the pendingContext.requestKey will remain valid until it is freed (after the callback is invoked).
                request.requestKey = pendingContext.requestKey;
                request.logicalAddress = pendingContext.logicalAddress;
                request.minAddress = pendingContext.minAddress;
                request.record = null;
                request.callbackQueue = sessionCtx.readyResponses;

                hlogBase.AsyncGetFromDisk(pendingContext.logicalAddress, IStreamBuffer.InitialIOSize, request);
                return new(StatusCode.Pending);
            }
            else
            {
                Debug.Fail($"Unexpected OperationStatus {operationStatus}");
                return new(StatusCode.Error);
            }
        }
    }
}