// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
        /// <returns>Operation status</returns>
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal Status HandleOperationStatus<TInput, TOutput, TContext>(
            TsavoriteExecutionContext<TInput, TOutput, TContext> sessionCtx,
            ref PendingContext<TInput, TOutput, TContext> pendingContext,
            OperationStatus operationStatus,
            out AsyncIOContext request)
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

                // Store the PendingContext struct directly in the dictionary's inline entry array. A reference
                // wrapper was measured to be net slower here: it cannot avoid the struct copy (the entry sites
                // build the context on the stack, so it is copied in either way) and adds pool rent/return plus a
                // heap indirection on every dictionary access.
                sessionCtx.ioPendingRequests.Add(pendingContext.id, pendingContext);
                if (!pendingContext.IsConditionalOp)
                {
                    // We may have come from an already-pending operation; keep the diskLogRecord on the caller's
                    // `pendingContext` (for disposal) but clear it on the dictionary copy so the next completion's
                    // TransferFrom sees an unset slot. CONDITIONAL_* ops instead keep it as their copy/push source.
                    CollectionsMarshal.GetValueRefOrNullRef(sessionCtx.ioPendingRequests, pendingContext.id).diskLogRecord = default;
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