﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool HandleImmediateRetryStatus<TInput, TOutput, TContext, TKeyLocker>(
                ref HashEntryInfo hei,
                OperationStatus internalStatus,
                ExecutionContext<TInput, TOutput, TContext> executionCtx,
                ref PendingContext<TInput, TOutput, TContext> pendingContext)
            where TKeyLocker : struct, ISessionLocker
            => (internalStatus & OperationStatus.BASIC_MASK) > OperationStatus.MAX_MAP_TO_COMPLETED_STATUSCODE
                && HandleRetryStatus<TInput, TOutput, TContext, TKeyLocker>(internalStatus, ref hei, executionCtx, ref pendingContext);

        /// <summary>
        /// Handle retry for operations that will not go pending (e.g., InternalLock)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TKeyLocker>(
                ref HashEntryInfo hei, OperationStatus internalStatus, ExecutionContext<TInput, TOutput, TContext> executionCtx)
            where TKeyLocker : struct, ISessionLocker
        {
            Debug.Assert(Kernel.Epoch.ThisInstanceProtected(), "Epoch should be protected in HandleImmediateNonPendingRetryStatus");
            switch (internalStatus)
            {
                case OperationStatus.RETRY_NOW:
                    _ = Thread.Yield();
                    return true;
                case OperationStatus.RETRY_LATER:
                    InternalRefresh(ref hei, executionCtx);
                    _ = Thread.Yield();
                    return true;
                default:
                    return false;
            }
        }

        private bool HandleRetryStatus<TInput, TOutput, TContext, TKeyLocker>(
                OperationStatus internalStatus,
                ref HashEntryInfo hei,
                ExecutionContext<TInput, TOutput, TContext> executionCtx,
                ref PendingContext<TInput, TOutput, TContext> pendingContext)
            where TKeyLocker : struct, ISessionLocker
        {
            Debug.Assert(Kernel.Epoch.ThisInstanceProtected(), "Epoch should be protected in HandleRetryStatus");
            switch (internalStatus)
            {
                case OperationStatus.RETRY_NOW:
                    _ = Thread.Yield();
                    return true;
                case OperationStatus.RETRY_LATER:
                    InternalRefresh(ref hei, executionCtx);
                    _ = Thread.Yield();
                    return true;
                case OperationStatus.CPR_SHIFT_DETECTED:
                    // Retry as (v+1) Operation
                    SynchronizeEpoch<TInput, TOutput, TContext, TKeyLocker>(ref hei, executionCtx, ref pendingContext);
                    return true;
                case OperationStatus.ALLOCATE_FAILED:
                    // Async handles this in its own way, as part of the *AsyncResult.Complete*() sequence.
                    Debug.Assert(!pendingContext.flushEvent.IsDefault(), "flushEvent is required for ALLOCATE_FAILED");
                    if (pendingContext.IsAsync)
                        return false;
                    try
                    {
                        Kernel.Epoch.Suspend();
                        pendingContext.flushEvent.Wait();
                    }
                    finally
                    {
                        pendingContext.flushEvent = default;
                        Kernel.Epoch.Resume();
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
        internal Status HandleOperationStatus<TInput, TOutput, TContext>(
                ExecutionContext<TInput, TOutput, TContext> sessionCtx,
                ref PendingContext<TInput, TOutput, TContext> pendingContext,
                OperationStatus operationStatus)
        {
            if (OperationStatusUtils.TryConvertToCompletedStatusCode(operationStatus, out Status status))
                return status;

            // This does not lock so no TKeyLocker is needed
            return HandleOperationStatus(sessionCtx, ref pendingContext, operationStatus, out _);
        }

        /// <summary>
        /// Performs appropriate handling based on the internal failure status of the trial.
        /// </summary>
        /// <param name="sessionCtx">Thread (or session) context under which operation was tried to execute.</param>
        /// <param name="pendingContext">Internal context of the operation.</param>
        /// <param name="operationStatus">Internal status of the trial.</param>
        /// <param name="request">IO request, if operation went pending</param>
        /// <returns>Operation status</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status HandleOperationStatus<TInput, TOutput, TContext>(
            ExecutionContext<TInput, TOutput, TContext> sessionCtx,
            ref PendingContext<TInput, TOutput, TContext> pendingContext,
            OperationStatus operationStatus,
            out AsyncIOContext<TKey, TValue> request)
        {
            Debug.Assert(operationStatus != OperationStatus.RETRY_NOW, "OperationStatus.RETRY_NOW should have been handled before HandleOperationStatus");
            Debug.Assert(operationStatus != OperationStatus.RETRY_LATER, "OperationStatus.RETRY_LATER should have been handled before HandleOperationStatus");
            Debug.Assert(operationStatus != OperationStatus.CPR_SHIFT_DETECTED, "OperationStatus.CPR_SHIFT_DETECTED should have been handled before HandleOperationStatus");

            request = default;

            if (OperationStatusUtils.TryConvertToCompletedStatusCode(operationStatus, out Status status))
                return status;

            if (operationStatus == OperationStatus.ALLOCATE_FAILED)
            {
                Debug.Assert(pendingContext.IsAsync, "Sync ops should have handled ALLOCATE_FAILED before HandleOperationStatus");
                Debug.Assert(!pendingContext.flushEvent.IsDefault(), "Expected flushEvent for ALLOCATE_FAILED");
                return new(StatusCode.Pending);
            }
            else if (operationStatus == OperationStatus.RECORD_ON_DISK)
            {
                Debug.Assert(pendingContext.flushEvent.IsDefault(), "Cannot have flushEvent with RECORD_ON_DISK");
                // Add context to dictionary
                pendingContext.id = sessionCtx.totalPending++;
                sessionCtx.ioPendingRequests.Add(pendingContext.id, pendingContext);

                // Issue asynchronous I/O request
                request.id = pendingContext.id;
                request.request_key = pendingContext.key;
                request.logicalAddress = pendingContext.logicalAddress;
                request.minAddress = pendingContext.minAddress;
                request.record = default;
                if (pendingContext.IsAsync)
                    request.asyncOperation = new TaskCompletionSource<AsyncIOContext<TKey, TValue>>(TaskCreationOptions.RunContinuationsAsynchronously);
                else
                    request.callbackQueue = sessionCtx.readyResponses;

                hlogBase.AsyncGetFromDisk(pendingContext.logicalAddress, hlog.GetAverageRecordSize(), request);
                return new(StatusCode.Pending);
            }
            else
            {
                Debug.Assert(pendingContext.IsAsync, "Sync ops should never return status.IsFaulted");
                return new(StatusCode.Error);
            }
        }
    }
}