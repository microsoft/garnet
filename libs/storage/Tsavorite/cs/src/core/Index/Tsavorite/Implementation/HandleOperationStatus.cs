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
            ref OperationState<TInput, TOutput, TContext> operationState)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            => (internalStatus & OperationStatus.BASIC_MASK) > OperationStatus.MAX_MAP_TO_COMPLETED_STATUSCODE
                && HandleRetryStatus(internalStatus, sessionFunctions, ref operationState);

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
            ref OperationState<TInput, TOutput, TContext> operationState)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            Debug.Assert(epoch.ThisInstanceProtected());

            switch (internalStatus)
            {
                case OperationStatus.RETRY_NOW:
                    // Reset operationState.logicalAddress so a prior in-memory match's address does not bleed into
                    // the retry's RecordMetadata if the retry takes an early-out (NOTFOUND, etc.). InternalRead/RMW/
                    // Upsert/Delete no longer pay for this reset on the first call (default(OperationState).logicalAddress
                    // is 0 == kInvalidAddress); only the cold retry path here does.
                    operationState.logicalAddress = LogAddress.kInvalidAddress;
                    _ = Thread.Yield();
                    return true;
                case OperationStatus.RETRY_LATER:
                    operationState.logicalAddress = LogAddress.kInvalidAddress;
                    InternalRefresh<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions);
                    _ = Thread.Yield();
                    return true;
                case OperationStatus.CPR_SHIFT_DETECTED:
                    // Retry as (v+1) Operation
                    operationState.logicalAddress = LogAddress.kInvalidAddress;
                    SynchronizeEpoch(sessionFunctions.Ctx, ref operationState, sessionFunctions);
                    return true;
                case OperationStatus.ALLOCATE_FAILED:
                    operationState.logicalAddress = LogAddress.kInvalidAddress;
                    // Async handles this in its own way, as part of the *AsyncResult.Complete*() sequence.
                    Debug.Assert(!operationState.flushEvent.IsDefault(), "flushEvent is required for ALLOCATE_FAILED");
                    try
                    {
                        epoch.Suspend();
                        operationState.flushEvent.Wait();
                    }
                    finally
                    {
                        operationState.flushEvent = default;
                        epoch.Resume();
                    }
                    return true;
                default:
                    // RECORD_ON_DISK falls here: do NOT reset operationState.logicalAddress, the caller (HandleOperationStatus)
                    // is about to issue the disk IO at that address.
                    return false;
            }
        }

        /// <summary>
        /// Performs appropriate handling based on the internal failure status of the trial.
        /// </summary>
        /// <param name="sessionCtx">Thread (or session) context under which operation was tried to execute.</param>
        /// <param name="operationState">Internal context of the operation.</param>
        /// <param name="operationStatus">Internal status of the trial.</param>
        /// <returns>Operation status</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status HandleOperationStatus<TInput, TOutput, TContext>(TsavoriteExecutionContext<TInput, TOutput, TContext> sessionCtx,
            ref OperationState<TInput, TOutput, TContext> operationState, OperationStatus operationStatus)
            => OperationStatusUtils.TryConvertToCompletedStatusCode(operationStatus, out var status)
                ? status
                : HandleOperationStatus(sessionCtx, ref operationState, operationStatus, out _);

        /// <summary>
        /// Performs appropriate handling based on the internal failure status of the trial.
        /// </summary>
        /// <param name="sessionCtx">Thread (or session) context under which operation was tried to execute.</param>
        /// <param name="operationState">Internal context of the operation.</param>
        /// <param name="operationStatus">Internal status of the trial.</param>
        /// <param name="request">IO request, if operation went pending</param>
        /// <returns>Operation status</returns>
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal Status HandleOperationStatus<TInput, TOutput, TContext>(
            TsavoriteExecutionContext<TInput, TOutput, TContext> sessionCtx,
            ref OperationState<TInput, TOutput, TContext> operationState,
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
                Debug.Assert(!operationState.flushEvent.IsDefault(), "Expected flushEvent for ALLOCATE_FAILED");
                Debug.Fail("Should have handled ALLOCATE_FAILED before HandleOperationStatus");
                return new(StatusCode.Pending);
            }
            else if (operationStatus == OperationStatus.RECORD_ON_DISK)
            {
                Debug.Assert(operationState.flushEvent.IsDefault(), "Cannot have flushEvent with RECORD_ON_DISK");

                // The pending-going helper (CreatePendingReadContext / CreatePendingRMWContext /
                // PrepareIOForConditionalOperation / PrepareIOForConditionalScan) has already rented the op and
                // populated `op.pendingState` in place. Pick it up here, snapshot the in-memory hot-path bookkeeping into
                // `op.baseOperationState`, fill the device-facing fields on the op base, and issue the IO.
                var op = operationState.pendingOp;
                Debug.Assert(op is not null, "RECORD_ON_DISK requires the pending-going helper to have stashed pendingOp");
                operationState.pendingOp = null;

                op.baseOperationState = operationState;
                op.baseOperationState.pendingOp = null;

                // For non-conditional ops, clear the pendingState's diskLogRecord so the next completion's TransferFrom sees
                // an empty pendingState. CONDITIONAL_* keep it as their copy/push source. On the first call from CreatePending*
                // the pendingState's diskLogRecord is already default; on a re-pend (pendingState moved from old op) the diskLogRecord
                // carries the previous IO's record image, which the next IO will overwrite — dispose it first to
                // release the SectorAlignedMemory buffer.
                if (!op.pendingState.IsConditionalOp && op.pendingState.diskLogRecord.IsSet)
                {
                    OnDisposeDiskRecord(ref op.pendingState.diskLogRecord, DisposeReason.DeserializedFromDisk);
                    op.pendingState.diskLogRecord.Dispose();
                    op.pendingState.diskLogRecord = default;
                }

                // Fill the device-facing fields directly (each set is a single store, no Buffer.BulkMoveWithWriteBarrier).
                // The diagnostic id lives on the AsyncIOContext (not on OperationState) so it is assigned here only.
                op.id = sessionCtx.totalPending++;
                // Copying the key is stable; the pendingState.requestKey will remain valid until it is freed (after the callback is invoked).
                op.requestKey = op.pendingState.requestKey;
                op.logicalAddress = operationState.logicalAddress;
                op.minAddress = op.pendingState.minAddress;
                op.record = null;
                op.callbackQueue = sessionCtx.readyResponses;

                // The IO record size is resolved on the first call to this method. Usually this is from InternalRead/InternalRMW before returning
                // RECORD_ON_DISK but may be from elsewhere such as ReadCache or ConditionalCopyToTail, so do the setting of initial IO record size here.
                if (op.baseOperationState.initialIORecordSize <= 0)
                    ResolveInitialIORecordSize(sessionCtx, ref op.baseOperationState);
                // Count the op as pending before issuing; the drain decrements when this op completes.
                sessionCtx.pendingCount++;
                hlogBase.AsyncGetFromDisk(operationState.logicalAddress, op.baseOperationState.initialIORecordSize, op);
                request = op;
                return new(StatusCode.Pending);
            }
            else
            {
                Debug.Fail($"Unexpected OperationStatus {operationStatus}");
                return new(StatusCode.Error);
            }
        }

        /// <summary>
        /// Resolves the initial IO record size for a pending disk read by checking the hierarchy:
        /// per-operation (highest priority) → session → store → default (lowest priority).
        /// Called from InternalRead and InternalRMW before returning <see cref="OperationStatus.RECORD_ON_DISK"/>.
        /// </summary>
        /// <param name="sessionCtx">The session execution context (session-level setting).</param>
        /// <param name="operationState">The pending context; its <see cref="OperationState{TInput,TOutput,TContext}.initialIORecordSize"/> holds
        ///     the per-operation value and will be overwritten with the resolved value.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ResolveInitialIORecordSize<TInput, TOutput, TContext>(
            TsavoriteExecutionContext<TInput, TOutput, TContext> sessionCtx,
            ref OperationState<TInput, TOutput, TContext> operationState)
        {
            // Priority: per-operation (highest) > session-level > store-level > IStreamBuffer.DefaultInitialIORecordSize.
            // Both UseDefaultInitialIORecordSize (-1) and 0 (from default struct init) are treated as "not set".
            var size = operationState.initialIORecordSize;
            if (size <= 0)
                size = sessionCtx.InitialIORecordSize;
            if (size <= 0)
                size = InitialIORecordSize;
            if (size <= 0)
                size = IStreamBuffer.DefaultInitialIORecordSize;
            operationState.initialIORecordSize = size;
        }
    }
}