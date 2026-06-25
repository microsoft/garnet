// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    public partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InternalRefresh<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            epoch.ProtectAndDrain();

            // Fast path: check if we are in an unchanged REST phase
            if (sessionFunctions.Ctx.SessionState.Phase == Phase.REST &&
                SystemState.Equal(sessionFunctions.Ctx.SessionState, stateMachineDriver.SystemState))
                return;

            while (true)
            {
                // Acquire a session-local copy of the system state
                sessionFunctions.Ctx.SessionState = stateMachineDriver.SystemState;

                switch (sessionFunctions.Ctx.SessionState.Phase)
                {
                    case Phase.IN_PROGRESS:
                        // Adjust session's effective state if there is an ongoing active transaction.
                        if (sessionFunctions.Ctx.txnVersion == sessionFunctions.Ctx.SessionState.Version - 1)
                        {
                            sessionFunctions.Ctx.SessionState = SystemState.Make(Phase.PREPARE, sessionFunctions.Ctx.txnVersion);
                        }
                        break;
                    case Phase.PREPARE_GROW:
                        // Session needs to wait in PREPARE_GROW phase unless it is in an active transaction.
                        // We cannot avoid spinning on hash table growth: operations (and transactions) in (v) have to drain
                        // out before we grow the hash table because the lock table is co-located with the hash table.
                        if (!sessionFunctions.Ctx.isAcquiredTransactional)
                        {
                            epoch.ProtectAndDrain();
                            _ = Thread.Yield();
                            continue;
                        }
                        break;
                }
                break;
            }
        }

        internal bool InternalCompletePending<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, bool wait = false,
                                                                                     CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs = null)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            while (true)
            {
                InternalCompletePendingRequests<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, completedOutputs);
                if (wait)
                    sessionFunctions.Ctx.WaitPending(epoch);

                if (sessionFunctions.Ctx.HasNoPendingRequests)
                    return true;

                InternalRefresh<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions);

                if (!wait)
                    return false;
                _ = Thread.Yield();
            }
        }

        internal bool InRestPhase() => stateMachineDriver.SystemState.Phase == Phase.REST;

        #region Complete Pending Requests
        internal void InternalCompletePendingRequests<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                                                                                             CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            _ = hlogBase.TryComplete();

            if (sessionFunctions.Ctx.readyResponses.Count == 0)
                return;

            // The ready queue now carries the AsyncIOContext (the pending op) directly; each
            // TryDequeue moves only its 8-byte reference. InternalCompletePendingRequest returns the
            // op to the per-session pool after consuming it.
            while (sessionFunctions.Ctx.readyResponses.TryDequeue(out AsyncIOContext request))
            {
                InternalCompletePendingRequest(sessionFunctions, ref request, completedOutputs);
            }
        }

        internal void InternalCompletePendingRequest<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref AsyncIOContext request,
                                                                                            CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // The op carries its OperationState directly (no ioPendingRequests dictionary). Async pending ops
            // are always PendingIoContext; completion-event / sync-scan ops are signaled in place and never
            // reach this per-session ready queue.
            var op = (PendingIoContext<TInput, TOutput, TContext>)request;
            Debug.Assert(op.completionEvent is null, "completion-event ops are signaled synchronously and never drained here");

            var stillPending = false;   // a disk-chain reissue reused this same op; it is back in flight
            var repended = false;       // a re-pend issued a distinct fresh op; this (old) op is done
            try
            {
                // Verify the device read on the run thread (the completion thread only enqueued the raw buffer).
                // On an incomplete record or key mismatch this re-issues the next chain read on this same op; the
                // op is then back in flight, so leave it pending and let its next completion drain it.
                if (!hlogBase.TryVerifyOrReissuePendingRead(ref request))
                {
                    stillPending = true;
                    return;
                }

                var status = InternalCompletePendingRequestFromContext(sessionFunctions, ref request, ref op.baseOperationState, ref op.pendingState, out var newRequest);
                if (completedOutputs is not null && status.IsCompletedSuccessfully)
                {
                    // Transfer things to outputs from the pendingState before we dispose it.
                    completedOutputs.TransferFrom(ref op.baseOperationState, ref op.pendingState, status);
                }
                if (status.IsPending)
                {
                    // Re-pended: a FRESH op (newRequest) was issued for the next hop. The ContinuePending* helper
                    // has already MOVED the pendingState's heap-owning fields (requestKey, input, diskLogRecord) into the
                    // new op via struct copy and cleared this pendingState. So this old op's pendingState is already default; the
                    // drain just clears the base context. The disk record just read was either moved as part of
                    // the pendingState transfer (CONDITIONAL_*) or already disposed via TransferFrom in
                    // InternalCompletePendingRequestFromContext (non-conditional).
                    repended = true;
                    Debug.Assert(newRequest is null || !ReferenceEquals(newRequest, request), "re-pend must issue a distinct op");
                    op.baseOperationState = default;
                }
            }
            finally
            {
                // A chain-walk reissue left this op in flight (still pending), so skip return/decrement entirely.
                if (!stillPending)
                {
                    if (!repended)
                    {
                        // Terminal completion OR an exception during completion processing: dispose this op's pendingState
                        // exactly once (returns the input container and disposes the disk record).
                        OnDisposeDiskRecord(ref op.pendingState.diskLogRecord, DisposeReason.DeserializedFromDisk);    // TODO: This may have been the source of a conditional insert or push, so the reason may be different.
                        op.pendingState.Dispose();
                    }
                    // DisposeRecord is idempotent (record is nulled after the first call), so this safely frees the op's
                    // raw read buffer on the exception path where InternalCompletePendingRequestFromContext did not reach it.
                    op.DisposeRecord();
                    sessionFunctions.Ctx.ReturnPendingIoContext(op);
                    // Decrement AFTER any re-pend has incremented its fresh op, so the count is never transiently
                    // zero across a hop and is decremented exactly once even if completion processing threw.
                    sessionFunctions.Ctx.pendingCount--;
                }
            }
        }

        /// <summary>
        /// Caller is expected to dispose the pendingState after this method completes
        /// </summary>
        internal unsafe Status InternalCompletePendingRequestFromContext<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref AsyncIOContext request,
                                                                    ref OperationState<TInput, TOutput, TContext> operationState,
                                                                    ref PendingState<TInput, TOutput, TContext> pendingState,
                                                                    out AsyncIOContext newRequest)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "InternalCompletePendingRequestFromContext requires epoch acquisition");
            newRequest = null;

            // If this was an operation that was trying to retrieve a target record, copy it into the pendingState.
            // CONDITIONAL_* operations do not care about the retrieved data; they only care whether a record was found.
            if (request.diskLogRecord.IsSet && !pendingState.IsConditionalOp)
                pendingState.TransferFrom(ref request.diskLogRecord, hlogBase.bufferPool);

            var internalStatus = pendingState.type switch
            {
                OperationType.READ => ContinuePendingRead(request, ref operationState, ref pendingState, sessionFunctions),
                OperationType.RMW => ContinuePendingRMW(request, ref operationState, ref pendingState, sessionFunctions),
                OperationType.CONDITIONAL_INSERT => ContinuePendingConditionalCopyToTail(request, ref operationState, ref pendingState, sessionFunctions),
                OperationType.CONDITIONAL_SCAN_PUSH => ContinuePendingConditionalScanPush(request, ref operationState, ref pendingState, sessionFunctions),
                _ => throw new TsavoriteException("Unexpected OperationType")
            };

            var status = HandleOperationStatus(sessionFunctions.Ctx, ref operationState, internalStatus, out newRequest);

            // If done, callback user code. Note: a re-pend may have moved the pendingState to a new op (then-cleared this pendingState);
            // the type check below uses the pendingState type captured BEFORE re-pend would have cleared it. Completion callbacks
            // only fire on terminal status (IsCompletedSuccessfully && !IsPending), where the pendingState is still intact.
            if (status.IsCompletedSuccessfully)
            {
                if (pendingState.type == OperationType.READ)
                {
                    sessionFunctions.ReadCompletionCallback(ref pendingState.diskLogRecord,
                                                     ref pendingState.input.Get(),
                                                     ref pendingState.output,
                                                     pendingState.userContext,
                                                     status,
                                                     new RecordMetadata(operationState.logicalAddress));
                }
                else if (pendingState.type == OperationType.RMW)
                {
                    sessionFunctions.RMWCompletionCallback(ref pendingState.diskLogRecord,
                                                     ref pendingState.input.Get(),
                                                     ref pendingState.output,
                                                     pendingState.userContext,
                                                     status,
                                                     new RecordMetadata(operationState.logicalAddress));
                }
            }

            hlog.OnDisposeDiskRecord(ref request.diskLogRecord, DisposeReason.DeserializedFromDisk);
            request.DisposeRecord();
            return status;
        }
        #endregion
    }
}