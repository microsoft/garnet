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
            // The op carries its PendingContext directly (no ioPendingRequests dictionary). Async pending ops
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

                var status = InternalCompletePendingRequestFromContext(sessionFunctions, ref request, ref op.pendingContext, out var newRequest);
                if (completedOutputs is not null && status.IsCompletedSuccessfully)
                {
                    // Transfer things to outputs from pendingContext before we dispose it.
                    completedOutputs.TransferFrom(ref op.pendingContext, status);
                }
                if (status.IsPending)
                {
                    // Re-pended: a FRESH op (newRequest) was issued for the next hop carrying a moved copy of
                    // pendingContext (diskLogRecord cleared for non-conditional ops). The record just read is
                    // stale; dispose it so its buffer isn't leaked (conditional ops keep it as the new op's
                    // copy/push source, so only dispose for non-conditional). The input/requestKey ownership has
                    // moved to newRequest, so clear this op's context WITHOUT disposing it.
                    repended = true;
                    Debug.Assert(newRequest is null || !ReferenceEquals(newRequest, request), "re-pend must issue a distinct op");
                    if (!op.pendingContext.IsConditionalOp && op.pendingContext.diskLogRecord.IsSet)
                    {
                        OnDisposeDiskRecord(ref op.pendingContext.diskLogRecord, DisposeReason.DeserializedFromDisk);
                        op.pendingContext.diskLogRecord.Dispose();
                    }
                    op.pendingContext = default;
                }
            }
            finally
            {
                // A chain-walk reissue left this op in flight (still pending), so skip return/decrement entirely.
                if (!stillPending)
                {
                    if (!repended)
                    {
                        // Terminal completion OR an exception during completion processing: dispose this op's context
                        // exactly once (returns the input container and disposes the disk record).
                        OnDisposeDiskRecord(ref op.pendingContext.diskLogRecord, DisposeReason.DeserializedFromDisk);    // TODO: This may have been the source of a conditional insert or push, so the reason may be different.
                        op.pendingContext.Dispose();
                    }
                    // DisposeRecord is idempotent (record is nulled after the first call), so this safely frees the op's
                    // raw read buffer on the exception path where InternalCompletePendingRequestFromContext did not reach it.
                    op.DisposeRecord();
                    sessionFunctions.Ctx.ReturnAsyncIOContext(op);
                    // Decrement AFTER any re-pend has incremented its fresh op, so the count is never transiently
                    // zero across a hop and is decremented exactly once even if completion processing threw.
                    sessionFunctions.Ctx.pendingCount--;
                }
            }
        }

        /// <summary>
        /// Caller is expected to dispose pendingContext after this method completes
        /// </summary>
        internal unsafe Status InternalCompletePendingRequestFromContext<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref AsyncIOContext request,
                                                                    ref PendingContext<TInput, TOutput, TContext> pendingContext, out AsyncIOContext newRequest)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "InternalCompletePendingRequestFromContext requires epoch acquisition");
            newRequest = null;

            // If this was an operation that was trying to retrieve a target record, copy it into the pendingContext.
            // CONDITIONAL_* operations do not care about the retrieved data; they only care whether a record was found.
            if (request.diskLogRecord.IsSet && !pendingContext.IsConditionalOp)
                pendingContext.TransferFrom(ref request.diskLogRecord, hlogBase.bufferPool);

            var internalStatus = pendingContext.type switch
            {
                OperationType.READ => ContinuePendingRead(request, ref pendingContext, sessionFunctions),
                OperationType.RMW => ContinuePendingRMW(request, ref pendingContext, sessionFunctions),
                OperationType.CONDITIONAL_INSERT => ContinuePendingConditionalCopyToTail(request, ref pendingContext, sessionFunctions),
                OperationType.CONDITIONAL_SCAN_PUSH => ContinuePendingConditionalScanPush(request, ref pendingContext, sessionFunctions),
                _ => throw new TsavoriteException("Unexpected OperationType")
            };

            var status = HandleOperationStatus(sessionFunctions.Ctx, ref pendingContext, internalStatus, out newRequest);

            // If done, callback user code
            if (status.IsCompletedSuccessfully)
            {
                if (pendingContext.type == OperationType.READ)
                {
                    sessionFunctions.ReadCompletionCallback(ref pendingContext.diskLogRecord,
                                                     ref pendingContext.input.Get(),
                                                     ref pendingContext.output,
                                                     pendingContext.userContext,
                                                     status,
                                                     new RecordMetadata(pendingContext.logicalAddress));
                }
                else if (pendingContext.type == OperationType.RMW)
                {
                    sessionFunctions.RMWCompletionCallback(ref pendingContext.diskLogRecord,
                                                     ref pendingContext.input.Get(),
                                                     ref pendingContext.output,
                                                     pendingContext.userContext,
                                                     status,
                                                     new RecordMetadata(pendingContext.logicalAddress));
                }
            }

            hlog.OnDisposeDiskRecord(ref request.diskLogRecord, DisposeReason.DeserializedFromDisk);
            request.DisposeRecord();
            return status;
        }
        #endregion
    }
}