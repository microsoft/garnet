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

            // The queue carries a heap-allocated wrapper (AsyncGetFromDiskResult) that holds the
            // AsyncIOContext reference, so each TryDequeue moves only an 8-byte wrapper reference.
            // The worker reads result.context then returns the wrapper to the allocator's pool so it
            // can be reused for the next IO.
            while (sessionFunctions.Ctx.readyResponses.TryDequeue(out AsyncGetFromDiskResult<AsyncIOContext> result))
            {
                // try/finally ensures the pooled wrapper is returned even if the user
                // callback inside InternalCompletePendingRequest throws — otherwise a
                // throwing callback would leak the wrapper forever and degrade pool
                // hit rate over time.
                try
                {
                    InternalCompletePendingRequest(sessionFunctions, ref result.context, completedOutputs);
                }
                finally
                {
                    hlogBase.ReturnAsyncGetFromDiskResult(result);
                }
            }
        }

        internal void InternalCompletePendingRequest<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref AsyncIOContext request,
                                                                                            CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Get and Remove this request.id pending holder from the dict (8-byte ref load, not a struct copy).
            if (sessionFunctions.Ctx.ioPendingRequests.Remove(request.id, out var holder))
            {
                var status = InternalCompletePendingRequestFromContext(sessionFunctions, ref request, ref holder.value, out var newRequest, holder);
                if (completedOutputs is not null && status.IsCompletedSuccessfully)
                {
                    // Transfer things to outputs from pendingContext before we dispose it.
                    completedOutputs.TransferFrom(ref holder.value, status);
                }
                if (!status.IsPending)
                {
                    OnDisposeDiskRecord(ref holder.value.diskLogRecord, DisposeReason.DeserializedFromDisk);    // TODO: This may have been the source of a conditional insert or push, so the reason may be different.
                    holder.value.Dispose();
                    // Completed: return both pooled instances to the per-session pools.
                    sessionFunctions.Ctx.ReturnAsyncIOContext(request);
                    sessionFunctions.Ctx.ReturnPendingContextHolder(holder);
                }
                else
                {
                    // Re-pended: the holder was re-added to the dictionary (owned by the next completion) and a
                    // FRESH AsyncIOContext (newRequest) was issued for the next hop. The completed `request` is now
                    // unreferenced and its record was already disposed, so return just it to the pool.
                    Debug.Assert(newRequest is null || !ReferenceEquals(newRequest, request), "re-pend must issue a distinct AsyncIOContext");
                    sessionFunctions.Ctx.ReturnAsyncIOContext(request);
                }
            }
        }

        /// <summary>
        /// Caller is expected to dispose pendingContext after this method completes
        /// </summary>
        internal unsafe Status InternalCompletePendingRequestFromContext<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref AsyncIOContext request,
                                                                    ref PendingContext<TInput, TOutput, TContext> pendingContext, out AsyncIOContext newRequest,
                                                                    PendingContextHolder<TInput, TOutput, TContext> holder = null)
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

            // Pass the same holder through to HandleOperationStatus so any re-read (RECORD_ON_DISK on completion)
            // can re-Add the same holder ref into the dict (no struct copy).
            var status = HandleOperationStatus(sessionFunctions.Ctx, ref pendingContext, internalStatus, out newRequest, holder);

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