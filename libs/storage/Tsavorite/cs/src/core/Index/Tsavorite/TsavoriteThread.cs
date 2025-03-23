﻿// Copyright (c) Microsoft Corporation.
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
                InternalCompletePendingRequests(sessionFunctions, completedOutputs);
                if (wait) sessionFunctions.Ctx.WaitPending(epoch);

                if (sessionFunctions.Ctx.HasNoPendingRequests) return true;

                InternalRefresh<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions);

                if (!wait) return false;
                Thread.Yield();
            }
        }

        internal bool InRestPhase() => stateMachineDriver.SystemState.Phase == Phase.REST;

        #region Complete Pending Requests
        internal void InternalCompletePendingRequests<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                                                                                             CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            _ = hlogBase.TryComplete();

            if (sessionFunctions.Ctx.readyResponses.Count == 0) return;

            while (sessionFunctions.Ctx.readyResponses.TryDequeue(out AsyncIOContext request))
                InternalCompletePendingRequest(sessionFunctions, request, completedOutputs);
        }

        internal void InternalCompletePendingRequest<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, AsyncIOContext request,
                                                                                            CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Get and Remove this request.id pending dictionary if it is there.
            if (sessionFunctions.Ctx.ioPendingRequests.Remove(request.id, out var pendingContext))
            {
                var status = InternalCompletePendingRequestFromContext(sessionFunctions, request, ref pendingContext, out _);
                if (completedOutputs is not null && status.IsCompletedSuccessfully)
                {
                    // Transfer things to outputs from pendingContext before we dispose it.
                    completedOutputs.TransferFrom(ref pendingContext, status);
                }
                if (!status.IsPending)
                    pendingContext.Dispose();
            }
        }

        /// <summary>
        /// Caller is expected to dispose pendingContext after this method completes
        /// </summary>
        internal unsafe Status InternalCompletePendingRequestFromContext<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, AsyncIOContext request,
                                                                    ref PendingContext<TInput, TOutput, TContext> pendingContext, out AsyncIOContext newRequest)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "InternalCompletePendingRequestFromContext requires epoch acquision");
            newRequest = default;

            // If NoKey, we do not have the key in the initial call and must use the key from the satisfied request.
            // With the new overload of CompletePending that returns CompletedOutputs, pendingContext must have the key.
            DiskLogRecord diskLogRecord = new((long)request.record.GetValidPointer());
            if (pendingContext.IsNoKey && pendingContext.key == default)
                pendingContext.key = hlog.GetKeyContainer(diskLogRecord.Key);
            var key = pendingContext.key.Get();

            OperationStatus internalStatus = pendingContext.type switch
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
                    sessionFunctions.ReadCompletionCallback(ref diskLogRecord,
                                                     ref pendingContext.input.Get(),
                                                     ref pendingContext.output,
                                                     pendingContext.userContext,
                                                     status,
                                                     new RecordMetadata(pendingContext.logicalAddress));
                }
                else
                {
                    sessionFunctions.RMWCompletionCallback(ref diskLogRecord,
                                                     ref pendingContext.input.Get(),
                                                     ref pendingContext.output,
                                                     pendingContext.userContext,
                                                     status,
                                                     new RecordMetadata(pendingContext.logicalAddress));
                }
            }

            DisposeRecord(ref diskLogRecord, DisposeReason.DeserializedFromDisk);
            request.Dispose();
            return status;
        }
        #endregion
    }
}