﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    public partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InternalRefresh<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            epoch.ProtectAndDrain();

            // Fast path: check if we are in unchanged REST phase
            if (sessionFunctions.Ctx.SessionState.Phase == Phase.REST && SystemState.Equal(sessionFunctions.Ctx.SessionState, stateMachineDriver.SystemState))
                return;

            while (true)
            {
                // Grab current system state into session state under epoch protection
                sessionFunctions.Ctx.SessionState = stateMachineDriver.SystemState;

                //   If a session is in PREPARE/PREPARE_GROW phase AND is not currently in a transaction
                //      Then the session will SPIN during Refresh until it is in (IN_PROGRESS, v+1).
                //
                //   That way no session can work in the PREPARE/PREPARE_GROW phase while any session works in IN_PROGRESS phase.
                //   This is safe, because the state machine is guaranteed to progress to (IN_PROGRESS, v+1) only if all sessions
                //   have reached PREPARE/PREPARE_GROW and all transactions have concluded (i.e., NumActiveLockingSessions == 0).
                //   See HybridLogCheckpointSMTask and IndexResizeSMTask for state machine progress condition.
                //
                //   Also, ClientSession.AcquireLockable() ensures that no new transactions are started in
                //   the PREPARE/PREPARE_GROW phase.
                if ((sessionFunctions.Ctx.phase == Phase.PREPARE || sessionFunctions.Ctx.phase == Phase.PREPARE_GROW)
                    && !sessionFunctions.Ctx.isAcquiredLockable)
                {
                    epoch.ProtectAndDrain();
                    _ = Thread.Yield();
                    continue;
                }
                break;
            }
        }

        internal bool InternalCompletePending<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, bool wait = false,
                                                                                     CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs = null)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
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
                                                                                             CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            _ = hlogBase.TryComplete();

            if (sessionFunctions.Ctx.readyResponses.Count == 0) return;

            while (sessionFunctions.Ctx.readyResponses.TryDequeue(out AsyncIOContext<TKey, TValue> request))
                InternalCompletePendingRequest(sessionFunctions, request, completedOutputs);
        }

        internal void InternalCompletePendingRequest<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, AsyncIOContext<TKey, TValue> request,
                                                                                            CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
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
        internal Status InternalCompletePendingRequestFromContext<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, AsyncIOContext<TKey, TValue> request,
                                                                    ref PendingContext<TInput, TOutput, TContext> pendingContext, out AsyncIOContext<TKey, TValue> newRequest)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "InternalCompletePendingRequestFromContext requires epoch acquision");
            newRequest = default;

            // If NoKey, we do not have the key in the initial call and must use the key from the satisfied request.
            // With the new overload of CompletePending that returns CompletedOutputs, pendingContext must have the key.
            if (pendingContext.NoKey && pendingContext.key == default)
                pendingContext.key = hlog.GetKeyContainer(ref hlog.GetContextRecordKey(ref request));
            ref TKey key = ref pendingContext.key.Get();

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
                    sessionFunctions.ReadCompletionCallback(ref key,
                                                     ref pendingContext.input.Get(),
                                                     ref pendingContext.output,
                                                     pendingContext.userContext,
                                                     status,
                                                     new RecordMetadata(pendingContext.recordInfo, pendingContext.logicalAddress));
                }
                else
                {
                    sessionFunctions.RMWCompletionCallback(ref key,
                                                     ref pendingContext.input.Get(),
                                                     ref pendingContext.output,
                                                     pendingContext.userContext,
                                                     status,
                                                     new RecordMetadata(pendingContext.recordInfo, pendingContext.logicalAddress));
                }
            }

            unsafe
            {
                ref RecordInfo recordInfo = ref hlog.GetInfoFromBytePointer(request.record.GetValidPointer());
                storeFunctions.DisposeRecord(ref hlog.GetContextRecordKey(ref request), ref hlog.GetContextRecordValue(ref request), DisposeReason.DeserializedFromDisk);
            }
            request.Dispose();
            return status;
        }
        #endregion
    }
}