// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
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
            kernel.epoch.ProtectAndDrain();

            // We check if we are in normal mode
            var newPhaseInfo = SystemState.Copy(ref systemState);
            if (sessionFunctions.Ctx.phase == Phase.REST && newPhaseInfo.Phase == Phase.REST && sessionFunctions.Ctx.version == newPhaseInfo.Version)
                return;

            while (true)
            {
                ThreadStateMachineStep(sessionFunctions.Ctx, sessionFunctions, default);

                // In prepare phases, after draining out ongoing multi-key ops, we may spin and get threads to
                // reach the next version before proceeding

                // If CheckpointVersionSwitchBarrier is set, then:
                //   If system is in PREPARE phase AND all multi-key ops have drained (NumActiveLockingSessions == 0):
                //      Then (PREPARE, v) threads will SPIN during Refresh until they are in (IN_PROGRESS, v+1).
                //
                //   That way no thread can work in the PREPARE phase while any thread works in IN_PROGRESS phase.
                //   This is safe, because the state machine is guaranteed to progress to (IN_PROGRESS, v+1) if all threads
                //   have reached PREPARE and all multi-key ops have drained (see VersionChangeTask.OnThreadState).
                if (CheckpointVersionSwitchBarrier &&
                    sessionFunctions.Ctx.phase == Phase.PREPARE &&
                    hlogBase.NumActiveLockingSessions == 0)
                {
                    kernel.epoch.ProtectAndDrain();
                    _ = Thread.Yield();
                    continue;
                }

                if (sessionFunctions.Ctx.phase == Phase.PREPARE_GROW &&
                    hlogBase.NumActiveLockingSessions == 0)
                {
                    kernel.epoch.ProtectAndDrain();
                    _ = Thread.Yield();
                    continue;
                }
                break;
            }
        }

        internal static void InitContext<TInput, TOutput, TContext>(TsavoriteExecutionContext<TInput, TOutput, TContext> ctx, int sessionID, string sessionName)
        {
            ctx.phase = Phase.REST;
            // The system version starts at 1. Because we do not know what the current state machine state is,
            // we need to play it safe and initialize context behind the system state. Otherwise the session may
            // never "catch up" with the rest of the system when stepping through the state machine as it is ahead.
            ctx.version = 1;
            ctx.markers = new bool[8];
            ctx.sessionID = sessionID;
            ctx.sessionName = sessionName;

            if (ctx.readyResponses is null)
            {
                ctx.readyResponses = new AsyncQueue<AsyncIOContext<TKey, TValue>>();
                ctx.ioPendingRequests = new Dictionary<long, PendingContext<TInput, TOutput, TContext>>();
                ctx.pendingReads = new AsyncCountDown();
            }
        }

        internal static void CopyContext<TInput, TOutput, TContext>(TsavoriteExecutionContext<TInput, TOutput, TContext> src, TsavoriteExecutionContext<TInput, TOutput, TContext> dst)
        {
            dst.phase = src.phase;
            dst.version = src.version;
            dst.threadStateMachine = src.threadStateMachine;
            dst.markers = src.markers;
            dst.sessionName = src.sessionName;
        }

        internal bool InternalCompletePending<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, bool wait = false,
                                                                                     CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs = null)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            while (true)
            {
                InternalCompletePendingRequests(sessionFunctions, completedOutputs);
                if (wait) sessionFunctions.Ctx.WaitPending(kernel.epoch);

                if (sessionFunctions.Ctx.HasNoPendingRequests) return true;

                InternalRefresh<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions);

                if (!wait) return false;
                Thread.Yield();
            }
        }

        internal bool InRestPhase() => systemState.Phase == Phase.REST;

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
            Debug.Assert(kernel.epoch.ThisInstanceProtected(), "InternalCompletePendingRequestFromContext requires epoch acquision");
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