// Copyright (c) Microsoft Corporation.
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
        /// <summary>
        /// In a stack context with a possible existing transient lock, unlock the transient lock, refresh, and relock.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InternalRefresh<TInput, TOutput, TContext>(ref HashEntryInfo hei, ExecutionContext<TInput, TOutput, TContext> executionCtx)
        {
            OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(ref hei);

            // Unlock for retry
            var lockState = stackCtx.hei.TransientLockState;
            switch (lockState)
            {
                case TransientLockState.TransientSLock:
                    TransientSUnlock<TInput, TOutput, TContext, TransientKeyLocker>(ref stackCtx);
                    break;
                case TransientLockState.TransientXLock:
                    TransientXUnlock<TInput, TOutput, TContext, TransientKeyLocker>(ref stackCtx);
                    break;
                default:
                    break;
            }

            stackCtx.ResetTransientLockTimeout();

            var internalStatus = OperationStatus.SUCCESS;
            do
            {
                Kernel.Epoch.ProtectAndDrain();
                DoThreadStateMachineStep(executionCtx);
                switch (lockState)
                {
                    case TransientLockState.TransientSLock:
                        TryTransientSLock<TInput, TOutput, TContext, TransientKeyLocker>(ref stackCtx, out internalStatus);
                        break;
                    case TransientLockState.TransientXLock:
                        TryTransientXLock<TInput, TOutput, TContext, TransientKeyLocker>(ref stackCtx, out internalStatus);
                        break;
                    default:
                        break;
                }
            } while (internalStatus == OperationStatus.RETRY_LATER);
        }

        /// <summary>
        /// In a stack context with no possible existing transient lock, just do the refresh
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InternalRefresh<TInput, TOutput, TContext>(ExecutionContext<TInput, TOutput, TContext> executionCtx)
        {
            Kernel.Epoch.ProtectAndDrain();
            DoThreadStateMachineStep(executionCtx);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void DoThreadStateMachineStep<TInput, TOutput, TContext>(ExecutionContext<TInput, TOutput, TContext> executionCtx)
        {
            // We check if we are in normal mode
            var newPhaseInfo = SystemState.Copy(ref systemState);
            if (executionCtx.phase == Phase.REST && newPhaseInfo.Phase == Phase.REST && executionCtx.version == newPhaseInfo.Version)
                return;

            while (true)
            {
                ThreadStateMachineStep(executionCtx, default);

                // In prepare phases, after draining out ongoing multi-key ops, we may spin and get threads to
                // reach the next version before proceeding

                // If CheckpointVersionSwitchBarrier is set, then:
                //   If system is in PREPARE phase AND all multi-key ops have drained (NumActiveLockingSessions == 0):
                //      Then (PREPARE, v) threads will SPIN during Refresh until they are in (IN_PROGRESS, v+1).
                //
                //   That way no thread can work in the PREPARE phase while any thread works in IN_PROGRESS phase.
                //   This is safe, because the state machine is guaranteed to progress to (IN_PROGRESS, v+1) if all threads
                //   have reached PREPARE and all multi-key ops have drained (see VersionChangeTask.OnThreadState).
                if (CheckpointVersionSwitchBarrier && executionCtx.phase == Phase.PREPARE && hlogBase.NumActiveTxnSessions == 0)
                {
                    Kernel.Epoch.ProtectAndDrain();
                    _ = Thread.Yield();
                    continue;
                }

                if (executionCtx.phase == Phase.PREPARE_GROW && hlogBase.NumActiveTxnSessions == 0)
                {
                    Kernel.Epoch.ProtectAndDrain();
                    _ = Thread.Yield();
                    continue;
                }
                break;
            }
        }

        internal static void InitContext<TInput, TOutput, TContext>(ExecutionContext<TInput, TOutput, TContext> ctx, int sessionID, string sessionName)
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
                ctx.ioPendingRequests = [];
                ctx.pendingReads = new AsyncCountDown();
            }
        }

        internal static void CopyContext<TInput, TOutput, TContext>(ExecutionContext<TInput, TOutput, TContext> src, ExecutionContext<TInput, TOutput, TContext> dst)
        {
            dst.phase = src.phase;
            dst.version = src.version;
            dst.threadStateMachine = src.threadStateMachine;
            dst.markers = src.markers;
            dst.sessionName = src.sessionName;
        }

        internal bool InternalCompletePending<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(
                TSessionFunctionsWrapper sessionFunctions, bool wait = false,
                CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs = null)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKeyLocker : struct, IKeyLocker
        {
            while (true)
            {
                // TKeyLocker is used within each ContinuePending*(ref TKey key...) call.
                InternalCompletePendingRequests<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(sessionFunctions, completedOutputs);
                if (wait)
                    sessionFunctions.ExecutionCtx.WaitPending(Kernel.Epoch);

                if (sessionFunctions.ExecutionCtx.HasNoPendingRequests) 
                    return true;

                // This refresh is outside the context of any individual key operation, so do no locking.
                InternalRefresh(sessionFunctions.ExecutionCtx);

                if (!wait) 
                    return false;
                _ = Thread.Yield();
            }
        }

        internal bool InRestPhase() => systemState.Phase == Phase.REST;

        #region Complete Pending Requests
        internal void InternalCompletePendingRequests<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(TSessionFunctionsWrapper sessionFunctions,
                CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKeyLocker : struct, IKeyLocker
        {
            _ = hlogBase.TryComplete();

            if (sessionFunctions.ExecutionCtx.readyResponses.Count == 0)
                return;

            while (sessionFunctions.ExecutionCtx.readyResponses.TryDequeue(out AsyncIOContext<TKey, TValue> request))
                InternalCompletePendingRequest<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(sessionFunctions, request, completedOutputs);
        }

        internal void InternalCompletePendingRequest<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(TSessionFunctionsWrapper sessionFunctions, AsyncIOContext<TKey, TValue> request,
                                                                                            CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKeyLocker : struct, IKeyLocker
        {
            // Get and Remove this request.id pending dictionary if it is there.
            if (sessionFunctions.ExecutionCtx.ioPendingRequests.Remove(request.id, out var pendingContext))
            {
                var status = InternalCompletePendingRequestFromContext<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(sessionFunctions, request, ref pendingContext, out _);
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
        internal Status InternalCompletePendingRequestFromContext<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(TSessionFunctionsWrapper sessionFunctions, AsyncIOContext<TKey, TValue> request,
                                                                    ref PendingContext<TInput, TOutput, TContext> pendingContext, out AsyncIOContext<TKey, TValue> newRequest)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKeyLocker : struct, IKeyLocker
        {
            Debug.Assert(Kernel.Epoch.ThisInstanceProtected(), "InternalCompletePendingRequestFromContext requires epoch acquision");
            newRequest = default;

            // If NoKey, we do not have the key in the initial call and must use the key from the satisfied request.
            // With the new overload of CompletePending that returns CompletedOutputs, pendingContext must have the key.
            if (pendingContext.NoKey && pendingContext.key == default)
                pendingContext.key = hlog.GetKeyContainer(ref hlog.GetContextRecordKey(ref request));
            ref TKey key = ref pendingContext.key.Get();

            OperationStatus internalStatus = pendingContext.type switch
            {
                OperationType.READ => ContinuePendingRead<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(request, ref pendingContext, sessionFunctions),
                OperationType.RMW => ContinuePendingRMW<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(request, ref pendingContext, sessionFunctions),
                OperationType.CONDITIONAL_INSERT => ContinuePendingConditionalCopyToTail<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(request, ref pendingContext, sessionFunctions),
                // No locking is done for IO'd iteration pushes
                OperationType.CONDITIONAL_SCAN_PUSH => ContinuePendingConditionalScanPush(request, ref pendingContext, sessionFunctions),
                _ => throw new TsavoriteException("Unexpected OperationType")
            };

            var status = HandleOperationStatus(sessionFunctions.ExecutionCtx, ref pendingContext, internalStatus, out newRequest);

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