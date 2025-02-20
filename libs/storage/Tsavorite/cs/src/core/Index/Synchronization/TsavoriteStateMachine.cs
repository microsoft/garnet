// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    public partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        // The current system state, defined as the combination of a phase and a version number. This value
        // is observed by all sessions and a state machine communicates its progress to sessions through
        // this value
        internal SystemState systemState;
        // This flag ensures that only one state machine is active at a given time.
        private volatile int stateMachineActive = 0;
        // The current state machine in the system. The value could be stale and point to the previous state machine
        // if no state machine is active at this time.
        private ISynchronizationStateMachine<TKey, TValue, TStoreFunctions, TAllocator> currentSyncStateMachine;
        private List<IStateMachineCallback<TKey, TValue, TStoreFunctions, TAllocator>> callbacks = new();
        internal long lastVersion;

        /// <summary>
        /// Any additional (user specified) metadata to write out with commit
        /// </summary>
        public byte[] CommitCookie { get; set; }

        private byte[] recoveredCommitCookie;
        /// <summary>
        /// User-specified commit cookie persisted with last recovered commit
        /// </summary>
        public byte[] RecoveredCommitCookie => recoveredCommitCookie;

        /// <summary>
        /// Get the current state machine state of the system
        /// </summary>
        public SystemState SystemState => systemState;

        /// <summary>
        /// Version number of the last checkpointed state
        /// </summary>
        public long LastCheckpointedVersion => lastVersion;

        /// <summary>
        /// Size (tail address) of current incremental snapshot delta log
        /// </summary>
        public long IncrementalSnapshotTailAddress => _lastSnapshotCheckpoint.deltaLog?.TailAddress ?? 0;

        /// <summary>
        /// Recovered version number (1 if started from clean slate)
        /// </summary>
        public long RecoveredVersion => systemState.Version;

        /// <summary>
        /// Current version number of the store
        /// </summary>
        public long CurrentVersion => systemState.Version;

        /// <summary>
        /// Registers the given callback to be invoked for every state machine transition. Not safe to call with
        /// concurrent Tsavorite operations. Note that registered callbacks execute as part of the critical
        /// section of Tsavorite's state transitions. Excessive synchronization or expensive computation in the callback
        /// may slow or halt state machine execution. For advanced users only. 
        /// </summary>
        /// <param name="callback"> callback to register </param>
        public void UnsafeRegisterCallback(IStateMachineCallback<TKey, TValue, TStoreFunctions, TAllocator> callback) => callbacks.Add(callback);

        /// <summary>
        /// Attempt to start the given state machine in the system if no other state machine is active.
        /// </summary>
        /// <param name="stateMachine">The state machine to start</param>
        /// <returns>true if the state machine has started, false otherwise</returns>
        private bool StartStateMachine(ISynchronizationStateMachine<TKey, TValue, TStoreFunctions, TAllocator> stateMachine)
        {
            // return immediately if there is a state machine under way.
            if (Interlocked.CompareExchange(ref stateMachineActive, 1, 0) != 0) return false;
            currentSyncStateMachine = stateMachine;

            // No latch required because the above Interlock guards against other tasks starting, and only a new task
            // is allowed to change Tsavorite global state from REST
            GlobalStateMachineStep(systemState);
            return true;
        }

        // Atomic transition from expectedState -> nextState
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool MakeTransition(SystemState expectedState, SystemState nextState)
        {
            if (Interlocked.CompareExchange(ref systemState.Word, nextState.Word, expectedState.Word) !=
                expectedState.Word) return false;
            Debug.WriteLine("Moved to {0}, {1}", nextState.Phase, nextState.Version);
            logger?.LogTrace("Moved to {0}, {1}",
                (nextState.Phase & Phase.INTERMEDIATE) == 0 ? nextState.Phase : "Intermediate (" + (nextState.Phase & ~Phase.INTERMEDIATE) + ")",
                nextState.Version);
            return true;
        }

        /// <summary>
        /// Steps the global state machine. This will change the current global system state and perform some actions
        /// as prescribed by the current state machine. This function has no effect if the current state is not
        /// the given expected state.
        /// </summary>
        /// <param name="expectedState">expected current global state</param>
        /// <param name="bumpEpoch">whether we bump the epoch for the final state transition</param>
        internal void GlobalStateMachineStep(SystemState expectedState, bool bumpEpoch = false)
        {
            // Between state transition, temporarily block any concurrent execution thread 
            // from progressing to prevent perceived inconsistencies
            var intermediate = SystemState.MakeIntermediate(expectedState);
            if (!MakeTransition(expectedState, intermediate)) return;

            var nextState = currentSyncStateMachine.NextState(expectedState);

            if (bumpEpoch)
                epoch.BumpCurrentEpoch(() => MakeTransitionWorker(intermediate, nextState));
            else
                MakeTransitionWorker(intermediate, nextState);
        }

        void MakeTransitionWorker(SystemState intermediate, SystemState nextState)
        {
            // Execute custom task logic
            currentSyncStateMachine.GlobalBeforeEnteringState(nextState, this);
            // Execute any additional callbacks in critical section
            foreach (var callback in callbacks)
                callback.BeforeEnteringState(nextState, this);

            var success = MakeTransition(intermediate, nextState);
            // Guaranteed to succeed, because other threads will always block while the system is in intermediate.
            Debug.Assert(success);
            currentSyncStateMachine.GlobalAfterEnteringState(nextState, this);

            // Mark the state machine done as we exit the state machine.
            if (nextState.Phase == Phase.REST) stateMachineActive = 0;
        }

        // Given the current global state, return the starting point of the state machine cycle
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static SystemState StartOfCurrentCycle(SystemState currentGlobalState)
        {
            return currentGlobalState.Phase < Phase.REST
                ? SystemState.Make(Phase.REST, currentGlobalState.Version - 1)
                : SystemState.Make(Phase.REST, currentGlobalState.Version);
        }

        // Given the current thread state and global state, fast forward the thread state to the
        // current state machine cycle if needed
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static SystemState FastForwardToCurrentCycle(SystemState threadState, SystemState targetStartState)
        {
            if (threadState.Version < targetStartState.Version ||
                threadState.Version == targetStartState.Version && threadState.Phase < targetStartState.Phase)
            {
                return targetStartState;
            }

            return threadState;
        }

        /// <summary>
        /// Check whether thread is in same cycle compared to current systemState
        /// </summary>
        /// <param name="ctx"></param>
        /// <param name="threadState"></param>
        /// <returns></returns>
        internal bool SameCycle<TInput, TOutput, TContext>(TsavoriteExecutionContext<TInput, TOutput, TContext> ctx, SystemState threadState)
        {
            if (ctx == null)
            {
                var _systemState = SystemState.Copy(ref systemState);
                SystemState.RemoveIntermediate(ref _systemState);
                return StartOfCurrentCycle(threadState).Version == StartOfCurrentCycle(_systemState).Version;
            }
            return ctx.threadStateMachine == currentSyncStateMachine;
        }

        /// <summary>
        /// Steps the thread's local state machine. Threads catch up to the current global state and performs
        /// necessary actions associated with the state as defined by the current state machine
        /// </summary>
        /// <param name="ctx">null if calling without a context (e.g. waiting on a checkpoint)</param>
        /// <param name="sessionFunctions">Tsavorite session.</param>
        /// <param name="valueTasks">Return list of tasks that caller needs to await, to continue checkpointing</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        private void ThreadStateMachineStep<TInput, TOutput, TContext, TSessionFunctionsWrapper>(
            TsavoriteExecutionContext<TInput, TOutput, TContext> ctx,
            TSessionFunctionsWrapper sessionFunctions,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where TSessionFunctionsWrapper : ISessionEpochControl
        {
            #region Capture current (non-intermediate) system state
            var currentTask = currentSyncStateMachine;
            var targetState = SystemState.Copy(ref systemState);
            SystemState.RemoveIntermediate(ref targetState);

            while (currentSyncStateMachine != currentTask)
            {
                currentTask = currentSyncStateMachine;
                targetState = SystemState.Copy(ref systemState);
                SystemState.RemoveIntermediate(ref targetState);
            }
            #endregion

            var currentState = ctx is null ? targetState : SystemState.Make(ctx.phase, ctx.version);
            var targetStartState = StartOfCurrentCycle(targetState);

            // No state machine associated with target, or target is in REST phase:
            // we can directly fast forward session to target state
            if (currentTask == null || targetState.Phase == Phase.REST)
            {
                if (ctx is not null)
                {
                    ctx.phase = targetState.Phase;
                    ctx.version = targetState.Version;
                    ctx.threadStateMachine = currentTask;
                }
                return;
            }

            #region Jump on and execute current state machine
            // We start at either the start point or our previous position in the state machine.
            // If we are calling from somewhere other than an execution thread (e.g. waiting on
            // a checkpoint to complete on a client app thread), we start at current system state
            var threadState = targetState;

            if (ctx is not null)
            {
                if (ctx.threadStateMachine == currentTask)
                {
                    threadState = currentState;
                }
                else
                {
                    threadState = targetStartState;
                    ctx.threadStateMachine = currentTask;
                }
            }

            var previousState = threadState;
            do
            {
                Debug.Assert(
                    (threadState.Version < targetState.Version) ||
                       (threadState.Version == targetState.Version &&
                       (threadState.Phase <= targetState.Phase || currentTask is IndexSnapshotStateMachine<TKey, TValue, TStoreFunctions, TAllocator>)
                    ));

                currentTask.OnThreadEnteringState(threadState, previousState, this, ctx, sessionFunctions, valueTasks, token);

                if (ctx is not null)
                {
                    ctx.phase = threadState.Phase;
                    ctx.version = threadState.Version;
                }

                previousState.Word = threadState.Word;
                threadState = currentTask.NextState(threadState);
                if (systemState.Word != targetState.Word)
                {
                    var tmp = SystemState.Copy(ref systemState);
                    if (currentSyncStateMachine == currentTask)
                    {
                        targetState = tmp;
                        SystemState.RemoveIntermediate(ref targetState);
                    }
                }
            } while (previousState.Word != targetState.Word);
            #endregion

            return;
        }
    }
}