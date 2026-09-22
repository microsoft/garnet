// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Driver for the state machine. This class is responsible for executing the state machine.
    /// </summary>
    public class StateMachineDriver
    {
        // Globally published phase and version.
        SystemState systemState;

        // The single state machine currently owning this driver; null while idle.
        IStateMachine stateMachine;

        // Already-started tasks that must complete before the driver can leave the current phase.
        // ProcessWaitingListAsync awaits these only after the transition-in epoch barrier completes.
        readonly List<(Task task, StateMachineTaskType type)> waitingList;

        // Completion source for the entire state-machine run, not an individual phase transition.
        TaskCompletionSource<bool> stateMachineCompleted;

        // Semaphore associated with the currently published state. MakeTransitionWorker releases it after
        // prior-epoch participants have advanced or suspended and GlobalAfterEnteringState has completed.
        // This does not include completion of tasks in waitingList.
        SemaphoreSlim waitForTransitionIn;

        // GlobalAfterEnteringState may run on an arbitrary epoch-drain thread, so its exception is captured
        // here and rethrown by ProcessWaitingListAsync on the state-machine driver path.
        Exception waitForTransitionInException;

        // Semaphore associated with the currently published state. GlobalStateMachineStep releases it as
        // soon as the next state is published; it does not wait for the transition-in epoch barrier.
        SemaphoreSlim waitForTransitionOut;

        // Version whose active transactions must drain before the state machine can advance.
        long lastVersion;
        TaskCompletionSource<bool> lastVersionTransactionsDone;

        List<IStateMachineCallback> callbacks;
        readonly LightEpoch epoch;
        readonly ILogger logger;

        // Active transaction counts are indexed by version parity; only two adjacent versions can be active.
        readonly long[] NumActiveTransactions;

        public SystemState SystemState => SystemState.Copy(ref systemState);

        public StateMachineDriver(LightEpoch epoch, ILogger logger = null)
        {
            this.epoch = epoch;
            this.systemState = SystemState.Make(Phase.REST, 1);
            this.waitingList = [];
            this.NumActiveTransactions = new long[2];
            this.logger = logger;
        }

        public void SetSystemState(SystemState state)
            => systemState = SystemState.Copy(ref state);

        internal long GetNumActiveTransactions(long txnVersion)
            => Interlocked.Read(ref NumActiveTransactions[txnVersion & 0x1]);

        void IncrementActiveTransactions(long txnVersion)
            => _ = Interlocked.Increment(ref NumActiveTransactions[txnVersion & 0x1]);

        void DecrementActiveTransactions(long txnVersion)
        {
            if (Interlocked.Decrement(ref NumActiveTransactions[txnVersion & 0x1]) == 0)
            {
                var _lastVersionTransactionsDone = lastVersionTransactionsDone;
                if (_lastVersionTransactionsDone != null && txnVersion == lastVersion)
                {
                    _lastVersionTransactionsDone.TrySetResult(true);
                }
            }
        }

        internal void TrackLastVersion(long version)
        {
            if (GetNumActiveTransactions(version) > 0)
            {
                // Set version number first, then create TCS
                lastVersion = version;
                lastVersionTransactionsDone = new(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            // We have to re-check the number of active transactions after assigning lastVersion and lastVersionTransactionsDone
            if (GetNumActiveTransactions(version) > 0)
                AddToWaitingList(lastVersionTransactionsDone.Task, StateMachineTaskType.LastVersionTransactionsDone);
        }

        internal void ResetLastVersion()
        {
            // First null TCS, then reset version number
            lastVersionTransactionsDone = null;
            lastVersion = 0;
        }

        /// <summary>
        /// Acquire a transaction version - this should be called before
        /// BeginLockable is called for all sessions in the transaction.
        /// </summary>
        /// <returns></returns>
        public long AcquireTransactionVersion()
        {
            var isProtected = epoch.ThisInstanceProtected();
            if (!isProtected)
                epoch.Resume();
            try
            {
                // We create a barrier preventing new transactions from starting in the PREPARE_GROW phase
                // since the lock table needs to be drained and transferred to the larger hash index.
                while (systemState.Phase == Phase.PREPARE_GROW)
                {
                    epoch.ProtectAndDrain();
                    _ = Thread.Yield();
                }
                var txnVersion = systemState.Version;
                Debug.Assert(txnVersion > 0);
                IncrementActiveTransactions(txnVersion);
                return txnVersion;
            }
            finally
            {
                if (!isProtected)
                    epoch.Suspend();
            }
        }

        /// <summary>
        /// Verify transaction version - this should be called after
        /// all locks have been acquired for the transaction.
        /// </summary>
        /// <returns></returns>
        public long VerifyTransactionVersion(long txnVersion)
        {
            var isProtected = epoch.ThisInstanceProtected();
            if (!isProtected)
                epoch.Resume();
            try
            {
                Debug.Assert(txnVersion > 0);
                var currentTxnVersion = systemState.Version;
                if (currentTxnVersion > txnVersion)
                {
                    // We transfer the active transaction from txnVersion to currentTxnVersion
                    Debug.Assert(currentTxnVersion == txnVersion + 1);
                    DecrementActiveTransactions(txnVersion);
                    IncrementActiveTransactions(currentTxnVersion);
                }
                return currentTxnVersion;
            }
            finally
            {
                if (!isProtected)
                    epoch.Suspend();
            }
        }

        /// <summary>
        /// End transaction running in specified version. Should be called
        /// after EndLockable() is called for all relevant sessions.
        /// </summary>
        /// <param name="txnVersion">Transaction version</param>
        /// <returns></returns>
        public void EndTransaction(long txnVersion)
            => DecrementActiveTransactions(txnVersion);

        internal void AddToWaitingList(Task waiter, StateMachineTaskType type)
        {
            // Callers start the operation before registering it. The driver awaits it after transition-in.
            if (waiter != null)
                waitingList.Add((waiter, type));
        }

        public bool Register(IStateMachine stateMachine, CancellationToken token = default)
        {
            if (Interlocked.CompareExchange(ref this.stateMachine, stateMachine, null) != null)
            {
                return false;
            }
            stateMachineCompleted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _ = Task.Run(async () => await RunStateMachine(token).ConfigureAwait(false));
            return true;
        }

        /// <summary>
        /// Registers the given callback to be invoked for every state machine transition. Not safe to call with
        /// concurrent Tsavorite operations. Excessive synchronization or expensive computation in the callback 
        /// may slow or halt state machine execution. For advanced users only.
        /// </summary>
        /// <param name="callback"> callback to register </param>
        public void UnsafeRegisterCallback(IStateMachineCallback callback)
        {
            callbacks ??= new();
            callbacks.Add(callback);
        }

        public async Task<bool> RunAsync(IStateMachine stateMachine, CancellationToken token = default)
        {
            if (Interlocked.CompareExchange(ref this.stateMachine, stateMachine, null) != null)
            {
                return false;
            }
            stateMachineCompleted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            await RunStateMachine(token).ConfigureAwait(false);
            return true;
        }

        public async Task<bool> CompleteAsync(CancellationToken token = default)
        {
            var _stateMachineCompleted = stateMachineCompleted;
            if (_stateMachineCompleted != null)
            {
                using var reg = token.Register(() => _stateMachineCompleted.TrySetCanceled());
                return await _stateMachineCompleted.Task.WithCancellationAsync(token).ConfigureAwait(false);
            }
            return false;
        }

        /// <summary>
        /// Steps the global state machine. This will change the current global system state and perform some actions
        /// as prescribed by the current state machine. This function has no effect if the current state is not
        /// the given expected state.
        /// </summary>
        /// <param name="expectedState">expected current global state</param>
        void GlobalStateMachineStep(SystemState expectedState)
        {
            if (!SystemState.Equal(expectedState, systemState))
                return;

            var nextState = stateMachine.NextState(systemState);

            // Run task-specific work while systemState still identifies the previous phase.
            stateMachine.GlobalBeforeEnteringState(nextState, this);

            // External callbacks have the same before-publication ordering as the state-machine task hooks.
            if (callbacks != null)
            {
                foreach (var callback in callbacks)
                    callback.BeforeEnteringState(nextState);
            }

            // Publish the new phase and version so subsequent session refreshes observe nextState.
            systemState.Word = nextState.Word;

            // Release the semaphore associated with the phase just exited.
            _ = waitForTransitionOut?.Release(int.MaxValue);

            // Install semaphores for the newly published phase. Its transition-out semaphore is released
            // when the following phase is published; its transition-in semaphore is released by the
            // epoch-drain callback below. These assignments occur after systemState is published.
            waitForTransitionOut = new SemaphoreSlim(0);
            waitForTransitionIn = new SemaphoreSlim(0);

            logger?.LogTrace("SMD: Moved to {0}, {1}", nextState.Phase, nextState.Version);

            Debug.Assert(!epoch.ThisInstanceProtected());
            try
            {
                epoch.Resume();

                // Associate MakeTransitionWorker with the prior epoch. It becomes eligible only after
                // participants still announcing that epoch have advanced through ProtectAndDrain or suspended.
                epoch.BumpCurrentEpoch(() => MakeTransitionWorker(nextState));
            }
            finally
            {
                epoch.Suspend();
            }
        }

        /// <summary>
        /// Wait for the state machine to change state out of currentState.
        /// </summary>
        /// <param name="currentState"></param>
        /// <returns></returns>
        public async Task WaitForStateChange(SystemState currentState)
        {
            // Capture before rechecking state so a racing transition that releases this semaphore is observed.
            var _waitForTransitionOut = waitForTransitionOut;
            if (SystemState.Equal(currentState, systemState))
            {
                await _waitForTransitionOut.WaitAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Wait for all thread participants to complete currentState.
        /// </summary>
        /// <param name="currentState"></param>
        /// <returns></returns>
        public async Task WaitForCompletion(SystemState currentState)
        {
            // First wait until currentState is no longer published.
            await WaitForStateChange(currentState).ConfigureAwait(false);

            // Then capture the newly published state and wait until its epoch transition and
            // GlobalAfterEnteringState hooks complete. Phase waiting-list tasks are not included.
            currentState = systemState;
            var _waitForTransitionIn = waitForTransitionIn;
            if (SystemState.Equal(currentState, systemState))
            {
                await _waitForTransitionIn.WaitAsync().ConfigureAwait(false);
            }
        }

        void MakeTransitionWorker(SystemState nextState)
        {
            try
            {
                // This is an epoch-drain action and may execute synchronously from BumpCurrentEpoch
                // or later on any thread that advances or suspends epoch protection.
                stateMachine.GlobalAfterEnteringState(nextState, this);
            }
            catch (Exception e)
            {
                // Propagate on the driver path rather than throwing on an arbitrary epoch-drain thread.
                waitForTransitionInException = e;

                logger?.LogError(e, "Exception in state machine transition worker");
            }
            finally
            {
                // Signal that the epoch transition and all after-transition hooks have finished.
                waitForTransitionIn.Release(int.MaxValue);
            }
        }

        async Task ProcessWaitingListAsync(CancellationToken token = default)
        {
            // Do not process phase tasks until the prior epoch has drained and after-transition hooks finish.
            await waitForTransitionIn.WaitAsync(token).ConfigureAwait(false);
            if (waitForTransitionInException != null)
            {
                throw waitForTransitionInException;
            }

            // These tasks were started by state-machine hooks and may have progressed concurrently with
            // the epoch transition. Awaiting them here prevents the driver from publishing the next phase.
            foreach (var (task, type) in waitingList)
            {
                try
                {
                    await task.WaitAsync(token).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger?.LogError(ex, "State machine task '{type}' faulted", type);
                    throw;
                }
            }
            waitingList.Clear();
        }

        async Task RunStateMachine(CancellationToken token = default)
        {
            Exception ex = null;
            try
            {
                do
                {
                    // Publish one transition, then wait for both transition-in and its registered phase work.
                    GlobalStateMachineStep(systemState);
                    await ProcessWaitingListAsync(token).ConfigureAwait(false);
                } while (systemState.Phase != Phase.REST);
            }
            catch (Exception e)
            {
                FastForwardStateMachineToRest();
                ReleaseAbortedStateMachineResources(e);
                logger?.LogError(e, "Exception in state machine");
                ex = e;
                throw;
            }
            finally
            {
                var _stateMachineCompleted = stateMachineCompleted;
                stateMachineCompleted = null;
                _ = Interlocked.Exchange(ref stateMachine, null);
                if (ex != null)
                {
                    // If the state machine stopped due to cancellation, propagate cancellation to the completion TCS
                    if (ex is OperationCanceledException || ex is TaskCanceledException)
                        _ = _stateMachineCompleted.TrySetCanceled();
                    else
                        _ = _stateMachineCompleted.TrySetException(ex);
                }
                else
                {
                    _ = _stateMachineCompleted.TrySetResult(true);
                }
            }
        }

        /// <summary>
        /// Lets the state machine release resources it acquired in the phases it entered before aborting, and
        /// complete anything the rest of the system is waiting on, since the REST phase that normally does both is
        /// never reached.
        /// </summary>
        /// <param name="exception">The exception that aborted the state machine.</param>
        void ReleaseAbortedStateMachineResources(Exception exception)
        {
            try
            {
                stateMachine.OnAbort(this, exception);
            }
            catch (Exception e)
            {
                // Must not replace the exception that aborted the state machine, which is the actionable one.
                logger?.LogError(e, "Exception while releasing the resources of an aborted state machine");
            }
        }

        void FastForwardStateMachineToRest()
        {
            // Move system state to the next REST phase
            while (systemState.Phase != Phase.REST)
            {
                systemState.Word = stateMachine.NextState(systemState).Word;
            }

            // Reset last version
            ResetLastVersion();

            // Release any waiters on existing transition-out semaphore
            if (waitForTransitionOut?.CurrentCount == 0)
                _ = waitForTransitionOut?.Release(int.MaxValue);

            // Failure recovery does not execute skipped transition hooks. Discard their synchronization state.
            waitForTransitionOut = null;
            waitForTransitionIn = null;

            // Clear any exception captured from an after-transition hook.
            waitForTransitionInException = null;

            // The failed run no longer waits for phase-specific asynchronous work.
            waitingList.Clear();
        }
    }
}