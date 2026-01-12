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
        SystemState systemState;
        IStateMachine stateMachine;
        readonly List<SemaphoreSlim> waitingList;
        TaskCompletionSource<bool> stateMachineCompleted;
        // All threads have entered the given state
        SemaphoreSlim waitForTransitionIn;
        Exception waitForTransitionInException;
        // All threads have exited the given state
        SemaphoreSlim waitForTransitionOut;
        // Transactions drained in last version
        public long lastVersion;
        public SemaphoreSlim lastVersionTransactionsDone;
        List<IStateMachineCallback> callbacks;
        readonly LightEpoch epoch;
        readonly ILogger logger;
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
                    _lastVersionTransactionsDone.Release();
                }
            }
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

        internal void AddToWaitingList(SemaphoreSlim waiter)
        {
            if (waiter != null)
                waitingList.Add(waiter);
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

            stateMachine.GlobalBeforeEnteringState(nextState, this);

            // Execute any additional registered callbacks
            if (callbacks != null)
            {
                foreach (var callback in callbacks)
                    callback.BeforeEnteringState(nextState);
            }

            // Write new phase
            systemState.Word = nextState.Word;

            // Release waiters for new phase
            _ = waitForTransitionOut?.Release(int.MaxValue);

            // Write new semaphores
            waitForTransitionOut = new SemaphoreSlim(0);
            waitForTransitionIn = new SemaphoreSlim(0);

            logger?.LogTrace("Moved to {0}, {1}", nextState.Phase, nextState.Version);

            Debug.Assert(!epoch.ThisInstanceProtected());
            try
            {
                epoch.Resume();
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
            var _waitForTransitionOut = waitForTransitionOut;
            if (SystemState.Equal(currentState, systemState))
            {
                await _waitForTransitionOut.WaitAsync();
            }
        }

        /// <summary>
        /// Wait for all thread participants to complete currentState.
        /// </summary>
        /// <param name="currentState"></param>
        /// <returns></returns>
        public async Task WaitForCompletion(SystemState currentState)
        {
            await WaitForStateChange(currentState);
            currentState = systemState;
            var _waitForTransitionIn = waitForTransitionIn;
            if (SystemState.Equal(currentState, systemState))
            {
                await _waitForTransitionIn.WaitAsync();
            }
        }

        void MakeTransitionWorker(SystemState nextState)
        {
            try
            {
                stateMachine.GlobalAfterEnteringState(nextState, this);
            }
            catch (Exception e)
            {
                // Store the exception to be thrown by state machine driver
                // We do not throw here as this epoch action may be executed in a different thread context
                waitForTransitionInException = e;

                logger?.LogError(e, "Exception in state machine transition worker");
            }
            finally
            {
                waitForTransitionIn.Release(int.MaxValue);
            }
        }

        async Task ProcessWaitingListAsync(CancellationToken token = default)
        {
            await waitForTransitionIn.WaitAsync(token);
            if (waitForTransitionInException != null)
            {
                throw waitForTransitionInException;
            }
            foreach (var waiter in waitingList)
            {
                await waiter.WaitAsync(token);
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
                    GlobalStateMachineStep(systemState);
                    await ProcessWaitingListAsync(token);
                } while (systemState.Phase != Phase.REST);
            }
            catch (Exception e)
            {
                FastForwardStateMachineToRest();
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

        void FastForwardStateMachineToRest()
        {
            // Move system state to the next REST phase
            while (systemState.Phase != Phase.REST)
            {
                systemState.Word = stateMachine.NextState(systemState).Word;
            }

            // Release any waiters on existing transition-out semaphore
            if (waitForTransitionOut?.CurrentCount == 0)
                _ = waitForTransitionOut?.Release(int.MaxValue);

            // Clear semaphores
            waitForTransitionOut = null;
            waitForTransitionIn = null;

            // Clear exception if any
            waitForTransitionInException = null;

            // Clear waiting list
            waitingList.Clear();
        }
    }
}