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
                if (lastVersionTransactionsDone != null && txnVersion == lastVersion)
                {
                    lastVersionTransactionsDone.Release();
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

            // Get the next state from the state machine definition. Each state machine knows it's own transition flow. They define it and basically expose it for the driver
            // to query and then execute the transitions by calling the 
            var nextState = stateMachine.NextState(systemState);

            // The state machine internally holds an array of tasks. This will iterate over each of those tasks and call BeforeEnteringState on each of them.
            // The tasks internally have logic that they may wish to perform before they can transition to the next state, so this is the hook for that.
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

            // Write new semaphore
            waitForTransitionOut = new SemaphoreSlim(0);
            waitForTransitionIn = new SemaphoreSlim(0);

            logger?.LogTrace("Moved to {0}, {1}", nextState.Phase, nextState.Version);

            // Below we register the MakeTransitionWorker to be called when all threads have passed the epoch acquired at 227. That is to say they have all seen the changes till now, and this guarantees that MakeTransitionWorker is only called after
            // everyone is seeing a view atleast fresh till this point in time.
            // The same epoch object is shared with the TsavoriteStore internally as the one held here. So we are essentially trying to use it to lazily communicate with all threads, and synchronize when a callback can be called safely.

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
            // notify each task within the state machine that we have entered the new state so they can call any logic they need to that they might have wanted to hook into.
            // For example after the state machine enters In-Progress hybrid log checkpoint task
            stateMachine.GlobalAfterEnteringState(nextState, this);
            waitForTransitionIn.Release(int.MaxValue);
        }

        async Task ProcessWaitingListAsync(CancellationToken token = default)
        {
            await waitForTransitionIn.WaitAsync(token);
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
                // Crux of transitioning through states will run here
                do
                {
                    GlobalStateMachineStep(systemState);
                    // wait for threads to say they have entered the new state, by releasing to waitForTransitionIn semaphore.
                    // This is basically blocking till the callback MakeTransitionWorker is called by the epoch system after all threads have seen the new state.
                    await ProcessWaitingListAsync(token);
                } while (systemState.Phase != Phase.REST);
            }
            catch (Exception e)
            {
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
    }
}