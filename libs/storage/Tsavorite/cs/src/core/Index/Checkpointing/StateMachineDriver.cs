﻿// Copyright (c) Microsoft Corporation.
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
        List<IStateMachineCallback> callbacks;
        readonly LightEpoch epoch;
        readonly ILogger logger;

        public SystemState SystemState => SystemState.Copy(ref systemState);

        public StateMachineDriver(LightEpoch epoch, SystemState initialState, ILogger logger = null)
        {
            this.epoch = epoch;
            this.systemState = initialState;
            this.waitingList = [];
            this.logger = logger;
        }

        public void SetSystemState(SystemState state)
        {
            systemState = SystemState.Copy(ref state);
        }

        internal void AddToWaitingList(SemaphoreSlim waiter)
        {
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

            // Write new semaphore
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
            AddToWaitingList(waitForTransitionIn);
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
            stateMachine.GlobalAfterEnteringState(nextState, this);
            waitForTransitionIn.Release(int.MaxValue);
        }

        async Task ProcessWaitingListAsync(CancellationToken token = default)
        {
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