// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    public class StateMachineDriver
    {
        SystemState systemState;
        IStateMachine stateMachine;
        readonly List<SemaphoreSlim> waitingList;
        SemaphoreSlim stateMachineCompleted;
        // All threads have entered the given state
        SemaphoreSlim waitForTransitionIn;
        // All threads have exited the given state
        SemaphoreSlim waitForTransitionOut;

        readonly LightEpoch epoch;
        readonly ILogger logger;

        public StateMachineDriver(LightEpoch epoch, SystemState initialState, ILogger logger = null)
        {
            this.epoch = epoch;
            this.systemState = initialState;
            this.waitingList = [];
            this.logger = logger;
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
            stateMachineCompleted = new SemaphoreSlim(0);
            _ = Task.Run(async () => await RunStateMachine(token));
            return true;
        }

        public async Task<bool> RunAsync(IStateMachine stateMachine, CancellationToken token = default)
        {
            if (Interlocked.CompareExchange(ref this.stateMachine, stateMachine, null) != null)
            {
                return false;
            }
            stateMachineCompleted = new SemaphoreSlim(0);
            await RunStateMachine(token);
            return true;
        }

        public async Task CompleteAsync(CancellationToken token = default)
        {
            var _stateMachineCompleted = stateMachineCompleted;
            if (_stateMachineCompleted != null)
            {
                await _stateMachineCompleted.WaitAsync(token);
            }
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

            // Write new phase
            systemState.Word = nextState.Word;

            // Release waiters for new phase
            waitForTransitionOut?.Release(int.MaxValue);

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
            waitingList.Add(waitForTransitionIn);
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
            try
            {
                do
                {
                    GlobalStateMachineStep(systemState);
                    await ProcessWaitingListAsync(token);
                } while (systemState.Phase != Phase.REST);
            }
            finally
            {
                var _stateMachineCompleted = stateMachineCompleted;
                stateMachineCompleted = null;
                _ = Interlocked.Exchange(ref stateMachine, null);
                _stateMachineCompleted.Release(int.MaxValue);
            }
        }
    }
}
