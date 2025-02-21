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
        List<SemaphoreSlim> waitingList;
        SemaphoreSlim stateMachineCompleted;

        readonly LightEpoch epoch;
        readonly ILogger logger;

        public StateMachineDriver(LightEpoch epoch, ILogger logger = null)
        {
            this.epoch = epoch;
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

            var nextState = stateMachine.NextState(systemState, out var bumpEpoch);

            if (bumpEpoch)
            {
                Debug.Assert(!epoch.ThisInstanceProtected());
                var waitForTransition = new SemaphoreSlim(0);
                try
                {
                    epoch.Resume();
                    epoch.BumpCurrentEpoch(() => MakeTransitionWorker(expectedState, nextState, waitForTransition));
                }
                finally
                {
                    epoch.Suspend();
                }
                waitingList.Add(waitForTransition);
            }
            else
            {
                MakeTransitionWorker(expectedState, nextState);
            }
        }

        void MakeTransitionWorker(SystemState currentState, SystemState nextState, SemaphoreSlim onComplete = null)
        {
            stateMachine.GlobalBeforeEnteringState(nextState, this);

            Debug.Assert(SystemState.Equal(currentState, systemState));
            systemState.Word = nextState.Word;
            logger?.LogTrace("Moved to {0}, {1}", nextState.Phase, nextState.Version);

            stateMachine.GlobalAfterEnteringState(nextState, this);

            onComplete?.Release();
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
                while (systemState.Phase != Phase.REST)
                {
                    GlobalStateMachineStep(systemState);
                    await ProcessWaitingListAsync(token);
                }
            }
            finally
            {
                var _stateMachineCompleted = stateMachineCompleted;
                stateMachineCompleted = null;
                _ = Interlocked.Exchange(ref stateMachine, null);
                _stateMachineCompleted.Release();
            }
        }
    }
}
