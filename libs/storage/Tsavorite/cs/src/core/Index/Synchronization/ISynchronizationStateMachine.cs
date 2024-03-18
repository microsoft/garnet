// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// A state machine defines a serious of actions that changes the system, which requires all sessions to
    /// synchronize and agree on certain time points. A full run of the state machine is defined as a cycle
    /// starting from REST and ending in REST, and only one state machine can be active at a given time.
    /// </summary>
    internal interface ISynchronizationStateMachine
    {
        /// <summary>
        /// Returns the version that we expect this state machine to end up at when back to REST, or -1 if not yet known.
        /// </summary>
        /// <returns> The version that we expect this state machine to end up at when back to REST </returns>
        long ToVersion();

        /// <summary>
        /// This function models the transition function of a state machine.
        /// </summary>
        /// <param name="start">The current state of the state machine</param>
        /// <returns> the next state in this state machine </returns>
        SystemState NextState(SystemState start);

        /// <summary>
        /// This function is invoked immediately before the global state machine enters the given state.
        /// </summary>
        /// <param name="next"></param>
        /// <param name="tsavorite"></param>
        /// <typeparam name="Key"></typeparam>
        /// <typeparam name="Value"></typeparam>
        void GlobalBeforeEnteringState<Key, Value>(SystemState next,
            TsavoriteKV<Key, Value> tsavorite);

        /// <summary>
        /// This function is invoked immediately after the global state machine enters the given state.
        /// </summary>
        /// <param name="next"></param>
        /// <param name="tsavorite"></param>
        /// <typeparam name="Key"></typeparam>
        /// <typeparam name="Value"></typeparam>
        void GlobalAfterEnteringState<Key, Value>(SystemState next,
            TsavoriteKV<Key, Value> tsavorite);

        /// <summary>
        /// This function is invoked for every thread when they refresh and observe a given state.
        ///
        /// Note that the function is not allowed to await when async is set to false.
        /// </summary>
        /// <param name="current"></param>
        /// <param name="prev"></param>
        /// <param name="tsavorite"></param>
        /// <param name="ctx"></param>
        /// <param name="tsavoriteSession"></param>
        /// <param name="valueTasks"></param>
        /// <param name="token"></param>
        /// <typeparam name="Key"></typeparam>
        /// <typeparam name="Value"></typeparam>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        /// <typeparam name="TsavoriteSession"></typeparam>
        /// <returns></returns>
        void OnThreadEnteringState<Key, Value, Input, Output, Context, TsavoriteSession>(SystemState current,
            SystemState prev,
            TsavoriteKV<Key, Value> tsavorite,
            TsavoriteKV<Key, Value>.TsavoriteExecutionContext<Input, Output, Context> ctx,
            TsavoriteSession tsavoriteSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where TsavoriteSession : ITsavoriteSession;
    }

    /// <summary>
    /// An ISynchronizationTask specifies logic to be run on a state machine, but does not specify a transition
    /// function. It is therefore possible to write common logic in an ISynchronizationTask and reuse it across
    /// multiple state machines, or to choose the task at runtime and achieve polymorphism in the behavior
    /// of a concrete state machine class.
    /// </summary>
    internal interface ISynchronizationTask
    {
        /// <summary>
        /// This function is invoked immediately before the global state machine enters the given state.
        /// </summary>
        /// <param name="next"></param>
        /// <param name="tsavorite"></param>
        /// <typeparam name="Key"></typeparam>
        /// <typeparam name="Value"></typeparam>
        void GlobalBeforeEnteringState<Key, Value>(
            SystemState next,
            TsavoriteKV<Key, Value> tsavorite);

        /// <summary>
        /// This function is invoked immediately after the global state machine enters the given state.
        /// </summary>
        /// <param name="next"></param>
        /// <param name="tsavorite"></param>
        /// <typeparam name="Key"></typeparam>
        /// <typeparam name="Value"></typeparam>
        void GlobalAfterEnteringState<Key, Value>(
            SystemState next,
            TsavoriteKV<Key, Value> tsavorite);

        /// <summary>
        /// This function is invoked for every thread when they refresh and observe a given state.
        ///
        /// Note that the function is not allowed to await when async is set to false.
        /// </summary>
        /// <param name="current"></param>
        /// <param name="prev"></param>
        /// <param name="tsavorite"></param>
        /// <param name="ctx"></param>
        /// <param name="tsavoriteSession"></param>
        /// <param name="valueTasks"></param>
        /// <param name="token"></param>
        /// <typeparam name="Key"></typeparam>
        /// <typeparam name="Value"></typeparam>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        /// <typeparam name="TsavoriteSession"></typeparam>
        /// <returns></returns>
        void OnThreadState<Key, Value, Input, Output, Context, TsavoriteSession>(
            SystemState current,
            SystemState prev,
            TsavoriteKV<Key, Value> tsavorite,
            TsavoriteKV<Key, Value>.TsavoriteExecutionContext<Input, Output, Context> ctx,
            TsavoriteSession tsavoriteSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where TsavoriteSession : ITsavoriteSession;
    }

    /// <summary>
    /// Abstract base class for ISynchronizationStateMachine that implements that state machine logic
    /// with ISynchronizationTasks
    /// </summary>
    internal abstract class SynchronizationStateMachineBase : ISynchronizationStateMachine
    {
        private readonly ISynchronizationTask[] tasks;
        private long toVersion = -1;


        /// <summary>
        /// Construct a new SynchronizationStateMachine with the given tasks. The order of tasks given is the
        /// order they are executed on each state machine.
        /// </summary>
        /// <param name="tasks">The ISynchronizationTasks to run on the state machine</param>
        protected SynchronizationStateMachineBase(params ISynchronizationTask[] tasks)
        {
            this.tasks = tasks;
        }

        /// <summary>
        /// Sets ToVersion for return. Defaults to -1 if not set
        /// </summary>
        /// <param name="v"> toVersion </param>
        protected void SetToVersion(long v) => toVersion = v;

        /// <inheritdoc />
        public long ToVersion() => toVersion;

        /// <inheritdoc />
        public abstract SystemState NextState(SystemState start);

        /// <inheritdoc />
        public void GlobalBeforeEnteringState<Key, Value>(SystemState next,
            TsavoriteKV<Key, Value> tsavorite)
        {
            foreach (var task in tasks)
                task.GlobalBeforeEnteringState(next, tsavorite);
        }

        /// <inheritdoc />
        public void GlobalAfterEnteringState<Key, Value>(SystemState next,
            TsavoriteKV<Key, Value> tsavorite)
        {
            foreach (var task in tasks)
                task.GlobalAfterEnteringState(next, tsavorite);
        }

        /// <inheritdoc />
        public void OnThreadEnteringState<Key, Value, Input, Output, Context, TsavoriteSession>(
            SystemState current,
            SystemState prev,
            TsavoriteKV<Key, Value> tsavorite,
            TsavoriteKV<Key, Value>.TsavoriteExecutionContext<Input, Output, Context> ctx,
            TsavoriteSession tsavoriteSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where TsavoriteSession : ITsavoriteSession
        {
            foreach (var task in tasks)
            {
                task.OnThreadState(current, prev, tsavorite, ctx, tsavoriteSession, valueTasks, token);
            }
        }
    }
}