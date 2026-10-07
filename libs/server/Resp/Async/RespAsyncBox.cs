// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;

namespace Garnet.server
{
    /// <summary>
    /// Heap home for a suspended command body's state machine, and the <see cref="IValueTaskSource"/> that
    /// backs the body's <see cref="System.Threading.Tasks.ValueTask"/>.
    /// </summary>
    /// <remarks>
    /// A box is rented when a command body first suspends and returned once that body completes, so a session
    /// that repeatedly runs the same command allocates one box for its lifetime. The resume delegate is built
    /// once per box, which is what keeps a suspension allocation-free.
    /// </remarks>
    internal abstract class RespAsyncBox : IValueTaskSource
    {
        ManualResetValueTaskSourceCore<bool> core;

        /// <summary>
        /// Session whose scope this body's continuations must run inside.
        /// </summary>
        internal RespServerSession Session;

        /// <summary>
        /// Delegate handed to awaiters when the body suspends. Allocated once per box.
        /// </summary>
        internal readonly Action ResumeAction;

        protected RespAsyncBox() => ResumeAction = Resume;

        internal short Version => core.Version;

        void Resume() => Session.ResumeAsyncCommand(this);

        /// <summary>
        /// Drives the boxed state machine. Called only from the session resume pump, which has already
        /// re-entered the session's response object and epoch scope.
        /// </summary>
        internal abstract void MoveNext();

        /// <summary>
        /// Drops the state machine so a pooled box does not root the command's locals.
        /// </summary>
        internal abstract void Clear();

        /// <summary>
        /// Parks a cleared box on the per-thread free list for its state machine type.
        /// </summary>
        internal abstract void ReturnToThreadCache();

        /// <summary>
        /// Prepares a rented box for a fresh suspension.
        /// </summary>
        internal void Rearm(RespServerSession session)
        {
            Session = session;
            core.Reset();
        }

        internal void SetResult() => core.SetResult(true);

        internal void SetException(Exception exception) => core.SetException(exception);

        /// <inheritdoc />
        public void GetResult(short token) => core.GetResult(token);

        /// <inheritdoc />
        public ValueTaskSourceStatus GetStatus(short token) => core.GetStatus(token);

        /// <inheritdoc />
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
            => core.OnCompleted(continuation, state, token, flags);
    }

    /// <summary>
    /// Box specialised to one compiler-generated state machine type.
    /// </summary>
    /// <typeparam name="TStateMachine">The command body's state machine.</typeparam>
    internal sealed class RespAsyncBox<TStateMachine> : RespAsyncBox where TStateMachine : IAsyncStateMachine
    {
        /// <summary>
        /// The state machine itself. Held as a field rather than behind an interface so
        /// <see cref="MoveNext"/> calls it by reference and mutations stick.
        /// </summary>
        internal TStateMachine StateMachine;

        /// <inheritdoc />
        internal override void MoveNext() => StateMachine.MoveNext();

        /// <inheritdoc />
        internal override void Clear()
        {
            StateMachine = default;
            Session = null;
        }

        /// <inheritdoc />
        internal override void ReturnToThreadCache() => RespAsyncBoxCache<TStateMachine>.Cached = this;
    }

    /// <summary>
    /// Per-thread, per-state-machine free list. Catches the case where a session alternates between command
    /// types, which the session's own single-box cache cannot.
    /// </summary>
    /// <typeparam name="TStateMachine">The command body's state machine.</typeparam>
    internal static class RespAsyncBoxCache<TStateMachine> where TStateMachine : IAsyncStateMachine
    {
        [ThreadStatic]
        internal static RespAsyncBox<TStateMachine> Cached;
    }
}