// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Garnet.server
{
    /// <summary>
    /// Async method builder for RESP command bodies.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A command body runs inside the session's resource scope: the network sender's response object is held,
    /// the cluster epoch is acquired, and <c>dcurr</c>/<c>dend</c> point into the response buffer. A plain
    /// <c>await</c> would resume the body on whatever thread completed the awaited operation, outside that
    /// scope, where writing the reply is not valid. This builder routes every continuation through
    /// <see cref="RespServerSession.ResumeAsyncCommand"/>, which re-enters the scope before driving the state
    /// machine, so any <c>await</c> inside a command body is safe.
    /// </para>
    /// <para>
    /// <see cref="Start{TStateMachine}"/> deliberately omits the <see cref="System.Threading.ExecutionContext"/>
    /// capture and restore that <see cref="AsyncValueTaskMethodBuilder"/> performs unconditionally, which is
    /// the bulk of the per-frame cost of an async method that completes synchronously. Command bodies carry
    /// their context in session fields rather than in ambient async-local state, so there is nothing to flow.
    /// </para>
    /// <para>
    /// Apply with <c>[AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]</c> on an <c>async ValueTask</c>
    /// method, and invoke it inside <see cref="RespServerSession.BeginAsyncCommand"/>.
    /// </para>
    /// </remarks>
    internal struct RespAsyncMethodBuilder
    {
        RespServerSession session;
        RespAsyncBox box;
        Exception exception;

        /// <summary>
        /// Creates a builder bound to the session whose command dispatch is currently on this thread.
        /// </summary>
        public static RespAsyncMethodBuilder Create()
        {
            var current = RespServerSession.CurrentAsyncCommandScope;
            if (current is null)
                ThrowNoScope();
            return new RespAsyncMethodBuilder { session = current };
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        static void ThrowNoScope()
            => throw new InvalidOperationException(
                $"An async RESP command body ran outside {nameof(RespServerSession)}.{nameof(RespServerSession.BeginAsyncCommand)}.");

        /// <summary>
        /// Starts the body on the dispatching thread, which already holds the session scope.
        /// </summary>
        /// <param name="stateMachine">The body's state machine.</param>
        /// <typeparam name="TStateMachine">The body's state machine type.</typeparam>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Start<TStateMachine>(ref TStateMachine stateMachine) where TStateMachine : IAsyncStateMachine
            => stateMachine.MoveNext();

        /// <summary>
        /// Unused: the state machine is boxed by this builder when it first suspends.
        /// </summary>
        /// <param name="stateMachine">The body's state machine.</param>
        public readonly void SetStateMachine(IAsyncStateMachine stateMachine) { }

        /// <summary>
        /// Completes the body.
        /// </summary>
        public readonly void SetResult() => box?.SetResult();

        /// <summary>
        /// Faults the body. The session observes this inside its scope and turns it into a RESP error.
        /// </summary>
        /// <param name="ex">The exception the body threw.</param>
        public void SetException(Exception ex)
        {
            if (box is not null)
                box.SetException(ex);
            else
                exception = ex;
        }

        /// <summary>
        /// The body's task. Completed (or faulted) when the body never suspended.
        /// </summary>
        public readonly ValueTask Task
        {
            get
            {
                if (box is not null)
                    return new ValueTask(box, box.Version);
                return exception is null ? default : ValueTask.FromException(exception);
            }
        }

        /// <summary>
        /// Suspends the body on an awaiter that flows execution context.
        /// </summary>
        /// <param name="awaiter">The awaiter the body is waiting on.</param>
        /// <param name="stateMachine">The body's state machine.</param>
        /// <typeparam name="TAwaiter">The awaiter type.</typeparam>
        /// <typeparam name="TStateMachine">The body's state machine type.</typeparam>
        public void AwaitOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
            where TAwaiter : INotifyCompletion
            where TStateMachine : IAsyncStateMachine
            => awaiter.OnCompleted(Suspend(ref stateMachine));

        /// <summary>
        /// Suspends the body on an awaiter that does not flow execution context.
        /// </summary>
        /// <param name="awaiter">The awaiter the body is waiting on.</param>
        /// <param name="stateMachine">The body's state machine.</param>
        /// <typeparam name="TAwaiter">The awaiter type.</typeparam>
        /// <typeparam name="TStateMachine">The body's state machine type.</typeparam>
        public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
            where TAwaiter : ICriticalNotifyCompletion
            where TStateMachine : IAsyncStateMachine
            => awaiter.UnsafeOnCompleted(Suspend(ref stateMachine));

        /// <summary>
        /// Moves the state machine to the heap on its first suspension and tells the session a resume is
        /// pending, returning the delegate the awaiter should invoke on completion.
        /// </summary>
        Action Suspend<TStateMachine>(ref TStateMachine stateMachine) where TStateMachine : IAsyncStateMachine
        {
            var pending = box;
            if (pending is null)
            {
                var typed = session.RentAsyncBox<TStateMachine>();

                // Publish the box into this builder -- which lives inside the state machine -- before the
                // state machine is copied, so the heap copy and this stack copy share one box.
                box = typed;
                typed.StateMachine = stateMachine;
                pending = typed;
            }
            else
            {
                // On any later suspension the state machine already lives in the box, and the reference
                // handed to this method is a reference to the box's own field.
                Debug.Assert(ReferenceEquals(pending, box));
            }

            // Arm the session before handing the delegate out: an awaiter may invoke it inline, on another
            // thread, before this call returns.
            session.BeginSuspension(pending);
            return pending.ResumeAction;
        }
    }
}