// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// Return type for an <c>async</c> method that is started and never awaited, such as a continuation of the
    /// network receive loop. It has no awaiter, so the compiler rejects any attempt to await it.
    /// </summary>
    /// <remarks>
    /// Methods returning this type run on <see cref="FireAndForgetMethodBuilder"/>, which keeps a pooled state
    /// machine box per thread. A method that suspends therefore allocates nothing once the pool is warm, which
    /// matters because the network receive path suspends once per parked session.
    /// </remarks>
    [AsyncMethodBuilder(typeof(FireAndForgetMethodBuilder))]
    public readonly struct FireAndForget
    {
    }

    /// <summary>
    /// Async method builder for <see cref="FireAndForget"/>.
    /// </summary>
    /// <remarks>
    /// Differs from <see cref="AsyncValueTaskMethodBuilder"/> in three ways, each of which removes work from
    /// the path a parked session takes: the state machine box is pooled per thread rather than allocated per
    /// call, no <see cref="ExecutionContext"/> is captured or restored, and no task object is produced because
    /// nothing can observe one.
    /// </remarks>
    public struct FireAndForgetMethodBuilder
    {
        FireAndForgetBox box;

        /// <summary>Creates a builder. Required by the compiler.</summary>
        /// <returns>A new builder.</returns>
        public static FireAndForgetMethodBuilder Create() => default;

        /// <summary>The method's result. Carries no state; exists only to satisfy the compiler.</summary>
        public FireAndForget Task => default;

        /// <summary>
        /// Runs the method body synchronously until it either completes or suspends.
        /// </summary>
        /// <typeparam name="TStateMachine">Compiler-generated state machine type.</typeparam>
        /// <param name="stateMachine">The state machine to drive.</param>
        /// <remarks>
        /// Unlike the framework builders this does not capture and restore <see cref="ExecutionContext"/>.
        /// Nothing on these paths reads ambient context; everything a continuation needs is reachable from the
        /// handler or session it belongs to.
        /// </remarks>
        public void Start<TStateMachine>(ref TStateMachine stateMachine) where TStateMachine : IAsyncStateMachine
            => stateMachine.MoveNext();

        /// <summary>Unused; the box owns the state machine. Required by the compiler.</summary>
        /// <param name="stateMachine">Ignored.</param>
        public readonly void SetStateMachine(IAsyncStateMachine stateMachine) { }

        /// <summary>Returns the box to its pool once the method body completes.</summary>
        public void SetResult()
        {
            var completed = box;
            box = null;
            completed?.Release();
        }

        /// <summary>
        /// Reports a fault escaping the method body.
        /// </summary>
        /// <param name="exception">The exception the body threw.</param>
        /// <remarks>
        /// There is no task to fault, so this rethrows on the thread pool exactly as <c>async void</c> does.
        /// Every <see cref="FireAndForget"/> method guards its own body, so reaching here means a guard itself
        /// failed and the connection's state is already unknown.
        /// </remarks>
        public void SetException(Exception exception)
        {
            // The faulted state machine stays with its box, which is therefore not pooled.
            box = null;
            ThreadPool.UnsafeQueueUserWorkItem(
                static state => ExceptionDispatchInfo.Capture((Exception)state).Throw(), exception);
        }

        /// <summary>
        /// Moves the state machine to the heap on its first suspension and hands the awaiter a delegate that
        /// resumes it.
        /// </summary>
        /// <typeparam name="TAwaiter">Awaiter type.</typeparam>
        /// <typeparam name="TStateMachine">Compiler-generated state machine type.</typeparam>
        /// <param name="awaiter">The awaiter to register with.</param>
        /// <param name="stateMachine">The state machine to resume.</param>
        public void AwaitOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
            where TAwaiter : INotifyCompletion
            where TStateMachine : IAsyncStateMachine
            => awaiter.OnCompleted(Suspend(ref stateMachine));

        /// <summary>
        /// Moves the state machine to the heap on its first suspension and hands the awaiter a delegate that
        /// resumes it, without flowing <see cref="ExecutionContext"/>.
        /// </summary>
        /// <typeparam name="TAwaiter">Awaiter type.</typeparam>
        /// <typeparam name="TStateMachine">Compiler-generated state machine type.</typeparam>
        /// <param name="awaiter">The awaiter to register with.</param>
        /// <param name="stateMachine">The state machine to resume.</param>
        public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
            where TAwaiter : ICriticalNotifyCompletion
            where TStateMachine : IAsyncStateMachine
            => awaiter.UnsafeOnCompleted(Suspend(ref stateMachine));

        Action Suspend<TStateMachine>(ref TStateMachine stateMachine) where TStateMachine : IAsyncStateMachine
        {
            if (box is not null)
            {
                // Already boxed: stateMachine is the box's own field, so there is nothing to copy.
                return box.MoveNextAction;
            }

            var rented = FireAndForgetBox<TStateMachine>.Rent();

            // Set before the copy below, so the builder inside the copied state machine shares this box rather
            // than renting a second one on the next suspension.
            box = rented;
            rented.StateMachine = stateMachine;
            return rented.MoveNextAction;
        }
    }

    /// <summary>
    /// Heap home for a suspended <see cref="FireAndForget"/> state machine.
    /// </summary>
    abstract class FireAndForgetBox
    {
        /// <summary>Resumption delegate, allocated once per box rather than once per suspension.</summary>
        internal readonly Action MoveNextAction;

        protected FireAndForgetBox() => MoveNextAction = MoveNext;

        protected abstract void MoveNext();

        internal abstract void Release();
    }

    /// <summary>
    /// Box specialised to one compiler-generated state machine type, with a per-thread free list.
    /// </summary>
    /// <typeparam name="TStateMachine">Compiler-generated state machine type.</typeparam>
    sealed class FireAndForgetBox<TStateMachine> : FireAndForgetBox where TStateMachine : IAsyncStateMachine
    {
        [ThreadStatic]
        static FireAndForgetBox<TStateMachine> cached;

        internal TStateMachine StateMachine;

        protected override void MoveNext() => StateMachine.MoveNext();

        internal override void Release()
        {
            // Drops the body's locals so a pooled box roots nothing. Safe to do from inside the body's own
            // MoveNext: the generated code returns as soon as SetResult does, reading no further state.
            StateMachine = default;
            cached = this;
        }

        internal static FireAndForgetBox<TStateMachine> Rent()
        {
            var box = cached;
            if (box is null)
                return new FireAndForgetBox<TStateMachine>();

            cached = null;
            return box;
        }
    }
}