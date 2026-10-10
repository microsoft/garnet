// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace Garnet.common
{
    /// <summary>
    /// A <see cref="SocketAsyncEventArgs"/> that is itself the completion source for the operation it
    /// carries, so an awaiting caller allocates nothing. One instance is created per direction per
    /// connection and reused for the connection's lifetime.
    /// </summary>
    /// <remarks>
    /// A socket operation that completes synchronously never reaches this type's completion path at all:
    /// <see cref="ReceiveAsync"/> and <see cref="SendAsync"/> return a value-backed
    /// <see cref="ValueTask{TResult}"/> in that case. The completion source is engaged only when the
    /// operation is genuinely pending, which is what keeps the common path free of any async machinery.
    /// <para>
    /// Continuations run inline on the IO completion thread. Garnet processes commands on that thread by
    /// design, and the awaiting receive loop is iterative, so resuming it inline continues a loop rather
    /// than growing the stack. The one exception is the late-subscription race in
    /// <see cref="OnCompleted(Action{object}, object, short, ValueTaskSourceOnCompletedFlags)"/>, where the
    /// operation finished before the awaiter subscribed and the continuation must be dispatched instead.
    /// </para>
    /// </remarks>
    internal sealed class GarnetSocketAwaitableEventArgs : SocketAsyncEventArgs, IValueTaskSource<SocketOperationResult>
    {
        /// <summary>
        /// Sentinel published into <see cref="continuation"/> once the operation has completed. A caller
        /// that finds it knows the result is already available.
        /// </summary>
        static readonly Action<object> CallbackCompleted = _ => throw new InvalidOperationException("Sentinel continuation must never run");

        /// <summary>
        /// Volatile because it is published by the IO completion thread and read by the awaiting thread
        /// with no other synchronization between them. Kestrel marks the equivalent field volatile and
        /// cites dotnet/runtime#84432 and dotnet/aspnetcore#50623 for the memory-ordering hazard.
        /// </summary>
        volatile Action<object> continuation;

        /// <summary>
        /// Creates the awaitable with <see cref="ExecutionContext"/> flow suppressed, so no capture or
        /// restore is paid on any socket completion.
        /// </summary>
        public GarnetSocketAwaitableEventArgs() : base(unsafeSuppressExecutionContextFlow: true)
        {
        }

        /// <summary>
        /// Issues a receive, returning a completed result when the socket satisfies it synchronously.
        /// </summary>
        /// <param name="socket">Socket to receive from.</param>
        /// <param name="buffer">Destination buffer.</param>
        /// <returns>The transferred byte count, or the error that ended the operation.</returns>
        public ValueTask<SocketOperationResult> ReceiveAsync(Socket socket, Memory<byte> buffer)
        {
            SetBuffer(buffer);
            if (socket.ReceiveAsync(this))
                return new ValueTask<SocketOperationResult>(this, 0);
            return new ValueTask<SocketOperationResult>(CurrentResult());
        }

        /// <summary>
        /// Issues a send, returning a completed result when the socket accepts it synchronously.
        /// </summary>
        /// <param name="socket">Socket to send on.</param>
        /// <param name="buffer">Source buffer.</param>
        /// <returns>The transferred byte count, or the error that ended the operation.</returns>
        public ValueTask<SocketOperationResult> SendAsync(Socket socket, Memory<byte> buffer)
        {
            SetBuffer(buffer);
            if (socket.SendAsync(this))
                return new ValueTask<SocketOperationResult>(this, 0);
            return new ValueTask<SocketOperationResult>(CurrentResult());
        }

        /// <summary>
        /// Reads the outcome currently recorded on this instance.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        SocketOperationResult CurrentResult()
            => SocketError == SocketError.Success
                ? new SocketOperationResult(BytesTransferred)
                : new SocketOperationResult(SocketError);

        /// <inheritdoc />
        protected override void OnCompleted(SocketAsyncEventArgs _)
        {
            var c = continuation;
            if (c != null || (c = Interlocked.CompareExchange(ref continuation, CallbackCompleted, null)) != null)
            {
                var state = UserToken;
                UserToken = null;
                continuation = CallbackCompleted;
                c(state);
            }
        }

        /// <inheritdoc />
        public SocketOperationResult GetResult(short token)
        {
            // Arm the instance for its next use. Only one operation per direction is ever in flight on a
            // connection, so clearing here is sufficient to make the instance reusable.
            continuation = null;
            return CurrentResult();
        }

        /// <inheritdoc />
        /// <remarks>
        /// A socket error is a result, not a fault: <see cref="GetResult"/> returns it inside the
        /// <see cref="SocketOperationResult"/> for the caller to inspect, and never throws. Reporting
        /// <see cref="ValueTaskSourceStatus.Faulted"/> would contradict that, and would make
        /// <c>IsCompletedSuccessfully</c> false on a path that has a perfectly good result to hand back.
        /// </remarks>
        public ValueTaskSourceStatus GetStatus(short token)
            => ReferenceEquals(continuation, CallbackCompleted) ? ValueTaskSourceStatus.Succeeded : ValueTaskSourceStatus.Pending;

        /// <inheritdoc />
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            UserToken = state;
            var prev = Interlocked.CompareExchange(ref this.continuation, continuation, null);
            if (ReferenceEquals(prev, CallbackCompleted))
            {
                // The operation completed before this subscription landed. Running the continuation here
                // would re-enter the awaiting frame on its own stack, so hand it to the pool instead.
                // Unsafe* skips an ExecutionContext capture this type has already opted out of.
                UserToken = null;
                ThreadPool.UnsafeQueueUserWorkItem(continuation, state, preferLocal: true);
            }
        }
    }
}