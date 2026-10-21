// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.networking;
using Microsoft.Extensions.Logging;

namespace Garnet.common
{
    /// <summary>
    /// TCP network sender
    /// </summary>
    public class GarnetTcpNetworkSender : NetworkSenderBase
    {
        /// <summary>
        /// Socket
        /// </summary>
        protected readonly Socket socket;

        /// <summary>
        /// Response object
        /// </summary>
        protected GarnetSaeaBuffer responseObject;

        /// <summary>
        /// Reusable SeaaBuffer
        /// </summary>
        readonly LightConcurrentStack<GarnetSaeaBuffer> saeaStack;

        /// <summary>
        /// Throttle
        /// </summary>
        protected readonly SemaphoreSlim throttle = new(0);

        /// <summary>
        /// Count of sends for throttling
        /// </summary>
        protected int throttleCount;

        /// <summary>
        /// Max concurrent sends (per session) for throttling
        /// </summary>
        protected readonly int ThrottleMax = 8;

        /// <summary>
        /// Longest a send waits for an earlier send to complete, in milliseconds, before the connection is
        /// closed as stalled; <see cref="Timeout.Infinite"/> waits indefinitely.
        /// </summary>
        readonly int sendTimeoutMs;

        /// <summary>
        /// Progressive spins a draining close performs before it blocks.
        /// </summary>
        const int DrainSpinIterations = 32;

        /// <summary>
        /// Progressive spins a send performs while waiting for a send slot before it blocks; this matches the spin
        /// <see cref="SemaphoreSlim.Wait()"/> performs before it blocks.
        /// </summary>
        const int CapacitySpinIterations = 140;

        readonly string remoteEndpoint;
        readonly string localEndpoint;

        readonly NetworkBufferSettings networkBufferSettings;
        readonly LimitedFixedBufferPool networkPool;

        /// <summary>
        /// NOTE: This variable should not be marked as readonly as it is a mutable struct
        /// </summary>
        SpinLock spinLock;

        private int closeRequested;

        /// <summary>
        /// GarnetTcpNetworkSender Constructor
        /// </summary>
        /// <param name="socket"></param>
        /// <param name="networkBufferSettings"></param>
        /// <param name="networkPool"></param>
        /// <param name="throttleMax"></param>
        /// <param name="sendTimeout">
        /// Longest a send waits for an earlier send to complete before the connection is closed as stalled.
        /// Zero or negative waits indefinitely.
        /// </param>
        public GarnetTcpNetworkSender(
            Socket socket,
            NetworkBufferSettings networkBufferSettings,
            LimitedFixedBufferPool networkPool,
            int throttleMax = 8,
            TimeSpan sendTimeout = default)
            : base(networkBufferSettings.sendBufferSize)
        {
            this.networkBufferSettings = networkBufferSettings;
            this.networkPool = networkPool;
            this.socket = socket;
            this.saeaStack = new(2 * ThrottleMax);
            this.responseObject = null;
            this.ThrottleMax = throttleMax;
            this.sendTimeoutMs = sendTimeout > TimeSpan.Zero ? (int)Math.Min(sendTimeout.TotalMilliseconds, int.MaxValue) : Timeout.Infinite;
            this.spinLock = new();
            this.closeRequested = 0;

            remoteEndpoint = socket.RemoteEndPoint is IPEndPoint remote ? $"{remote.Address}:{remote.Port}" : "";
            localEndpoint = socket.LocalEndPoint is IPEndPoint local ? $"{local.Address}:{local.Port}" : "";
        }


        /// <inheritdoc />
        public override string RemoteEndpointName => remoteEndpoint;

        /// <inheritdoc />
        public override string LocalEndpointName => localEndpoint;

        /// <inheritdoc />
        public override bool IsLocalConnection()
        {
            if (socket.RemoteEndPoint is IPEndPoint ip)
            {
                return IPAddress.IsLoopback(ip.Address);
            }

            if (socket.RemoteEndPoint is UnixDomainSocketEndPoint)
            {
                return true;
            }

            return false;
        }

        /// <inheritdoc />
        public override void Enter()
        {
            var lockTaken = false;
            spinLock.Enter(ref lockTaken);
            Debug.Assert(lockTaken);
        }

        /// <inheritdoc />
        public override unsafe void EnterAndGetResponseObject(out byte* head, out byte* tail)
        {
            var lockTaken = false;
            spinLock.Enter(ref lockTaken);
            Debug.Assert(lockTaken);
            Debug.Assert(responseObject == null);
            if (!saeaStack.TryPop(out responseObject, out bool disposed))
            {
                if (disposed)
                    ThrowDisposed();
                responseObject = new GarnetSaeaBuffer(SeaaBuffer_Completed, networkBufferSettings, networkPool);
            }
            head = responseObject.buffer.entryPtr;
            tail = responseObject.buffer.entryPtr + responseObject.buffer.entry.Length;
        }

        /// <inheritdoc />
        public override void Exit()
        {
            spinLock.Exit();
        }

        /// <inheritdoc />
        public override unsafe void ExitAndReturnResponseObject()
        {
            if (responseObject != null)
            {
                ReturnBuffer(responseObject);
                responseObject = null;
            }
            spinLock.Exit();
        }


        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void GetResponseObject()
        {
            if (responseObject == null)
            {
                if (!saeaStack.TryPop(out responseObject, out bool disposed))
                {
                    if (disposed)
                        ThrowDisposed();
                    responseObject = new GarnetSaeaBuffer(SeaaBuffer_Completed, networkBufferSettings, networkPool);
                }
            }
        }

        static void ThrowDisposed()
            => throw new ObjectDisposedException("GarnetTcpNetworkSender");

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void ReturnResponseObject()
        {
            if (responseObject != null)
            {
                ReturnBuffer(responseObject);
                responseObject = null;
            }
        }

        void ReturnBuffer(GarnetSaeaBuffer buffer)
        {
            Debug.Assert(buffer != null);

            // While the budget is binding, a buffer larger than the adapted send size must not go back on the
            // per-connection stack: it would be handed straight back out and the connection would never
            // converge on the smaller size. Dispose it so the pool decides, and the next acquire allocates at
            // the adapted size. Unpressured this is inert, since nothing is larger than the target.
            var budget = networkPool.Budget;
            if (budget.IsUnderPressure && buffer.buffer.entry.Length > budget.TargetSendBufferSize)
            {
                buffer.Dispose();
                return;
            }

            if (!saeaStack.TryPush(buffer))
                buffer.Dispose();
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override unsafe byte* GetResponseObjectHead()
        {
            if (responseObject != null)
                return responseObject.buffer.entryPtr;
            return base.GetResponseObjectHead();
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override unsafe byte* GetResponseObjectTail()
        {
            if (responseObject != null)
                return responseObject.buffer.entryPtr + responseObject.buffer.entry.Length;
            return base.GetResponseObjectTail();
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool SendResponse(int offset, int size)
        {
            var _r = responseObject;
            if (_r == null) return false;
            responseObject = null;
            try
            {
                // If this does not throw, _r is ReturnBuffer()ed when it completes.
                Send(socket, _r, offset, size);
            }
            catch
            {
                ReturnBuffer(_r);
                if (Interlocked.Decrement(ref throttleCount) >= ThrottleMax)
                    throttle.Release();
                // Rethrow exception as session is not usable
                throw;
            }
            return true;
        }

        /// <inheritdoc />
        public override void SendResponse(byte[] buffer, int offset, int count, object context)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc />
        public override void SendCallback(object context)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc />
        public override void Dispose() => DisposeNetworkSender(false);

        /// <inheritdoc />
        public override void DisposeNetworkSender(bool waitForSendCompletion)
        {
            // Invariant: every in-flight send completes before the buffer stack and throttle are disposed, since a
            // send's completion returns its buffer to the stack and may release the throttle. Draining also closes
            // the sender, so any later send completes immediately without reaching the socket.
            //
            // A graceful close first lets in-flight sends reach the peer, giving up once no send has completed for the
            // send timeout (never, when the timeout is infinite). An abortive close, or a graceful one that gives up,
            // disposes the socket instead. That aborts the in-flight sends, so the unbounded drain after it is prompt
            // and still upholds the invariant.
            if (!waitForSendCompletion || !TryDrainSends(sendTimeoutMs))
            {
                socket.Dispose();
                _ = TryDrainSends(Timeout.Infinite);
            }

            // Empty and dispose the stack
            saeaStack.Dispose();

            throttle.Dispose();
            if (waitForSendCompletion)
                socket.Dispose();
        }

        /// <summary>
        /// Waits for in-flight sends to complete, then closes the sender to further sends.
        /// </summary>
        /// <param name="timeoutMs">Longest to wait without any send completing, or <see cref="Timeout.Infinite"/>.</param>
        /// <returns>False if no send completed within <paramref name="timeoutMs"/>; the sender is then still open.</returns>
        bool TryDrainSends(int timeoutMs)
        {
            // Sends complete within microseconds while the peer reads, so spin progressively first
            var spinner = ConfiguredSpinWait.BoundedProgressive(DrainSpinIterations);
            do
            {
                if (TryCloseIfDrained())
                    return true;
            }
            while (spinner.TryWait());

            // A peer that reads slowly, or not at all, can take far longer. Blocking on a task, rather than sleeping,
            // lets the thread pool inject a replacement for this thread while it waits.
            return AsyncUtils.BlockingWait(DrainSendsAsync(timeoutMs));
        }

        async Task<bool> DrainSendsAsync(int timeoutMs)
        {
            using var poll = new PeriodicTimer(TimeSpan.FromMilliseconds(1));
            var pending = Volatile.Read(ref throttleCount);
            var progressAt = Environment.TickCount64;
            while (!TryCloseIfDrained())
            {
                var count = Volatile.Read(ref throttleCount);
                if (count != pending)
                {
                    pending = count;
                    progressAt = Environment.TickCount64;
                }
                else if (timeoutMs != Timeout.Infinite && Environment.TickCount64 - progressAt >= timeoutMs)
                {
                    return false;
                }

                _ = await poll.WaitForNextTickAsync().ConfigureAwait(false);
            }
            return true;
        }

        /// <summary>
        /// Closes the sender to further sends if no send is in flight, or reports that it is already closed.
        /// </summary>
        bool TryCloseIfDrained()
            => Volatile.Read(ref throttleCount) < 0 || Interlocked.CompareExchange(ref throttleCount, int.MinValue, 0) == 0;

        /// <inheritdoc />
        public override void Throttle()
        {
            // Short circuit for common case of no network overload
            if (throttleCount < ThrottleMax) return;

            // We are throttling, so wait for throttle to be released by some ongoing sender
            var cnt = Interlocked.Increment(ref throttleCount);
            if (cnt < 0)
            {
                Interlocked.Decrement(ref cnt);
                return;
            }
            if (cnt > ThrottleMax)
                throttle.Wait();

            // Release throttle, since we used up one slot
            if (Interlocked.Decrement(ref throttleCount) >= ThrottleMax)
                throttle.Release();
        }

        /// <inheritdoc />
        public override bool TryClose()
        {
            // Only one caller gets to invoke Close, as we'd expect subsequent ones to fail and throw
            if (Interlocked.CompareExchange(ref closeRequested, 1, 0) != 0)
            {
                return false;
            }

            try
            {
                // This close should cause all outstanding requests to fail.
                // 
                // We don't distinguish between clients closing their end of the Socket
                // and us forcing it closed on request.
                socket.Close();
            }
            catch
            {
                // Best effort, just swallow any exceptions
            }

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void Send(Socket socket, GarnetSaeaBuffer sendObject, int offset, int size)
        {
            var cnt = Interlocked.Increment(ref throttleCount);
            if (cnt < 0)
            {
                sendObject.socketEventAsyncArgs.UserToken = sendObject;
                SeaaBuffer_Completed(null, sendObject.socketEventAsyncArgs);
                return;
            }
            if (cnt > ThrottleMax)
                WaitForSendCapacity();

            // Debug.WriteLine($"SEND: [{size}][{System.Text.Encoding.UTF8.GetString(new Span<byte>(sendObject.socketEventAsyncArgs.Buffer, offset, size)).Replace("\n", "|").Replace("\r", "")}]");

            // Reset send buffer
            sendObject.socketEventAsyncArgs.SetBuffer(offset, size);
            // Set user context to reusable object handle for disposal when send is done
            sendObject.socketEventAsyncArgs.UserToken = sendObject;
            if (!socket.SendAsync(sendObject.socketEventAsyncArgs))
                SeaaBuffer_Completed(null, sendObject.socketEventAsyncArgs);
        }

        /// <summary>
        /// Waits for an in-flight send to complete, which frees a send slot for the caller.
        /// </summary>
        /// <remarks>
        /// The caller is usually a thread-pool thread that also serves other connections. Blocking on a task lets
        /// the thread pool inject a replacement thread, whereas blocking on the semaphore does not, so other
        /// connections continue to be served while this one waits. A peer that accepts no data for
        /// the send timeout has its connection closed, which also completes its in-flight sends.
        /// <para>
        /// A completion that frees a slot for a waiter decrements the send count before it releases the semaphore,
        /// so the count can reach zero, letting the sender be disposed, while that release is still to come. A waiter
        /// therefore leaves only once it has taken its release, even after a timeout; otherwise the release could
        /// land on a disposed semaphore.
        /// </para>
        /// <para>
        /// The timeout is applied to the wait on the untimed semaphore task rather than passed to the semaphore,
        /// because a timed semaphore wait allocates a timer and async state on every call. The untimed task also
        /// stays queued after a timeout, so it is the one that takes the owed release.
        /// </para>
        /// </remarks>
        /// <exception cref="GarnetException">No send completed within the send timeout.</exception>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private void WaitForSendCapacity()
        {
            // A send usually completes shortly while the peer reads, so spin progressively, as the semaphore's own
            // blocking wait does, before paying for a task and a blocking wait.
            var spinner = ConfiguredSpinWait.BoundedProgressive(CapacitySpinIterations, sleep1Threshold: -1);
            do
            {
                if (throttle.Wait(0))
                    return;
            }
            while (spinner.TryWait());

            var slot = throttle.WaitAsync();
            if (AsyncUtils.BlockingWait(slot, sendTimeoutMs))
                return;

            // Closing the socket completes every in-flight send, so the release this waiter is owed arrives promptly
            _ = TryClose();
            AsyncUtils.BlockingWait(slot);
            throw new GarnetException($"Closed connection to {remoteEndpoint}: no send completed within {sendTimeoutMs} ms",
                LogLevel.Warning, clientResponse: false, disposeSession: true);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SeaaBuffer_Completed(object sender, SocketAsyncEventArgs e)
        {
            ReturnBuffer((GarnetSaeaBuffer)e.UserToken);
            if (Interlocked.Decrement(ref throttleCount) >= ThrottleMax)
            {
#if DEBUG
                ExceptionInjectionHelper.ResetAndWait(ExceptionInjectionType.Sender_Pause_Before_Slot_Release);
#endif
                throttle.Release();
            }
        }
    }
}