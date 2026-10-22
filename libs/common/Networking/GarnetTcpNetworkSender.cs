// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using Garnet.networking;

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
        /// Count of sends in flight that were issued with <see cref="SendResponseNoWait"/>. Kept apart from
        /// <see cref="throttleCount"/>, whose blocked senders are paired with permits on <see cref="throttle"/>: a
        /// completion with no waiter to match would release a permit nobody asked for, and the throttle would
        /// stop blocking.
        /// </summary>
        int nonBlockingSends;

        /// <summary>
        /// The waiters to notify when <see cref="nonBlockingSends"/> drops below <see cref="ThrottleMax"/>.
        /// Guarded by <see cref="capacityLock"/>. A connection serves several: the session that owns it, and
        /// each publisher waiting to deliver to it.
        /// </summary>
        List<ISendCapacityWaiter> capacityWaiters;

        /// <summary>
        /// Number of entries in <see cref="capacityWaiters"/>, read without the lock so that a completion with
        /// no one waiting, which is nearly every completion, does not take it
        /// </summary>
        int capacityWaiterCount;

        readonly object capacityLock = new();

        /// <summary>
        /// Open, then closing while the sends already in flight drain, then closed. A send that starts after the
        /// state leaves open is dropped.
        /// </summary>
        int closeState;

        const int Open = 0;
        const int Closing = 1;
        const int Closed = 2;

        /// <summary>
        /// Max concurrent sends (per session) for throttling
        /// </summary>
        protected readonly int ThrottleMax = 8;

        /// <summary>
        /// Longest the connection may stay stalled without a send completing, in milliseconds, before it is
        /// closed; <see cref="Timeout.Infinite"/> waits indefinitely. See <see cref="IsStalled"/>.
        /// </summary>
        readonly int sendTimeoutMs;

        /// <summary>
        /// Watches a stalled connection for a send to complete. Created the first time the connection stalls, and
        /// scheduled only while it is stalled. Guarded by <see cref="stallLock"/>.
        /// </summary>
        Timer stallTimer;

        /// <summary>
        /// Whether <see cref="stallTimer"/> is watching. Written under <see cref="stallLock"/>, and read without
        /// it by the sends that stall the connection and the completions that record progress.
        /// </summary>
        volatile bool stallWatchArmed;

        /// <summary>
        /// When a send last completed while <see cref="stallWatchArmed"/>, or when the watch was armed
        /// </summary>
        long lastProgressTicks;

        readonly object stallLock = new();

        /// <summary>
        /// Ends the connection, with its session, when it stalls for <see cref="sendTimeoutMs"/> or is closed on
        /// request; the sender closes only itself when there is none
        /// </summary>
        ISenderOwner owner;

        readonly string remoteEndpoint;
        readonly string localEndpoint;

        readonly NetworkBufferSettings networkBufferSettings;
        readonly LimitedFixedBufferPool networkPool;

        /// <summary>
        /// Guards <see cref="responseObject"/>. Thread-owner tracking is off, because a command that suspends
        /// with an operation in flight keeps the response object across its wait and releases it from the
        /// thread that resumes the session, which is not the thread that took the lock.
        /// </summary>
        /// <remarks>
        /// NOTE: This variable should not be marked as readonly as it is a mutable struct
        /// </remarks>
        SpinLock spinLock;

        private int closeRequested;

        /// <summary>
        /// GarnetTcpNetworkSender Constructor
        /// </summary>
        /// <param name="socket"></param>
        /// <param name="networkBufferSettings"></param>
        /// <param name="throttleMax"></param>
        /// <param name="sendTimeout">
        /// Longest the connection may go without a send completing, while it has the most sends outstanding that
        /// it allows or is closing with sends in flight, before it is closed. Zero or negative waits indefinitely.
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
            this.ThrottleMax = Math.Max(1, throttleMax);
            this.sendTimeoutMs = sendTimeout > TimeSpan.Zero ? (int)Math.Min(sendTimeout.TotalMilliseconds, int.MaxValue) : Timeout.Infinite;
            this.spinLock = new(enableThreadOwnerTracking: false);
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
        public override bool TryEnter()
        {
            var lockTaken = false;
            spinLock.TryEnter(ref lockTaken);
            return lockTaken;
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
            _r.nonBlockingSend = false;
            Send(_r, offset, size);
            return true;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool SendResponseNoWait(int offset, int size)
        {
            var _r = responseObject;
            if (_r == null) return false;
            responseObject = null;
            _r.nonBlockingSend = true;

            // Count the send before reading the state: a close that begins after this either finds the send in
            // flight and waits for it, or is found by the read below and drops the send
            Interlocked.Increment(ref nonBlockingSends);
            if (Volatile.Read(ref closeState) != Open)
            {
                FinishSend(_r);
                return false;
            }

            StartSend(_r, offset, size);
            if (!IsSendSaturated)
                return false;

            if (sendTimeoutMs != Timeout.Infinite && !stallWatchArmed)
                WatchForStall();
            return true;
        }

        /// <inheritdoc />
        public override bool IsSendSaturated => Volatile.Read(ref nonBlockingSends) >= ThrottleMax;

        /// <inheritdoc />
        public override bool TryRegisterSendCapacityWaiter(ISendCapacityWaiter waiter)
        {
            // The completion that frees capacity decrements the sends outstanding before reading the waiters, so
            // whichever of the two goes second sees the other
            AddSendCapacityWaiter(waiter);

            if (IsSendSaturated)
                return true;

            // Capacity is already available. Withdraw the waiter, unless a completion took it first, in which
            // case it will be called and the caller must wait for that.
            return !WithdrawSendCapacityWaiter(waiter);
        }

        /// <inheritdoc />
        public override void AddSendCapacityWaiter(ISendCapacityWaiter waiter)
        {
            lock (capacityLock)
            {
                (capacityWaiters ??= []).Add(waiter);

                // An interlocked write, so that the waiter is published before the caller reads why it waits
                _ = Interlocked.Exchange(ref capacityWaiterCount, capacityWaiters.Count);
            }
        }

        /// <inheritdoc />
        public override void NotifySendCapacityWaiters()
        {
            if (Volatile.Read(ref capacityWaiterCount) != 0)
                NotifyCapacityWaiters();
        }

        /// <inheritdoc />
        public override bool WithdrawSendCapacityWaiter(ISendCapacityWaiter waiter)
        {
            lock (capacityLock)
            {
                if (capacityWaiters is null || !capacityWaiters.Remove(waiter))
                    return false;

                Volatile.Write(ref capacityWaiterCount, capacityWaiters.Count);
                return true;
            }
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

        /// <summary>
        /// Whether the sender still accepts sends
        /// </summary>
        protected bool IsOpen => Volatile.Read(ref closeState) == Open;

        /// <summary>
        /// Whether the sender has been closed, which no longer waits for any send
        /// </summary>
        protected bool IsClosed => Volatile.Read(ref closeState) == Closed;

        /// <inheritdoc />
        public override void Dispose() => DisposeNetworkSender(false);

        /// <inheritdoc />
        public override void DisposeNetworkSender(bool waitForSendCompletion)
        {
            if (!waitForSendCompletion)
            {
                CloseNow(graceful: false);
                return;
            }

            // Stop taking sends and let the ones in flight finish. The last of them closes the sender, so no
            // thread waits here, which matters because this is called from the thread that handles the
            // connection's network callbacks.
            if (Interlocked.CompareExchange(ref closeState, Closing, Open) != Open)
                return;

            TryFinishClose();
            if (sendTimeoutMs != Timeout.Infinite && !stallWatchArmed && IsStalled)
                WatchForStall();
        }

        /// <summary>
        /// Closes the sender if it is draining and nothing is left in flight
        /// </summary>
        protected void TryFinishClose()
        {
            if (Volatile.Read(ref closeState) == Closing &&
                Volatile.Read(ref throttleCount) == 0 &&
                Volatile.Read(ref nonBlockingSends) == 0)
                CloseNow(graceful: true);
        }

        void CloseNow(bool graceful)
        {
            if (Interlocked.Exchange(ref closeState, Closed) == Closed)
                return;

            // Disposing a socket that has an operation pending, which a connection always has in its receive,
            // resets the connection and drops what is still queued to send. Shutting down the send side first
            // queues the end of the stream behind that data, so the peer reads all of it.
            if (graceful)
            {
                try
                {
                    socket.Shutdown(SocketShutdown.Send);
                }
                catch (Exception e) when (e is SocketException or ObjectDisposedException)
                {
                    // The peer or the network handler has already ended the connection, so nothing is left to deliver
                }
            }

            // Sends still in flight fail with the socket, and return their buffers to the disposed stack, which
            // disposes them. The throttle is not disposed: those completions still release it.
            socket.Dispose();
            saeaStack.Dispose();

            lock (stallLock)
            {
                stallWatchArmed = false;
                stallTimer?.Dispose();
                stallTimer = null;
            }
        }

        /// <summary>
        /// Whether the connection is waiting on its client to accept data: it has the most sends outstanding that
        /// it allows, or it is closing with sends still in flight
        /// </summary>
        bool IsStalled
            => Volatile.Read(ref closeState) switch
            {
                Open => IsSendSaturated,
                Closing => Volatile.Read(ref nonBlockingSends) != 0 || Volatile.Read(ref throttleCount) != 0,
                _ => false,
            };

        /// <summary>
        /// Arms the stall watch for a connection that has just stalled, unless it is already armed
        /// </summary>
        /// <remarks>
        /// The caller has made the connection stall, by a send or by closing, before reading
        /// <see cref="stallWatchArmed"/>, and <see cref="OnStallTimer"/> disarms before reading whether it is
        /// stalled. So a stall that begins as the watch is disarmed is seen by one of the two.
        /// </remarks>
        [MethodImpl(MethodImplOptions.NoInlining)]
        void WatchForStall()
        {
            lock (stallLock)
            {
                if (stallWatchArmed || IsClosed)
                    return;

                ArmStallWatch();
            }
        }

        void ArmStallWatch()
        {
            Debug.Assert(Monitor.IsEntered(stallLock));

            Volatile.Write(ref lastProgressTicks, Environment.TickCount64);
            stallWatchArmed = true;

            if (stallTimer is null)
            {
                // The timer would otherwise capture the execution context of whichever command stalled the connection
                using (ExecutionContext.SuppressFlow())
                    stallTimer = new Timer(static sender => ((GarnetTcpNetworkSender)sender).OnStallTimer(), this, Timeout.Infinite, Timeout.Infinite);
            }

            _ = stallTimer.Change(sendTimeoutMs, Timeout.Infinite);
        }

        /// <summary>
        /// Closes the connection if it is still stalled and no send has completed for the send timeout, and
        /// otherwise watches on for the rest of the timeout, or stops watching if it is no longer stalled
        /// </summary>
        void OnStallTimer()
        {
            lock (stallLock)
            {
                if (!stallWatchArmed)
                    return;

                if (!IsStalled)
                {
                    stallWatchArmed = false;

                    // Rearm for a stall that began after the read above and found the watch still armed
                    Interlocked.MemoryBarrier();
                    if (IsStalled)
                        ArmStallWatch();
                    return;
                }

                var remaining = sendTimeoutMs - (Environment.TickCount64 - Volatile.Read(ref lastProgressTicks));
                if (remaining > 0)
                {
                    _ = stallTimer.Change(remaining, Timeout.Infinite);
                    return;
                }

                stallWatchArmed = false;
            }

            // Outside the lock, because closing the connection closes this sender, which takes it
            if (owner is { } o)
                o.OnSendStalled(TimeSpan.FromMilliseconds(sendTimeoutMs));
            else
                CloseNow(graceful: false);
        }

        /// <summary>
        /// Sets what ends the connection when it stalls for the send timeout or is closed on request
        /// </summary>
        internal void SetOwner(ISenderOwner owner)
            => this.owner = owner;

        /// <inheritdoc />
        public override void Throttle()
        {
            // Short circuit for common case of no network overload
            if (throttleCount < ThrottleMax) return;

            // We are throttling, so wait for throttle to be released by some ongoing sender
            var cnt = Interlocked.Increment(ref throttleCount);
            if (cnt > ThrottleMax)
                throttle.Wait();

            // Release throttle, since we used up one slot
            CompleteBlockingSend();
        }

        /// <inheritdoc />
        public override bool TryClose()
        {
            if (owner is { } o)
                return o.TryClose();

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
        private void Send(GarnetSaeaBuffer sendObject, int offset, int size)
        {
            var cnt = Interlocked.Increment(ref throttleCount);
            if (cnt > ThrottleMax)
                throttle.Wait();

            // The slot is held from the increment above, so a close that begins after the read below waits for
            // this send. If it began earlier, drop the send but still release the slot, so a sender queued
            // behind this one is not stranded.
            if (Volatile.Read(ref closeState) != Open)
            {
                FinishSend(sendObject);
                return;
            }

            StartSend(sendObject, offset, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void StartSend(GarnetSaeaBuffer sendObject, int offset, int size)
        {
            var args = sendObject.socketEventAsyncArgs;

            // Debug.WriteLine($"SEND: [{size}][{System.Text.Encoding.UTF8.GetString(new Span<byte>(args.Buffer, offset, size)).Replace("\n", "|").Replace("\r", "")}]");

            // Reset send buffer
            args.SetBuffer(offset, size);
            // Set user context to reusable object handle for disposal when send is done
            args.UserToken = sendObject;

            bool pending;
            try
            {
                pending = socket.SendAsync(args);
            }
            catch
            {
                // The send never started, so no completion will arrive
                FinishSend(sendObject);
                // Rethrow exception as session is not usable
                throw;
            }

            // The socket finished the send synchronously, so there is no completion callback to wait for
            if (!pending)
            {
                RecordSendProgress();
                FinishSend(sendObject);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SeaaBuffer_Completed(object sender, SocketAsyncEventArgs e)
        {
#if DEBUG
            ExceptionInjectionHelper.ResetAndWait(ExceptionInjectionType.Sender_Pause_Before_Slot_Release);
#endif
            RecordSendProgress();
            FinishSend((GarnetSaeaBuffer)e.UserToken);
        }

        /// <summary>
        /// Notes that a send has completed, for a stall watch that is armed. Dropped sends do not count.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void RecordSendProgress()
        {
            if (stallWatchArmed)
                Volatile.Write(ref lastProgressTicks, Environment.TickCount64);
        }

        /// <summary>
        /// Returns the buffer of a send that has finished, or that was dropped before it started, and releases
        /// the slot it held
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void FinishSend(GarnetSaeaBuffer sendObject)
        {
            // Read the tag first: once returned, the buffer can be popped and retagged by another send
            var nonBlocking = sendObject.nonBlockingSend;
            ReturnBuffer(sendObject);
            if (nonBlocking)
                CompleteNonBlockingSend();
            else
                CompleteBlockingSend();
        }

        /// <summary>
        /// Releases the slot held by a send, or by a <see cref="Throttle"/> call, that waited on the throttle
        /// </summary>
        protected void CompleteBlockingSend()
        {
            if (Interlocked.Decrement(ref throttleCount) >= ThrottleMax)
                throttle.Release();

            if (Volatile.Read(ref closeState) != Open)
                TryFinishClose();
        }

        private void CompleteNonBlockingSend()
        {
            if (Interlocked.Decrement(ref nonBlockingSends) < ThrottleMax && Volatile.Read(ref capacityWaiterCount) != 0)
                NotifyCapacityWaiters();

            if (Volatile.Read(ref closeState) != Open)
                TryFinishClose();
        }

        /// <summary>
        /// Calls every registered waiter, once. All of them are woken rather than one, because a waiter that
        /// finds the capacity gone again, or no longer needs it, would otherwise leave the next one parked with
        /// nothing left to wake it.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private void NotifyCapacityWaiters()
        {
            ISendCapacityWaiter only = null;
            ISendCapacityWaiter[] many = null;
            int count;

            // Taken under the lock and called outside it, so a waiter withdrawing concurrently is called at
            // most once, and one that registers again from its callback does not meet the lock held.
            lock (capacityLock)
            {
                count = capacityWaiters?.Count ?? 0;
                if (count == 0)
                    return;

                if (count == 1)
                {
                    only = capacityWaiters[0];
                }
                else
                {
                    many = ArrayPool<ISendCapacityWaiter>.Shared.Rent(count);
                    capacityWaiters.CopyTo(many);
                }

                capacityWaiters.Clear();
                Volatile.Write(ref capacityWaiterCount, 0);
            }

            if (only is not null)
            {
                only.OnSendCapacity();
                return;
            }

            for (var i = 0; i < count; i++)
                many[i].OnSendCapacity();
            ArrayPool<ISendCapacityWaiter>.Shared.Return(many, clearArray: true);
        }
    }
}