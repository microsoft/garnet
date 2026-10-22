// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Garnet.networking;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// A sender that takes whatever it is given, reports itself full or not as the test says, and hands the
    /// capacity waiters registered with it back to the test to signal.
    /// </summary>
    /// <remarks>
    /// A real sender has two ways to send: <see cref="SendResponseNoWait"/> returns at once and says whether the
    /// sender is full, and <see cref="SendResponse(int, int)"/> waits for room first. This one makes the
    /// difference observable: <see cref="WaitingSends"/> counts the calls that took the waiting way, and
    /// <see cref="WaitingSendsBlock"/> holds them while the sender is full. Like a real sender it is entered
    /// exclusively, so a writer that waits for its holder is observable too.
    /// </remarks>
    internal sealed unsafe class ControlledSender : INetworkSender
    {
        readonly byte[] buffer;
        readonly byte* head;
        readonly object gate = new();
        readonly object waitGate = new();
        readonly object waiterGate = new();
        readonly List<byte> sent = [];
        readonly List<ISendCapacityWaiter> waiters = [];
        int registrations;
        int waitingSends;
        int entered;
        int failNextSend;
        volatile bool full;
        volatile bool closed;
        volatile bool waitingSendsBlock;
        volatile Func<bool> saturation;
        Action onNextSend;
        Action onNextExit;
        Func<bool> failTryEnterUntil;
        bool waitingSendsReleased;

        public ControlledSender(int bufferSize)
        {
            buffer = GC.AllocateArray<byte>(bufferSize, pinned: true);
            head = (byte*)Unsafe.AsPointer(ref buffer[0]);
        }

        public bool Full { set => full = value; }

        /// <summary>
        /// Decides what <see cref="IsSendSaturated"/> reports, in place of <see cref="Full"/>, each time it is
        /// read. Lets room return between two reads, as it does when a send completes between a publisher's
        /// looks at a subscriber's connection.
        /// </summary>
        public Func<bool> Saturation { set => saturation = value; }

        /// <summary>
        /// Runs once, inside the next send, after it is recorded: while its writer still holds the sender.
        /// </summary>
        public Action OnNextSend { set => Volatile.Write(ref onNextSend, value); }

        /// <summary>
        /// Runs once, inside the next exit, after the sender is let go: before its writer does anything else.
        /// </summary>
        public Action OnNextExit { set => Volatile.Write(ref onNextExit, value); }

        /// <summary>
        /// Makes <see cref="TryEnter"/> fail, as if another writer held the sender, until the condition holds;
        /// from then on it behaves normally.
        /// </summary>
        public Func<bool> FailTryEnterUntil { set => Volatile.Write(ref failTryEnterUntil, value); }

        /// <summary>Whether <see cref="FailTryEnterUntil"/> is still making entries fail.</summary>
        public bool FailingTryEnter => Volatile.Read(ref failTryEnterUntil) != null;

        /// <summary>
        /// Makes the next send throw, as a send does on a connection closed under it, without recording anything.
        /// </summary>
        public bool FailNextSend { set => Volatile.Write(ref failNextSend, value ? 1 : 0); }

        /// <summary>
        /// Makes <see cref="SendResponse(int, int)"/> wait while the sender is full, until
        /// <see cref="ReleaseWaitingSends"/> or the sender is closed, as a real sender waits for room.
        /// </summary>
        public bool WaitingSendsBlock { set => waitingSendsBlock = value; }

        /// <summary>How many registrations have been made, including those withdrawn since.</summary>
        public int Registrations => Volatile.Read(ref registrations);

        /// <summary>How many waiters are registered now.</summary>
        public int Waiting
        {
            get
            {
                lock (waiterGate)
                    return waiters.Count;
            }
        }

        /// <summary>How many sends have called <see cref="SendResponse(int, int)"/>.</summary>
        public int WaitingSends => Volatile.Read(ref waitingSends);

        public bool Closed => closed;

        public string Sent
        {
            get
            {
                lock (gate)
                    return Encoding.ASCII.GetString([.. sent]);
            }
        }

        /// <summary>
        /// Calls every registered waiter the way the completion of a send does, which takes them first.
        /// </summary>
        public void SignalCapacity()
        {
            ClassicAssert.IsTrue(Waiting > 0, "no session is waiting for the sender to have room");
            NotifySendCapacityWaiters();
        }

        /// <summary>Lets the sends waiting for room, and every later one, through.</summary>
        public void ReleaseWaitingSends()
        {
            lock (waitGate)
            {
                waitingSendsReleased = true;
                Monitor.PulseAll(waitGate);
            }
        }

        void Record(int offset, int size)
        {
            if (Interlocked.Exchange(ref failNextSend, 0) != 0)
                throw new ObjectDisposedException("socket");

            lock (gate)
                sent.AddRange(new ReadOnlySpan<byte>(head + offset, size).ToArray());

            Interlocked.Exchange(ref onNextSend, null)?.Invoke();
        }

        public MaxSizeSettings GetMaxSizeSettings { get; } = new();

        public string RemoteEndpointName => "";

        public string LocalEndpointName => "";

        public bool IsLocalConnection() => true;

        /// <summary>Whether a writer has entered the sender and not yet exited.</summary>
        public bool Entered => Volatile.Read(ref entered) != 0;

        public void Enter()
        {
            var spinner = new SpinWait();
            while (!TryEnter())
                spinner.SpinOnce();
        }

        public bool TryEnter()
        {
            var until = Volatile.Read(ref failTryEnterUntil);
            if (until != null)
            {
                if (!until())
                    return false;
                _ = Interlocked.CompareExchange(ref failTryEnterUntil, null, until);
            }

            return Interlocked.CompareExchange(ref entered, 1, 0) == 0;
        }

        public void EnterAndGetResponseObject(out byte* head, out byte* tail)
        {
            Enter();
            head = this.head;
            tail = this.head + buffer.Length;
        }

        public void Exit()
        {
            ClassicAssert.IsTrue(Entered, "exited a sender nobody had entered");

            // A full fence, as the interface requires
            _ = Interlocked.Exchange(ref entered, 0);

            Interlocked.Exchange(ref onNextExit, null)?.Invoke();
        }

        public void ExitAndReturnResponseObject() => Exit();

        public void GetResponseObject() { }

        public void ReturnResponseObject() { }

        public byte* GetResponseObjectHead() => head;

        public byte* GetResponseObjectTail() => head + buffer.Length;

        public bool SendResponse(int offset, int size)
        {
            _ = Interlocked.Increment(ref waitingSends);
            lock (waitGate)
            {
                while (waitingSendsBlock && full && !waitingSendsReleased && !closed)
                    _ = Monitor.Wait(waitGate);
            }

            Record(offset, size);
            return true;
        }

        public bool SendResponseNoWait(int offset, int size)
        {
            Record(offset, size);
            return full;
        }

        public bool IsSendSaturated => saturation?.Invoke() ?? full;

        public bool TryRegisterSendCapacityWaiter(ISendCapacityWaiter capacityWaiter)
        {
            AddSendCapacityWaiter(capacityWaiter);
            return full || !WithdrawSendCapacityWaiter(capacityWaiter);
        }

        public void AddSendCapacityWaiter(ISendCapacityWaiter capacityWaiter)
        {
            lock (waiterGate)
                waiters.Add(capacityWaiter);
            _ = Interlocked.Increment(ref registrations);
        }

        public void NotifySendCapacityWaiters()
        {
            ISendCapacityWaiter[] registered;
            lock (waiterGate)
            {
                registered = [.. waiters];
                waiters.Clear();
            }

            foreach (var waiter in registered)
                waiter.OnSendCapacity();
        }

        public bool WithdrawSendCapacityWaiter(ISendCapacityWaiter capacityWaiter)
        {
            lock (waiterGate)
                return waiters.Remove(capacityWaiter);
        }

        public void SendResponse(byte[] buffer, int offset, int count, object context) { }

        public void SendCallback(object context) { }

        public void DisposeNetworkSender(bool waitForSendCompletion) => Close();

        public void Throttle() { }

        public bool TryClose() => false;

        public void Dispose() => Close();

        void Close()
        {
            closed = true;
            ReleaseWaitingSends();
        }
    }
}