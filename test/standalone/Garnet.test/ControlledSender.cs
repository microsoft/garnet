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
    /// capacity waiter registered with it back to the test to signal.
    /// </summary>
    /// <remarks>
    /// A real sender has two ways to send: <see cref="SendResponseNoWait"/> returns at once and says whether the
    /// sender is full, and <see cref="SendResponse(int, int)"/> waits for room first. This one makes the
    /// difference observable: <see cref="WaitingSends"/> counts the calls that took the waiting way, and
    /// <see cref="WaitingSendsBlock"/> holds them while the sender is full.
    /// </remarks>
    internal sealed unsafe class ControlledSender : INetworkSender
    {
        readonly byte[] buffer;
        readonly byte* head;
        readonly object gate = new();
        readonly object waitGate = new();
        readonly List<byte> sent = [];
        ISendCapacityWaiter waiter;
        int registrations;
        int waitingSends;
        volatile bool full;
        volatile bool closed;
        volatile bool waitingSendsBlock;
        bool waitingSendsReleased;

        public ControlledSender(int bufferSize)
        {
            buffer = GC.AllocateArray<byte>(bufferSize, pinned: true);
            head = (byte*)Unsafe.AsPointer(ref buffer[0]);
        }

        public bool Full { set => full = value; }

        /// <summary>
        /// Makes <see cref="SendResponse(int, int)"/> wait while the sender is full, until
        /// <see cref="ReleaseWaitingSends"/> or the sender is closed, as a real sender waits for room.
        /// </summary>
        public bool WaitingSendsBlock { set => waitingSendsBlock = value; }

        public int Registrations => Volatile.Read(ref registrations);

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
        /// Calls the registered waiter the way the completion of a send does, which takes the waiter first.
        /// </summary>
        public void SignalCapacity()
        {
            var registered = Interlocked.Exchange(ref waiter, null);
            ClassicAssert.IsNotNull(registered, "no session is waiting for the sender to have room");
            registered.OnSendCapacity();
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
            lock (gate)
                sent.AddRange(new ReadOnlySpan<byte>(head + offset, size).ToArray());
        }

        public MaxSizeSettings GetMaxSizeSettings { get; } = new();

        public string RemoteEndpointName => "";

        public string LocalEndpointName => "";

        public bool IsLocalConnection() => true;

        public void Enter() { }

        public void EnterAndGetResponseObject(out byte* head, out byte* tail)
        {
            head = this.head;
            tail = this.head + buffer.Length;
        }

        public void Exit() { }

        public void ExitAndReturnResponseObject() { }

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

        public bool IsSendSaturated => full;

        public bool TryRegisterSendCapacityWaiter(ISendCapacityWaiter capacityWaiter)
        {
            Volatile.Write(ref waiter, capacityWaiter);
            _ = Interlocked.Increment(ref registrations);
            if (full)
                return true;

            Volatile.Write(ref waiter, null);
            return false;
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
