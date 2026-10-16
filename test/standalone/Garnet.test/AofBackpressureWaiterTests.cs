// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Tests of the registry in <see cref="AofBackpressure"/> that wakes sessions parked until the AOF admits
    /// appends again. A session registers when admission is closed; a shared timer wakes every registered session
    /// once it is open. The shipper publishes only when the watermark advances, so the registry cannot rely on a
    /// publish to learn that admission reopened, and these tests change the lag without announcing it.
    /// </summary>
    /// <remarks>
    /// No log is attached to the gate, so the live tail reads as zero and a watermark of -n is a lag of n bytes.
    /// The budget is 1000 bytes, which makes the publish interval 125 bytes and the threshold at which admission
    /// closes a lag above 875.
    /// </remarks>
    [TestFixture]
    public class AofBackpressureWaiterTests : TestBase
    {
        const long Budget = 1000;
        const long ClosedLag = 900;
        const long OpenLag = 100;

        static readonly TimeSpan Bound = TimeSpan.FromSeconds(15);

        sealed class Waiter : IAofGateWaiter
        {
            int notifications;

            internal int Notifications => Volatile.Read(ref notifications);

            internal void Reset() => Volatile.Write(ref notifications, 0);

            internal bool WaitNotified() => SpinWait.SpinUntil(() => Notifications > 0, Bound);

            public void OnGateOpen() => _ = Interlocked.Increment(ref notifications);
        }

        /// <summary>A waiter that registers again from inside its own notification, closing the gate first.</summary>
        sealed class ReRegisteringWaiter(AofBackpressure gate) : IAofGateWaiter
        {
            int notifications;
            int registered;

            internal int Notifications => Volatile.Read(ref notifications);

            internal bool Registered => Volatile.Read(ref registered) != 0;

            public void OnGateOpen()
            {
                if (Interlocked.Increment(ref notifications) != 1)
                    return;

                // The shipper falls behind again, and the session registers for the next opening
                SetLag(gate, ClosedLag);
                if (gate.TryRegisterWaiter(this))
                    Volatile.Write(ref registered, 1);
            }
        }

        sealed class ThrowingWaiter : IAofGateWaiter
        {
            public void OnGateOpen() => throw new InvalidOperationException("A waiter that fails when it is woken");
        }

        static AofBackpressure CreateGate()
            => new(new GarnetServerOptions { AofPhysicalSublogCount = 1, AofSyncMaxLagBytes = Budget });

        static void SetLag(AofBackpressure gate, long lag) => gate.PublishShippedAddress(0, -lag);

        [Test]
        public void ARegistrationIsRefusedUnlessAdmissionIsClosed()
        {
            using var gate = CreateGate();
            var waiter = new Waiter();

            // Admission closes above a lag of 875, so 875 itself is open
            SetLag(gate, 875);
            ClassicAssert.IsFalse(gate.TryRegisterWaiter(waiter));
            ClassicAssert.AreEqual(0, gate.WaiterCount);

            SetLag(gate, 876);
            ClassicAssert.IsTrue(gate.TryRegisterWaiter(waiter));
            ClassicAssert.AreEqual(1, gate.WaiterCount);
        }

        [Test]
        public void AdmissionClosesOnePublishIntervalBeforeAnAppenderWouldStall()
        {
            using var gate = CreateGate();

            // A session that could still be admitted would otherwise run into the stall that holds a thread
            SetLag(gate, ClosedLag);
            ClassicAssert.IsTrue(gate.AdmissionClosed());

            var append = Task.Run(() => gate.Wait(0, 0));
            ClassicAssert.IsTrue(append.Wait(Bound), "An append within the budget was made to wait");
        }

        [Test]
        public void AWaiterIsNotifiedOnceAdmissionReopensWithoutAnyPublishAnnouncingIt()
        {
            using var gate = CreateGate();
            var waiter = new Waiter();

            SetLag(gate, ClosedLag);
            ClassicAssert.IsTrue(gate.TryRegisterWaiter(waiter));

            // Many ticks of the timer find admission closed, and none of them lets the waiter go
            Thread.Sleep(100);
            ClassicAssert.AreEqual(0, waiter.Notifications, "A waiter was woken while admission was closed");
            ClassicAssert.AreEqual(1, gate.WaiterCount);

            SetLag(gate, OpenLag);
            ClassicAssert.IsTrue(waiter.WaitNotified(), "A waiter was not woken after admission reopened");

            Thread.Sleep(50);
            ClassicAssert.AreEqual(1, waiter.Notifications, "A waiter was woken more than once");
            ClassicAssert.AreEqual(0, gate.WaiterCount);
        }

        [Test]
        public void AWaiterThatRegistersAgainAfterBeingWokenIsWokenAgain()
        {
            using var gate = CreateGate();
            var waiter = new Waiter();

            for (var round = 0; round < 5; round++)
            {
                waiter.Reset();
                SetLag(gate, ClosedLag);
                ClassicAssert.IsTrue(gate.TryRegisterWaiter(waiter), $"Round {round}");

                SetLag(gate, OpenLag);
                ClassicAssert.IsTrue(waiter.WaitNotified(), $"Round {round}: a waiter was not woken");
            }
        }

        [Test]
        public void AWaiterThatRegistersFromItsNotificationWaitsForTheNextOpening()
        {
            using var gate = CreateGate();
            var waiter = new ReRegisteringWaiter(gate);

            SetLag(gate, ClosedLag);
            ClassicAssert.IsTrue(gate.TryRegisterWaiter(waiter));
            SetLag(gate, OpenLag);
            ClassicAssert.IsTrue(SpinWait.SpinUntil(() => waiter.Registered, Bound), "The waiter did not register again from its notification");

            // The registration made while waking belongs to the next pass, which has to wait for another opening
            Thread.Sleep(100);
            ClassicAssert.AreEqual(1, waiter.Notifications, "A waiter was woken again while admission was closed");
            ClassicAssert.AreEqual(1, gate.WaiterCount);

            SetLag(gate, OpenLag);
            ClassicAssert.IsTrue(SpinWait.SpinUntil(() => waiter.Notifications > 1, Bound), "The waiter was not woken by the next opening");
        }

        [Test]
        public void DisposingTheGateWakesTheWaitersAndRefusesNewOnes()
        {
            var gate = CreateGate();
            var waiter = new Waiter();

            SetLag(gate, ClosedLag);
            ClassicAssert.IsTrue(gate.TryRegisterWaiter(waiter));

            gate.Dispose();

            ClassicAssert.AreEqual(1, waiter.Notifications, "A waiter was still waiting after the gate shut down");
            ClassicAssert.IsFalse(gate.TryRegisterWaiter(new Waiter()), "A session parked on a gate that has shut down");
        }

        [Test]
        public void AWaiterThatFailsToBeWokenDoesNotLeaveTheOthersWaiting()
        {
            var gate = CreateGate();
            var before = new Waiter();
            var after = new Waiter();

            SetLag(gate, ClosedLag);
            ClassicAssert.IsTrue(gate.TryRegisterWaiter(before));
            ClassicAssert.IsTrue(gate.TryRegisterWaiter(new ThrowingWaiter()));
            ClassicAssert.IsTrue(gate.TryRegisterWaiter(after));

            Assert.DoesNotThrow(gate.Dispose);

            ClassicAssert.AreEqual(1, before.Notifications);
            ClassicAssert.AreEqual(1, after.Notifications, "A waiter was left waiting behind one that failed");
        }

        /// <summary>
        /// Waking the sessions parked on the gate is repeated every time the shipper falls behind and catches up,
        /// which under sustained throttling is many times a second. It reuses its lists rather than allocating them.
        /// </summary>
        [Test]
        public void WakingTheWaitersAllocatesNothingOnceTheirListsHaveGrown()
        {
            const int Waiters = 128;
            const int WarmUpRounds = 20;
            const int Rounds = 100;

            using var gate = CreateGate();
            var waiters = Enumerable.Range(0, Waiters).Select(_ => new Waiter()).ToArray();

            // Reports whether every waiter registered and was woken. It asserts nothing itself, since an NUnit
            // assertion allocates and would be measured with it.
            bool Round()
            {
                foreach (var waiter in waiters)
                    waiter.Reset();

                SetLag(gate, ClosedLag);
                foreach (var waiter in waiters)
                {
                    if (!gate.TryRegisterWaiter(waiter))
                        return false;
                }

                SetLag(gate, OpenLag);
                var start = Stopwatch.GetTimestamp();
                foreach (var waiter in waiters)
                {
                    while (waiter.Notifications == 0)
                    {
                        if (Stopwatch.GetElapsedTime(start) > Bound)
                            return false;
                        Thread.Yield();
                    }
                }

                return true;
            }

            for (var i = 0; i < WarmUpRounds; i++)
                ClassicAssert.IsTrue(Round(), $"Warm-up round {i} did not wake every waiter");

            var completed = 0;
            var before = GC.GetTotalAllocatedBytes(precise: true);
            while (completed < Rounds && Round())
                completed++;
            var allocated = GC.GetTotalAllocatedBytes(precise: true) - before;

            ClassicAssert.AreEqual(Rounds, completed, "A round did not wake every waiter");

            // Allocating a list per wake costs over 2 KB a round with this many waiters. The allowance covers
            // whatever else the test process does while the rounds run.
            ClassicAssert.Less(allocated, 32 * 1024, $"Waking {Waiters} waiters {Rounds} times allocated {allocated} bytes");
        }

        /// <summary>
        /// A session registers when it finds admission closed, and the timer wakes whoever is registered once it is
        /// open. A registration can land at any point of the timer's own pass, so a waiter left behind by one that
        /// lands between the timer's last look and its stopping would wait for ever.
        /// </summary>
        [Test]
        public void NoWaiterIsLeftWaitingWhileAdmissionKeepsClosingAndReopening()
        {
            const int Registrars = 4;

            using var gate = CreateGate();
            var registered = new ConcurrentBag<Waiter>();
            using var stop = new CancellationTokenSource();

            var tasks = new Task[Registrars + 1];
            tasks[0] = Task.Factory.StartNew(() =>
            {
                var closed = false;
                while (!stop.IsCancellationRequested)
                {
                    closed = !closed;
                    SetLag(gate, closed ? ClosedLag : OpenLag);
                    Thread.Yield();
                }
            }, TaskCreationOptions.LongRunning);

            for (var i = 1; i <= Registrars; i++)
            {
                tasks[i] = Task.Factory.StartNew(() =>
                {
                    while (!stop.IsCancellationRequested)
                    {
                        var waiter = new Waiter();
                        if (gate.TryRegisterWaiter(waiter))
                            registered.Add(waiter);
                        Thread.Yield();
                    }
                }, TaskCreationOptions.LongRunning);
            }

            Thread.Sleep(1000);
            stop.Cancel();
            ClassicAssert.IsTrue(Task.WaitAll(tasks, Bound), "The threads did not stop");

            SetLag(gate, OpenLag);
            ClassicAssert.IsNotEmpty(registered, "No registration was made, so nothing was tested");

            foreach (var waiter in registered)
                ClassicAssert.IsTrue(waiter.WaitNotified(), "A waiter was left waiting after admission reopened for good");

            // Admission can reopen between two registrations and close again before the timer looks, which is why
            // the count is only checked once everything has been woken.
            Thread.Sleep(50);
            foreach (var waiter in registered)
                ClassicAssert.AreEqual(1, waiter.Notifications, "A waiter was woken more than once");
            ClassicAssert.AreEqual(0, gate.WaiterCount);
        }
    }
}
