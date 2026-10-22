// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers the rendezvous a blocking command parks on, directly rather than through a server. The orderings
    /// that matter here -- a signal that overtakes the wait, a teardown that overtakes the registration, and a
    /// signal racing a teardown -- are decided by a few instructions on two threads, so a test driving them
    /// through a socket would reach them only by luck. Driving the primitive itself makes each one exact.
    /// </summary>
    [TestFixture]
    public class SessionSignalTests : TestBase
    {
        const string Name = "waiters";

        /// <summary>
        /// Upper bound on any wait here. Every wait in this fixture is released by the line above it, so the
        /// limit only exists to fail a lost wakeup instead of hanging the run.
        /// </summary>
        static readonly TimeSpan WaitLimit = TimeSpan.FromSeconds(30);

        [Test]
        public async Task ASignalDeliveredBeforeTheWaitBeginsStillCompletesIt()
        {
            var registry = new SessionSignalRegistry();
            var signal = new SessionSignal();

            registry.Register(Name, signal);

            // The real ordering: a session is published to signallers before it reaches its await, so a
            // signaller can arrive first. Registration must therefore leave the signal already armed.
            ClassicAssert.AreEqual(1, registry.SignalAll(Name));

            var wait = signal.WaitAsync();
            ClassicAssert.IsTrue(wait.IsCompleted);
            await wait;
        }

        [Test]
        public async Task AWaiterIsWokenWhileItIsAwaiting()
        {
            var registry = new SessionSignalRegistry();
            var signal = new SessionSignal();

            registry.Register(Name, signal);
            var wait = signal.WaitAsync().AsTask();
            ClassicAssert.IsFalse(wait.IsCompleted);

            ClassicAssert.AreEqual(1, registry.SignalAll(Name));
            await wait.WaitAsync(WaitLimit);
        }

        [Test]
        public void SignallingANameNobodyWaitsOnWakesNothing()
        {
            var registry = new SessionSignalRegistry();

            ClassicAssert.AreEqual(0, registry.SignalAll(Name));

            var signal = new SessionSignal();
            registry.Register(Name, signal);
            ClassicAssert.AreEqual(0, registry.SignalAll("other"));
        }

        [Test]
        public async Task SignallingWakesEveryWaiterOnTheNameExactlyOnce()
        {
            var registry = new SessionSignalRegistry();
            var signals = new SessionSignal[8];
            var waits = new Task[signals.Length];

            for (var i = 0; i < signals.Length; i++)
            {
                signals[i] = new SessionSignal();
                registry.Register(Name, signals[i]);
                waits[i] = signals[i].WaitAsync().AsTask();
            }

            ClassicAssert.AreEqual(signals.Length, registry.SignalAll(Name));
            await Task.WhenAll(waits).WaitAsync(WaitLimit);

            // The waiters were consumed by the first signal, not left behind to be woken twice.
            ClassicAssert.AreEqual(0, registry.SignalAll(Name));
        }

        /// <summary>
        /// A session that is torn down while parked is never resumed, so nothing its command would have run
        /// after the await takes its signal off the name. The teardown has to.
        /// </summary>
        [Test]
        public void ATeardownTakesTheSignalOffItsName()
        {
            var registry = new SessionSignalRegistry();
            var stays = new SessionSignal();
            var leaves = new SessionSignal();

            registry.Register(Name, stays);
            registry.Register(Name, leaves);
            ClassicAssert.AreEqual(2, registry.WaiterCount(Name));

            leaves.Cancel();

            ClassicAssert.AreEqual(1, registry.WaiterCount(Name), "the torn-down session is still listed");
            ClassicAssert.AreEqual(1, registry.SignalAll(Name));
        }

        /// <summary>
        /// Teardown releases a wait by faulting it, so the command body unwinds instead of running on to
        /// write a reply into a response object the session is giving back.
        /// </summary>
        [Test]
        public void ATeardownCompletesAWaitInFlight()
        {
            var registry = new SessionSignalRegistry();
            var signal = new SessionSignal();

            registry.Register(Name, signal);
            var wait = signal.WaitAsync().AsTask();
            ClassicAssert.IsFalse(wait.IsCompleted);

            signal.Cancel();
            _ = Assert.ThrowsAsync<OperationCanceledException>(async () => await wait.WaitAsync(WaitLimit));
        }

        [Test]
        public void ATeardownThatPrecedesTheWaitStillCompletesIt()
        {
            var registry = new SessionSignalRegistry();
            var signal = new SessionSignal();

            // Teardown runs on the network thread and can overtake a command body that is on its way to its
            // await. Nothing will complete the wait afterwards, so the wait itself has to notice, and the
            // registration has to notice too, because the teardown found nothing listed to take off.
            signal.Cancel();
            registry.Register(Name, signal);

            ClassicAssert.AreEqual(0, registry.WaiterCount(Name), "a signal registered after its teardown stays listed");

            var wait = signal.WaitAsync();
            ClassicAssert.IsTrue(wait.IsCompleted);
            _ = Assert.ThrowsAsync<OperationCanceledException>(async () => await wait);
        }

        /// <summary>
        /// A teardown can land at any step of a registration, including after the signal has joined the list
        /// and before it has been told which list that is. Wherever it lands the signal must not stay listed,
        /// or a later signal would count a session that is gone as woken.
        /// </summary>
        [Test]
        public async Task ATeardownRacingARegistrationNeverLeavesTheSignalListed()
        {
            var registry = new SessionSignalRegistry();

            for (var round = 0; round < 20_000; round++)
            {
                var signal = new SessionSignal();

                using var start = new SemaphoreSlim(0, 2);
                var registering = Task.Run(() =>
                {
                    start.Wait();
                    registry.Register(Name, signal);
                });
                var tearingDown = Task.Run(() =>
                {
                    start.Wait();

                    // Sweeps the teardown across the steps of the registration
                    Thread.SpinWait(round % 64);
                    signal.Cancel();
                });

                start.Release(2);
                await Task.WhenAll(registering, tearingDown).WaitAsync(WaitLimit);

                ClassicAssert.AreEqual(0, registry.WaiterCount(Name), $"round {round}: a torn-down signal stayed listed");
            }
        }

        [Test]
        public async Task ASignalIsReusableAcrossWaits()
        {
            var registry = new SessionSignalRegistry();
            var signal = new SessionSignal();

            for (var i = 0; i < 16; i++)
            {
                registry.Register(Name, signal);
                var wait = signal.WaitAsync().AsTask();
                ClassicAssert.AreEqual(1, registry.SignalAll(Name));
                await wait.WaitAsync(WaitLimit);
            }
        }

        [Test]
        public async Task ASignalRacingATeardownCompletesTheWaitOnce()
        {
            var registry = new SessionSignalRegistry();

            for (var round = 0; round < 5_000; round++)
            {
                var signal = new SessionSignal();
                registry.Register(Name, signal);
                var wait = signal.WaitAsync().AsTask();

                using var start = new SemaphoreSlim(0, 2);
                var signalling = Task.Run(() =>
                {
                    start.Wait();
                    return registry.SignalAll(Name);
                });
                var tearingDown = Task.Run(() =>
                {
                    start.Wait();
                    signal.Cancel();
                });

                start.Release(2);

                // Either may win, but completing the wait twice throws out of whichever ran second.
                await Task.WhenAll(signalling, tearingDown).WaitAsync(WaitLimit);

                // The winner decides how the wait ends: a signal satisfies it, a teardown faults it.
                try
                {
                    await wait.WaitAsync(WaitLimit);
                }
                catch (OperationCanceledException)
                {
                }
            }
        }

        [Test]
        public async Task ConcurrentRegistrationsAndSignalsWakeEveryWaiter()
        {
            var registry = new SessionSignalRegistry();
            const int Waiters = 8;
            const int Rounds = 500;

            var workers = new Task[Waiters];
            for (var w = 0; w < Waiters; w++)
            {
                workers[w] = Task.Run(async () =>
                {
                    var signal = new SessionSignal();
                    for (var round = 0; round < Rounds; round++)
                    {
                        registry.Register(Name, signal);
                        await signal.WaitAsync();
                    }
                });
            }

            var all = Task.WhenAll(workers);
            var deadline = DateTime.UtcNow + WaitLimit;
            while (!all.IsCompleted && DateTime.UtcNow < deadline)
                _ = registry.SignalAll(Name);

            await all.WaitAsync(WaitLimit);
        }

        /// <summary>
        /// A session signals from inside its own batch, where it holds its response object and, in cluster
        /// mode, its epoch. Waking the parked session inline would run that session's entire batch nested
        /// inside both, re-entering the epoch on one thread and leaving the depth of a wakeup chain bounded
        /// only by how many sessions wake each other.
        /// </summary>
        [Test]
        public void AWakeupNeverRunsOnTheSignallingThread()
        {
            var registry = new SessionSignalRegistry();
            var signal = new SessionSignal();

            registry.Register(Name, signal);

            using var woken = new ManualResetEventSlim();
            var wokenOn = 0;

            // A suspended body subscribes from a network thread, where there is no synchronization context
            // to capture, which is what leaves the dispatch decision to the source itself. NUnit installs
            // one, and a captured context would post the continuation elsewhere no matter what the source
            // decided -- hiding the very choice under test -- so the subscription is made without it.
            var awaiter = signal.WaitAsync().GetAwaiter();
            var testContext = SynchronizationContext.Current;
            SynchronizationContext.SetSynchronizationContext(null);
            try
            {
                awaiter.UnsafeOnCompleted(() =>
                {
                    wokenOn = Environment.CurrentManagedThreadId;
                    woken.Set();
                });
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(testContext);
            }

            var signalledOn = Environment.CurrentManagedThreadId;
            ClassicAssert.AreEqual(1, registry.SignalAll(Name));

            ClassicAssert.IsTrue(woken.Wait(WaitLimit), "the waiter was never woken");
            ClassicAssert.AreNotEqual(signalledOn, wokenOn,
                "the wakeup ran on the signalling thread, nesting the woken session's batch inside the " +
                "signaller's response object and epoch");

            awaiter.GetResult();
        }
    }
}