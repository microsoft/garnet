// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Tests of <see cref="ProgressSignal"/>, which parks the session that feeds a replica's background replay while the
    /// replay lags, in place of spinning on the network receive thread. That thread is also the one that has to run the
    /// replay's own network traffic, so a waiter that held it would stall the progress it waits for.
    /// </summary>
    [TestFixture]
    public class ProgressSignalTests : TestBase
    {
        static readonly TimeSpan Bound = TimeSpan.FromSeconds(15);

        [Test]
        public void AConditionThatDoesNotHoldCompletesWithoutAllocating()
        {
            const int calls = 10_000;
            var signal = new ProgressSignal();
            Func<bool> clear = static () => false;
            var completed = 0;

            // Warm up so that one-time initialization is not counted
            _ = signal.WaitWhileAsync(clear, CancellationToken.None);

            var before = GC.GetAllocatedBytesForCurrentThread();
            for (var i = 0; i < calls; i++)
            {
                if (signal.WaitWhileAsync(clear, CancellationToken.None).IsCompletedSuccessfully)
                    completed++;
            }
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            ClassicAssert.AreEqual(calls, completed);
            ClassicAssert.AreEqual(0, allocated, "Waiting on a condition that does not hold must not allocate");
        }

        [Test]
        public async Task APulseWakesAWaiterWhoseConditionHasCleared()
        {
            var signal = new ProgressSignal();
            var blocked = true;

            var wait = signal.WaitWhileAsync(() => Volatile.Read(ref blocked), CancellationToken.None).AsTask();
            ClassicAssert.IsFalse(wait.IsCompleted, "The waiter must park while the condition holds");

            Volatile.Write(ref blocked, false);
            signal.Pulse();

            await wait.WaitAsync(Bound);
        }

        [Test]
        public async Task APulseWhileTheConditionStillHoldsLeavesTheWaiterParked()
        {
            var signal = new ProgressSignal();
            var blocked = true;
            var evaluations = 0;

            var wait = signal.WaitWhileAsync(() =>
            {
                _ = Interlocked.Increment(ref evaluations);
                return Volatile.Read(ref blocked);
            }, CancellationToken.None).AsTask();
            // The entry check, then the check after the waiter publishes its wake-up
            ClassicAssert.AreEqual(2, Volatile.Read(ref evaluations));

            signal.Pulse();
            // The woken waiter evaluates once more and, the condition still holding, publishes a new wake-up and evaluates again
            _ = SpinWait.SpinUntil(() => Volatile.Read(ref evaluations) >= 4 || wait.IsCompleted, Bound);
            ClassicAssert.IsFalse(wait.IsCompleted, "A waiter whose condition still holds must stay parked");
            ClassicAssert.AreEqual(4, Volatile.Read(ref evaluations));

            Volatile.Write(ref blocked, false);
            signal.Pulse();
            await wait.WaitAsync(Bound);
        }

        [Test]
        public void AChangeThatLandsBetweenTheEntryCheckAndTheParkStillWakesTheWaiter()
        {
            var signal = new ProgressSignal();
            var blocked = true;
            var evaluations = 0;

            // The entry check is the first evaluation. The second runs once the waiter has published its wake-up, so it is
            // where a change that lands in the window after the entry check must be seen or must find the wake-up. Here the
            // change lands right then, clearing the condition and pulsing, while the evaluation reports the value it read
            // before. Only a waiter that published its wake-up first is woken.
            var wait = signal.WaitWhileAsync(() =>
            {
                var holds = Volatile.Read(ref blocked);
                if (Interlocked.Increment(ref evaluations) == 2)
                {
                    Volatile.Write(ref blocked, false);
                    signal.Pulse();
                }
                return holds;
            }, CancellationToken.None).AsTask();

            ClassicAssert.IsTrue(wait.Wait(Bound), "A change landing after the entry check must not strand the waiter");
        }

        [Test]
        public void CancellingTheTokenFaultsTheWaiterWithoutResumingItOnTheCanceller()
        {
            var signal = new ProgressSignal();
            using var cts = new CancellationTokenSource();
            var wait = signal.WaitWhileAsync(static () => true, cts.Token).AsTask();
            var probe = new ResumeProbe(wait, Bound);

            var cancellerId = -1;
            var canceller = new Thread(() =>
            {
                cancellerId = Environment.CurrentManagedThreadId;
                cts.Cancel();
                // Staying alive until the waiter resumes keeps the pool from reusing this thread's id
                _ = probe.Wait();
            });
            canceller.Start();

            ClassicAssert.IsTrue(probe.Wait(), "Cancelling must release the waiter");
            ClassicAssert.IsTrue(canceller.Join(Bound));
            ClassicAssert.IsTrue(wait.IsCanceled, "Cancellation must fault the waiter");
            ClassicAssert.AreNotEqual(cancellerId, probe.ThreadId, "The waiter must resume on the pool rather than on the thread that cancelled");
        }

        [Test]
        public void APulseDoesNotResumeTheWaiterOnThePulsingThread()
        {
            var signal = new ProgressSignal();
            var blocked = true;
            var wait = signal.WaitWhileAsync(() => Volatile.Read(ref blocked), CancellationToken.None).AsTask();
            var probe = new ResumeProbe(wait, Bound);

            var pulserId = -1;
            var pulser = new Thread(() =>
            {
                pulserId = Environment.CurrentManagedThreadId;
                Volatile.Write(ref blocked, false);
                signal.Pulse();
                _ = probe.Wait();
            });
            pulser.Start();

            ClassicAssert.IsTrue(probe.Wait(), "The pulse must release the waiter");
            ClassicAssert.IsTrue(pulser.Join(Bound));
            ClassicAssert.IsTrue(wait.IsCompletedSuccessfully);
            ClassicAssert.AreNotEqual(pulserId, probe.ThreadId, "The waiter must resume on the pool rather than on the thread that pulsed");
        }

        [Test]
        public async Task AWaiterThatRacesItsPulserIsNeverStranded()
        {
            const int rounds = 20_000;
            var signal = new ProgressSignal();
            var target = 0;
            var progress = 0;

            var pulser = Task.Factory.StartNew(() =>
            {
                for (var round = 1; round <= rounds; round++)
                {
                    // Land the change as soon as the waiter starts waiting for it, which is when it is closest to parking
                    while (Volatile.Read(ref target) < round)
                        Thread.Yield();

                    Volatile.Write(ref progress, round);
                    signal.Pulse();
                }
            }, TaskCreationOptions.LongRunning);

            for (var round = 1; round <= rounds; round++)
            {
                var wanted = round;
                Volatile.Write(ref target, wanted);
                await signal.WaitWhileAsync(() => Volatile.Read(ref progress) < wanted, CancellationToken.None).AsTask().WaitAsync(Bound);
            }

            await pulser.WaitAsync(Bound);
        }
    }
}