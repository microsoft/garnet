// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Tests of <see cref="AsyncRendezvous"/>, the one-shot cohort rendezvous whose participants park instead of
    /// blocking a thread. Replica sublog streams rendezvous from their network receive callbacks, so a waiter that
    /// blocked would stall the very callback that must deliver the arrival it is waiting for.
    /// </summary>
    [TestFixture]
    public class AsyncRendezvousTests : TestBase
    {
        static readonly TimeSpan Bound = TimeSpan.FromSeconds(15);

        [Test]
        public async Task TheLastArrivalReleasesEveryParkedParticipant()
        {
            var rendezvous = new AsyncRendezvous(3);

            var first = rendezvous.SignalAndWaitAsync(Bound).AsTask();
            var second = rendezvous.SignalAndWaitAsync(Bound).AsTask();
            ClassicAssert.IsFalse(first.IsCompleted, "An early arrival must park");
            ClassicAssert.IsFalse(second.IsCompleted, "An early arrival must park");

            var last = rendezvous.SignalAndWaitAsync(Bound);
            ClassicAssert.IsTrue(last.IsCompletedSuccessfully, "The arrival that completes the cohort must not wait");
            ClassicAssert.IsTrue(last.Result);

            ClassicAssert.IsTrue(await first.WaitAsync(Bound));
            ClassicAssert.IsTrue(await second.WaitAsync(Bound));

            var late = rendezvous.SignalAndWaitAsync(Bound);
            ClassicAssert.IsTrue(late.IsCompletedSuccessfully && late.Result, "An arrival after completion must not wait");
        }

        [Test]
        public async Task ATimedOutParticipantIsReleasedAloneAndStillCounts()
        {
            var rendezvous = new AsyncRendezvous(3);

            var timedOut = await rendezvous.SignalAndWaitAsync(TimeSpan.FromMilliseconds(50)).AsTask().WaitAsync(Bound);
            ClassicAssert.IsFalse(timedOut, "A participant whose timeout elapsed is released with false");

            var second = rendezvous.SignalAndWaitAsync(Bound).AsTask();
            ClassicAssert.IsFalse(second.IsCompleted);

            var third = rendezvous.SignalAndWaitAsync(Bound);
            ClassicAssert.IsTrue(third.IsCompletedSuccessfully, "The timed-out arrival must still count toward the cohort");
            ClassicAssert.IsTrue(await second.WaitAsync(Bound));
        }

        [Test]
        public async Task AnInfiniteTimeoutWaitsUntilTheCohortArrives()
        {
            var rendezvous = new AsyncRendezvous(2);

            var first = rendezvous.SignalAndWaitAsync(Timeout.InfiniteTimeSpan).AsTask();
            await Task.Delay(200);
            ClassicAssert.IsFalse(first.IsCompleted, "An infinite timeout must not release the waiter");

            _ = rendezvous.SignalAndWaitAsync(Timeout.InfiniteTimeSpan);
            ClassicAssert.IsTrue(await first.WaitAsync(Bound));
        }

        [Test]
        public void CancellingTheTokenFaultsTheWaiterWithoutResumingItOnTheCanceller()
        {
            var rendezvous = new AsyncRendezvous(2);
            using var cts = new CancellationTokenSource();
            var parked = rendezvous.SignalAndWaitAsync(Timeout.InfiniteTimeSpan, cts.Token).AsTask();
            var probe = new ResumeProbe(parked, Bound);

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
            ClassicAssert.IsTrue(parked.IsCanceled, "Cancellation must fault the waiter");
            ClassicAssert.AreNotEqual(cancellerId, probe.ThreadId, "The waiter must resume on the pool rather than on the thread that cancelled");
        }

        [Test]
        public void TheLastArrivalDoesNotResumeParkedWaitersOnItsOwnThread()
        {
            var rendezvous = new AsyncRendezvous(2);
            var parked = rendezvous.SignalAndWaitAsync(Bound).AsTask();
            var probe = new ResumeProbe(parked, Bound);

            var arriverId = -1;
            var arriver = new Thread(() =>
            {
                arriverId = Environment.CurrentManagedThreadId;
                _ = rendezvous.SignalAndWaitAsync(Bound);
                _ = probe.Wait();
            });
            arriver.Start();

            ClassicAssert.IsTrue(probe.Wait(), "The last arrival must release the waiter");
            ClassicAssert.IsTrue(arriver.Join(Bound));
            ClassicAssert.IsTrue(parked.IsCompletedSuccessfully);
            ClassicAssert.AreNotEqual(arriverId, probe.ThreadId, "The waiter must resume on the pool rather than on the last arriver's thread");
        }

        [Test]
        public async Task ConcurrentCohortsAlwaysComplete()
        {
            const int participants = 8;

            for (var round = 0; round < 200; round++)
            {
                var rendezvous = new AsyncRendezvous(participants);
                var arrivals = Enumerable.Range(0, participants)
                    .Select(_ => Task.Run(async () => await rendezvous.SignalAndWaitAsync(Bound)))
                    .ToArray();

                var results = await Task.WhenAll(arrivals).WaitAsync(Bound);
                ClassicAssert.IsTrue(results.All(result => result), $"Round {round}: every participant must see the cohort complete");
            }
        }
    }
}