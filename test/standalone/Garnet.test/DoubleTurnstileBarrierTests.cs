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
    /// Invariant tests for <see cref="DoubleTurnstileBarrier"/>, the centralized two-phase rendezvous
    /// barrier that replaced the per-task leader/worker handshakes in parallel AOF replay.
    ///
    /// These guard the properties that structurally eliminate the parallel-replay permit-steal race:
    ///   1. No participant advances past a page until the whole cohort has passed both barriers, so a
    ///      fast participant can never lap another and double-apply / skip a page (rendezvous property).
    ///   2. A full cycle drains the internal count back to zero, so nothing leaks across cycles and the
    ///      barrier is safely reusable an unbounded number of times.
    ///   3. Timeout and cancellation surface as <see cref="GarnetException"/> and
    ///      <see cref="OperationCanceledException"/> respectively.
    /// </summary>
    [TestFixture]
    public class DoubleTurnstileBarrierTests
    {
        [Test]
        [TestCase(2)]
        [TestCase(4)]
        [TestCase(8)]
        [TestCase(16)]
        public void RepeatedCyclesRendezvousExactlyOnce(int workerCount)
        {
            // participantCount = workers + 1 leader (the leader participates like everyone else).
            var barrier = new DoubleTurnstileBarrier(workerCount + 1);
            const int cycles = 1000;
            var processed = 0;
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

            var workers = new Task[workerCount];
            for (var w = 0; w < workerCount; w++)
            {
                workers[w] = Task.Run(async () =>
                {
                    for (var i = 0; i < cycles; i++)
                    {
                        await barrier.SignalWorkReadyWaitAsync(cancellationToken: cts.Token).ConfigureAwait(false);
                        _ = Interlocked.Increment(ref processed);
                        await barrier.SignalWorkCompletedWaitAsync(cancellationToken: cts.Token).ConfigureAwait(false);
                    }
                });
            }

            for (var i = 0; i < cycles; i++)
            {
                barrier.SignalWorkReadyWait(cancellationToken: cts.Token);
                barrier.SignalWorkCompletedWait(cancellationToken: cts.Token);
                // The completion rendezvous guarantees every worker finished this cycle's increment before
                // the leader is released. A steal/lap or a leaked permit would break this exact count.
                ClassicAssert.AreEqual((i + 1) * workerCount, Volatile.Read(ref processed));
            }

            Assert.DoesNotThrowAsync(async () => await Task.WhenAll(workers));
            ClassicAssert.AreEqual(cycles * workerCount, processed);
        }

        [Test]
        public void SingleParticipantNeverBlocks()
        {
            // With a single participant, that participant is always the last arriver and releases nobody.
            var barrier = new DoubleTurnstileBarrier(1);
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

            Assert.DoesNotThrow(() =>
            {
                for (var i = 0; i < 1000; i++)
                {
                    barrier.SignalWorkReadyWait(TimeSpan.FromSeconds(5), cts.Token);
                    barrier.SignalWorkCompletedWait(TimeSpan.FromSeconds(5), cts.Token);
                }
            });
        }

        [Test]
        public void FastParticipantBlocksUntilCohortArrives()
        {
            var barrier = new DoubleTurnstileBarrier(2);
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var released = false;

            var fast = Task.Run(() =>
            {
                barrier.SignalWorkReadyWait(cancellationToken: cts.Token);
                Volatile.Write(ref released, true);
            });

            // The fast participant must stay parked at the ready barrier until the second participant arrives.
            Thread.Sleep(200);
            ClassicAssert.IsFalse(Volatile.Read(ref released), "Participant proceeded before the cohort assembled.");

            barrier.SignalWorkReadyWait(cancellationToken: cts.Token);
            Assert.DoesNotThrow(() => fast.Wait(TimeSpan.FromSeconds(10)));
            ClassicAssert.IsTrue(Volatile.Read(ref released));
        }

        [Test]
        public void SignalWorkReadyWaitThrowsGarnetExceptionOnTimeout()
        {
            var barrier = new DoubleTurnstileBarrier(2);
            // Only one of two participants arrives, so the barrier can never assemble.
            _ = Assert.Throws<GarnetException>(() => barrier.SignalWorkReadyWait(TimeSpan.FromMilliseconds(50)));
        }

        [Test]
        public void SignalWorkCompletedThrowsGarnetExceptionOnTimeout()
        {
            var barrier = new DoubleTurnstileBarrier(2);
            _ = Assert.Throws<GarnetException>(() => barrier.SignalWorkCompletedWait(TimeSpan.FromMilliseconds(50)));
        }

        [Test]
        public void SignalWorkReadyWaitAsyncThrowsGarnetExceptionOnTimeout()
        {
            var barrier = new DoubleTurnstileBarrier(2);
            _ = Assert.ThrowsAsync<GarnetException>(async () => await barrier.SignalWorkReadyWaitAsync(TimeSpan.FromMilliseconds(50)));
        }

        [Test]
        public void SignalWorkReadyWaitThrowsOnCancellation()
        {
            var barrier = new DoubleTurnstileBarrier(2);
            using var cts = new CancellationTokenSource();
            var waiter = Task.Run(() => barrier.SignalWorkReadyWait(cancellationToken: cts.Token));

            Thread.Sleep(100);
            cts.Cancel();

            var ex = Assert.ThrowsAsync<OperationCanceledException>(async () => await waiter);
            Assert.That(ex, Is.Not.Null);
        }

        [Test]
        public async Task SignalWorkCompletedAsyncThrowsOnCancellation()
        {
            var barrier = new DoubleTurnstileBarrier(2);
            using var cts = new CancellationTokenSource();
            var waiter = barrier.SignalWorkCompletedWaitAsync(cancellationToken: cts.Token);

            await Task.Delay(100);
            await cts.CancelAsync();

            _ = Assert.ThrowsAsync<OperationCanceledException>(async () => await waiter);
        }

        [Test]
        public void ConstructorRejectsNonPositiveParticipantCount()
        {
            _ = Assert.Throws<ArgumentOutOfRangeException>(() => new DoubleTurnstileBarrier(0));
            _ = Assert.Throws<ArgumentOutOfRangeException>(() => new DoubleTurnstileBarrier(-1));
        }
    }
}