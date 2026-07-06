// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Layer 2 deterministic reproduction of the parallel AOF replay race.
    ///
    /// Root cause (see AofParallelReplaySerialTests for the Layer 1 discriminator that proves the apply
    /// logic itself is correct): the <see cref="LeaderFollowerBarrier"/> coordinates the leader and N
    /// replay tasks with three CUMULATIVE / fungible counting semaphores (workReady, workCompleted,
    /// resetReady), each Release(N)'d per page. Permits are not tied to a specific participant, so a fast
    /// participant can consume a resetReady permit LEAKED from the previous cycle, round-trip, and grab a
    /// second workReady permit for the next page — processing that page twice — while a slow participant is
    /// starved and processes it zero times. The leader's WaitCompleted merely drains N workCompleted
    /// permits ("2 from one task + 0 from another == N == done") and advances the replication offset.
    ///
    /// This maps exactly to the two CI failures: INCR entries applied twice (double) and SET entries never
    /// applied (miss), while the replication offset still advances.
    ///
    /// The reproduction is fully deterministic (no timing / no iteration count). It drives the real barrier
    /// with a 2-participant leader/participant harness and uses the barrier's test-only
    /// <see cref="LeaderFollowerBarrier.AfterWorkCompletedReleased"/> seam to freeze the "slow" participant
    /// in the exact window between "signalled completion" and "consumed its reset permit". That leaves a
    /// leaked reset permit which the "fast" participant steals — and because the leader needs 2 completions
    /// for page B while the slow participant is frozen, the fast participant is FORCED to process page B
    /// twice for the leader to ever proceed.
    ///
    /// EXPECTED TO FAIL on the current cumulative-semaphore barrier (that is the point — it proves the race
    /// deterministically). It is marked [Explicit] so it does not run in normal CI while the barrier is
    /// still buggy. Once the barrier is changed to per-task workReady[i]/workCompleted[i] semaphores (the
    /// endorsed fix), the permit steal is structurally impossible; the invariant asserted here then holds
    /// and the [Explicit] attribute should be removed so it guards against regressions.
    /// </summary>
    [TestFixture]
    public class AofParallelReplayBarrierRaceTests
    {
        const int FastParticipant = 0;
        const int SlowParticipant = 1;
        const int PageA = 1;
        const int PageB = 2;

        // Flows through awaits so the barrier seam (invoked synchronously inside SignalCompleted) can tell
        // which participant is currently completing.
        static readonly AsyncLocal<int> ParticipantId = new();

        [Test]
        [Explicit("Deterministically reproduces the parallel-replay barrier permit-steal race; expected to FAIL until the barrier is switched to per-task semaphores. Remove [Explicit] once fixed.")]
        public void LeaderFollowerBarrierAllowsDoubleProcessAndStarvation()
        {
            const int participantCount = 2;
            var barrier = new LeaderFollowerBarrier(participantCount);
            using var cts = new CancellationTokenSource();

            // Per-participant ledger of which pages each participant processed.
            var processed = new List<int>[participantCount];
            for (var i = 0; i < participantCount; i++)
                processed[i] = new List<int>();
            var ledgerLock = new object();

            var currentPage = 0;

            // Freeze the slow participant after it releases its completion permit but before it consumes
            // its reset permit — leaving a leaked reset permit for the fast participant to steal.
            using var slowReachedReset = new ManualResetEventSlim(false);
            using var slowMayConsumeReset = new ManualResetEventSlim(false);

            barrier.AfterWorkCompletedReleased = () =>
            {
                if (ParticipantId.Value == SlowParticipant)
                {
                    slowReachedReset.Set();
                    slowMayConsumeReset.Wait();
                }
            };

            Task RunParticipant(int id) => Task.Run(async () =>
            {
                ParticipantId.Value = id;
                try
                {
                    while (!cts.IsCancellationRequested)
                    {
                        await barrier.WaitReadyWorkAsync(cts.Token).ConfigureAwait(false);
                        var page = Volatile.Read(ref currentPage);
                        lock (ledgerLock)
                            processed[id].Add(page);
                        barrier.SignalCompleted(cts.Token);
                    }
                }
                catch (OperationCanceledException)
                {
                    // Expected on teardown.
                }
            });

            var fast = RunParticipant(FastParticipant);
            var slow = RunParticipant(SlowParticipant);

            var leaderTimeout = TimeSpan.FromSeconds(30);
            try
            {
                // ---- Page A: both participants process it once. ----
                Volatile.Write(ref currentPage, PageA);
                barrier.SignalWorkReady();
                ClassicAssert.IsTrue(barrier.WaitCompleted(leaderTimeout, cts.Token), "Leader timed out draining page A completions.");
                barrier.Release();

                // Confirm the slow participant is frozen in the reset window (it released its page-A
                // completion permit but has NOT yet consumed its page-A reset permit).
                ClassicAssert.IsTrue(slowReachedReset.Wait(leaderTimeout), "Slow participant never reached the reset seam.");

                // ---- Page B: slow participant is frozen; leader still needs 2 completions. ----
                // On the buggy barrier the fast participant steals the slow participant's leaked reset
                // permit and processes page B twice so the leader's WaitCompleted can succeed.
                Volatile.Write(ref currentPage, PageB);
                barrier.SignalWorkReady();
                ClassicAssert.IsTrue(barrier.WaitCompleted(leaderTimeout, cts.Token), "Leader timed out draining page B completions.");
                barrier.Release();
            }
            finally
            {
                // Always release the frozen participant and tear down, even if an assert failed above.
                slowMayConsumeReset.Set();
                cts.Cancel();
                try { Task.WaitAll(new[] { fast, slow }, leaderTimeout); }
                catch (AggregateException) { /* participants observe cancellation */ }
            }

            int PageBCount(int participant)
            {
                lock (ledgerLock)
                    return processed[participant].Count(p => p == PageB);
            }

            // The invariant that MUST hold (and does, once the barrier uses per-task semaphores):
            // every participant processes every page exactly once.
            ClassicAssert.AreEqual(1, PageBCount(FastParticipant), "Fast participant did not process page B exactly once (double-apply).");
            ClassicAssert.AreEqual(1, PageBCount(SlowParticipant), "Slow participant did not process page B exactly once (starved / missed apply).");
        }
    }
}
