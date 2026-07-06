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
    /// Invariant tests for <see cref="WorkReadyComplete"/>, the per-task leader/worker handshake that
    /// replaced the shared cumulative-count barrier in parallel AOF replay.
    ///
    /// These guard the two properties that structurally eliminate the parallel-replay permit-steal race
    /// (previously reproduced by the now-deleted AofParallelReplayBarrierRaceTests):
    ///   1. A full leader/worker cycle hands over exactly one unit of work and returns both semaphores to
    ///      zero, so nothing leaks across cycles.
    ///   2. Each direction is bounded to a single permit: a stray second signal throws
    ///      <see cref="SemaphoreFullException"/> instead of silently accumulating a stealable permit.
    /// </summary>
    [TestFixture]
    public class WorkReadyCompleteTests
    {
        [Test]
        public void RepeatedCyclesHandOverExactlyOneUnitOfWork()
        {
            var signal = new WorkReadyComplete();
            const int cycles = 100;
            var processed = 0;
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

            var worker = Task.Run(async () =>
            {
                for (var i = 0; i < cycles; i++)
                {
                    await signal.WaitReadyWorkAsync(cts.Token).ConfigureAwait(false);
                    _ = Interlocked.Increment(ref processed);
                    signal.SignalCompleted();
                }
            });

            for (var i = 0; i < cycles; i++)
            {
                signal.SignalWorkReady();
                ClassicAssert.IsTrue(signal.WaitCompleted(TimeSpan.FromSeconds(30), cts.Token), "Leader timed out waiting for worker completion.");
                // After a full cycle the worker has processed exactly i+1 units of work.
                ClassicAssert.AreEqual(i + 1, Volatile.Read(ref processed));
            }

            Assert.DoesNotThrowAsync(async () => await worker);
            ClassicAssert.AreEqual(cycles, processed);
        }

        [Test]
        public void DoubleSignalWorkReadyThrowsSemaphoreFull()
        {
            var signal = new WorkReadyComplete();
            signal.SignalWorkReady();
            // Second hand-off before the worker consumed the first is a coordination bug and must fail loudly.
            _ = Assert.Throws<SemaphoreFullException>(() => signal.SignalWorkReady());
        }

        [Test]
        public void DoubleSignalCompletedThrowsSemaphoreFull()
        {
            var signal = new WorkReadyComplete();
            signal.SignalCompleted();
            // Second completion before the leader consumed the first would be the double-apply signature.
            _ = Assert.Throws<SemaphoreFullException>(() => signal.SignalCompleted());
        }

        [Test]
        public void WaitCompletedReturnsFalseOnTimeoutWhenNoCompletion()
        {
            var signal = new WorkReadyComplete();
            ClassicAssert.IsFalse(signal.WaitCompleted(TimeSpan.FromMilliseconds(50)), "WaitCompleted should time out when the worker never signals.");
        }
    }
}
