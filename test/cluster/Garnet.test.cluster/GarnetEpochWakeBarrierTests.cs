// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Garnet.cluster;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Component tests for the wake-based epoch barrier added to <see cref="GarnetEpoch{TEpochObserver}"/>
    /// (<c>BumpAndWaitForEpochTransitionWakeAsync</c> + <c>NotifyEpochReleased</c>). The barrier is
    /// exercised in isolation through an injected <see cref="IEpochObserver"/>, with no live
    /// server or network, so these tests target the wake/park protocol itself: correctness, the
    /// lost-wake-safe reset/re-check race, cancellation, and the allocation-free waker hot path.
    /// </summary>
    [TestFixture, NonParallelizable]
    internal class GarnetEpochWakeBarrierTests
    {
        /// <summary>
        /// Controllable observer source backing a list of per-session observed epochs (0 == idle).
        /// Mirrors <c>ServerEpochObserverSource</c>'s predicate while letting a test drive quiescence.
        /// Declared as a struct to satisfy <see cref="GarnetEpoch{TEpochObserver}"/>'s struct
        /// constraint; its reference-typed backing fields are shared across the value copy the epoch
        /// holds, so mutations made through the test's copy are observed by the barrier's scan.
        /// </summary>
        struct ControllableObserverSource : IEpochObserver
        {
            readonly List<long> epochs;
            readonly object gate;

            public ControllableObserverSource()
            {
                epochs = [];
                gate = new object();
            }

            public readonly int AddSession(long epoch)
            {
                lock (gate)
                {
                    epochs.Add(epoch);
                    return epochs.Count - 1;
                }
            }

            public readonly void SetEpoch(int index, long value)
            {
                lock (gate)
                {
                    epochs[index] = value;
                }
            }

            public readonly bool AllSessionsQuiesced(long targetEpoch)
            {
                lock (gate)
                {
                    foreach (var e in epochs)
                    {
                        if (e != 0 && e < targetEpoch)
                            return false;
                    }
                    return true;
                }
            }
        }

        static GarnetEpoch<ControllableObserverSource> CreateEpoch(ControllableObserverSource source)
            => new(source);

        [Test, CancelAfter(10_000)]
        public async Task FastPathCompletesWhenAlreadyQuiesced()
        {
            // No active observers -> the bump should complete on the fast spin path.
            var source = new ControllableObserverSource();
            var epoch = CreateEpoch(source);

            var completed = await epoch.BumpAndWaitForEpochTransitionAsync();

            ClassicAssert.IsTrue(completed);
        }

        [Test, CancelAfter(10_000)]
        public async Task IdleAndAheadSessionsDoNotBlock()
        {
            var source = new ControllableObserverSource();
            var epoch = CreateEpoch(source);

            // One idle (0) and one already ahead of any future target.
            source.AddSession(0);
            source.AddSession(long.MaxValue);

            var completed = await epoch.BumpAndWaitForEpochTransitionAsync();

            ClassicAssert.IsTrue(completed);
        }

        [Test, CancelAfter(15_000)]
        public async Task WakeCompletesWhenBlockingSessionReleases()
        {
            var source = new ControllableObserverSource();
            var epoch = CreateEpoch(source);

            // Session observed the current epoch; after the bump it is strictly behind the target.
            var idx = source.AddSession(epoch.GetCurrentEpoch());

            var waitTask = epoch.BumpAndWaitForEpochTransitionAsync();

            // The bump cannot complete while the session remains behind.
            var raced = await Task.WhenAny(waitTask, Task.Delay(250));
            ClassicAssert.AreNotEqual(waitTask, raced, "bump completed before the blocking session released");

            // Release: clear the session then signal, exactly as ReleaseCurrentEpoch will in Stage 3.
            source.SetEpoch(idx, 0);
            epoch.NotifyEpochReleased();

            ClassicAssert.IsTrue(await waitTask);
        }

        [Test, CancelAfter(15_000)]
        public async Task ReAcquireAtOrAboveTargetUnblocks()
        {
            var source = new ControllableObserverSource();
            var epoch = CreateEpoch(source);

            var start = epoch.GetCurrentEpoch();
            var idx = source.AddSession(start);

            var waitTask = epoch.BumpAndWaitForEpochTransitionAsync();

            var raced = await Task.WhenAny(waitTask, Task.Delay(250));
            ClassicAssert.AreNotEqual(waitTask, raced);

            // A session that re-acquires at the new (or higher) epoch is also quiesced.
            source.SetEpoch(idx, epoch.GetCurrentEpoch());
            epoch.NotifyEpochReleased();

            ClassicAssert.IsTrue(await waitTask);
        }

        [Test, CancelAfter(10_000)]
        public async Task CancellationReturnsFalse()
        {
            var source = new ControllableObserverSource();
            var epoch = CreateEpoch(source);

            // Permanently blocking session.
            source.AddSession(epoch.GetCurrentEpoch());

            using var cts = new CancellationTokenSource();
            var waitTask = epoch.BumpAndWaitForEpochTransitionAsync(token: cts.Token);

            var raced = await Task.WhenAny(waitTask, Task.Delay(250));
            ClassicAssert.AreNotEqual(waitTask, raced);

            cts.Cancel();

            ClassicAssert.IsFalse(await waitTask);
        }

        [Test, CancelAfter(10_000)]
        public void NotifyWithoutWaiterIsAllocationFreeNoOp()
        {
            var source = new ControllableObserverSource();
            var epoch = CreateEpoch(source);

            // Warm up the JIT so the measurement reflects steady-state behavior.
            for (var i = 0; i < 16; i++)
                epoch.NotifyEpochReleased();

            var before = GC.GetAllocatedBytesForCurrentThread();
            for (var i = 0; i < 100_000; i++)
                epoch.NotifyEpochReleased();
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            ClassicAssert.AreEqual(0, allocated, "waker hot path must not allocate when no waiter is parked");
        }

        [Test, CancelAfter(60_000)]
        public async Task LostWakeSoak()
        {
            // Stress the reset -> re-check -> await race between a parked waiter and a releasing
            // session across many iterations. Every bump must complete; a lost wake would stall the
            // loop until the 50ms park backstop re-scans, so a regression shows up as the fixture
            // timeout rather than a silent pass.
            var source = new ControllableObserverSource();
            var epoch = CreateEpoch(source);
            var idx = source.AddSession(epoch.GetCurrentEpoch());

            const int iterations = 5_000;
            for (var i = 0; i < iterations; i++)
            {
                // Make the session block relative to the upcoming target.
                source.SetEpoch(idx, epoch.GetCurrentEpoch());

                // Start the releaser first so it races the bump's spin/park window, exactly as a live
                // session releasing its epoch would overlap an in-flight bump.
                var releaseTask = Task.Run(() =>
                {
                    source.SetEpoch(idx, 0);
                    epoch.NotifyEpochReleased();
                });

                ClassicAssert.IsTrue(await epoch.BumpAndWaitForEpochTransitionAsync(),
                    $"wake was lost on iteration {i}");
                await releaseTask;
            }
        }
    }
}