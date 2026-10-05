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
    /// Component tests for the self-poll epoch barrier in <see cref="GarnetEpoch{TEpochObserver}"/>
    /// (<c>BumpAndWaitForEpochTransitionAsync</c>). The barrier is exercised in isolation through an
    /// injected <see cref="IEpochObserver"/>, with no live server or network, so these tests target the
    /// wait protocol itself: fast-path quiescence, the jittered backoff re-scan that discovers a
    /// released session without any waker, concurrent bumps each converging independently, and
    /// cancellation.
    /// </summary>
    [TestFixture, NonParallelizable]
    internal class GarnetEpochBarrierTests
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

        // Small park slices so the backoff re-scan path is reached quickly in tests.
        static readonly TimeSpan BaseParkDelay = TimeSpan.FromMilliseconds(1);
        static readonly TimeSpan MaxParkDelay = TimeSpan.FromMilliseconds(10);

        static GarnetEpoch<ControllableObserverSource> CreateEpoch(ControllableObserverSource source)
            => new(source, BaseParkDelay, MaxParkDelay);

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
        public async Task PollCompletesWhenBlockingSessionReleases()
        {
            // The bump parks on the backoff poll while the session is behind the target, and the next
            // re-scan after the session clears must complete it -- no notification is involved.
            var source = new ControllableObserverSource();
            var epoch = CreateEpoch(source);

            // Session observed the current epoch; after the bump it is strictly behind the target.
            var idx = source.AddSession(epoch.GetCurrentEpoch());

            var waitTask = epoch.BumpAndWaitForEpochTransitionAsync();

            // The bump cannot complete while the session remains behind.
            var raced = await Task.WhenAny(waitTask, Task.Delay(250));
            ClassicAssert.AreNotEqual(waitTask, raced, "bump completed before the blocking session released");

            // Release: clear the session, exactly as ReleaseCurrentEpoch does. A self-poll re-scan
            // discovers it without any waker.
            source.SetEpoch(idx, 0);

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
            var waitTask = epoch.BumpAndWaitForEpochTransitionAsync(cts.Token);

            var raced = await Task.WhenAny(waitTask, Task.Delay(250));
            ClassicAssert.AreNotEqual(waitTask, raced);

            cts.Cancel();

            ClassicAssert.IsFalse(await waitTask);
        }

        [Test, CancelAfter(15_000)]
        public async Task ConcurrentBumpsEachConvergeOnRelease()
        {
            // Two bumps park on their own independent backoff schedules at the same time. With no waker
            // and no shared coordination, each must observe the single release on its next re-scan.
            var source = new ControllableObserverSource();
            var epoch = CreateEpoch(source);
            var idx = source.AddSession(epoch.GetCurrentEpoch());

            var first = epoch.BumpAndWaitForEpochTransitionAsync();
            var second = epoch.BumpAndWaitForEpochTransitionAsync();

            // Let both bumps pass the adaptive spin and park; neither can complete while the session blocks.
            var both = Task.WhenAll(first, second);
            var raced = await Task.WhenAny(both, Task.Delay(250));
            ClassicAssert.AreNotEqual(both, raced, "a bump completed before the blocking session released");

            // One release: both parked bumps must independently converge on their next poll.
            source.SetEpoch(idx, 0);

            ClassicAssert.IsTrue(await first);
            ClassicAssert.IsTrue(await second);
        }

        [Test, CancelAfter(60_000)]
        public async Task ReleaseRaceSoak()
        {
            // Stress the race between a parked bump's backoff re-scan and a releasing session across
            // many iterations. Every bump must complete; a regression that failed to observe the
            // release would stall the loop until the fixture timeout rather than pass silently.
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
                var releaseTask = Task.Run(() => source.SetEpoch(idx, 0));

                ClassicAssert.IsTrue(await epoch.BumpAndWaitForEpochTransitionAsync(),
                    $"bump stalled on iteration {i}");
                await releaseTask;
            }
        }
    }
}