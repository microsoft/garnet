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
    [TestFixture]
    public class WaiterQueueTests : TestBase
    {
        readonly record struct ResourceRequest(int Units);

        readonly struct ResourceTracker : IResourceTracker<ResourceRequest>
        {
            sealed class TrackerState(int capacity)
            {
                internal readonly int capacity = capacity;
                internal int blocked;
                internal int inUse;
                internal int peakInUse;
                internal int reservationAttempts;
                internal int pauseNextReservation;
                internal ManualResetEventSlim reservationPaused;
                internal ManualResetEventSlim resumeReservation;
            }

            readonly TrackerState state;

            internal ResourceTracker(int capacity)
            {
                state = new TrackerState(capacity);
            }

            internal bool Blocked
            {
                get => Volatile.Read(ref state.blocked) != 0;
                set => Volatile.Write(ref state.blocked, value ? 1 : 0);
            }

            internal int InUse => Volatile.Read(ref state.inUse);
            internal int PeakInUse => Volatile.Read(ref state.peakInUse);
            internal int ReservationAttempts => Volatile.Read(ref state.reservationAttempts);

            internal void PauseNextReservation(ManualResetEventSlim paused, ManualResetEventSlim resume)
            {
                state.reservationPaused = paused;
                state.resumeReservation = resume;
                Volatile.Write(ref state.pauseNextReservation, 1);
            }

            public void Validate(in ResourceRequest requestResource)
            {
                if (requestResource.Units <= 0)
                    throw new ArgumentOutOfRangeException(nameof(requestResource));
                if (requestResource.Units > state.capacity)
                    throw new InvalidOperationException("Request exceeds capacity.");
            }

            public bool TryReserve(in ResourceRequest requestResource)
            {
                Interlocked.Increment(ref state.reservationAttempts);
                if (Interlocked.Exchange(ref state.pauseNextReservation, 0) != 0)
                {
                    state.reservationPaused.Set();
                    state.resumeReservation.Wait();
                    return false;
                }

                while (true)
                {
                    var current = Volatile.Read(ref state.inUse);
                    if (Blocked || current > state.capacity - requestResource.Units)
                        return false;

                    var next = current + requestResource.Units;
                    if (Interlocked.CompareExchange(ref state.inUse, next, current) != current)
                        continue;

                    UpdatePeak(next);
                    return true;
                }
            }

            public void Release(in ResourceRequest requestResource)
            {
                while (true)
                {
                    var current = Volatile.Read(ref state.inUse);
                    if (requestResource.Units > current)
                        throw new InvalidOperationException("Release exceeds active usage.");
                    if (Interlocked.CompareExchange(ref state.inUse, current - requestResource.Units, current) == current)
                        return;
                }
            }

            void UpdatePeak(int value)
            {
                var current = Volatile.Read(ref state.peakInUse);
                while (value > current)
                {
                    var observed = Interlocked.CompareExchange(ref state.peakInUse, value, current);
                    if (observed == current)
                        return;
                    current = observed;
                }
            }
        }

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        [Test]
        public void ConstructorRejectsNegativeEnqueueSpinLimit()
        {
            var tracker = new ResourceTracker(1);
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                _ = new WaiterQueue<ResourceTracker, ResourceRequest>(tracker, maxEnqueueSpinCount: -1));
        }

        [Test]
        public async Task NewArrivalCanReserveWithoutInspectingBacklog()
        {
            var tracker = new ResourceTracker(4);
            using var queue = new WaiterQueue<ResourceTracker, ResourceRequest>(tracker, spinCount: 0);
            var four = new ResourceRequest(4);
            var one = new ResourceRequest(1);
            queue.Admit(four);

            var first = queue.AdmitAsync(four).AsTask();
            queue.Release(one);
            var second = queue.AdmitAsync(one).AsTask();

            ClassicAssert.AreEqual(1, queue.WaiterCount);
            ClassicAssert.IsFalse(first.IsCompleted);
            ClassicAssert.IsTrue(await second.ConfigureAwait(false));

            queue.Release(four);
            await first.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            queue.Release(four);
            ClassicAssert.AreEqual(0, tracker.InUse);
            ClassicAssert.AreEqual(4, tracker.PeakInUse);
        }

        [Test]
        public async Task ExternalDrainAdmitsNewlyAvailableResource()
        {
            var tracker = new ResourceTracker(1) { Blocked = true };
            using var queue = new WaiterQueue<ResourceTracker, ResourceRequest>(tracker, spinCount: 0);
            var request = new ResourceRequest(1);

            var waiter = queue.AdmitAsync(request).AsTask();
            ClassicAssert.IsFalse(waiter.IsCompleted);

            tracker.Blocked = false;
            queue.Drain();
            await waiter.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

            queue.Release(request);
            ClassicAssert.AreEqual(0, tracker.InUse);
        }

        [Test]
        public async Task RetriesBeforeAsyncWait()
        {
            const int spinCount = 4;
            var tracker = new ResourceTracker(1) { Blocked = true };
            using var queue = new WaiterQueue<ResourceTracker, ResourceRequest>(tracker, spinCount: spinCount);
            var request = new ResourceRequest(1);

            var waiter = queue.AdmitAsync(request).AsTask();

            ClassicAssert.AreEqual(spinCount + 2, tracker.ReservationAttempts);
            ClassicAssert.AreEqual(1, queue.WaiterCount);
            ClassicAssert.IsFalse(waiter.IsCompleted);

            tracker.Blocked = false;
            queue.Drain();
            await waiter.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            queue.Release(request);
        }

        [Test]
        public async Task ConcurrentDrainRequestsDoNotStrandWaiters()
        {
            const int waiterCount = 160;
            var tracker = new ResourceTracker(1) { Blocked = true };
            using var queue = new WaiterQueue<ResourceTracker, ResourceRequest>(tracker, ringPageCount: 8, spinCount: 0);
            var request = new ResourceRequest(1);
            var waiters = new Task[waiterCount];

            for (var i = 0; i < waiters.Length; i++)
            {
                waiters[i] = Task.Run(async () =>
                {
                    await queue.AdmitAsync(request).ConfigureAwait(false);
                    queue.Release(request);
                });
            }

            ClassicAssert.IsTrue(
                SpinWait.SpinUntil(() => queue.WaiterCount == waiterCount, TimeSpan.FromSeconds(5)),
                "Not all requests entered the waiter queue.");

            tracker.Blocked = false;
            Parallel.For(0, 16, _ => queue.Drain());
            var allWaiters = Task.WhenAll(waiters);
            var completed = await Task.WhenAny(allWaiters, Task.Delay(TimeSpan.FromSeconds(5))).ConfigureAwait(false);
            ClassicAssert.AreSame(allWaiters, completed,
                $"Waiters were stranded: queued={queue.WaiterCount}, inUse={tracker.InUse}, attempts={tracker.ReservationAttempts}.");
            await allWaiters.ConfigureAwait(false);

            ClassicAssert.AreEqual(0, queue.WaiterCount);
            ClassicAssert.AreEqual(0, tracker.InUse);
            ClassicAssert.AreEqual(1, tracker.PeakInUse);
        }

        [Test]
        public async Task ConcurrentDrainAfterFailedReservationIsNotLost()
        {
            var tracker = new ResourceTracker(1) { Blocked = true };
            using var queue = new WaiterQueue<ResourceTracker, ResourceRequest>(tracker, spinCount: 0);
            using var reservationPaused = new ManualResetEventSlim();
            using var resumeReservation = new ManualResetEventSlim();
            var request = new ResourceRequest(1);
            var waiter = queue.AdmitAsync(request).AsTask();
            tracker.PauseNextReservation(reservationPaused, resumeReservation);

            var firstDrain = Task.Run(queue.Drain);
            ClassicAssert.IsTrue(reservationPaused.Wait(TimeSpan.FromSeconds(5)), "The first drain did not reach reservation.");

            tracker.Blocked = false;
            queue.Drain();
            resumeReservation.Set();

            await firstDrain.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            ClassicAssert.IsTrue(await waiter.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
            queue.Release(request);
        }

        [Test]
        public async Task FullWaiterLogReturnsFalse()
        {
            var tracker = new ResourceTracker(1) { Blocked = true };
            using var queue = new WaiterQueue<ResourceTracker, ResourceRequest>(tracker, ringPageSize: 1, ringPageCount: 1, spinCount: 0);
            var request = new ResourceRequest(1);

            var first = queue.AdmitAsync(request).AsTask();

            ClassicAssert.AreEqual(1, queue.WaiterCount);
            ClassicAssert.IsFalse(queue.Admit(request));
            ClassicAssert.IsFalse(await queue.AdmitAsync(request).ConfigureAwait(false));

            tracker.Blocked = false;
            queue.Drain();
            ClassicAssert.IsTrue(await first.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
            queue.Release(request);
        }

        [Test]
        public async Task CancelingHeadRequestsAnotherDrain()
        {
            var tracker = new ResourceTracker(2);
            using var queue = new WaiterQueue<ResourceTracker, ResourceRequest>(tracker, spinCount: 0);
            using var cts = new CancellationTokenSource();
            var two = new ResourceRequest(2);
            var one = new ResourceRequest(1);
            queue.Admit(two);

            var first = queue.AdmitAsync(two, cts.Token).AsTask();
            var second = queue.AdmitAsync(one).AsTask();
            queue.Release(one);
            cts.Cancel();

            Assert.ThrowsAsync<OperationCanceledException>(async () => await first.ConfigureAwait(false));
            await second.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            queue.Release(two);
            ClassicAssert.AreEqual(0, tracker.InUse);
        }

        [Test]
        public async Task CancelingMiddlePreservesSurvivorOrder()
        {
            var tracker = new ResourceTracker(1) { Blocked = true };
            using var queue = new WaiterQueue<ResourceTracker, ResourceRequest>(tracker, spinCount: 0);
            using var cts = new CancellationTokenSource();
            var request = new ResourceRequest(1);

            var first = queue.AdmitAsync(request).AsTask();
            var canceled = queue.AdmitAsync(request, cts.Token).AsTask();
            var third = queue.AdmitAsync(request).AsTask();

            cts.Cancel();
            Assert.ThrowsAsync<OperationCanceledException>(async () => await canceled.ConfigureAwait(false));

            tracker.Blocked = false;
            queue.Drain();
            ClassicAssert.IsTrue(await first.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
            ClassicAssert.IsFalse(third.IsCompleted);

            queue.Release(request);
            ClassicAssert.IsTrue(await third.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
            queue.Release(request);

            ClassicAssert.AreEqual(0, queue.WaiterCount);
            ClassicAssert.AreEqual(0, tracker.InUse);
        }

        [Test]
        public void DisposeWakesParkedWaiters()
        {
            var tracker = new ResourceTracker(1);
            var queue = new WaiterQueue<ResourceTracker, ResourceRequest>(tracker, spinCount: 0);
            var request = new ResourceRequest(1);
            queue.Admit(request);
            var waiter = queue.AdmitAsync(request).AsTask();

            queue.Dispose();

            Assert.ThrowsAsync<ObjectDisposedException>(async () => await waiter.ConfigureAwait(false));
            queue.Release(request);
            ClassicAssert.AreEqual(0, tracker.InUse);
        }
    }
}