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

        readonly struct ResourceThrottle : IResourceThrottle<ResourceRequest>
        {
            sealed class ThrottleState(int capacity)
            {
                internal readonly int capacity = capacity;
                internal int blocked;
                internal int inUse;
                internal int peakInUse;
                internal int reservationAttempts;
                internal int pauseNextReservation;
                internal int failPausedReservation;
                internal ManualResetEventSlim reservationPaused;
                internal ManualResetEventSlim resumeReservation;
            }

            readonly ThrottleState state;

            internal ResourceThrottle(int capacity)
            {
                state = new ThrottleState(capacity);
            }

            internal bool Blocked
            {
                get => Volatile.Read(ref state.blocked) != 0;
                set => Volatile.Write(ref state.blocked, value ? 1 : 0);
            }

            internal int InUse => Volatile.Read(ref state.inUse);
            internal int PeakInUse => Volatile.Read(ref state.peakInUse);
            internal int ReservationAttempts => Volatile.Read(ref state.reservationAttempts);

            internal void PauseNextReservation(ManualResetEventSlim paused, ManualResetEventSlim resume, bool failAfterPause = true)
            {
                state.reservationPaused = paused;
                state.resumeReservation = resume;
                Volatile.Write(ref state.failPausedReservation, failAfterPause ? 1 : 0);
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
                    if (Volatile.Read(ref state.failPausedReservation) != 0)
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
            var throttle = new ResourceThrottle(1);
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                _ = new WaiterQueue<ResourceThrottle, ResourceRequest>(throttle, maxEnqueueSpinCount: -1));
        }

        [Test]
        public async Task NewArrivalWaitsBehindQueuedHead()
        {
            var throttle = new ResourceThrottle(4);
            using var queue = new WaiterQueue<ResourceThrottle, ResourceRequest>(throttle, spinCount: 0);
            var four = new ResourceRequest(4);
            var three = new ResourceRequest(3);
            var one = new ResourceRequest(1);
            queue.Admit(four);

            var first = queue.AdmitAsync(four).AsTask();
            queue.Release(one);
            var second = queue.AdmitAsync(one).AsTask();

            ClassicAssert.AreEqual(2, queue.WaiterCount);
            ClassicAssert.IsFalse(first.IsCompleted);
            ClassicAssert.IsFalse(second.IsCompleted);

            queue.Release(three);
            await first.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            ClassicAssert.IsFalse(second.IsCompleted);

            queue.Release(four);
            await second.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            queue.Release(one);
            ClassicAssert.AreEqual(0, throttle.InUse);
            ClassicAssert.AreEqual(4, throttle.PeakInUse);
        }

        [Test]
        public async Task FastReservationRacingWithBacklogIsRolledBack()
        {
            var throttle = new ResourceThrottle(1);
            using var queue = new WaiterQueue<ResourceThrottle, ResourceRequest>(throttle, spinCount: 0);
            using var reservationPaused = new ManualResetEventSlim();
            using var resumeReservation = new ManualResetEventSlim();
            var request = new ResourceRequest(1);

            throttle.PauseNextReservation(reservationPaused, resumeReservation, failAfterPause: false);
            var first = Task.Run(() => queue.Admit(request));
            ClassicAssert.IsTrue(reservationPaused.Wait(TimeSpan.FromSeconds(5)), "The fast reservation did not pause.");

            throttle.Blocked = true;
            var second = queue.AdmitAsync(request).AsTask();
            ClassicAssert.IsTrue(
                SpinWait.SpinUntil(() => queue.WaiterCount == 1, TimeSpan.FromSeconds(5)),
                "The second request did not enter the waiter queue.");

            throttle.Blocked = false;
            resumeReservation.Set();

            ClassicAssert.IsTrue(await second.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
            ClassicAssert.IsFalse(first.IsCompleted);

            queue.Release(request);
            ClassicAssert.IsTrue(await first.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
            queue.Release(request);
            ClassicAssert.AreEqual(0, throttle.InUse);
        }

        [Test]
        public async Task ExternalDrainAdmitsNewlyAvailableResource()
        {
            var throttle = new ResourceThrottle(1) { Blocked = true };
            using var queue = new WaiterQueue<ResourceThrottle, ResourceRequest>(throttle, spinCount: 0);
            var request = new ResourceRequest(1);

            var waiter = queue.AdmitAsync(request).AsTask();
            ClassicAssert.IsFalse(waiter.IsCompleted);

            throttle.Blocked = false;
            queue.Drain();
            await waiter.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

            queue.Release(request);
            ClassicAssert.AreEqual(0, throttle.InUse);
        }

        [Test]
        public async Task RetriesBeforeAsyncWait()
        {
            const int spinCount = 4;
            var throttle = new ResourceThrottle(1) { Blocked = true };
            using var queue = new WaiterQueue<ResourceThrottle, ResourceRequest>(throttle, spinCount: spinCount);
            var request = new ResourceRequest(1);

            var waiter = queue.AdmitAsync(request).AsTask();

            ClassicAssert.AreEqual(spinCount + 2, throttle.ReservationAttempts);
            ClassicAssert.AreEqual(1, queue.WaiterCount);
            ClassicAssert.IsFalse(waiter.IsCompleted);

            throttle.Blocked = false;
            queue.Drain();
            await waiter.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            queue.Release(request);
        }

        [Test]
        public async Task ConcurrentDrainRequestsDoNotStrandWaiters()
        {
            const int waiterCount = 160;
            var throttle = new ResourceThrottle(1) { Blocked = true };
            using var queue = new WaiterQueue<ResourceThrottle, ResourceRequest>(throttle, ringPageCount: 8, spinCount: 0);
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

            throttle.Blocked = false;
            Parallel.For(0, 16, _ => queue.Drain());
            var allWaiters = Task.WhenAll(waiters);
            var completed = await Task.WhenAny(allWaiters, Task.Delay(TimeSpan.FromSeconds(5))).ConfigureAwait(false);
            ClassicAssert.AreSame(allWaiters, completed,
                $"Waiters were stranded: queued={queue.WaiterCount}, inUse={throttle.InUse}, attempts={throttle.ReservationAttempts}.");
            await allWaiters.ConfigureAwait(false);

            ClassicAssert.AreEqual(0, queue.WaiterCount);
            ClassicAssert.AreEqual(0, throttle.InUse);
            ClassicAssert.AreEqual(1, throttle.PeakInUse);
        }

        [Test]
        public async Task ConcurrentDrainAfterFailedReservationIsNotLost()
        {
            var throttle = new ResourceThrottle(1) { Blocked = true };
            using var queue = new WaiterQueue<ResourceThrottle, ResourceRequest>(throttle, spinCount: 0);
            using var reservationPaused = new ManualResetEventSlim();
            using var resumeReservation = new ManualResetEventSlim();
            var request = new ResourceRequest(1);
            var waiter = queue.AdmitAsync(request).AsTask();
            throttle.PauseNextReservation(reservationPaused, resumeReservation);

            var firstDrain = Task.Run(queue.Drain);
            ClassicAssert.IsTrue(reservationPaused.Wait(TimeSpan.FromSeconds(5)), "The first drain did not reach reservation.");

            throttle.Blocked = false;
            queue.Drain();
            resumeReservation.Set();

            await firstDrain.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            ClassicAssert.IsTrue(await waiter.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
            queue.Release(request);
        }

        [Test]
        public async Task FullWaiterLogReturnsFalse()
        {
            var throttle = new ResourceThrottle(1) { Blocked = true };
            using var queue = new WaiterQueue<ResourceThrottle, ResourceRequest>(throttle, ringPageSize: 1, ringPageCount: 1, spinCount: 0);
            var request = new ResourceRequest(1);

            var first = queue.AdmitAsync(request).AsTask();

            ClassicAssert.AreEqual(1, queue.WaiterCount);
            ClassicAssert.IsFalse(queue.Admit(request));
            ClassicAssert.IsFalse(await queue.AdmitAsync(request).ConfigureAwait(false));

            throttle.Blocked = false;
            queue.Drain();
            ClassicAssert.IsTrue(await first.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
            queue.Release(request);
        }

        [Test]
        public async Task CancelingHeadRequestsAnotherDrain()
        {
            var throttle = new ResourceThrottle(2);
            using var queue = new WaiterQueue<ResourceThrottle, ResourceRequest>(throttle, spinCount: 0);
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
            ClassicAssert.AreEqual(0, throttle.InUse);
        }

        [Test]
        public async Task CancelingMiddlePreservesSurvivorOrder()
        {
            var throttle = new ResourceThrottle(1) { Blocked = true };
            using var queue = new WaiterQueue<ResourceThrottle, ResourceRequest>(throttle, spinCount: 0);
            using var cts = new CancellationTokenSource();
            var request = new ResourceRequest(1);

            var first = queue.AdmitAsync(request).AsTask();
            var canceled = queue.AdmitAsync(request, cts.Token).AsTask();
            var third = queue.AdmitAsync(request).AsTask();

            cts.Cancel();
            Assert.ThrowsAsync<OperationCanceledException>(async () => await canceled.ConfigureAwait(false));

            throttle.Blocked = false;
            queue.Drain();
            ClassicAssert.IsTrue(await first.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
            ClassicAssert.IsFalse(third.IsCompleted);

            queue.Release(request);
            ClassicAssert.IsTrue(await third.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
            queue.Release(request);

            ClassicAssert.AreEqual(0, queue.WaiterCount);
            ClassicAssert.AreEqual(0, throttle.InUse);
        }

        [Test]
        public void DisposeWakesParkedWaiters()
        {
            var throttle = new ResourceThrottle(1);
            var queue = new WaiterQueue<ResourceThrottle, ResourceRequest>(throttle, spinCount: 0);
            var request = new ResourceRequest(1);
            queue.Admit(request);
            var waiter = queue.AdmitAsync(request).AsTask();

            queue.Dispose();

            Assert.ThrowsAsync<ObjectDisposedException>(async () => await waiter.ConfigureAwait(false));
            queue.Release(request);
            ClassicAssert.AreEqual(0, throttle.InUse);
        }
    }
}