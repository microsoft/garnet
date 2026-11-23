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
    public class MemoryThrottleTests : TestBase
    {
        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        [Test]
        public async Task WaiterQueueUsesTrackerForAsyncAdmission()
        {
            var throttle = new MemoryThrottle(64);
            using var queue = new WaiterQueue<MemoryThrottle, int>(throttle, spinCount: 0);
            ClassicAssert.IsTrue(queue.Admit(64));

            var waiter = queue.AdmitAsync(1).AsTask();
            ClassicAssert.IsFalse(waiter.IsCompleted);

            queue.Release(64);
            ClassicAssert.IsTrue(await waiter.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
            ClassicAssert.AreEqual(1, throttle.InUseBytes);
            ClassicAssert.AreEqual(64, throttle.PeakInUseBytes);

            queue.Release(1);
            ClassicAssert.AreEqual(0, throttle.InUseBytes);
        }

        [Test]
        public void RequestLargerThanCapacityFailsValidation()
        {
            var throttle = new MemoryThrottle(64);
            using var queue = new WaiterQueue<MemoryThrottle, int>(throttle);

            var exception = Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await queue.AdmitAsync(65).ConfigureAwait(false));
            StringAssert.Contains("65 bytes exceeds the configured maximum of 64 bytes", exception.Message);
            ClassicAssert.AreEqual(0, throttle.InUseBytes);
        }

        [Test]
        public void TryReserveRejectsRequestLargerThanCapacity()
        {
            var throttle = new MemoryThrottle(64);
            var resourceThrottle = (IResourceThrottle<int>)throttle;

            var exception = Assert.Throws<InvalidOperationException>(() => resourceThrottle.TryReserve(65));
            StringAssert.Contains("65 bytes exceeds the configured maximum of 64 bytes", exception.Message);
            ClassicAssert.AreEqual(0, throttle.InUseBytes);
        }

        [Test]
        public void ReleasingMoreThanReservedFailsWithoutCorruptingAccounting()
        {
            var throttle = new MemoryThrottle(64);
            using var queue = new WaiterQueue<MemoryThrottle, int>(throttle);
            ClassicAssert.IsTrue(queue.Admit(32));

            Assert.Throws<InvalidOperationException>(() => queue.Release(33));
            ClassicAssert.AreEqual(32, throttle.InUseBytes);
            queue.Release(32);
        }

        [Test]
        public void CancellationDoesNotConsumeCapacity()
        {
            var throttle = new MemoryThrottle(64);
            using var queue = new WaiterQueue<MemoryThrottle, int>(throttle, spinCount: 0);
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
            ClassicAssert.IsTrue(queue.Admit(64));

            Assert.ThrowsAsync<OperationCanceledException>(async () =>
                await queue.AdmitAsync(1, cts.Token).ConfigureAwait(false));

            ClassicAssert.AreEqual(64, throttle.InUseBytes);
            queue.Release(64);
        }

        [Test]
        public void DisposingQueueWakesBlockedAdmission()
        {
            var throttle = new MemoryThrottle(64);
            var queue = new WaiterQueue<MemoryThrottle, int>(throttle, spinCount: 0);
            ClassicAssert.IsTrue(queue.Admit(64));
            var waiter = queue.AdmitAsync(1).AsTask();

            queue.Dispose();

            Assert.ThrowsAsync<ObjectDisposedException>(async () => await waiter.ConfigureAwait(false));
            queue.Release(64);
            ClassicAssert.AreEqual(0, throttle.InUseBytes);
        }

        [Test]
        public async Task NewArrivalWaitsBehindQueuedHead()
        {
            var throttle = new MemoryThrottle(4);
            using var queue = new WaiterQueue<MemoryThrottle, int>(throttle, spinCount: 0);
            ClassicAssert.IsTrue(queue.Admit(4));

            var first = queue.AdmitAsync(4).AsTask();
            queue.Release(1);
            var second = queue.AdmitAsync(1).AsTask();

            ClassicAssert.AreEqual(2, queue.WaiterCount);
            ClassicAssert.IsFalse(first.IsCompleted);
            ClassicAssert.IsFalse(second.IsCompleted);

            queue.Release(3);
            ClassicAssert.IsTrue(await first.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
            ClassicAssert.IsFalse(second.IsCompleted);

            queue.Release(4);
            ClassicAssert.IsTrue(await second.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
            queue.Release(1);
            ClassicAssert.AreEqual(0, throttle.InUseBytes);
        }

        [Test]
        public async Task CancelingHeadAllowsNextRequestToAdvance()
        {
            var throttle = new MemoryThrottle(4);
            using var queue = new WaiterQueue<MemoryThrottle, int>(throttle, spinCount: 0);
            using var cts = new CancellationTokenSource();
            ClassicAssert.IsTrue(queue.Admit(4));

            var first = queue.AdmitAsync(4, cts.Token).AsTask();
            var second = queue.AdmitAsync(1).AsTask();
            cts.Cancel();

            Assert.ThrowsAsync<OperationCanceledException>(async () => await first.ConfigureAwait(false));
            ClassicAssert.AreEqual(1, queue.WaiterCount);

            queue.Release(1);
            ClassicAssert.IsTrue(await second.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
            queue.Release(4);
        }

        [Test]
        public async Task CancelingHeadOfFullQueueReclaimsWrappedSlot()
        {
            var throttle = new MemoryThrottle(1);
            using var queue = new WaiterQueue<MemoryThrottle, int>(
                throttle,
                ringPageSize: 2,
                ringPageCount: 1,
                spinCount: 0);
            using var cts = new CancellationTokenSource();
            ClassicAssert.IsTrue(queue.Admit(1));

            var canceled = queue.AdmitAsync(1, cts.Token).AsTask();
            var second = queue.AdmitAsync(1).AsTask();
            ClassicAssert.IsFalse(await queue.AdmitAsync(1).ConfigureAwait(false));

            cts.Cancel();
            Assert.ThrowsAsync<OperationCanceledException>(async () => await canceled.ConfigureAwait(false));
            ClassicAssert.AreEqual(1, throttle.InUseBytes);

            var wrapped = queue.AdmitAsync(1).AsTask();
            ClassicAssert.IsFalse(wrapped.IsCompleted);

            queue.Release(1);
            ClassicAssert.IsTrue(await second.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
            ClassicAssert.IsFalse(wrapped.IsCompleted);

            queue.Release(1);
            ClassicAssert.IsTrue(await wrapped.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
            queue.Release(1);

            ClassicAssert.AreEqual(0, throttle.InUseBytes);
        }

        [Test]
        public async Task FullWaiterQueueReturnsFalse()
        {
            var throttle = new MemoryThrottle(1);
            using var queue = new WaiterQueue<MemoryThrottle, int>(
                throttle,
                ringPageSize: 1,
                ringPageCount: 1,
                spinCount: 0);
            ClassicAssert.IsTrue(queue.Admit(1));
            var queued = queue.AdmitAsync(1);

            ClassicAssert.IsFalse(queue.Admit(1));
            queue.Release(1);
            ClassicAssert.IsTrue(await queued.ConfigureAwait(false));
            queue.Release(1);
        }

        [Test]
        public async Task ConcurrentAdmissionsDoNotExceedCapacity()
        {
            const int capacity = 256;
            var throttle = new MemoryThrottle(capacity);
            using var queue = new WaiterQueue<MemoryThrottle, int>(throttle);

            var workers = new Task[8];
            for (var worker = 0; worker < workers.Length; worker++)
            {
                workers[worker] = Task.Run(() =>
                {
                    for (var i = 0; i < 2_000; i++)
                    {
                        if (!queue.Admit(64))
                            throw new InvalidOperationException("Waiter queue capacity was exhausted.");
                        queue.Release(64);
                    }
                });
            }

            await Task.WhenAll(workers).ConfigureAwait(false);
            ClassicAssert.LessOrEqual(throttle.PeakInUseBytes, capacity);
            ClassicAssert.AreEqual(0, throttle.InUseBytes);
        }
    }
}