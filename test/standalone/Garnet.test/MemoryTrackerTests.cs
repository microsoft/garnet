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
    public class MemoryTrackerTests : TestBase
    {
        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        [Test]
        public async Task WaiterQueueUsesTrackerForAsyncAdmission()
        {
            var tracker = new MemoryTracker(64);
            using var queue = new WaiterQueue<int>(tracker, spinCount: 0);
            ClassicAssert.IsTrue(queue.Admit(64));

            var waiter = queue.AdmitAsync(1).AsTask();
            ClassicAssert.IsFalse(waiter.IsCompleted);

            queue.Release(64);
            ClassicAssert.IsTrue(await waiter.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
            ClassicAssert.AreEqual(1, tracker.InUseBytes);
            ClassicAssert.AreEqual(64, tracker.PeakInUseBytes);

            queue.Release(1);
            ClassicAssert.AreEqual(0, tracker.InUseBytes);
        }

        [Test]
        public void RequestLargerThanCapacityFailsValidation()
        {
            var tracker = new MemoryTracker(64);
            using var queue = new WaiterQueue<int>(tracker);

            var exception = Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await queue.AdmitAsync(65).ConfigureAwait(false));
            StringAssert.Contains("65 bytes exceeds the configured maximum of 64 bytes", exception.Message);
            ClassicAssert.AreEqual(0, tracker.InUseBytes);
        }

        [Test]
        public void ReleasingMoreThanReservedFailsWithoutCorruptingAccounting()
        {
            var tracker = new MemoryTracker(64);
            using var queue = new WaiterQueue<int>(tracker);
            ClassicAssert.IsTrue(queue.Admit(32));

            Assert.Throws<InvalidOperationException>(() => queue.Release(33));
            ClassicAssert.AreEqual(32, tracker.InUseBytes);
            queue.Release(32);
        }

        [Test]
        public void CancellationDoesNotConsumeCapacity()
        {
            var tracker = new MemoryTracker(64);
            using var queue = new WaiterQueue<int>(tracker, spinCount: 0);
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
            ClassicAssert.IsTrue(queue.Admit(64));

            Assert.ThrowsAsync<OperationCanceledException>(async () =>
                await queue.AdmitAsync(1, cts.Token).ConfigureAwait(false));

            ClassicAssert.AreEqual(64, tracker.InUseBytes);
            queue.Release(64);
        }

        [Test]
        public void DisposingQueueWakesBlockedAdmission()
        {
            var tracker = new MemoryTracker(64);
            var queue = new WaiterQueue<int>(tracker, spinCount: 0);
            ClassicAssert.IsTrue(queue.Admit(64));
            var waiter = queue.AdmitAsync(1).AsTask();

            queue.Dispose();

            Assert.ThrowsAsync<ObjectDisposedException>(async () => await waiter.ConfigureAwait(false));
            queue.Release(64);
            ClassicAssert.AreEqual(0, tracker.InUseBytes);
        }

        [Test]
        public async Task NewArrivalCanReserveWithoutInspectingBacklog()
        {
            var tracker = new MemoryTracker(4);
            using var queue = new WaiterQueue<int>(tracker, spinCount: 0);
            ClassicAssert.IsTrue(queue.Admit(4));

            var first = queue.AdmitAsync(4).AsTask();
            queue.Release(1);
            var second = queue.AdmitAsync(1).AsTask();

            ClassicAssert.AreEqual(1, queue.WaiterCount);
            ClassicAssert.IsFalse(first.IsCompleted);
            ClassicAssert.IsTrue(await second.ConfigureAwait(false));

            queue.Release(4);
            ClassicAssert.IsTrue(await first.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
            queue.Release(4);
            ClassicAssert.AreEqual(0, tracker.InUseBytes);
        }

        [Test]
        public async Task CancelingHeadAllowsNextRequestToAdvance()
        {
            var tracker = new MemoryTracker(4);
            using var queue = new WaiterQueue<int>(tracker, spinCount: 0);
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
        public async Task FullWaiterQueueReturnsFalse()
        {
            var tracker = new MemoryTracker(1);
            using var queue = new WaiterQueue<int>(
                tracker,
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
            var tracker = new MemoryTracker(capacity);
            using var queue = new WaiterQueue<int>(tracker);

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
            ClassicAssert.LessOrEqual(tracker.PeakInUseBytes, capacity);
            ClassicAssert.AreEqual(0, tracker.InUseBytes);
        }
    }
}