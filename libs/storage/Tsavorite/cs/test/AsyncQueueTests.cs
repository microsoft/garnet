// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// Covers the wakeup protocol of <see cref="AsyncQueue{T}"/>. Its uncancellable async waiter is
    /// served by a reusable single-waiter completion source rather than by a fresh semaphore waiter,
    /// so the arm/signal handshake has to be exercised directly: a lost wakeup parks the owning
    /// session forever, which is why these tests fail by timing out rather than by asserting.
    /// </summary>
    [TestFixture]
    public class AsyncQueueTests
    {
        const int TimeoutMs = 120_000;

        /// <summary>
        /// Forces the consumer to park on every single item. The producer refuses to enqueue item
        /// <c>i+1</c> until item <c>i</c> has been drained, so the gate is armed, signalled and reset
        /// once per iteration and the test spends all its time in the window where a producer decides
        /// whether a waiter is present. Anything that lets an arm go unobserved, or that lets a stale
        /// signal satisfy the following round, strands the consumer and trips the timeout.
        /// </summary>
        [Test]
        public void AsyncWaiterIsWokenForEveryEnqueue()
        {
            const int Items = 20_000;
            var queue = new AsyncQueue<int>();
            var consumed = 0;
            var seen = new List<int>(Items);

            var consumer = Task.Run(async () =>
            {
                while (seen.Count < Items)
                {
                    await queue.WaitForEntryAsync().ConfigureAwait(false);
                    while (queue.TryDequeue(out var item))
                    {
                        seen.Add(item);
                        _ = Interlocked.Increment(ref consumed);
                    }
                }
            });

            var producer = Task.Run(() =>
            {
                for (var i = 0; i < Items; i++)
                {
                    // Hand off one at a time so the consumer is parked, not polling, when we enqueue.
                    while (Volatile.Read(ref consumed) != i)
                        Thread.SpinWait(1);
                    queue.Enqueue(i);
                }
            });

            ClassicAssert.IsTrue(Task.WhenAll(consumer, producer).Wait(TimeoutMs),
                $"A wakeup was lost: the consumer drained {Volatile.Read(ref consumed)} of {Items} items before the queue stopped waking it.");

            ClassicAssert.AreEqual(Items, seen.Count);
            for (var i = 0; i < Items; i++)
                ClassicAssert.AreEqual(i, seen[i], "Items were not drained in enqueue order.");
        }

        /// <summary>
        /// Runs the producer flat out against a parking consumer so that enqueues land all over the
        /// arm/recheck window rather than only while the consumer is already parked. This is the case
        /// where the waiter has to notice the entry itself, because the producer sampled the waiter
        /// count before the arm was published.
        /// </summary>
        [Test]
        public void AsyncWaiterDoesNotMissAnEntryEnqueuedWhileItArms()
        {
            const int Items = 200_000;
            var queue = new AsyncQueue<int>();
            var consumed = 0;

            var consumer = Task.Run(async () =>
            {
                while (consumed < Items)
                {
                    await queue.WaitForEntryAsync().ConfigureAwait(false);
                    while (queue.TryDequeue(out _))
                        consumed++;
                }
            });

            var producer = Task.Run(() =>
            {
                for (var i = 0; i < Items; i++)
                    queue.Enqueue(i);
            });

            ClassicAssert.IsTrue(Task.WhenAll(consumer, producer).Wait(TimeoutMs),
                $"A wakeup was lost: the consumer drained {consumed} of {Items} items before the queue stopped waking it.");
            ClassicAssert.AreEqual(Items, consumed);
        }

        /// <summary>
        /// A semaphore waiter and a gate waiter are tracked by separate counters, so a producer has to
        /// signal both. Enqueuing a single entry must release both of them.
        /// </summary>
        [Test]
        public void EnqueueWakesBothASyncAndAnAsyncWaiter()
        {
            var queue = new AsyncQueue<int>();
            using var syncWaiterEntered = new ManualResetEventSlim(false);

            var syncWaiter = Task.Run(() =>
            {
                syncWaiterEntered.Set();
                queue.WaitForEntry();
            });

            var asyncWaiter = Task.Run(async () => await queue.WaitForEntryAsync().ConfigureAwait(false));

            // Let both waiters get as far as parking before the single entry arrives.
            syncWaiterEntered.Wait();
            Thread.Sleep(250);
            queue.Enqueue(1);

            ClassicAssert.IsTrue(Task.WhenAll(syncWaiter, asyncWaiter).Wait(TimeoutMs),
                "A single Enqueue did not release both the sync and the async waiter.");
        }

        /// <summary>
        /// A cancellable token routes to the semaphore, which is what observes cancellation; the gate
        /// path is reserved for the uncancellable park. Both the already-cancelled and the
        /// cancelled-while-waiting cases must still surface cancellation. The semaphore reports the
        /// two with different derived types, so match on the common base.
        /// </summary>
        [Test]
        public void CancellableWaitStillObservesCancellation()
        {
            var queue = new AsyncQueue<int>();

            using var cancelledLater = new CancellationTokenSource();
            var pending = queue.WaitForEntryAsync(cancelledLater.Token);
            ClassicAssert.IsFalse(pending.IsCompleted, "The wait should not complete while the queue is empty.");
            cancelledLater.Cancel();
            _ = Assert.CatchAsync<OperationCanceledException>(async () => await pending.ConfigureAwait(false));

            using var cancelledAlready = new CancellationTokenSource();
            cancelledAlready.Cancel();
            _ = Assert.CatchAsync<OperationCanceledException>(async () => await queue.WaitForEntryAsync(cancelledAlready.Token).ConfigureAwait(false));
        }

        /// <summary>
        /// The gate is reused across rounds, so the completion source is reset once per wait. Awaiting
        /// a wait that is already satisfied must still hand back a correctly versioned result rather
        /// than the previous round's.
        /// </summary>
        [Test]
        public async Task SatisfiedWaitCompletesWithoutParking()
        {
            var queue = new AsyncQueue<int>();

            for (var i = 0; i < 1000; i++)
            {
                queue.Enqueue(i);
                var wait = queue.WaitForEntryAsync();
                ClassicAssert.IsTrue(wait.IsCompleted, "A wait on a non-empty queue should complete synchronously.");
                await wait.ConfigureAwait(false);
                ClassicAssert.IsTrue(queue.TryDequeue(out var item));
                ClassicAssert.AreEqual(i, item);
            }
        }
    }
}