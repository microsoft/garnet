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
        /// The same strict one-at-a-time handoff as <see cref="AsyncWaiterIsWokenForEveryEnqueue"/>, but over
        /// the work-item park instead of the <c>ValueTask</c> one. That park has no task at all - the producer
        /// resumes the consumer by queueing it to the thread pool directly - so it reaches the gate through a
        /// different branch of <c>Signal</c> and needs its own coverage.
        /// </summary>
        [Test]
        public void WorkItemWaiterIsWokenForEveryEnqueue()
        {
            const int Items = 20_000;
            var queue = new AsyncQueue<int>();
            var consumer = new WorkItemDrainLoop(queue, Items, recordOrder: true);

            var producer = Task.Run(() =>
            {
                for (var i = 0; i < Items; i++)
                {
                    // Hand off one at a time so the consumer is parked, not polling, when we enqueue.
                    while (Volatile.Read(ref consumer.Consumed) != i)
                        Thread.SpinWait(1);
                    queue.Enqueue(i);
                }
            });

            consumer.Start();

            ClassicAssert.IsTrue(Task.WhenAll(consumer.Completion, producer).Wait(TimeoutMs),
                $"A wakeup was lost: the consumer drained {Volatile.Read(ref consumer.Consumed)} of {Items} items before the queue stopped waking it.");

            ClassicAssert.AreEqual(Items, consumer.Seen.Count);
            for (var i = 0; i < Items; i++)
                ClassicAssert.AreEqual(i, consumer.Seen[i], "Items were not drained in enqueue order.");
        }

        /// <summary>
        /// Runs the producer flat out against a work-item consumer, so enqueues land across the whole
        /// arm/recheck window rather than only while the consumer is parked. Covers the case the strict
        /// handoff cannot reach: the producer sampled the waiter count before the arm was published, so the
        /// waiter has to notice the entry itself and signal itself awake.
        /// </summary>
        [Test]
        public void WorkItemWaiterDoesNotMissAnEntryEnqueuedWhileItArms()
        {
            const int Items = 200_000;
            var queue = new AsyncQueue<int>();
            var consumer = new WorkItemDrainLoop(queue, Items, recordOrder: false);

            consumer.Start();

            var producer = Task.Run(() =>
            {
                for (var i = 0; i < Items; i++)
                    queue.Enqueue(i);
            });

            ClassicAssert.IsTrue(Task.WhenAll(consumer.Completion, producer).Wait(TimeoutMs),
                $"A wakeup was lost: the consumer drained {Volatile.Read(ref consumer.Consumed)} of {Items} items before the queue stopped waking it.");
            ClassicAssert.AreEqual(Items, Volatile.Read(ref consumer.Consumed));
        }

        /// <summary>
        /// A consumer that parks on <see cref="AsyncQueue{T}.TryWaitForEntry"/> and is resumed by being queued
        /// to the thread pool, which is the shape pending-completion uses to park without allocating a task.
        /// </summary>
        sealed class WorkItemDrainLoop : IThreadPoolWorkItem
        {
            readonly AsyncQueue<int> queue;
            readonly int target;
            readonly TaskCompletionSource completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

            internal readonly List<int> Seen;
            internal int Consumed;

            internal WorkItemDrainLoop(AsyncQueue<int> queue, int target, bool recordOrder)
            {
                this.queue = queue;
                this.target = target;
                Seen = recordOrder ? new List<int>(target) : null;
            }

            internal Task Completion => completion.Task;

            internal void Start() => ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);

            public void Execute()
            {
                try
                {
                    while (Volatile.Read(ref Consumed) < target)
                    {
                        while (queue.TryDequeue(out var item))
                        {
                            Seen?.Add(item);
                            _ = Interlocked.Increment(ref Consumed);
                        }

                        // Returns false when an entry arrived while arming, in which case drain again rather
                        // than wait. Returning true means the gate holds this work item and will queue it.
                        if (Volatile.Read(ref Consumed) < target && queue.TryWaitForEntry(this))
                            return;
                    }

                    _ = completion.TrySetResult();
                }
                catch (Exception ex)
                {
                    _ = completion.TrySetException(ex);
                }
            }
        }

        /// <summary>
        /// One gate serves both park shapes over the life of a queue - pending completion parks a work item,
        /// while the ready-to-complete path parks a <c>ValueTask</c> - so arming for one must not leave the
        /// other's registration behind. A gate that kept a spent work item would satisfy the following
        /// <c>ValueTask</c> wait by queueing that work item a second time instead of completing the task,
        /// stranding its waiter.
        /// </summary>
        [Test]
        public void GateServesAValueTaskWaitAfterAWorkItemWait()
        {
            var queue = new AsyncQueue<int>();
            var resumed = new ManualResetEventSlim(false);
            var workItem = new SignalOnExecute(resumed);

            ClassicAssert.IsTrue(queue.TryWaitForEntry(workItem), "The queue was empty, so this had to park.");
            queue.Enqueue(1);
            ClassicAssert.IsTrue(resumed.Wait(TimeoutMs), "The work item park was never resumed.");
            ClassicAssert.IsTrue(queue.TryDequeue(out _));

            // Same gate, now taking the ValueTask shape.
            var wait = queue.WaitForEntryAsync();
            ClassicAssert.IsFalse(wait.IsCompleted, "The queue was empty, so this had to park.");
            queue.Enqueue(2);
            ClassicAssert.IsTrue(wait.AsTask().Wait(TimeoutMs),
                "The ValueTask wait was never completed: the gate still held the previous park's work item.");
            ClassicAssert.IsTrue(queue.TryDequeue(out _));
        }

        sealed class SignalOnExecute(ManualResetEventSlim resumed) : IThreadPoolWorkItem
        {
            public void Execute() => resumed.Set();
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