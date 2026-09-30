// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [TestFixture]
    public class LightBoundedFifoQueueTests : TestBase
    {
        sealed class PooledItem : IDisposable
        {
            internal bool IsDisposed { get; private set; }

            public void Dispose() => IsDisposed = true;
        }

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        [Test]
        public void RingBufferIndexerMapsLogicalAddressesAcrossWrap()
        {
            var buffer = new RingBoundedBuffer<long>(pageSize: 2, pageCount: 2);

            for (var address = 0L; address < buffer.Capacity; address++)
                buffer[address] = address;

            for (var address = 0L; address < buffer.Capacity; address++)
                ClassicAssert.AreEqual(address, buffer[address]);

            buffer[buffer.Capacity] = 4;
            ClassicAssert.AreEqual(4, buffer[0]);
        }

        [Test]
        public void QueueRejectsNegativeSpinLimit()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                _ = new LightBoundedFifoQueue<string>(pageSize: 1, pageCount: 1, maxSpinCount: -1));
        }

        [Test]
        public void FailedOperationsReturnInvalidHandle()
        {
            using var queue = new LightBoundedFifoQueue<string>(pageSize: 1, pageCount: 1, maxSpinCount: 0);

            ClassicAssert.IsFalse(queue.TryPeek(out var emptyHandle, out _));
            ClassicAssert.IsFalse(emptyHandle.IsValid);
            ClassicAssert.AreEqual(-1, emptyHandle.Address);

            ClassicAssert.IsTrue(queue.TryEnqueue("value", out _));
            ClassicAssert.IsFalse(queue.TryEnqueue("full", out var fullHandle));
            ClassicAssert.IsFalse(fullHandle.IsValid);
            ClassicAssert.AreEqual(-1, fullHandle.Address);
        }

        [Test]
        public void QueuePreservesOrderAcrossWrapAndReportsCapacity()
        {
            using var queue = new LightBoundedFifoQueue<string>(pageSize: 2, pageCount: 2, maxSpinCount: 10);

            ClassicAssert.IsTrue(queue.TryEnqueue("a", out _));
            ClassicAssert.IsTrue(queue.TryEnqueue("b", out _));
            ClassicAssert.IsTrue(queue.TryEnqueue("c", out _));
            ClassicAssert.IsTrue(queue.TryEnqueue("d", out _));
            ClassicAssert.IsFalse(queue.TryEnqueue("full", out _));
            ClassicAssert.AreEqual(4, queue.Count);

            ClassicAssert.IsTrue(queue.TryPeek(out var first, out var firstItem));
            ClassicAssert.AreEqual("a", firstItem);
            ClassicAssert.IsTrue(queue.TryDequeue(first, out firstItem));
            ClassicAssert.AreEqual("a", firstItem);
            ClassicAssert.IsTrue(queue.TryEnqueue("e", out _));

            foreach (var expected in new[] { "b", "c", "d", "e" })
            {
                ClassicAssert.IsTrue(queue.TryPeek(out var handle, out var item));
                ClassicAssert.AreEqual(expected, item);
                ClassicAssert.IsTrue(queue.TryDequeue(handle, out item));
                ClassicAssert.AreEqual(expected, item);
            }

            ClassicAssert.AreEqual(0, queue.Count);
            ClassicAssert.IsFalse(queue.TryPeek(out _, out _));
        }

        [Test]
        public void ConcurrentEnqueueReservesEachBoundedAddressOnce()
        {
            const int capacity = 64;
            using var queue = new LightBoundedFifoQueue<string>(pageSize: 8, pageCount: 8, maxSpinCount: 10);
            var addresses = new ConcurrentDictionary<long, byte>();
            var accepted = 0;

            Parallel.For(0, capacity * 8, i =>
            {
                if (!queue.TryEnqueue(i.ToString(), out var handle))
                    return;

                ClassicAssert.IsTrue(addresses.TryAdd(handle.Address, 0));
                Interlocked.Increment(ref accepted);
            });

            ClassicAssert.AreEqual(capacity, accepted);
            ClassicAssert.AreEqual(capacity, addresses.Count);
            ClassicAssert.AreEqual(capacity, queue.Count);
            ClassicAssert.IsFalse(queue.TryEnqueue("full", out _));
        }

        [Test]
        public void RemovedEntryBecomesOrderedTombstone()
        {
            using var queue = new LightBoundedFifoQueue<string>(pageSize: 2, pageCount: 2, maxSpinCount: 10);

            ClassicAssert.IsTrue(queue.TryEnqueue("a", out var first));
            ClassicAssert.IsTrue(queue.TryEnqueue("b", out var removed));
            ClassicAssert.IsTrue(queue.TryEnqueue("c", out var third));

            ClassicAssert.IsTrue(queue.TryRemove(removed, out var removedItem));
            ClassicAssert.AreEqual("b", removedItem);
            ClassicAssert.IsFalse(queue.TryRemove(removed, out _));
            ClassicAssert.AreEqual(2, queue.Count);

            ClassicAssert.IsTrue(queue.TryDequeue(first, out var firstItem));
            ClassicAssert.AreEqual("a", firstItem);
            ClassicAssert.IsTrue(queue.TryPeek(out var next, out var nextItem));
            ClassicAssert.AreEqual(third.Address, next.Address);
            ClassicAssert.AreEqual("c", nextItem);
        }

        [Test]
        public void CompleteAddingRetainsPublishedItems()
        {
            using var queue = new LightBoundedFifoQueue<string>(pageSize: 1, pageCount: 2, maxSpinCount: 10);
            ClassicAssert.IsTrue(queue.TryEnqueue("value", out var handle));

            queue.CompleteAdding();

            Assert.Throws<ObjectDisposedException>(() => queue.TryEnqueue("rejected", out _));
            ClassicAssert.IsTrue(queue.TryDequeue(handle, out var item));
            ClassicAssert.AreEqual("value", item);
        }

        [Test]
        public void QueueOwnsItemReuseAndDisposal()
        {
            var queue = new LightBoundedFifoQueue<PooledItem>(
                pageSize: 1,
                pageCount: 1,
                maxSpinCount: 10,
                itemFactory: static () => new PooledItem(),
                itemDisposer: static item => item.Dispose(),
                maxPooledItems: 1);

            var first = queue.Rent();
            queue.Return(first);
            var reused = queue.Rent();
            ClassicAssert.AreSame(first, reused);
            queue.Return(reused);

            queue.Dispose();
            ClassicAssert.IsTrue(reused.IsDisposed);
        }
    }
}