// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [TestFixture]
    public class LightFifoQueueTests : TestBase
    {
        sealed class PooledItem : IDisposable
        {
            internal bool IsDisposed { get; private set; }

            public void Dispose() => IsDisposed = true;
        }

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        [Test]
        public void QueuePreservesOrderAcrossWrapAndReportsCapacity()
        {
            using var queue = new LightFifoQueue<string>(pageSize: 2, pageCount: 2);

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
        public void RemovedEntryBecomesOrderedTombstone()
        {
            using var queue = new LightFifoQueue<string>(pageSize: 2, pageCount: 2);

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
            using var queue = new LightFifoQueue<string>(pageSize: 1, pageCount: 2);
            ClassicAssert.IsTrue(queue.TryEnqueue("value", out var handle));

            queue.CompleteAdding();

            Assert.Throws<ObjectDisposedException>(() => queue.TryEnqueue("rejected", out _));
            ClassicAssert.IsTrue(queue.TryDequeue(handle, out var item));
            ClassicAssert.AreEqual("value", item);
        }

        [Test]
        public void QueueOwnsItemReuseAndDisposal()
        {
            var queue = new LightFifoQueue<PooledItem>(
                pageSize: 1,
                pageCount: 1,
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