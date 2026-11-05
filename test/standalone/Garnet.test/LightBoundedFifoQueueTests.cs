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
    public class LightBoundedFifoQueueTests : TestBase
    {
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
        public void EmptyAndFullOperationsReturnFalse()
        {
            using var queue = new LightBoundedFifoQueue<string>(pageSize: 1, pageCount: 1, maxSpinCount: 0);

            ClassicAssert.IsFalse(queue.TryPeek(out _));
            ClassicAssert.IsFalse(queue.TryDequeue(out _));

            ClassicAssert.IsTrue(queue.TryEnqueue("value"));
            ClassicAssert.IsFalse(queue.TryEnqueue("full"));
        }

        [Test]
        public void QueuePreservesOrderAcrossWrapAndReportsCapacity()
        {
            using var queue = new LightBoundedFifoQueue<string>(pageSize: 2, pageCount: 2, maxSpinCount: 10);

            ClassicAssert.IsTrue(queue.TryEnqueue("a"));
            ClassicAssert.IsTrue(queue.TryEnqueue("b"));
            ClassicAssert.IsTrue(queue.TryEnqueue("c"));
            ClassicAssert.IsTrue(queue.TryEnqueue("d"));
            ClassicAssert.IsFalse(queue.TryEnqueue("full"));
            ClassicAssert.AreEqual(4, queue.Count);

            ClassicAssert.IsTrue(queue.TryPeek(out var firstItem));
            ClassicAssert.AreEqual("a", firstItem);
            ClassicAssert.IsTrue(queue.TryDequeue(out firstItem));
            ClassicAssert.AreEqual("a", firstItem);
            ClassicAssert.IsTrue(queue.TryEnqueue("e"));

            foreach (var expected in new[] { "b", "c", "d", "e" })
            {
                ClassicAssert.IsTrue(queue.TryPeek(out var item));
                ClassicAssert.AreEqual(expected, item);
                ClassicAssert.IsTrue(queue.TryDequeue(out item));
                ClassicAssert.AreEqual(expected, item);
            }

            ClassicAssert.AreEqual(0, queue.Count);
            ClassicAssert.IsFalse(queue.TryPeek(out _));
        }

        [Test]
        public void ConcurrentEnqueueDoesNotExceedCapacity()
        {
            const int capacity = 64;
            using var queue = new LightBoundedFifoQueue<string>(pageSize: 8, pageCount: 8, maxSpinCount: 10);
            var accepted = 0;

            Parallel.For(0, capacity * 8, i =>
            {
                if (!queue.TryEnqueue(i.ToString()))
                    return;

                Interlocked.Increment(ref accepted);
            });

            ClassicAssert.AreEqual(capacity, accepted);
            ClassicAssert.AreEqual(capacity, queue.Count);
            ClassicAssert.IsFalse(queue.TryEnqueue("full"));
        }

        [Test]
        public void CompleteAddingRetainsPublishedItems()
        {
            using var queue = new LightBoundedFifoQueue<string>(pageSize: 1, pageCount: 2, maxSpinCount: 10);
            ClassicAssert.IsTrue(queue.TryEnqueue("value"));

            queue.CompleteAdding();

            Assert.Throws<ObjectDisposedException>(() => queue.TryEnqueue("rejected"));
            ClassicAssert.IsTrue(queue.TryDequeue(out var item));
            ClassicAssert.AreEqual("value", item);
        }
    }
}