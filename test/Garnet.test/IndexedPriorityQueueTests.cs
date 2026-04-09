// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common.Collections;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [TestFixture]
    public class IndexedPriorityQueueTests
    {
        #region Basic Operations

        [Test]
        public void EmptyQueueHasCountZero()
        {
            var q = new IndexedPriorityQueue<string, int>();
            ClassicAssert.AreEqual(0, q.Count);
        }

        [Test]
        public void EnqueueIncreasesCount()
        {
            var q = new IndexedPriorityQueue<string, int>();
            q.EnqueueOrUpdate("a", 10);
            ClassicAssert.AreEqual(1, q.Count);
            q.EnqueueOrUpdate("b", 20);
            ClassicAssert.AreEqual(2, q.Count);
        }

        [Test]
        public void DequeueReturnsMinPriority()
        {
            var q = new IndexedPriorityQueue<string, int>();
            q.EnqueueOrUpdate("high", 100);
            q.EnqueueOrUpdate("low", 1);
            q.EnqueueOrUpdate("mid", 50);

            ClassicAssert.AreEqual("low", q.Dequeue());
            ClassicAssert.AreEqual("mid", q.Dequeue());
            ClassicAssert.AreEqual("high", q.Dequeue());
        }

        [Test]
        public void DequeueOnEmptyThrows()
        {
            var q = new IndexedPriorityQueue<string, int>();
            Assert.Throws<InvalidOperationException>(() => q.Dequeue());
        }

        [Test]
        public void TryPeekReturnsFalseWhenEmpty()
        {
            var q = new IndexedPriorityQueue<string, int>();
            ClassicAssert.IsFalse(q.TryPeek(out _, out _));
        }

        [Test]
        public void TryPeekReturnsMinWithoutRemoving()
        {
            var q = new IndexedPriorityQueue<string, int>();
            q.EnqueueOrUpdate("a", 5);
            q.EnqueueOrUpdate("b", 3);

            ClassicAssert.IsTrue(q.TryPeek(out var key, out var priority));
            ClassicAssert.AreEqual("b", key);
            ClassicAssert.AreEqual(3, priority);
            ClassicAssert.AreEqual(2, q.Count);
        }

        [Test]
        public void ExistsReturnsTrueForEnqueuedElement()
        {
            var q = new IndexedPriorityQueue<string, int>();
            q.EnqueueOrUpdate("a", 10);

            ClassicAssert.IsTrue(q.Exists("a"));
            ClassicAssert.IsFalse(q.Exists("b"));
        }

        #endregion

        #region In-Place Update

        [Test]
        public void EnqueueOrUpdateUpdatesExistingElement()
        {
            var q = new IndexedPriorityQueue<string, int>();
            q.EnqueueOrUpdate("a", 100);
            q.EnqueueOrUpdate("b", 50);

            // Update "a" to lower priority — should become the new min
            q.EnqueueOrUpdate("a", 1);

            ClassicAssert.AreEqual(2, q.Count, "Update should not add a new entry");
            ClassicAssert.IsTrue(q.TryPeek(out var key, out var priority));
            ClassicAssert.AreEqual("a", key);
            ClassicAssert.AreEqual(1, priority);
        }

        [Test]
        public void ChangePriorityMovesElementDown()
        {
            var q = new IndexedPriorityQueue<string, int>();
            q.EnqueueOrUpdate("a", 1);
            q.EnqueueOrUpdate("b", 10);
            q.EnqueueOrUpdate("c", 20);

            // Move "a" to highest priority value — "b" should become min
            q.ChangePriority("a", 100);

            q.TryPeek(out var key, out _);
            ClassicAssert.AreEqual("b", key);
        }

        [Test]
        public void ChangePriorityMovesElementUp()
        {
            var q = new IndexedPriorityQueue<string, int>();
            q.EnqueueOrUpdate("a", 50);
            q.EnqueueOrUpdate("b", 10);
            q.EnqueueOrUpdate("c", 30);

            // Move "a" to lowest — should become min
            q.ChangePriority("a", 1);

            q.TryPeek(out var key, out _);
            ClassicAssert.AreEqual("a", key);
        }

        [Test]
        public void RepeatedUpdatesDoNotBloatCount()
        {
            var q = new IndexedPriorityQueue<string, int>();
            q.EnqueueOrUpdate("a", 10);

            for (int i = 0; i < 100; i++)
            {
                q.EnqueueOrUpdate("a", i);
            }

            ClassicAssert.AreEqual(1, q.Count, "Repeated updates should not increase count");
        }

        #endregion

        #region Priority Lookup

        [Test]
        public void GetPriorityReturnsCurrentPriority()
        {
            var q = new IndexedPriorityQueue<string, int>();
            q.EnqueueOrUpdate("a", 42);

            q.GetPriority("a", out var priority);
            ClassicAssert.AreEqual(42, priority);
        }

        [Test]
        public void TryGetPriorityReturnsFalseForMissing()
        {
            var q = new IndexedPriorityQueue<string, int>();
            ClassicAssert.IsFalse(q.TryGetPriority("missing", out _));
        }

        [Test]
        public void TryGetPriorityReflectsUpdates()
        {
            var q = new IndexedPriorityQueue<string, int>();
            q.EnqueueOrUpdate("a", 10);
            q.EnqueueOrUpdate("a", 99);

            ClassicAssert.IsTrue(q.TryGetPriority("a", out var priority));
            ClassicAssert.AreEqual(99, priority);
        }

        #endregion

        #region Removal

        [Test]
        public void TryRemoveReturnsFalseForMissing()
        {
            var q = new IndexedPriorityQueue<string, int>();
            ClassicAssert.IsFalse(q.TryRemove("missing"));
        }

        [Test]
        public void TryRemoveRemovesElement()
        {
            var q = new IndexedPriorityQueue<string, int>();
            q.EnqueueOrUpdate("a", 10);
            q.EnqueueOrUpdate("b", 20);

            ClassicAssert.IsTrue(q.TryRemove("a"));
            ClassicAssert.AreEqual(1, q.Count);
            ClassicAssert.IsFalse(q.Exists("a"));
            ClassicAssert.AreEqual("b", q.Dequeue());
        }

        [Test]
        public void TryRemoveMiddleElementMaintainsHeapOrder()
        {
            var q = new IndexedPriorityQueue<string, int>();
            q.EnqueueOrUpdate("a", 1);
            q.EnqueueOrUpdate("b", 5);
            q.EnqueueOrUpdate("c", 3);
            q.EnqueueOrUpdate("d", 10);
            q.EnqueueOrUpdate("e", 7);

            q.TryRemove("c");

            // Remaining should dequeue in order: a(1), b(5), e(7), d(10)
            ClassicAssert.AreEqual("a", q.Dequeue());
            ClassicAssert.AreEqual("b", q.Dequeue());
            ClassicAssert.AreEqual("e", q.Dequeue());
            ClassicAssert.AreEqual("d", q.Dequeue());
        }

        [Test]
        public void TryRemoveLastElement()
        {
            var q = new IndexedPriorityQueue<string, int>();
            q.EnqueueOrUpdate("only", 1);

            ClassicAssert.IsTrue(q.TryRemove("only"));
            ClassicAssert.AreEqual(0, q.Count);
            ClassicAssert.IsFalse(q.TryPeek(out _, out _));
        }

        #endregion

        #region Custom Comparer (byte[] keys)

        [Test]
        public void ByteArrayComparerMatchesByContent()
        {
            var comparer = new ByteArrayComparer();
            var q = new IndexedPriorityQueue<byte[], long>(comparer);

            var key1 = new byte[] { 1, 2, 3 };
            var key1Copy = new byte[] { 1, 2, 3 };
            var key2 = new byte[] { 4, 5, 6 };

            q.EnqueueOrUpdate(key1, 100);
            q.EnqueueOrUpdate(key2, 200);

            // Update using a different byte[] with same content
            q.EnqueueOrUpdate(key1Copy, 50);

            ClassicAssert.AreEqual(2, q.Count, "Should match by content, not reference");

            q.TryPeek(out var minKey, out var minPriority);
            ClassicAssert.AreEqual(50, minPriority);
            ClassicAssert.IsTrue(comparer.Equals(key1, minKey));
        }

        [Test]
        public void ByteArrayComparerTryGetPriorityByContent()
        {
            var comparer = new ByteArrayComparer();
            var q = new IndexedPriorityQueue<byte[], long>(comparer);

            var key = new byte[] { 10, 20 };
            var keyCopy = new byte[] { 10, 20 };

            q.EnqueueOrUpdate(key, 42);

            ClassicAssert.IsTrue(q.TryGetPriority(keyCopy, out var priority));
            ClassicAssert.AreEqual(42, priority);
        }

        [Test]
        public void ByteArrayComparerTryRemoveByContent()
        {
            var comparer = new ByteArrayComparer();
            var q = new IndexedPriorityQueue<byte[], long>(comparer);

            var key = new byte[] { 7, 8, 9 };
            var keyCopy = new byte[] { 7, 8, 9 };

            q.EnqueueOrUpdate(key, 100);
            ClassicAssert.IsTrue(q.TryRemove(keyCopy));
            ClassicAssert.AreEqual(0, q.Count);
        }

        [Test]
        public void WithoutComparerByteArrayUsesReferenceEquality()
        {
            var q = new IndexedPriorityQueue<byte[], long>();

            var key1 = new byte[] { 1, 2, 3 };
            var key1Copy = new byte[] { 1, 2, 3 };

            q.EnqueueOrUpdate(key1, 100);
            q.EnqueueOrUpdate(key1Copy, 50);

            // Without comparer, these are different keys
            ClassicAssert.AreEqual(2, q.Count, "Default comparer uses reference equality for byte[]");
        }

        #endregion

        #region Stress / Ordering

        [Test]
        public void LargeInsertDequeueMaintainsOrder()
        {
            var q = new IndexedPriorityQueue<int, int>();
            var rng = new Random(42);
            var count = 1000;

            for (int i = 0; i < count; i++)
            {
                q.EnqueueOrUpdate(i, rng.Next(0, 100000));
            }

            ClassicAssert.AreEqual(count, q.Count);

            int prev = int.MinValue;
            while (q.Count > 0)
            {
                q.TryPeek(out _, out var priority);
                ClassicAssert.GreaterOrEqual(priority, prev, "Dequeue order should be non-decreasing");
                prev = priority;
                q.Dequeue();
            }
        }

        [Test]
        public void InterleavedInsertUpdateRemoveDequeue()
        {
            var q = new IndexedPriorityQueue<string, int>();

            q.EnqueueOrUpdate("a", 50);
            q.EnqueueOrUpdate("b", 30);
            q.EnqueueOrUpdate("c", 70);
            q.EnqueueOrUpdate("d", 10);

            // Update
            q.EnqueueOrUpdate("c", 5);  // c becomes min
            q.TryPeek(out var min, out _);
            ClassicAssert.AreEqual("c", min);

            // Remove min
            q.TryRemove("c");
            q.TryPeek(out min, out _);
            ClassicAssert.AreEqual("d", min);

            // Add new
            q.EnqueueOrUpdate("e", 1);
            q.TryPeek(out min, out _);
            ClassicAssert.AreEqual("e", min);

            // Drain and verify order
            ClassicAssert.AreEqual("e", q.Dequeue());  // 1
            ClassicAssert.AreEqual("d", q.Dequeue());  // 10
            ClassicAssert.AreEqual("b", q.Dequeue());  // 30
            ClassicAssert.AreEqual("a", q.Dequeue());  // 50
            ClassicAssert.AreEqual(0, q.Count);
        }

        [Test]
        public void GrowAndShrinkBehavior()
        {
            var q = new IndexedPriorityQueue<int, int>();

            // Grow
            for (int i = 0; i < 100; i++)
                q.EnqueueOrUpdate(i, i);

            ClassicAssert.AreEqual(100, q.Count);

            // Shrink by removing most
            for (int i = 0; i < 90; i++)
                q.Dequeue();

            ClassicAssert.AreEqual(10, q.Count);

            // Remaining should still be ordered
            int prev = int.MinValue;
            while (q.Count > 0)
            {
                q.TryPeek(out _, out var p);
                ClassicAssert.GreaterOrEqual(p, prev);
                prev = p;
                q.Dequeue();
            }
        }

        #endregion

        /// <summary>
        /// Simple byte[] equality comparer for tests.
        /// </summary>
        private class ByteArrayComparer : IEqualityComparer<byte[]>
        {
            public bool Equals(byte[] x, byte[] y)
            {
                if (x == null && y == null) return true;
                if (x == null || y == null) return false;
                if (x.Length != y.Length) return false;
                for (int i = 0; i < x.Length; i++)
                    if (x[i] != y[i]) return false;
                return true;
            }

            public int GetHashCode(byte[] obj)
            {
                if (obj == null) return 0;
                int hash = 17;
                foreach (var b in obj)
                    hash = hash * 31 + b;
                return hash;
            }
        }
    }
}