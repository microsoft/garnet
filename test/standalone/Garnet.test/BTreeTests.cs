// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Garnet.server;
using Garnet.server.BTreeIndex;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    using Value = Value;

    [TestFixture]
    public unsafe class BTreeTests
    {
        static StreamID[] streamIDs;
        static ulong N = 100_000;

        [SetUp]
        public void Setup()
        {
            streamIDs = new StreamID[N];
            for (ulong i = 0; i < N; i++)
            {
                streamIDs[i] = new StreamID(i + 1, 0);
            }
        }

        [TearDown]
        public void TearDown()
        { }

        [Test]
        [Category("INIT")]
        public void InitBTreeLeafNode()
        {
            // var memoryBlock = (IntPtr*)Marshal.AllocHGlobal(BTreeNode.PAGE_SIZE).ToPointer();
            var memoryBlock = (IntPtr*)NativeMemory.AlignedAlloc((nuint)BTreeNode.PAGE_SIZE, (nuint)BTreeNode.PAGE_SIZE);
            var leaf = BTreeNode.Create(BTreeNodeType.Leaf, memoryBlock);
            ClassicAssert.AreEqual(leaf->info->type, BTreeNodeType.Leaf);
            ClassicAssert.AreEqual(leaf->info->count, 0);

            // free the leaf
            BTree.FreeNode(ref leaf);

            leaf = null;
        }

        [Test]
        [Category("INSERT")]
        public void Insert()
        {
            var tree = new BTree((uint)BTreeNode.PAGE_SIZE);
            ClassicAssert.AreEqual(tree.FastInserts, 0);
            ClassicAssert.AreEqual(tree.LeafCount, 1);
            ClassicAssert.AreEqual(tree.InternalCount, 0);

            for (ulong i = 0; i < N; i++)
            {
                tree.InsertIntoTail((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0]), new Value((long)(i + 1)));
            }
            ClassicAssert.AreEqual(tree.FastInserts, N);
            tree.Deallocate();
        }

        [Test]
        [Category("LOOKUP")]
        public void PointLookup()
        {
            var tree = new BTree((uint)BTreeNode.PAGE_SIZE);

            for (ulong i = 0; i < N; i++)
            {
                tree.InsertIntoTail((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0]), new Value((long)streamIDs[i].getMS()));
            }

            for (ulong i = 0; i < N; i++)
            {
                ClassicAssert.AreEqual(tree.Get((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0])).address, streamIDs[i].getMS());
            }

            tree.Deallocate();
        }

        [Test]
        [Category("LOOKUP")]
        public void RangeLookup()
        {
            var tree = new BTree(4096);

            for (ulong i = 0; i < N; i++)
            {
                tree.InsertIntoTail((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0]), new Value((long)streamIDs[i].getMS()));
            }

            int count = tree.Get((byte*)Unsafe.AsPointer(ref streamIDs[N - 200].idBytes[0]), (byte*)Unsafe.AsPointer(ref streamIDs[N - 1].idBytes[0]), out Value startVal, out Value endVal, out List<Value> list);
            ClassicAssert.AreEqual(count, N - 1 - (N - 200) + 1);
            ClassicAssert.AreEqual(list.Count, 0);
            ClassicAssert.AreEqual(startVal.address, streamIDs[N - 200].getMS());
            ClassicAssert.AreEqual(endVal.address, streamIDs[N - 1].getMS());

            tree.Deallocate();
        }

        [Test]
        [Category("Delete")]
        public void Delete()
        {
            var tree = new BTree((uint)BTreeNode.PAGE_SIZE);
            for (ulong i = 0; i < N; i++)
            {
                tree.InsertIntoTail((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0]), new Value((long)streamIDs[i].getMS()));
            }

            // delete 10% of keys at random 
            Random rand = new Random();
            uint delCount = 0;
            for (ulong i = 0; i < N / 10; i++)
            {
                ulong idx = (ulong)rand.Next(0, (int)N);
                bool deleted = tree.Delete((byte*)Unsafe.AsPointer(ref streamIDs[idx].idBytes[0]));
                if (deleted)
                {
                    delCount++;
                }
            }
            ClassicAssert.AreEqual(tree.ValidCount, N - delCount);
            tree.Deallocate();
        }

        [Test]
        [Category("Trim")]
        public void TrimByLength()
        {
            var tree = new BTree((uint)BTreeNode.PAGE_SIZE);
            for (ulong i = 0; i < N; i++)
            {
                tree.InsertIntoTail((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0]), new Value((long)streamIDs[i].getMS()));
            }

            var trimLength = 5000UL; // keep only the newest 5000 entries
            var leavesBefore = tree.LeafCount;
            tree.TrimByLength(trimLength, out var validKeysRemoved, out var headValue, out var headValidKey, out var numLeavesDeleted);

            // Exact post-trim live count is the source of truth (stats.numValidKeys).
            ClassicAssert.AreEqual(N - trimLength, validKeysRemoved);
            ClassicAssert.AreEqual(trimLength, tree.ValidCount);

            // Whole dead head leaves must have been physically reclaimed.
            ClassicAssert.Greater(numLeavesDeleted, 0u);
            ClassicAssert.Less(tree.LeafCount, leavesBefore);

            // New head is the smallest surviving entry: streamIDs[N - trimLength].
            ClassicAssert.IsTrue(headValue.Valid);
            ClassicAssert.AreEqual((long)streamIDs[N - trimLength].getMS(), headValue.address);

            // The index must remain structurally consistent and safe to free.
            tree.ValidateStructure();
            tree.Deallocate();
        }

        [Test]
        [Category("TrimByID")]
        public void TrimByID()
        {
            var tree = new BTree((uint)BTreeNode.PAGE_SIZE);
            for (ulong i = 0; i < N; i++)
            {
                tree.InsertIntoTail((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0]), new Value((long)streamIDs[i].getMS()));
            }

            var cutoffIdx = N - 1000;
            var streamIDToTrim = streamIDs[cutoffIdx]; // tombstone everything strictly below this id
            var leavesBefore = tree.LeafCount;
            tree.TrimByID((byte*)Unsafe.AsPointer(ref streamIDToTrim.idBytes[0]), out var validKeysRemoved, out var headValue, out var headValidKey, out var numLeavesDeleted);

            ClassicAssert.AreEqual(cutoffIdx, validKeysRemoved);
            ClassicAssert.AreEqual(N - cutoffIdx, tree.ValidCount);

            ClassicAssert.Greater(numLeavesDeleted, 0u);
            ClassicAssert.Less(tree.LeafCount, leavesBefore);

            // New head is the cutoff id itself (smallest id >= cutoff).
            ClassicAssert.IsTrue(headValue.Valid);
            ClassicAssert.AreEqual((long)streamIDToTrim.getMS(), headValue.address);

            tree.ValidateStructure();
            tree.Deallocate();
        }

        /// <summary>
        /// Trimming only part of the first leaf must not free any whole leaf (the boundary
        /// leaf keeps live entries and stays as head with its dead prefix tombstoned).
        /// </summary>
        [Test]
        [Category("Trim")]
        public void TrimWithinLeafReclaimsNoLeaves()
        {
            var tree = new BTree((uint)BTreeNode.PAGE_SIZE);
            int small = BTreeNode.LEAF_CAPACITY / 2; // single leaf, half full
            for (int i = 0; i < small; i++)
            {
                tree.InsertIntoTail((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0]), new Value((long)streamIDs[i].getMS()));
            }

            ClassicAssert.AreEqual(1UL, tree.LeafCount);
            tree.TrimByLength((ulong)(small - 3), out var removed, out var headValue, out _, out var numLeavesDeleted);

            ClassicAssert.AreEqual(3UL, removed);
            ClassicAssert.AreEqual((ulong)(small - 3), tree.ValidCount);
            ClassicAssert.AreEqual(0u, numLeavesDeleted);  // nothing whole died
            ClassicAssert.AreEqual(1UL, tree.LeafCount);
            ClassicAssert.AreEqual((long)streamIDs[3].getMS(), headValue.address);

            tree.ValidateStructure();
            tree.Deallocate();
        }

        /// <summary>
        /// Trimming a multi-level tree down to a few tail entries must reclaim the whole left
        /// side, collapse the root, shrink depth, and leave a structurally-valid tree that is
        /// safe to Deallocate (the prior structural-trim crash class).
        /// </summary>
        [Test]
        [Category("Trim")]
        public void TrimReclaimCollapsesRootAndShrinksDepth()
        {
            var tree = new BTree((uint)BTreeNode.PAGE_SIZE);
            for (ulong i = 0; i < N; i++)
            {
                tree.InsertIntoTail((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0]), new Value((long)streamIDs[i].getMS()));
            }
            var internalsBefore = tree.InternalCount;
            ClassicAssert.Greater(internalsBefore, 0UL, "test requires a multi-level tree");

            // Keep only the newest 2 entries — forces deep reclamation + root collapse.
            tree.TrimByLength(2UL, out _, out var headValue, out _, out var numLeavesDeleted);

            ClassicAssert.AreEqual(2UL, tree.ValidCount);
            ClassicAssert.Greater(numLeavesDeleted, 0u);
            ClassicAssert.Less(tree.InternalCount, internalsBefore);
            ClassicAssert.AreEqual((long)streamIDs[N - 2].getMS(), headValue.address);

            tree.ValidateStructure();
            tree.Deallocate();
        }

        /// <summary>
        /// Trimming everything collapses the tree back to a single tail leaf at depth 1 with no
        /// internal nodes, and is safe to Deallocate.
        /// </summary>
        [Test]
        [Category("Trim")]
        public void TrimEverythingCollapsesToTail()
        {
            var tree = new BTree((uint)BTreeNode.PAGE_SIZE);
            for (ulong i = 0; i < N; i++)
            {
                tree.InsertIntoTail((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0]), new Value((long)streamIDs[i].getMS()));
            }

            tree.TrimByLength(0UL, out var removed, out var headValue, out _, out var numLeavesDeleted);

            ClassicAssert.AreEqual(N, removed);
            ClassicAssert.AreEqual(0UL, tree.ValidCount);
            ClassicAssert.IsFalse(headValue.Valid);        // no live head entry remains
            ClassicAssert.AreEqual(1UL, tree.LeafCount);    // only the tail leaf survives
            ClassicAssert.AreEqual(0UL, tree.InternalCount);
            ClassicAssert.Greater(numLeavesDeleted, 0u);

            tree.ValidateStructure();

            // Stream must still accept new inserts after collapsing to the tail.
            tree.InsertIntoTail((byte*)Unsafe.AsPointer(ref streamIDs[0].idBytes[0]), new Value(123));
            ClassicAssert.AreEqual(1UL, tree.ValidCount);
            tree.ValidateStructure();

            tree.Deallocate();
        }

        /// <summary>
        /// Approximate (whole-leaf) trimming must also reclaim the dead leaves it tombstones.
        /// </summary>
        [Test]
        [Category("Trim")]
        public void ApproximateTrimReclaimsWholeLeaves()
        {
            var tree = new BTree((uint)BTreeNode.PAGE_SIZE);
            for (ulong i = 0; i < N; i++)
            {
                tree.InsertIntoTail((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0]), new Value((long)streamIDs[i].getMS()));
            }
            var leavesBefore = tree.LeafCount;

            tree.TrimByLength(5000UL, out _, out _, out _, out var numLeavesDeleted, approximateTrimming: true);

            // Approximate trim keeps at least the requested number of entries.
            ClassicAssert.GreaterOrEqual(tree.ValidCount, 5000UL);
            ClassicAssert.Greater(numLeavesDeleted, 0u);
            ClassicAssert.Less(tree.LeafCount, leavesBefore);

            tree.ValidateStructure();
            tree.Deallocate();
        }

        /// <summary>
        /// Under repeated head-trims interleaved with inserts, index memory must stay bounded
        /// (reclamation keeps freeing the left edge) and the tree must stay valid throughout.
        /// </summary>
        [Test]
        [Category("Trim")]
        public void RepeatedTrimKeepsIndexBounded()
        {
            var tree = new BTree((uint)BTreeNode.PAGE_SIZE);
            ulong nextId = 1;
            ulong window = 2000;

            // Prime the window.
            for (ulong i = 0; i < window; i++)
            {
                var id = new StreamID(nextId, 0);
                tree.InsertIntoTail((byte*)Unsafe.AsPointer(ref id.idBytes[0]), new Value((long)nextId));
                nextId++;
            }

            ulong leafHighWater = tree.LeafCount;
            for (int round = 0; round < 20; round++)
            {
                // Append another window's worth of entries...
                for (ulong i = 0; i < window; i++)
                {
                    var id = new StreamID(nextId, 0);
                    tree.InsertIntoTail((byte*)Unsafe.AsPointer(ref id.idBytes[0]), new Value((long)nextId));
                    nextId++;
                }
                // ...then trim back down to the window size.
                tree.TrimByLength(window, out _, out _, out _, out _);
                ClassicAssert.AreEqual(window, tree.ValidCount);
                tree.ValidateStructure();
                leafHighWater = Math.Max(leafHighWater, tree.LeafCount);
            }

            // Leaf count must not grow without bound across 20 rounds (each adds `window`
            // entries that are then trimmed). A small constant multiple of the window's leaves
            // is the steady state; assert we are far below the un-reclaimed growth.
            ulong leavesForOneWindow = (window / (ulong)BTreeNode.LEAF_CAPACITY) + 2;
            ClassicAssert.LessOrEqual(tree.LeafCount, leavesForOneWindow * 3);
            ClassicAssert.LessOrEqual(leafHighWater, leavesForOneWindow * 3);

            tree.Deallocate();
        }


        /// <summary>
        /// Validates that Range Get returns the same addresses as point Get for 100K entries.
        /// Reproduces the crash where BTree returns corrupted addresses under high entry count.
        /// </summary>
        [Test]
        [Category("LOOKUP")]
        public void RangeLookupAddressIntegrity100K()
        {
            // First, print sizeof(Value) to check alignment
            int valueSize = sizeof(Value);
            TestContext.Out.WriteLine($"sizeof(Value) = {valueSize}");
            TestContext.Out.WriteLine($"LEAF_CAPACITY = {BTreeNode.LEAF_CAPACITY}");
            ClassicAssert.AreEqual(8, valueSize, "sizeof(Value) should be 8");
        }

        /// <summary>
        /// Tests Range Get with keys that fall on leaf node boundaries.
        /// </summary>
        [Test]
        [Category("LOOKUP")]
        public void RangeLookupCrossLeafBoundary()
        {
            const ulong entryCount = 100_000;
            var ids = new StreamID[entryCount];
            const ulong firstAddr = 64;
            const ulong recordSize = 44;

            var tree = new BTree((uint)BTreeNode.PAGE_SIZE);

            for (ulong i = 0; i < entryCount; i++)
            {
                ids[i] = new StreamID(i + 1, 0);
                ulong addr = firstAddr + i * recordSize;
                tree.InsertIntoTail((byte*)Unsafe.AsPointer(ref ids[i].idBytes[0]), new Value((long)addr));
            }

            int leafCapacity = BTreeNode.LEAF_CAPACITY;

            // Test ranges that cross leaf boundaries
            for (int boundary = leafCapacity - 5; boundary < (int)entryCount - leafCapacity; boundary += leafCapacity)
            {
                ulong startIdx = (ulong)Math.Max(0, boundary - 10);
                ulong endIdx = (ulong)Math.Min((int)entryCount - 1, boundary + 10);

                ulong expectedStartAddr = firstAddr + startIdx * recordSize;
                ulong expectedEndAddr = firstAddr + endIdx * recordSize;

                int count = tree.Get(
                    (byte*)Unsafe.AsPointer(ref ids[startIdx].idBytes[0]),
                    (byte*)Unsafe.AsPointer(ref ids[endIdx].idBytes[0]),
                    out Value startVal, out Value endVal,
                    out List<Value> tombstones);

                ClassicAssert.AreEqual(expectedStartAddr, startVal.address,
                    $"Cross-boundary startVal mismatch at boundary {boundary}: expected {expectedStartAddr}, got {startVal.address}");
                ClassicAssert.AreEqual(expectedEndAddr, endVal.address,
                    $"Cross-boundary endVal mismatch at boundary {boundary}: expected {expectedEndAddr}, got {endVal.address}");
            }

            tree.Deallocate();
        }
    }
}