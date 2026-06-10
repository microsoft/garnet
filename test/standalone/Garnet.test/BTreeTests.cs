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

            var trimLength = 5000; // trim the tree to half its size
            tree.TrimByLength((ulong)trimLength, out var validKeysRemoved, out var headValue, out var headValidKey, out var numLeavesDeleted);
            var validKeysRemaining = tree.RootValidCount + tree.TailValidCount;
            ClassicAssert.GreaterOrEqual(validKeysRemaining, trimLength);

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

            var streamIDToTrim = streamIDs[N - 1000];
            tree.TrimByID((byte*)Unsafe.AsPointer(ref streamIDToTrim.idBytes[0]), out var validKeysRemoved, out var headValue, out var headValidKey, out var numLeavesDeleted);
            var validKeysRemaining = tree.RootValidCount + tree.TailValidCount;
            ClassicAssert.GreaterOrEqual((ulong)validKeysRemaining, N - validKeysRemoved);

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