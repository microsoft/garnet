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
        static ulong N = 50000;

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
                tree.Insert((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0]), new Value(i + 1));
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
                tree.Insert((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0]), new Value(streamIDs[i].ms));
            }

            for (ulong i = 0; i < N; i++)
            {
                ClassicAssert.AreEqual(tree.Get((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0])).address, streamIDs[i].ms);
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
                tree.Insert((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0]), new Value(streamIDs[i].ms));
            }

            int count = tree.Get((byte*)Unsafe.AsPointer(ref streamIDs[N - 200].idBytes[0]), (byte*)Unsafe.AsPointer(ref streamIDs[N - 1].idBytes[0]), out Value startVal, out Value endVal, out List<Value> list);
            ClassicAssert.AreEqual(count, N - 1 - (N - 200) + 1);
            ClassicAssert.AreEqual(list.Count, 0);
            ClassicAssert.AreEqual(startVal.address, streamIDs[N - 200].ms);
            ClassicAssert.AreEqual(endVal.address, streamIDs[N - 1].ms);

            tree.Deallocate();
        }

        [Test]
        [Category("Delete")]
        public void Delete()
        {
            var tree = new BTree((uint)BTreeNode.PAGE_SIZE);
            for (ulong i = 0; i < N; i++)
            {
                tree.Insert((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0]), new Value(streamIDs[i].ms));
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
                tree.Insert((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0]), new Value(streamIDs[i].ms));
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
                tree.Insert((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0]), new Value(streamIDs[i].ms));
            }

            var streamIDToTrim = streamIDs[N - 1000];
            tree.TrimByID((byte*)Unsafe.AsPointer(ref streamIDToTrim.idBytes[0]), out var validKeysRemoved, out var headValue, out var headValidKey, out var numLeavesDeleted);
            var validKeysRemaining = tree.RootValidCount + tree.TailValidCount;
            ClassicAssert.GreaterOrEqual((ulong)validKeysRemaining, N - validKeysRemoved);

            tree.Deallocate();
        }
    }
}
