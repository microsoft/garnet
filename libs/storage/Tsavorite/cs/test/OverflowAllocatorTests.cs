// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;
using System;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;
using System.Collections.Concurrent;
using static Tsavorite.core.OverflowAllocator;
using System.Collections.Generic;
using System.Threading;

namespace Tsavorite.test
{
    /// <summary>
    /// This tests <see cref="FixedSizePages"/> and <see cref="OversizePages"/>, which in turn tests some aspects of <see cref="MultiLevelPageArray{_int_}"/>.
    /// </summary>
    [TestFixture]
    class OverflowAllocatorTests
    {
        OverflowAllocator allocator;

        public enum PageSize { Default, Small }

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir);

            int pageSizeBits = LogSettings.kDefaultOverflowFixedPageSizeBits;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is PageSize pageSizeArg)
                {
                    if (pageSizeArg == PageSize.Small)
                        pageSizeBits -= 6;
                    continue;
                }
            }

            allocator = new(1 << pageSizeBits);
        }

        [TearDown]
        public void TearDown()
        {
            DeleteDirectory(MethodTestDir);
            allocator.Clear();
        }

        [Test]
        [Category(ObjectIdMapCategory), Category(MultiLevelPageArrayCategory), Category(SmokeTestCategory)]
        public unsafe void FixedSizePagesBasicTest([Values(1, 8)] int numThreads, [Values] PageSize pageSize)
        {
            Assert.That(allocator.fixedSizePages.PageAllocator.IsInitialized, Is.False);

            // Allocate enough to fill past the first MultiLevelPageArray.InitialBookSize chapters.
            const int allocationsPerThread = 10_000;
            const int maxAllocationSize = FixedSizePages.MaxExternalBlockSize;

            ConcurrentQueue<IntPtr> pointers = new();

            void runLoadThread(int tid)
            {
                Random rng = new(tid);
                for (var ii = 0; ii < allocationsPerThread; ++ii)
                {
                    // Assert.That() does reflection and allocates a ConstraintResult class instance, so use a bare test to filter for it in inner loops.
                    var ptr = allocator.Allocate(rng.Next(1, maxAllocationSize), zeroInit: false);
                    if (ptr is null)
                        Assert.Fail("ptr should not be null");
                    pointers.Enqueue((IntPtr)ptr);
                }
            }

            Task[] tasks = new Task[numThreads];   // Task rather than Thread for propagation of exceptions.
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                tasks[t] = Task.Factory.StartNew(() => runLoadThread(tid));
            }
            Task.WaitAll(tasks);

            Assert.That(allocator.oversizePages.PageAllocator.IsInitialized, Is.False);
            var allocatedPointers = new HashSet<IntPtr>(pointers);

            // Now test the freelist loading.
            void runLoadFreeListThread(int tid)
            {
                while (pointers.TryDequeue(out var ptr))
                    allocator.Free(ptr);
            }

            Array.Clear(tasks);
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                tasks[t] = Task.Factory.StartNew(() => runLoadFreeListThread(tid));
            }
            Task.WaitAll(tasks);

            // Traceback all freelists and ensure sizes and total count
            {
                int allocationCount = 0;
                for (int ii = 0; ii < allocator.fixedSizePages.freeBins.Length; ++ii)
                {
                    var bin = allocator.fixedSizePages.freeBins[ii];
                    var binSize = 1 << (ii + FixedSizePages.MinPowerOf2);
                    for (BlockHeader* blockPtr = (BlockHeader*)bin; blockPtr is not null; blockPtr = FixedSizePages.GetNextPointer(blockPtr))
                    {
                        ++allocationCount;

                        // We should always have allocated this pointer because we have not done any freelist pop yet, so there can't be splits.
                        Assert.That(allocatedPointers.Contains((IntPtr)BlockHeader.ToUserAddress(blockPtr)));

                        if (blockPtr->AllocatedSize != binSize)
                            Assert.Fail($"AllocatedSize expected {binSize}, actual {blockPtr->AllocatedSize}, pt 1");
                        if (blockPtr->UserSize > binSize - sizeof(BlockHeader))
                            Assert.Fail($"UserSize max expected {binSize - sizeof(BlockHeader)}, actual {blockPtr->UserSize}, pt 1");
                    }
                }
                Assert.That(allocationCount, Is.EqualTo(allocationsPerThread * numThreads));
            }

            // Test popping from the freelist.
            void runPopFromFreeListThread(int tid)
            {
                Random rng = new(tid);

                // Randomly select a bin and if it's non-empty, Pop from it.
                while (true)
                {
                    int binIndex = rng.Next(0, allocator.fixedSizePages.freeBins.Length);
                    if (allocator.fixedSizePages.TryPopFromFreeList(binIndex, out var ptr))
                    {
                        var blockPtr = BlockHeader.FromUserAddress((long)ptr);
                        Assert.That((long)BlockHeader.ToUserAddress(blockPtr) == (long)ptr);

                        // We cannot assert that this pointer is found because we may do splits from higher bins during this test phase.
                        var isSplitFragment = !allocatedPointers.Contains((IntPtr)ptr);

                        var binSize = 1 << (binIndex + FixedSizePages.MinPowerOf2);
                        if (blockPtr->AllocatedSize != binSize)
                            Assert.Fail($"AllocatedSize expected {binSize}, actual {blockPtr->AllocatedSize}, isSplitFragment {isSplitFragment}, pt 2");
                        if (blockPtr->UserSize > binSize - sizeof(BlockHeader))
                            Assert.Fail($"UserSize max expected {binSize - sizeof(BlockHeader)}, actual {blockPtr->UserSize}, isSplitFragment {isSplitFragment}, pt 2");
                    }
                    else
                    {
                        // This can happen due to splitting of free list elements above binIndex putting a new fragment into binIndex.. and then
                        // by the time we test it here, it can be zero because it was popped by another thread.
                        var head = allocator.fixedSizePages.freeBins[binIndex];
                        if (head != 0)
                        {
                            var blockPtr = (BlockHeader*)head;
                            var userPointer = BlockHeader.ToUserAddress(blockPtr);

                            // If it's there after we failed a pop, it should be a split fragment.
                            var isSplitFragment = !allocatedPointers.Contains((IntPtr)userPointer);
                            Assert.That(isSplitFragment, Is.True, $"Failed to pop non-empty bin when a non-splitFragment was there");

                            // We can't compare block sizes here, because we are not popping 'head', so other threads can pop and split it.
                        }

                        // Stop if no non-empty bins are available.
                        bool found = false;
                        for (var ii = 0; ii < allocator.fixedSizePages.freeBins.Length; ++ii)
                        {
                            if (allocator.fixedSizePages.freeBins[ii] != IntPtr.Zero)
                            {
                                found = true;
                                break;
                            }
                        }
                        if (!found)
                            return;
                    }
                }
            }

            Array.Clear(tasks);
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                tasks[t] = Task.Factory.StartNew(() => runPopFromFreeListThread(tid));
            }
            Task.WaitAll(tasks);

            // Check next-higher bin lookup and splitting
            {
                const int largeBinIndex = 4;
                const int largeBinSize = 1 << (largeBinIndex + FixedSizePages.MinPowerOf2);
                const int midBinIndex = 3;
                const int midBinSize = 1 << (midBinIndex + FixedSizePages.MinPowerOf2);
                const int smallBinIndex = 2;
                const int smallBinSize = 1 << (smallBinIndex + FixedSizePages.MinPowerOf2);
                int makeUserSize(int binSize) => binSize - sizeof(BlockHeader) * 2;

                var ptr = allocator.Allocate(makeUserSize(largeBinSize), zeroInit: false);
                allocator.Free((long)ptr);
                Assert.That(allocator.fixedSizePages.freeBins[largeBinIndex], Is.Not.Zero);

                // Now allocate a smaller size and ensure it's split off from the larger bin.
                ptr = allocator.Allocate(makeUserSize(smallBinSize), zeroInit: false);

                // We should see that the large bin is empty and the mid and small bins have one item each of the
                // max userSize for that bin (as they are the split-off halves).
                Assert.That(allocator.fixedSizePages.freeBins[largeBinIndex], Is.Zero);

                // There should be one intermediate piece in the mid bin.
                Assert.That(allocator.fixedSizePages.freeBins[midBinIndex], Is.Not.Zero);
                var blockPtr = (BlockHeader*)allocator.fixedSizePages.freeBins[midBinIndex];
                Assert.That((long)blockPtr, Is.Not.Zero);
                Assert.That(blockPtr->AllocatedSize, Is.EqualTo(midBinSize));
                Assert.That(blockPtr->UserSize, Is.EqualTo(midBinSize - sizeof(BlockHeader)));

                // There should be one piece in the small bin because we split the intermediate piece in two.
                Assert.That(allocator.fixedSizePages.freeBins[smallBinIndex], Is.Not.Zero);
                blockPtr = (BlockHeader*)allocator.fixedSizePages.freeBins[smallBinIndex];
                Assert.That((long)blockPtr, Is.Not.Zero);
                Assert.That(blockPtr->AllocatedSize, Is.EqualTo(smallBinSize));
                Assert.That(blockPtr->UserSize, Is.EqualTo(smallBinSize - sizeof(BlockHeader)));

                // As long as we have the small blockPtr handy, let's test realloc too.
                // Shrink--stays in-place
                Assert.That(allocator.fixedSizePages.TryRealloc(blockPtr, makeUserSize(smallBinSize) - 20, out ptr), Is.True);
                Assert.That((long)ptr, Is.EqualTo((long)blockPtr + sizeof(BlockHeader)));
                // Grow--too big for in-place
                Assert.That(allocator.fixedSizePages.TryRealloc(blockPtr, makeUserSize(smallBinSize) + 2000, out _), Is.False);
                // Grow--fits in-place
                Assert.That(allocator.fixedSizePages.TryRealloc(blockPtr, makeUserSize(smallBinSize), out _), Is.True);
            }
        }

        [Test]
        [Category(ObjectIdMapCategory), Category(MultiLevelPageArrayCategory), Category(SmokeTestCategory)]
        public unsafe void FixedSizePagesStressTest([Values(8)] int numThreads, [Values] PageSize pageSize)
        {
            Assert.That(allocator.fixedSizePages.PageAllocator.IsInitialized, Is.False);

            const int allocationsPerThread = 1_000_000;
            const int maxAllocationSize = FixedSizePages.MaxExternalBlockSize;

            ConcurrentQueue<IntPtr> pointers = new();

            // This will mostly exercise the Asserts in the allocators.
            void runStressThread(int tid)
            {
                Random rng = new(tid);
                for (var ii = 0; ii < allocationsPerThread; ++ii)
                {
                    var rand = rng.Next(1, maxAllocationSize);
                    var doAlloc = (rand & 0xff) < 0x80;

                    if (doAlloc)
                    {
                        var ptr = allocator.Allocate(rng.Next(1, maxAllocationSize), zeroInit: false);
                        pointers.Enqueue((IntPtr)ptr);
                    }
                    else
                    {
                        if (pointers.TryDequeue(out var ptr))
                            allocator.Free(ptr);
                    }
                }
            }

            Task[] tasks = new Task[numThreads];   // Task rather than Thread for propagation of exceptions.
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                tasks[t] = Task.Factory.StartNew(() => runStressThread(tid));
            }
            Task.WaitAll(tasks);
        }

        [Test]
        [Category(ObjectIdMapCategory), Category(MultiLevelPageArrayCategory), Category(SmokeTestCategory)]
        public unsafe void OversizePagesBasicTest([Values(1, 8)] int numThreads, [Values] PageSize pageSize)
        {
            Assert.That(allocator.fixedSizePages.PageAllocator.IsInitialized, Is.False);

            // Allocate enough to fill past the first MultiLevelPageArray.InitialBookSize chapters.
            const int allocationsPerThread = 10_000;
            const int minAllocationSize = FixedSizePages.MaxExternalBlockSize + 1;
            const int maxAllocationSize = minAllocationSize * 4;

            ConcurrentQueue<IntPtr> pointers = new();

            void runLoadThread(int tid)
            {
                Random rng = new(tid);
                for (var ii = 0; ii < allocationsPerThread; ++ii)
                {
                    // Assert.That() does reflection and allocates a ConstraintResult class instance, so use a bare test to filter for it in inner loops.
                    var ptr = allocator.Allocate(rng.Next(minAllocationSize, maxAllocationSize), zeroInit: false);
                    if (ptr is null)
                        Assert.Fail("ptr should not be null");
                    pointers.Enqueue((IntPtr)ptr);
                }
            }

            Task[] tasks = new Task[numThreads];   // Task rather than Thread for propagation of exceptions.
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                tasks[t] = Task.Factory.StartNew(() => runLoadThread(tid));
            }
            Task.WaitAll(tasks);

            Assert.That(allocator.fixedSizePages.PageAllocator.IsInitialized, Is.False);
            var allocatedPointers = new HashSet<IntPtr>(pointers);

            // Now test the freelist loading.
            void runLoadFreeListThread(int tid)
            {
                while (pointers.TryDequeue(out var ptr))
                    allocator.Free(ptr);
            }

            Array.Clear(tasks);
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                tasks[t] = Task.Factory.StartNew(() => runLoadFreeListThread(tid));
            }
            Task.WaitAll(tasks);

            Assert.That(allocator.oversizePages.freeSlots.freeNodes.IsNil, Is.True, "We should not have any freed items in the oversize freelist's stack");

            // Traceback the freelist and ensure the associated pointer is cleared in the main array.
            int allocationCount = 0;
            {
                var node = allocator.oversizePages.freeSlots.stack;
                while (!node.IsNil)
                {
                    var element = allocator.oversizePages.freeSlots.elementArray[node.Slot];
                    if (allocator.oversizePages.PageAllocator.pageArray[element.Item] != IntPtr.Zero)
                        Assert.Fail($"Page {element.Item} should be null in the main array");
                    node = element.Node;
                    ++allocationCount;
                }
            }
            Assert.That(allocationCount, Is.EqualTo(allocationsPerThread * numThreads));
            Assert.That(allocator.oversizePages.freeSlots.elementArray.Count, Is.EqualTo(allocationCount));
            Assert.That(allocator.oversizePages.freeSlots.MaxCount, Is.EqualTo(allocationCount));

            // Test popping from the freelist. It should not allocate new nodes
            var oversizeAllocationCount = allocator.oversizePages.PageAllocator.TailPageOffset.Page;
            var popCount = 0;
            void runPopFromFreeListThread(int tid)
            {
                Random rng = new(tid);

                // Verify the freeList pops all expected nodes.
                while (popCount < allocationCount)
                {
                    if (allocator.oversizePages.freeSlots.TryPop(out var ptr))
                        _ = Interlocked.Increment(ref popCount);
                }
                Assert.That(allocator.oversizePages.freeSlots.IsEmpty, Is.True, "FreeSlots should be empty");
                Assert.That(allocator.oversizePages.PageAllocator.TailPageOffset.Page, Is.EqualTo(oversizeAllocationCount), "No additional pages should have been allocated");
            }

            Array.Clear(tasks);
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                tasks[t] = Task.Factory.StartNew(() => runPopFromFreeListThread(tid));
            }
            Task.WaitAll(tasks);

            Assert.That(allocator.oversizePages.freeSlots.IsEmpty, Is.True, "Oversize FreeList should be empty");
        }

        [Test]
        [Category(ObjectIdMapCategory), Category(MultiLevelPageArrayCategory), Category(SmokeTestCategory)]
        public unsafe void OversizePagesStressTest([Values(8)] int numThreads, [Values] PageSize pageSize)
        {
            Assert.That(allocator.fixedSizePages.PageAllocator.IsInitialized, Is.False);

            const int allocationsPerThread = 100_000;
            const int minAllocationSize = FixedSizePages.MaxExternalBlockSize + 1;
            const int maxAllocationSize = minAllocationSize * 4;

            ConcurrentQueue<IntPtr> pointers = new();

            // This will mostly exercise the Asserts in the allocators.
            void runStressThread(int tid)
            {
                Random rng = new(tid);
                for (var ii = 0; ii < allocationsPerThread; ++ii)
                {
                    var rand = rng.Next(minAllocationSize, maxAllocationSize);
                    var doAlloc = (rand & 0xff) < 0x80;

                    if (doAlloc)
                    {
                        var ptr = allocator.Allocate(rand, zeroInit: false);
                        pointers.Enqueue((IntPtr)ptr);
                    }
                    else
                    {
                        if (pointers.TryDequeue(out var ptr))
                            allocator.Free(ptr);
                    }
                }
            }

            Task[] tasks = new Task[numThreads];   // Task rather than Thread for propagation of exceptions.
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                tasks[t] = Task.Factory.StartNew(() => runStressThread(tid));
            }
            Task.WaitAll(tasks);
        }
    }
}