// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;
using System;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;
using System.Collections.Concurrent;

namespace Tsavorite.test
{
    /// <summary>
    /// This tests <see cref="OverflowAllocator.FixedSizePages"/> and <see cref="OverflowAllocator.OversizePages"/>,
    /// which in turn tests some aspects of <see cref="MultiLevelPageArray{_int_}"/>.
    /// </summary>
    [TestFixture]
    class OverflowAllocatorTests
    {
        OverflowAllocator allocator;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir);
            allocator = new(1 << LogSettings.kDefaultOverflowFixedPageSizeBits);
        }

        [TearDown]
        public void TearDown()
        {
            DeleteDirectory(MethodTestDir);
        }

        [Test]
        [Category(ObjectIdMapCategory), Category(MultiLevelPageArrayCategory), Category(SmokeTestCategory)]
        public unsafe void FixedSizePagesTest1([Values(1, 8)] int numThreads)
        {
            Assert.That(allocator.fixedSizePages.PageAllocator.IsInitialized, Is.False);

            // Allocate enough to fill past the first MultiLevelPageArray.InitialBookSize chapters.
            const int allocationsPerThread = 1_000;
            const int maxAllocationSize = OverflowAllocator.FixedSizePages.MaxBlockSize;

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

            // TODO: Traceback all freelists and ensure sizes and total count
            // TODO: Test block splitting from initial allocation
            // TODO: Check next-higher bins lookup and splitting

            // Finally, test the freelist allocation.
            void runAllocateFromFreeListThread(int tid)
            {
                Random rng = new(tid);

                // TODO: Randomly select a bin and if it's non-empty, allocate from it. Stop when no non-empty bins are available.
            }

            Array.Clear(tasks);
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                tasks[t] = Task.Factory.StartNew(() => runAllocateFromFreeListThread(tid));
            }
            Task.WaitAll(tasks);
        }
    }
}