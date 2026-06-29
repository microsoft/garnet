// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
#if DEBUG
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
#endif
using System.Threading.Tasks;
using Garnet.test;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.Objects
{
    /// <summary>
    /// This also tests <see cref="MultiLevelPageArray{TestObjectValue}"/> and <see cref="SimpleConcurrentStack{_int_}"/>,
    /// which in turn tests <see cref="MultiLevelPageArray{_int_}"/>.
    /// </summary>
    [TestFixture]
    class ObjectIdMapTests : TestBase
    {
        ObjectIdMap map;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir);
            map = new();
        }

        [TearDown]
        public void TearDown()
        {
            DeleteDirectory(MethodTestDir);
        }

        private void LoadMap(int numThreads, int pagesPerThread, int blocksPerPage)
        {
            Assert.That(map.objectArray.IsInitialized, Is.False);

            void runLoadThread(int tid)
            {
                // Reduce memory stress by reusing the same object because we are not doing operations on it; it's just a null/not-null indicator in the slot.
                var valueObject = new TestObjectValue();

                for (var page = 0; page < pagesPerThread; ++page)
                {
                    for (int block = 0; block < blocksPerPage; ++block)
                    {
                        // Assert.That() does reflection and allocates a ConstraintResult class instance, which is slow, so use a bare test to filter for it in inner loops.
                        var objectId = map.Allocate();
                        if (objectId >= map.Count)
                            Assert.Fail("objectId should be < map.Count");
                        map.Set(objectId, valueObject);
                    }
                }
            }

            Task[] tasks = new Task[numThreads];   // Task rather than Thread for propagation of exceptions.
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                tasks[t] = Task.Factory.StartNew(() => runLoadThread(tid));
            }
            Task.WaitAll(tasks);
        }

        [Test]
        [Category(ObjectIdMapCategory), Category(MultiLevelPageArrayCategory), Category(SmokeTestCategory)]
        [Repeat(1000)]  // Repeat is an intended part of the test due to thread-timing non-determinism; writing the iteration count would slow things so we don't
        public void ObjectIdMapTestStressInitialAllocs([Values(1, 8)] int numThreads)
        {
            // Focus on the initial allocation which lazily creates the book array.
            LoadMap(numThreads, pagesPerThread: 1, blocksPerPage: 1);

            Assert.That(map.Count, Is.EqualTo(numThreads));

            // We don't run this test with enough threads to go beyond this.
            Assert.That(map.objectArray.book.Length, Is.EqualTo(MultiLevelPageArray.InitialBookSize));
        }

        [Test]
        [Category(ObjectIdMapCategory), Category(MultiLevelPageArrayCategory), Category(SmokeTestCategory)]
        //[Repeat(100)]   // Repeat is an intended part of the test due to thread-timing non-determinism; writing the iteration count would slow things so we don't
        public void ObjectIdMapTestStressAllocAndFree([Values(1, 8)] int numThreads)
        {
            Assert.That(map.objectArray.IsInitialized, Is.False);

            // Allocate enough to fill past the first MultiLevelPageArray.InitialBookSize pages.
            var pagesPerThread = MultiLevelPageArray.InitialBookSize / numThreads + 1;
            LoadMap(numThreads, pagesPerThread, blocksPerPage: MultiLevelPageArray.PageSize);

            var allocatedCount = map.Count;
            Assert.That(allocatedCount, Is.EqualTo(pagesPerThread * MultiLevelPageArray.PageSize * numThreads));
            Assert.That(map.objectArray.book.Length, Is.GreaterThan(MultiLevelPageArray.InitialBookSize));

            // Now test the freelist loading.
            void runLoadFreeListThread(int tid)
            {
                Random rng = new(tid);

                // Free() from a thread-specific page to threads aren't freeing the same objectId; in actual use,
                // we'd Allocate() which does per-thread ownership instead.
                for (var block = 0; block < MultiLevelPageArray.PageSize; ++block)
                {
                    // After being freed, the slot in the objectVector should be cleared (so objects are freed as early as possible).
                    var objectId = tid * MultiLevelPageArray.PageSize + block;
                    map.Free(objectId);
                    Assert.That(map.GetHeapObject(objectId), Is.Null, "map.GetHeapObject(objectId) should be null after Free() pt 1");
                }
            }

            Task[] tasks = new Task[numThreads];   // Task rather than Thread for propagation of exceptions.
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                tasks[t] = Task.Factory.StartNew(() => runLoadFreeListThread(tid));
            }
            Task.WaitAll(tasks);

            Assert.That(map.freeSlots.MaxCount, Is.EqualTo(MultiLevelPageArray.PageSize * numThreads), "All freed items should have been added to the the freeList elementArray");
            Assert.That(map.freeSlots.stack.IsNil, Is.False, "All freed items should be in the stack");
            Assert.That(map.freeSlots.freeNodes.IsNil, Is.True, "No freed items should be in the freeList");

            // Finally, test the freelist allocation.
            void runAllocateFromFreeListThread(int tid)
            {
                Random rng = new(tid);

                // Free() from a thread-specific page to threads aren't freeing the same objectId; in actual use,
                // we'd Allocate() which does per-thread ownership instead.
                for (var block = 0; block < MultiLevelPageArray.PageSize; ++block)
                {
                    var objectId = map.Allocate();

                    // The request should have been satisfied from the freeList, not another allocation.
                    if (objectId >= allocatedCount)
                        Assert.Fail("objectId should be less than allocatedCount");

                    // Make sure the slot in the objectVector is still cleared.
                    Assert.That(map.GetHeapObject(objectId), Is.Null, "map.GetHeapObject(objectId) should be null after Free() pt 2");
                }
            }

            Array.Clear(tasks);
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                tasks[t] = Task.Factory.StartNew(() => runAllocateFromFreeListThread(tid));
            }
            Task.WaitAll(tasks);

            Assert.That(map.Count, Is.EqualTo(allocatedCount));

            Assert.That(map.freeSlots.stack.IsNil, Is.True, "No freed items should be in the stack");
            Assert.That(map.freeSlots.freeNodes.IsNil, Is.False, "All freed items should be in the freeList");
            Assert.That(map.freeSlots.elementArray.Count, Is.EqualTo(MultiLevelPageArray.PageSize * numThreads), "No freed items should have been added to the the freeList elementArray");
        }

#if DEBUG
        /// <summary>
        /// Multi-threaded stress test for the OOM rewind-and-retry path in <c>MultiLevelPageArray&lt;TElement&gt;.CompleteAsPageOwner</c>.
        /// Uses the DEBUG-only <c>testInjectAddPageFailure</c> hook to force <c>MultiLevelPageArray&lt;TElement&gt;.AddPage</c> to throw
        /// <see cref="OutOfMemoryException"/> on a configured set of page indices for a bounded number of attempts. After all threads finish,
        /// verifies the structural invariants: every successful id is unique, ids exactly cover [0, Count), no id is leaked or duplicated,
        /// and every reachable slot is usable for <see cref="ObjectIdMap.GetHeapObject"/>. The injection hook only exists in DEBUG builds,
        /// so in Release the test body is replaced with <see cref="Assert.Ignore(string)"/>.
        /// </summary>
#endif
        [Test]
        [Category(ObjectIdMapCategory), Category(MultiLevelPageArrayCategory)]
        public void ObjectIdMapTestStressAddPageOOM([Values(1, 4, 8)] int numThreads, [Values(2, 5)] int failuresPerPage)
        {
#if !DEBUG
            Assert.Ignore("Does not run in release build");
#else
            // For each new page index, throw OOM the first 'failuresPerPage' times we are asked to allocate it; thereafter succeed.
            // Bounded so the test deterministically terminates: under contention multiple threads may race for the natural-owner role on
            // any given page, and each loss is one of the bounded failures. The remainingFailures map below is keyed by page index.
            var remainingFailures = new ConcurrentDictionary<int, int>();
            map.objectArray.testInjectAddPageFailure = pageIdx =>
            {
                while (true)
                {
                    var current = remainingFailures.GetOrAdd(pageIdx, failuresPerPage);
                    if (current <= 0)
                        return false;
                    if (remainingFailures.TryUpdate(pageIdx, current - 1, current))
                        return true;
                }
            };

            // We want to cross several page boundaries so the OOM path is exercised for multiple pages. Choose enough total allocations
            // to fill past the initial book size; the exact count is not load-bearing, only that it crosses multiple page boundaries.
            const int totalPages = MultiLevelPageArray.InitialBookSize * 2;   // exercises at least one book grow
            var totalSuccessfulAllocations = totalPages * MultiLevelPageArray.PageSize;
            var allocationsPerThread = totalSuccessfulAllocations / numThreads;

            var successfulIds = new ConcurrentBag<int>();
            var oomCount = 0;
            var valueObject = new TestObjectValue();

            void runThread(int tid)
            {
                var done = 0;
                while (done < allocationsPerThread)
                {
                    int id;
                    try
                    {
                        id = map.Allocate();
                    }
                    catch (OutOfMemoryException)
                    {
                        // Expected for the natural-owner thread of a page whose injection counter is still > 0. The rewind in
                        // CompleteAsPageOwner has restored tail; another thread (or this thread on its next call) becomes the new
                        // natural owner. Loop back and retry without counting this as a successful allocation.
                        _ = Interlocked.Increment(ref oomCount);
                        continue;
                    }

                    successfulIds.Add(id);
                    map.Set(id, valueObject);
                    ++done;
                }
            }

            var tasks = new Task[numThreads];
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                tasks[t] = Task.Factory.StartNew(() => runThread(tid));
            }
            Task.WaitAll(tasks);

            // Invariant 1: count matches the high-water mark.
            Assert.That(successfulIds.Count, Is.EqualTo(allocationsPerThread * numThreads),
                "Every successful Allocate must contribute exactly one id");
            Assert.That(map.Count, Is.EqualTo(successfulIds.Count),
                "map.Count must equal the number of successful Allocates (no orphan ids in the rewind-and-retry design)");

            // Invariant 2: ids are unique and cover [0, Count) exactly. The rewind-and-retry path must not produce duplicates or gaps.
            var sortedIds = new List<int>(successfulIds);
            sortedIds.Sort();
            for (var i = 0; i < sortedIds.Count; i++)
                Assert.That(sortedIds[i], Is.EqualTo(i), $"id at position {i} must equal {i} (no gap, no duplicate); got {sortedIds[i]}");

            // Invariant 3: every returned slot is genuinely usable -- we wrote into it during the loop above, so GetHeapObject must
            // return the value (and would throw NRE if the slot's page were still a sentinel/null array).
            foreach (var id in sortedIds)
                Assert.That(map.GetHeapObject(id), Is.SameAs(valueObject), $"Slot {id} must hold the value we set");

            // Sanity: we should have actually exercised the OOM path. With failuresPerPage * (totalPages - 1) expected throws minimum
            // (page 0 doesn't go through the page-turn path), oomCount should be >= failuresPerPage. (Use >= rather than == because
            // CAS contention can produce extra losses past the initial bounded count if multiple threads race for the natural-owner
            // role on the same page across retries.)
            Assert.That(oomCount, Is.GreaterThanOrEqualTo(failuresPerPage),
                $"OOM injection should have fired at least {failuresPerPage} times; got {oomCount}");
#endif
        }
    }
}