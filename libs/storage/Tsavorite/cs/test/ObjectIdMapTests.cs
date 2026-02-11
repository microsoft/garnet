// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Allure.NUnit;
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
    [AllureNUnit]
    [TestFixture]
    class ObjectIdMapTests : AllureTestBase
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

        [Test]
        [Category(ObjectIdMapCategory), Category(MultiLevelPageArrayCategory), Category(SmokeTestCategory)]
        public unsafe void ObjectIdMapTest1([Values(1, 8)] int numThreads)
        {
            Assert.That(map.objectArray.IsInitialized, Is.False);

            // Allocate enough to fill past the first MultiLevelPageArray.InitialBookSize chapters.
            var chaptersPerThread = MultiLevelPageArray.InitialBookSize / numThreads + 1;

            void runLoadThread(int tid)
            {
                // Reduce memory stress by reusing the same object because we are not doing operations on it; it's just a null/not-null indicator in the slot.
                var valueObject = new TestObjectValue();

                for (var chapter = 0; chapter < chaptersPerThread; ++chapter)
                {
                    for (int page = 0; page < MultiLevelPageArray.ChapterSize; ++page)
                    {
                        // Assert.That() does reflection and allocates a ConstraintResult class instance, so use a bare test to filter for it in inner loops.
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

            var allocatedCount = map.Count;
            Assert.That(allocatedCount, Is.EqualTo(chaptersPerThread * MultiLevelPageArray.ChapterSize * numThreads));
            Assert.That(map.objectArray.book.Length, Is.GreaterThan(MultiLevelPageArray.InitialBookSize));

            // Now test the freelist loading.
            void runLoadFreeListThread(int tid)
            {
                Random rng = new(tid);

                // Free() from a thread-specific chapter to threads aren't freeing the same objectId; in actual use,
                // we'd Allocate() which does per-thread ownership instead.
                for (var page = 0; page < MultiLevelPageArray.ChapterSize; ++page)
                {
                    // After being freed, the slot in the objectVector should be cleared (so objects are freed as early as possible).
                    var objectId = tid * MultiLevelPageArray.ChapterSize + page;
                    map.Free(objectId);
                    Assert.That(map.GetHeapObject(objectId), Is.Null, "map.GetHeapObject(objectId) should be null after Free() pt 1");
                }
            }

            Array.Clear(tasks);
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                tasks[t] = Task.Factory.StartNew(() => runLoadFreeListThread(tid));
            }
            Task.WaitAll(tasks);

            Assert.That(map.freeSlots.MaxCount, Is.EqualTo(MultiLevelPageArray.ChapterSize * numThreads), "All freed items should have been added to the the freeList elementArray");
            Assert.That(map.freeSlots.stack.IsNil, Is.False, "All freed items should be in the stack");
            Assert.That(map.freeSlots.freeNodes.IsNil, Is.True, "No freed items should be in the freeList");

            // Finally, test the freelist allocation.
            void runAllocateFromFreeListThread(int tid)
            {
                Random rng = new(tid);

                // Free() from a thread-specific chapter to threads aren't freeing the same objectId; in actual use,
                // we'd Allocate() which does per-thread ownership instead.
                for (var page = 0; page < MultiLevelPageArray.ChapterSize; ++page)
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
            Assert.That(map.freeSlots.elementArray.Count, Is.EqualTo(MultiLevelPageArray.ChapterSize * numThreads), "No freed items should have been added to the the freeList elementArray");
        }
    }
}