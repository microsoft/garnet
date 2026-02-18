// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.LargeObjects
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>;

    [AllureNUnit]
    [TestFixture]
    internal class LargeObjectTests : AllureTestBase
    {
        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => DeleteDirectory(MethodTestDir);

        [Test]
        [Category("TsavoriteKV")]
        [TestCase(CheckpointType.Snapshot, RandomMode.Rng, 10, 400)]        // in-mem log with 1 objectlog segment
        [TestCase(CheckpointType.Snapshot, RandomMode.NoRng, 10, 200)]      // in-mem log with 1 objectlog segment
        [TestCase(CheckpointType.Snapshot, RandomMode.Rng, 2000, 18000)]    // Eviction to disk with about 4 objectlog segments
        [TestCase(CheckpointType.Snapshot, RandomMode.NoRng, 2000, 9000)]   // Eviction to disk with 4 objectlog segments
        [TestCase(CheckpointType.FoldOver, RandomMode.Rng, 10, 400)]        // in-mem log with 1 objectlog segment
        [TestCase(CheckpointType.FoldOver, RandomMode.NoRng, 10, 200)]      // in-mem log with 1 objectlog segment
        [TestCase(CheckpointType.FoldOver, RandomMode.Rng, 2000, 18000)]    // Eviction to disk with about 4 objectlog segments
        [TestCase(CheckpointType.FoldOver, RandomMode.NoRng, 2000, 9000)]   // Eviction to disk with 4 objectlog segments
        public async ValueTask LargeObjectTest([Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType, RandomMode rngMode, int numObjects, int numItems)
        {
            Guid token = default;

            // Step 1: Create and populate store.
            using (var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "LargeObjectTest.log")))
            using (var objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "LargeObjectTest.obj.log")))
            using (var store = new TsavoriteKV<ClassStoreFunctions, ClassAllocator>(CreateKVSettings(log, objlog)
                , StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)))
            using (var session = store.NewSession<TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
            {
                var bContext = session.BasicContext;
                Random rng = new Random(33);

                for (int key = 0; key < numObjects; key++)
                {
                    var mykey = new TestObjectKey { key = key };
                    var value = new TestLargeObjectValue(rngMode == RandomMode.Rng ? 1 + rng.Next(numItems) : numItems);
                    _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref mykey), value, Empty.Default);
                }

                // Validate read before checkpoint
                DoRead(session, numObjects, store);

                _ = store.TryInitiateFullCheckpoint(out token, checkpointType);
                await store.CompleteCheckpointAsync();
            }

            // Step 1: Create and recover store.
            using (var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "LargeObjectTest.log")))
            using (var objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "LargeObjectTest.obj.log")))
            using (var store = new TsavoriteKV<ClassStoreFunctions, ClassAllocator>(CreateKVSettings(log, objlog)
                , StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)))
            {
                _ = store.Recover(token);

                using (var session = store.NewSession<TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
                    DoRead(session, numObjects, store);
            }

            static void DoRead(ClientSession<TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions, ClassStoreFunctions, ClassAllocator> session,
                int numObjects, TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
            {
                TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Object };
                TestLargeObjectOutput output = new();
                var bContext = session.BasicContext;

                for (int keycnt = 0; keycnt < numObjects; keycnt++)
                {
                    var key = new TestObjectKey { key = keycnt };
                    var status = bContext.Read(SpanByte.FromPinnedVariable(ref key), ref input, ref output, Empty.Default);

                    if (status.IsPending)
                        (status, output) = bContext.GetSinglePendingResult();
                    Assert.That(status.Found, Is.True, $"Read failed for key {keycnt} with status {status}");

                    // Sample every 5th item so it's not quite so slow as this test isn't [Explicit]
                    for (int i = 0; i < output.valueObject.value.Length; i += 5)
                        Assert.That(output.valueObject.value[i], Is.EqualTo((byte)(output.valueObject.value.Length + i)));

                    // Make sure we test the last item.
                    Assert.That(output.valueObject.value[^1], Is.EqualTo((byte)(output.valueObject.value.Length * 2 - 1)));
                }
            }

            static KVSettings CreateKVSettings(IDevice log, IDevice objlog) => new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                PageSize = 1L << 13,                // 8 KB
                LogMemorySize = 1L << 16,              // 64 KB
                SegmentSize = 1L << 17,             // 128 KB
                ObjectLogSegmentSize = 1L << 22,    // 4 MB
                CheckpointDir = MethodTestDir
            };
        }

        [Test]
        [Category("TsavoriteKV")]
        [TestCase(CheckpointType.Snapshot, RandomMode.Rng, 10, 1_000, 10_000)]      // 10 x 40 MB: 10 objects x 1,000 lists x 5,000 (average) 8-byte items = 10 * 1,000 * 40,000 bytes = 400MB (1 segment)
        [TestCase(CheckpointType.Snapshot, RandomMode.NoRng, 10, 1_000, 10_000)]    // 10 x 40 MB: 10 objects x 1,000 lists x 5,000 8-byte items = 10 * 1,000 * 40,000 bytes = 400MB (1 segment)
        [TestCase(CheckpointType.Snapshot, RandomMode.Rng, 1, 20_000, 70_000)]      // 1 x 4 GB: 1 object x 20,000 lists x 35,000 (average) 8-byte items = 1 * 20,000 * 280,000 bytes = over 5GB (4 or 5 segments, tests high length byte)
        [TestCase(CheckpointType.Snapshot, RandomMode.NoRng, 1, 20_000, 25_000)]    // 1 x 4 GB: 1 object x 20,000 lists x 25,000 8-byte items = 1 * 20,000 * 200,000 bytes = just under 4GB (3 segments)
        [Explicit("Long running, high-memory test")]
        public async ValueTask MultiListObjectTest([Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType, RandomMode rngMode, int numObjects, int numLists, int numItems)
        {
            Guid token = default;

            // Step 1: Create and populate store.
            using (var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "MultiListObjectTest.log")))
            using (var objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "MultiListObjectTest.obj.log")))
            using (var store = new TsavoriteKV<ClassStoreFunctions, ClassAllocator>(CreateKVSettings(log, objlog)
                , StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestMultiListObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)))
            using (var session = store.NewSession<TestMultiListObjectInput, TestMultiListObjectOutput, Empty, TestMultiListObjectFunctions>(new TestMultiListObjectFunctions()))
            {
                var bContext = session.BasicContext;
                Random rng = new Random(33);

                // Create the objects 
                for (int key = 0; key < numObjects; key++)
                {
                    var mykey = new TestObjectKey { key = key };
                    var value = new TestMultiListObjectValue(key, numLists, numItems, rngMode == RandomMode.Rng ? rng : null);
                    _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref mykey), value, Empty.Default);
                }

                // Validate read before checkpoint
                DoRead(session, numObjects, store);

                _ = store.TryInitiateFullCheckpoint(out token, checkpointType);
                await store.CompleteCheckpointAsync();
            }

            // Step 1: Create and recover store.
            using (var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "MultiListObjectTest.log")))
            using (var objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "MultiListObjectTest.obj.log")))
            using (var store = new TsavoriteKV<ClassStoreFunctions, ClassAllocator>(CreateKVSettings(log, objlog)
                , StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestMultiListObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)))
            {
                _ = store.Recover(token);

                using (var session = store.NewSession<TestMultiListObjectInput, TestMultiListObjectOutput, Empty, TestMultiListObjectFunctions>(new TestMultiListObjectFunctions()))
                    DoRead(session, numObjects, store);
            }

            static void DoRead(ClientSession<TestMultiListObjectInput, TestMultiListObjectOutput, Empty, TestMultiListObjectFunctions, ClassStoreFunctions, ClassAllocator> session,
                int numOps, TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
            {
                var bContext = session.BasicContext;

                for (int keycnt = 0; keycnt < numOps; keycnt++)
                {
                    TestMultiListObjectInput input = new()
                    {
                        objectIndex = keycnt,
                        listIndex = 12,     // TODO: vary listIndex by rng and have multiple.
                        itemIndex = 24
                    };
                    TestMultiListObjectOutput output = new();

                    var key = new TestObjectKey { key = keycnt };
                    var status = bContext.Read(SpanByte.FromPinnedVariable(ref key), ref input, ref output, Empty.Default);

                    if (status.IsPending)
                        (status, output) = bContext.GetSinglePendingResult();
                    Assert.That(status.Found, Is.True, $"Read failed for key {keycnt} with status {status}");
                    Assert.That(output.oldValue, Is.EqualTo(input.ExpectedOutputValue));

                    for (int i = 0; i < output.valueObject.lists.Length; i++)
                    {
                        // Sample every 50th item so it's not too slow
                        for (var j = 0; j < output.valueObject.lists[i].Count; j += 50)
                            Assert.That(output.valueObject.lists[i][j], Is.EqualTo(TestMultiListObjectValue.CreateValue(keycnt, i, j)));

                        // Make sure we test the last item.
                        Assert.That(output.valueObject.lists[i][^1], Is.EqualTo(TestMultiListObjectValue.CreateValue(keycnt, i, output.valueObject.lists[i].Count - 1)));
                    }
                }
            }

            static KVSettings CreateKVSettings(IDevice log, IDevice objlog) => new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                PageSize = 1L << 13,                // 8 KB
                LogMemorySize = 1L << 16,              // 64 KB
                SegmentSize = 1L << 17,             // 128 KB
                ObjectLogSegmentSize = 1L << 30,    // 1 GB
                CheckpointDir = MethodTestDir
            };
        }
    }
}