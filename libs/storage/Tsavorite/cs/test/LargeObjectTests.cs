// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.LargeObjects
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>;

    [TestFixture]
    internal class LargeObjectTests
    {
        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => DeleteDirectory(MethodTestDir);

        [Test]
        [Category("TsavoriteKV")]
        public async ValueTask LargeObjectTest(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType
            )
        {
            int maxSize = 100;
            int numOps = 5000;

            TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Object };
            TestLargeObjectOutput output = new();
            Guid token = default;

            // Step 1: Create and populate store.
            using (var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "LargeObjectTest.log")))
            using (var objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "LargeObjectTest.obj.log")))
            using (var store = new TsavoriteKV<ClassStoreFunctions, ClassAllocator>(
                new()
                {
                    IndexSize = 1L << 13,
                    LogDevice = log,
                    ObjectLogDevice = objlog,
                    MutableFraction = 0.1,
                    PageSize = 1L << 21,
                    MemorySize = 1L << 26,
                    CheckpointDir = MethodTestDir
                }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)))
            using (var session = store.NewSession<TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
            {
                var bContext = session.BasicContext;
                Random r = new Random(33);

                for (int key = 0; key < numOps; key++)
                {
                    var mykey = new TestObjectKey { key = key };
                    var value = new TestLargeObjectValue(1 + r.Next(maxSize));
                    _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref mykey), value, Empty.Default);
                }

                _ = store.TryInitiateFullCheckpoint(out token, checkpointType);
                await store.CompleteCheckpointAsync();
            }

            // Step 1: Create and recover store.
            using (var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "LargeObjectTest.log")))
            using (var objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "LargeObjectTest.obj.log")))
            using (var store = new TsavoriteKV<ClassStoreFunctions, ClassAllocator>(
                    new()
                    {
                        IndexSize = 1L << 13,
                        LogDevice = log,
                        ObjectLogDevice = objlog,
                        MutableFraction = 0.1,
                        PageSize = 1L << 21,
                        MemorySize = 1L << 26,
                        CheckpointDir = MethodTestDir
                    }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)))
            {
                _ = store.Recover(token);

                using (var session = store.NewSession<TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
                {
                    var bContext = session.BasicContext;

                    for (int keycnt = 0; keycnt < numOps; keycnt++)
                    {
                        var key = new TestObjectKey { key = keycnt };
                        var status = bContext.Read(SpanByte.FromPinnedVariable(ref key), ref input, ref output, Empty.Default);

                        if (status.IsPending)
                            (status, output) = bContext.GetSinglePendingResult();

                        for (int i = 0; i < output.valueObject.value.Length; i++)
                            ClassicAssert.AreEqual((byte)(output.valueObject.value.Length + i), output.valueObject.value[i]);
                    }
                }
            }
        }
    }
}