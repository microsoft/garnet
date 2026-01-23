// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.largeobjects
{
    using ClassAllocator = GenericAllocator<MyKey, MyLargeValue, StoreFunctions<MyKey, MyLargeValue, MyKey.Comparer, DefaultRecordDisposer<MyKey, MyLargeValue>>>;
    using ClassStoreFunctions = StoreFunctions<MyKey, MyLargeValue, MyKey.Comparer, DefaultRecordDisposer<MyKey, MyLargeValue>>;

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
        public async ValueTask LargeObjectTest(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType
            )
        {
            int maxSize = 100;
            int numOps = 5000;

            MyInput input = default;
            MyLargeOutput output = new MyLargeOutput();
            Guid token = default;

            // Step 1: Create and populate store.
            using (var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "LargeObjectTest.log")))
            using (var objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "LargeObjectTest.obj.log")))
            using (var store = new TsavoriteKV<MyKey, MyLargeValue, ClassStoreFunctions, ClassAllocator>(
                new()
                {
                    IndexSize = 1L << 13,
                    LogDevice = log,
                    ObjectLogDevice = objlog,
                    MutableFraction = 0.1,
                    PageSize = 1L << 21,
                    MemorySize = 1L << 26,
                    CheckpointDir = MethodTestDir
                }, StoreFunctions<MyKey, MyLargeValue>.Create(new MyKey.Comparer(), () => new MyKeySerializer(), () => new MyLargeValueSerializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)))
            using (var session = store.NewSession<MyInput, MyLargeOutput, Empty, MyLargeFunctions>(new MyLargeFunctions()))
            {
                var bContext = session.BasicContext;
                Random r = new Random(33);

                for (int key = 0; key < numOps; key++)
                {
                    var mykey = new MyKey { key = key };
                    var value = new MyLargeValue(1 + r.Next(maxSize));
                    _ = bContext.Upsert(ref mykey, ref value, Empty.Default);
                }

                _ = store.TryInitiateFullCheckpoint(out token, checkpointType);
                await store.CompleteCheckpointAsync();
            }

            // Step 1: Create and recover store.
            using (var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "LargeObjectTest.log")))
            using (var objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "LargeObjectTest.obj.log")))
            using (var store = new TsavoriteKV<MyKey, MyLargeValue, ClassStoreFunctions, ClassAllocator>(
                    new()
                    {
                        IndexSize = 1L << 13,
                        LogDevice = log,
                        ObjectLogDevice = objlog,
                        MutableFraction = 0.1,
                        PageSize = 1L << 21,
                        MemorySize = 1L << 26,
                        CheckpointDir = MethodTestDir
                    }, StoreFunctions<MyKey, MyLargeValue>.Create(new MyKey.Comparer(), () => new MyKeySerializer(), () => new MyLargeValueSerializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)))
            {
                _ = store.Recover(token);

                using (var session = store.NewSession<MyInput, MyLargeOutput, Empty, MyLargeFunctions>(new MyLargeFunctions()))
                {
                    var bContext = session.BasicContext;

                    for (int keycnt = 0; keycnt < numOps; keycnt++)
                    {
                        var key = new MyKey { key = keycnt };
                        var status = bContext.Read(ref key, ref input, ref output, Empty.Default);

                        if (status.IsPending)
                            (status, output) = bContext.GetSinglePendingResult();

                        for (int i = 0; i < output.value.value.Length; i++)
                            ClassicAssert.AreEqual((byte)(output.value.value.Length + i), output.value.value[i]);
                    }
                }
            }
        }
    }
}