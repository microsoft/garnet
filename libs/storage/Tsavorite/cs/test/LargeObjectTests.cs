// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.largeobjects
{
    [TestFixture]
    internal class LargeObjectTests
    {
        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => DeleteDirectory(MethodTestDir);

        [Test]
        [Category("TsavoriteKV")]
        public async ValueTask LargeObjectTest([Values] CheckpointType checkpointType)
        {
            int maxSize = 100;
            int numOps = 5000;

            MyInput input = default;
            MyLargeOutput output = new MyLargeOutput();
            Guid token = default;

            using (var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "LargeObjectTest.log")))
            using (var objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "LargeObjectTest.obj.log")))
            using (var store = new TsavoriteKV<MyKey, MyLargeValue>(128,
                new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, PageSizeBits = 21, MemorySizeBits = 26 },
                new CheckpointSettings { CheckpointDir = MethodTestDir },
                new SerializerSettings<MyKey, MyLargeValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyLargeValueSerializer() }))
            using (var session = store.NewSession<MyInput, MyLargeOutput, Empty, MyLargeFunctions>(new MyLargeFunctions()))
            {
                var bContext = session.BasicContext;
                Random r = new Random(33);

                for (int key = 0; key < numOps; key++)
                {
                    var mykey = new MyKey { key = key };
                    var value = new MyLargeValue(1 + r.Next(maxSize));
                    bContext.Upsert(ref mykey, ref value, Empty.Default);
                }

                store.TryInitiateFullCheckpoint(out token, checkpointType);
                await store.CompleteCheckpointAsync();
            }

            using (var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "LargeObjectTest.log")))
            using (var objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "LargeObjectTest.obj.log")))
            using (var store = new TsavoriteKV<MyKey, MyLargeValue>(128,
                    new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, PageSizeBits = 21, MemorySizeBits = 26 },
                    new CheckpointSettings { CheckpointDir = MethodTestDir },
                    new SerializerSettings<MyKey, MyLargeValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyLargeValueSerializer() }))
            {
                store.Recover(token);

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
                            Assert.AreEqual((byte)(output.value.value.Length + i), output.value.value[i]);
                    }
                }
            }
        }
    }
}