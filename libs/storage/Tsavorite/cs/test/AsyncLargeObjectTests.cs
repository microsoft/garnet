// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.async
{
    [TestFixture]
    internal class LargeObjectTests
    {
        private TsavoriteKV<MyKey, MyLargeValue> store1;
        private TsavoriteKV<MyKey, MyLargeValue> store2;
        private IDevice log, objlog;
        private readonly MyLargeFunctions functions = new();

        [SetUp]
        public void Setup()
        {
            TestUtils.RecreateDirectory(TestUtils.MethodTestDir);
        }

        [TearDown]
        public void TearDown()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("TsavoriteKV")]
        public async Task LargeObjectTest([Values] CheckpointType checkpointType)
        {
            MyInput input = default;
            MyLargeOutput output = new();

            log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "LargeObjectTest.log"));
            objlog = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "LargeObjectTest.obj.log"));

            store1 = new(128,
                new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, PageSizeBits = 21, MemorySizeBits = 26 },
                new CheckpointSettings { CheckpointDir = TestUtils.MethodTestDir },
                new SerializerSettings<MyKey, MyLargeValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyLargeValueSerializer() }
                );

            int maxSize = 100;
            int numOps = 5000;

            using (var s = store1.NewSession<MyInput, MyLargeOutput, Empty, MyLargeFunctions>(functions))
            {
                var bContext = s.BasicContext;
                Random r = new Random(33);
                for (int key = 0; key < numOps; key++)
                {
                    var mykey = new MyKey { key = key };
                    var value = new MyLargeValue(1 + r.Next(maxSize));
                    bContext.Upsert(ref mykey, ref value, Empty.Default);
                }
            }

            store1.TryInitiateFullCheckpoint(out Guid token, checkpointType);
            await store1.CompleteCheckpointAsync();

            store1.Dispose();
            log.Dispose();
            objlog.Dispose();

            log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "LargeObjectTest.log"));
            objlog = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "LargeObjectTest.obj.log"));

            store2 = new(128,
                new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, PageSizeBits = 21, MemorySizeBits = 26 },
                new CheckpointSettings { CheckpointDir = TestUtils.MethodTestDir },
                new SerializerSettings<MyKey, MyLargeValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyLargeValueSerializer() }
                );

            store2.Recover(token);
            using (var s2 = store2.NewSession<MyInput, MyLargeOutput, Empty, MyLargeFunctions>(functions))
            {
                var bContext = s2.BasicContext;
                for (int keycnt = 0; keycnt < numOps; keycnt++)
                {
                    var key = new MyKey { key = keycnt };
                    var status = bContext.Read(ref key, ref input, ref output, Empty.Default);

                    if (status.IsPending)
                        await bContext.CompletePendingAsync();
                    else
                    {
                        for (int i = 0; i < output.value.value.Length; i++)
                        {
                            Assert.AreEqual((byte)(output.value.value.Length + i), output.value.value[i]);
                        }
                    }
                }
            }

            store2.Dispose();

            log.Dispose();
            objlog.Dispose();
        }
    }
}