// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.largeobjects
{
    [TestFixture]
    internal class LargeObjectTests
    {
        private TsavoriteKV<MyKey, MyLargeValue> store1;
        private TsavoriteKV<MyKey, MyLargeValue> store2;
        private IDevice log, objlog;
        private string test_path;

        [SetUp]
        public void Setup()
        {
            test_path = TestUtils.MethodTestDir;
            TestUtils.RecreateDirectory(test_path);
        }

        [TearDown]
        public void TearDown()
        {
            TestUtils.DeleteDirectory(test_path);
        }

        [TestCase(CheckpointType.FoldOver)]
        [TestCase(CheckpointType.Snapshot)]
        [Category("TsavoriteKV")]
        public void LargeObjectTest(CheckpointType checkpointType)
        {
            MyInput input = default;
            MyLargeOutput output = new MyLargeOutput();

            log = Devices.CreateLogDevice(test_path + "/LargeObjectTest.log");
            objlog = Devices.CreateLogDevice(test_path + "/LargeObjectTest.obj.log");

            store1 = new TsavoriteKV<MyKey, MyLargeValue>
                (128,
                new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, PageSizeBits = 21, MemorySizeBits = 26 },
                new CheckpointSettings { CheckpointDir = test_path },
                new SerializerSettings<MyKey, MyLargeValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyLargeValueSerializer() }
                );

            int maxSize = 100;
            int numOps = 5000;

            var session1 = store1.NewSession<MyInput, MyLargeOutput, Empty, MyLargeFunctions>(new MyLargeFunctions());
            Random r = new Random(33);
            for (int key = 0; key < numOps; key++)
            {
                var mykey = new MyKey { key = key };
                var value = new MyLargeValue(1 + r.Next(maxSize));
                session1.Upsert(ref mykey, ref value, Empty.Default, 0);
            }
            session1.Dispose();

            store1.TryInitiateFullCheckpoint(out Guid token, checkpointType);
            store1.CompleteCheckpointAsync().GetAwaiter().GetResult();
            store1.Dispose();
            log.Dispose();
            objlog.Dispose();

            log = Devices.CreateLogDevice(test_path + "/LargeObjectTest.log");
            objlog = Devices.CreateLogDevice(test_path + "/LargeObjectTest.obj.log");

            store2 = new TsavoriteKV<MyKey, MyLargeValue>
                (128,
                new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, PageSizeBits = 21, MemorySizeBits = 26 },
                new CheckpointSettings { CheckpointDir = test_path },
                new SerializerSettings<MyKey, MyLargeValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyLargeValueSerializer() }
                );

            store2.Recover(token);

            var session2 = store2.NewSession<MyInput, MyLargeOutput, Empty, MyLargeFunctions>(new MyLargeFunctions());
            for (int keycnt = 0; keycnt < numOps; keycnt++)
            {
                var key = new MyKey { key = keycnt };
                var status = session2.Read(ref key, ref input, ref output, Empty.Default, 0);

                if (status.IsPending)
                    session2.CompletePending(true);
                else
                {
                    for (int i = 0; i < output.value.value.Length; i++)
                    {
                        Assert.AreEqual((byte)(output.value.value.Length + i), output.value.value[i]);
                    }
                }
            }
            session2.Dispose();

            store2.Dispose();

            log.Dispose();
            objlog.Dispose();
        }
    }
}