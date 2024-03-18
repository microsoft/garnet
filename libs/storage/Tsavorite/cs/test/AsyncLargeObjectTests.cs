// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
        private string test_path;
        private readonly MyLargeFunctions functions = new();

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

        [Test]
        [Category("TsavoriteKV")]
        public async Task LargeObjectTest([Values] CheckpointType checkpointType)
        {
            MyInput input = default;
            MyLargeOutput output = new();

            log = Devices.CreateLogDevice(test_path + "/LargeObjectTest.log");
            objlog = Devices.CreateLogDevice(test_path + "/LargeObjectTest.obj.log");

            store1 = new(128,
                new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, PageSizeBits = 21, MemorySizeBits = 26 },
                new CheckpointSettings { CheckpointDir = test_path },
                new SerializerSettings<MyKey, MyLargeValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyLargeValueSerializer() }
                );

            int maxSize = 100;
            int numOps = 5000;

            using (var s = store1.NewSession<MyInput, MyLargeOutput, Empty, MyLargeFunctions>(functions))
            {
                Random r = new Random(33);
                for (int key = 0; key < numOps; key++)
                {
                    var mykey = new MyKey { key = key };
                    var value = new MyLargeValue(1 + r.Next(maxSize));
                    s.Upsert(ref mykey, ref value, Empty.Default, 0);
                }
            }

            store1.TryInitiateFullCheckpoint(out Guid token, checkpointType);
            await store1.CompleteCheckpointAsync();

            store1.Dispose();
            log.Dispose();
            objlog.Dispose();

            log = Devices.CreateLogDevice(test_path + "/LargeObjectTest.log");
            objlog = Devices.CreateLogDevice(test_path + "/LargeObjectTest.obj.log");

            store2 = new(128,
                new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, PageSizeBits = 21, MemorySizeBits = 26 },
                new CheckpointSettings { CheckpointDir = test_path },
                new SerializerSettings<MyKey, MyLargeValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyLargeValueSerializer() }
                );

            store2.Recover(token);
            using (var s2 = store2.NewSession<MyInput, MyLargeOutput, Empty, MyLargeFunctions>(functions))
            {
                for (int keycnt = 0; keycnt < numOps; keycnt++)
                {
                    var key = new MyKey { key = keycnt };
                    var status = s2.Read(ref key, ref input, ref output, Empty.Default, 0);

                    if (status.IsPending)
                        await s2.CompletePendingAsync();
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