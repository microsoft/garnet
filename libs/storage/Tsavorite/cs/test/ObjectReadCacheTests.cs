// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.ReadCacheTests
{
    [TestFixture]
    internal class ObjectReadCacheTests
    {
        private TsavoriteKV<MyKey, MyValue> store;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            var readCacheSettings = new ReadCacheSettings { MemorySizeBits = 15, PageSizeBits = 10 };
            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/ObjectReadCacheTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/ObjectReadCacheTests.obj.log", deleteOnClose: true);

            store = new TsavoriteKV<MyKey, MyValue>
                (128,
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MemorySizeBits = 15, PageSizeBits = 10, ReadCacheSettings = readCacheSettings },
                serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
                );
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ObjectDiskWriteReadCache()
        {
            using var session = store.NewSession<MyInput, MyOutput, Empty, MyFunctions>(new MyFunctions());

            MyInput input = default;

            for (int i = 0; i < 2000; i++)
            {
                var key = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key, ref value, Empty.Default, 0);
            }
            session.CompletePending(true);

            // Evict all records from main memory of hybrid log
            store.Log.FlushAndEvict(true);

            // Read 2000 keys - all should be served from disk, populating and evicting the read cache FIFO
            for (int i = 0; i < 2000; i++)
            {
                MyOutput output = new();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status.IsPending);
                session.CompletePending(true);
            }

            // Read last 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status.Found);
                Assert.AreEqual(value.value, output.value.value);
            }

            // Evict the read cache entirely
            store.ReadCache.FlushAndEvict(true);

            // Read 100 keys - all should be served from disk, populating cache
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status.IsPending);
                session.CompletePending(true);
            }

            // Read 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status.Found);
                Assert.AreEqual(value.value, output.value.value);
            }


            // Upsert to overwrite the read cache
            for (int i = 1900; i < 1950; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i + 1 };
                session.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            // RMW to overwrite the read cache
            for (int i = 1950; i < 2000; i++)
            {
                var key1 = new MyKey { key = i };
                input = new MyInput { value = 1 };
                var status = session.RMW(ref key1, ref input, Empty.Default, 0);
                if (status.IsPending)
                    session.CompletePending(true);
            }

            // Read the 100 keys
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i + 1 };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status.Found, $"key = {key1.key}");
                Assert.AreEqual(value.value, output.value.value);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjectDiskWriteReadCache2()
        {
            using var session = store.NewSession<MyInput, MyOutput, Empty, MyFunctions>(new MyFunctions());

            MyInput input = default;

            for (int i = 0; i < 2000; i++)
            {
                var key = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key, ref value, Empty.Default, 0);
            }
            session.CompletePending(true);

            // Dispose the hybrid log from memory entirely
            store.Log.DisposeFromMemory();

            // Read 2000 keys - all should be served from disk, populating and evicting the read cache FIFO
            for (int i = 0; i < 2000; i++)
            {
                MyOutput output = new();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status.IsPending);
                session.CompletePending(true);
            }

            // Read last 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status.Found);
                Assert.AreEqual(value.value, output.value.value);
            }

            // Evict the read cache entirely
            store.ReadCache.FlushAndEvict(true);

            // Read 100 keys - all should be served from disk, populating cache
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status.IsPending);
                session.CompletePending(true);
            }

            // Read 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new();
                MyKey key1 = new() { key = i };
                MyValue value = new() { value = i };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status.Found);
                Assert.AreEqual(value.value, output.value.value);
            }
        }
    }
}