// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test.ReadCacheTests
{
    using ClassAllocator = GenericAllocator<MyKey, MyValue, StoreFunctions<MyKey, MyValue, MyKey.Comparer, DefaultRecordDisposer<MyKey, MyValue>>>;
    using ClassStoreFunctions = StoreFunctions<MyKey, MyValue, MyKey.Comparer, DefaultRecordDisposer<MyKey, MyValue>>;

    [AllureNUnit]
    [TestFixture]
    internal class ObjectReadCacheTests : AllureTestBase
    {
        private TsavoriteKV<MyKey, MyValue, ClassStoreFunctions, ClassAllocator> store;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "ObjectReadCacheTests.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "ObjectReadCacheTests.obj.log"), deleteOnClose: true);

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MemorySize = 1L << 15,
                PageSize = 1L << 10,
                ReadCacheMemorySize = 1L << 15,
                ReadCachePageSize = 1L << 10,
                ReadCacheEnabled = true
            }, StoreFunctions<MyKey, MyValue>.Create(new MyKey.Comparer(), () => new MyKeySerializer(), () => new MyValueSerializer(), DefaultRecordDisposer<MyKey, MyValue>.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
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
            TestUtils.OnTearDown();
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ObjectDiskWriteReadCache()
        {
            using var session = store.NewSession<MyInput, MyOutput, Empty, MyFunctions>(new MyFunctions());
            var bContext = session.BasicContext;

            MyInput input = default;

            for (int i = 0; i < 2000; i++)
            {
                var key = new MyKey { key = i };
                var value = new MyValue { value = i };
                bContext.Upsert(ref key, ref value, Empty.Default);
            }
            bContext.CompletePending(true);

            // Evict all records from main memory of hybrid log
            store.Log.FlushAndEvict(true);

            // Read 2000 keys - all should be served from disk, populating and evicting the read cache FIFO
            for (int i = 0; i < 2000; i++)
            {
                MyOutput output = new();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = bContext.Read(ref key1, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.IsPending);
                bContext.CompletePending(true);
            }

            // Read last 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = bContext.Read(ref key1, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(value.value, output.value.value);
            }

            // Evict the read cache entirely
            store.ReadCache.FlushAndEvict(true);

            // Read 100 keys - all should be served from disk, populating cache
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = bContext.Read(ref key1, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.IsPending);
                bContext.CompletePending(true);
            }

            // Read 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = bContext.Read(ref key1, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(value.value, output.value.value);
            }


            // Upsert to overwrite the read cache
            for (int i = 1900; i < 1950; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i + 1 };
                bContext.Upsert(ref key1, ref value, Empty.Default);
            }

            // RMW to overwrite the read cache
            for (int i = 1950; i < 2000; i++)
            {
                var key1 = new MyKey { key = i };
                input = new MyInput { value = 1 };
                var status = bContext.RMW(ref key1, ref input, Empty.Default);
                if (status.IsPending)
                    bContext.CompletePending(true);
            }

            // Read the 100 keys
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i + 1 };

                var status = bContext.Read(ref key1, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.Found, $"key = {key1.key}");
                ClassicAssert.AreEqual(value.value, output.value.value);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjectDiskWriteReadCache2()
        {
            using var session = store.NewSession<MyInput, MyOutput, Empty, MyFunctions>(new MyFunctions());
            var bContext = session.BasicContext;

            MyInput input = default;

            for (int i = 0; i < 2000; i++)
            {
                var key = new MyKey { key = i };
                var value = new MyValue { value = i };
                bContext.Upsert(ref key, ref value, Empty.Default);
            }
            bContext.CompletePending(true);

            // Dispose the hybrid log from memory entirely
            store.Log.DisposeFromMemory();

            // Read 2000 keys - all should be served from disk, populating and evicting the read cache FIFO
            for (int i = 0; i < 2000; i++)
            {
                MyOutput output = new();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = bContext.Read(ref key1, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.IsPending);
                bContext.CompletePending(true);
            }

            // Read last 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = bContext.Read(ref key1, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(value.value, output.value.value);
            }

            // Evict the read cache entirely
            store.ReadCache.FlushAndEvict(true);

            // Read 100 keys - all should be served from disk, populating cache
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = bContext.Read(ref key1, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.IsPending);
                bContext.CompletePending(true);
            }

            // Read 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new();
                MyKey key1 = new() { key = i };
                MyValue value = new() { value = i };

                var status = bContext.Read(ref key1, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(value.value, output.value.value);
            }
        }
    }
}