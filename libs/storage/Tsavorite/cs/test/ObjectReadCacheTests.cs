// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test.ReadCacheTests
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>;

    [TestFixture]
    internal class ObjectReadCacheTests
    {
        private TsavoriteKV<ClassStoreFunctions, ClassAllocator> store;
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
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer(), DefaultRecordDisposer.Instance)
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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ObjectDiskWriteReadCache()
        {
            using var session = store.NewSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;

            TestObjectInput input = default;

            for (int i = 0; i < 2000; i++)
            {
                var key = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key), value, Empty.Default);
            }
            _ = bContext.CompletePending(true);

            // Evict all records from main memory of hybrid log
            store.Log.FlushAndEvict(true);

            // Read 2000 keys - all should be served from disk, populating and evicting the read cache FIFO
            for (int i = 0; i < 2000; i++)
            {
                TestObjectOutput output = new();
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };

                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.IsPending);
                _ = bContext.CompletePending(true);
            }

            // Read last 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                TestObjectOutput output = new();
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };

                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(value.value, output.value.value);
            }

            // Evict the read cache entirely
            store.ReadCache.FlushAndEvict(true);

            // Read 100 keys - all should be served from disk, populating cache
            for (int i = 1900; i < 2000; i++)
            {
                TestObjectOutput output = new();
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };

                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.IsPending);
                _ = bContext.CompletePending(true);
            }

            // Read 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                TestObjectOutput output = new();
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };

                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(value.value, output.value.value);
            }

            const int valueAdd = 100_000;

            // Upsert to overwrite the read cache
            for (int i = 1900; i < 1950; i++)
            {
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i + valueAdd };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), value, Empty.Default);
            }

            // RMW to overwrite the read cache
            for (int i = 1950; i < 2000; i++)
            {
                var key1 = new TestObjectKey { key = i };
                input = new TestObjectInput { value = valueAdd };
                var status = bContext.RMW(SpanByte.FromPinnedVariable(ref key1), ref input, Empty.Default);
                if (status.IsPending)
                    _ = bContext.CompletePending(true);
            }

            // Read the 100 keys
            for (int i = 1900; i < 2000; i++)
            {
                TestObjectOutput output = new();
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i + valueAdd };

                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.Found, $"key = {key1.key}");
                ClassicAssert.AreEqual(value.value, output.value.value);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjectDiskWriteReadCache2()
        {
            using var session = store.NewSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;

            TestObjectInput input = default;

            for (int i = 0; i < 2000; i++)
            {
                var key = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key), value, Empty.Default);
            }
            _ = bContext.CompletePending(true);

            store.Log.FlushAndEvict(wait: true);

            // Read 2000 keys - all should be served from disk, populating and evicting the read cache FIFO
            for (int i = 0; i < 2000; i++)
            {
                TestObjectOutput output = new();
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };

                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.IsPending);
                _ = bContext.CompletePending(true);
            }

            // Read last 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                TestObjectOutput output = new();
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };

                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(value.value, output.value.value);
            }

            // Evict the read cache entirely
            store.ReadCache.FlushAndEvict(true);

            // Read 100 keys - all should be served from disk, populating cache
            for (int i = 1900; i < 2000; i++)
            {
                TestObjectOutput output = new();
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };

                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.IsPending);
                _ = bContext.CompletePending(true);
            }

            // Read 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                TestObjectOutput output = new();
                TestObjectKey key1 = new() { key = i };
                TestObjectValue value = new() { value = i };

                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(value.value, output.value.value);
            }
        }
    }
}