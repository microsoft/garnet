// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using ClassAllocator = ObjectAllocator<TestObjectValue, StoreFunctions<TestObjectValue, TestObjectKey.Comparer, DefaultRecordDisposer<TestObjectValue>>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectValue, TestObjectKey.Comparer, DefaultRecordDisposer<TestObjectValue>>;

    [TestFixture]
    internal class ObjectTests
    {
        private TsavoriteKV<TestObjectValue, ClassStoreFunctions, ClassAllocator> store;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectTests.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectTests.obj.log"), deleteOnClose: true);

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                MemorySize = 1L << 15,
                PageSize = 1L << 10
            }, StoreFunctions<TestObjectValue>.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer(), DefaultRecordDisposer<TestObjectValue>.Instance)
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
            DeleteDirectory(MethodTestDir);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ObjectInMemWriteRead()
        {
            using var session = store.NewSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;

            TestObjectKey keyStruct = new() { key = 9999999 };
            SpanByte key = SpanByteFrom(ref keyStruct);
            TestObjectValue value = new() { value = 23 };

            TestObjectInput input = default;
            TestObjectOutput output = default;

            _ = bContext.Upsert(key, value, Empty.Default);
            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            ClassicAssert.AreEqual(value.value, output.value.value);
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjectInMemWriteRead2()
        {
            using var session = store.NewSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;

            TestObjectKey key1Struct = new() { key = 8999998 };
            SpanByte key1 = SpanByteFrom(ref key1Struct);
            TestObjectInput input1 = new() { value = 23 };
            TestObjectOutput output = new();

            _ = bContext.RMW(key1, ref input1, Empty.Default);

            TestObjectKey key2Struct = new() { key = 8999999 };
            SpanByte key2 = SpanByteFrom(ref key2Struct);
            TestObjectInput input2 = new() { value = 24 };
            _ = bContext.RMW(key2, ref input2, Empty.Default);

            _ = bContext.Read(key1, ref input1, ref output, Empty.Default);

            ClassicAssert.AreEqual(input1.value, output.value.value);

            _ = bContext.Read(key2, ref input2, ref output, Empty.Default);
            ClassicAssert.AreEqual(input2.value, output.value.value);

        }


        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ObjectDiskWriteRead()
        {
            using var session = store.NewSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;

            for (int i = 0; i < 2000; i++)
            {
                var key1Struct = new TestObjectKey { key = i };
                SpanByte key = SpanByteFrom(ref key1Struct);
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(key, value, Empty.Default);
                // store.ShiftReadOnlyAddress(store.LogTailAddress);
            }

            TestObjectKey key2Struct = new() { key = 23 };
            SpanByte key2 = SpanByteFrom(ref key2Struct);
            TestObjectInput input = new();
            TestObjectOutput g1 = new();
            var status = bContext.Read(key2, ref input, ref g1, Empty.Default);

            if (status.IsPending)
            {
                _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                (status, g1) = GetSinglePendingResult(outputs);
            }

            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(23, g1.value.value);

            key2Struct.key = 99999;
            status = bContext.Read(key2, ref input, ref g1, Empty.Default);

            if (status.IsPending)
                (status, _) = bContext.GetSinglePendingResult();
            ClassicAssert.IsFalse(status.Found);

            // Update last 100 using RMW in memory
            for (int i = 1900; i < 2000; i++)
            {
                var keyStruct = new TestObjectKey { key = i };
                SpanByte key = SpanByteFrom(ref keyStruct);
                input = new TestObjectInput { value = 1 };
                status = bContext.RMW(key, ref input, Empty.Default);
                ClassicAssert.IsFalse(status.IsPending, "Expected RMW to complete in-memory");
            }

            // Update first 100 using RMW from storage
            for (int i = 0; i < 100; i++)
            {
                var keyStruct = new TestObjectKey { key = i };
                SpanByte key = SpanByteFrom(ref keyStruct);
                input = new TestObjectInput { value = 1 };
                status = bContext.RMW(key, ref input, Empty.Default);
                if (status.IsPending)
                    _ = bContext.CompletePending(true);
            }

            for (int i = 0; i < 2000; i++)
            {
                var output = new TestObjectOutput();
                var keyStruct = new TestObjectKey { key = i };
                SpanByte key = SpanByteFrom(ref keyStruct);
                var value = new TestObjectValue { value = i };

                status = bContext.Read(key, ref input, ref output, Empty.Default);
                if (status.IsPending)
                    (status, output) = bContext.GetSinglePendingResult();
                else
                {
                    if (i < 100 || i >= 1900)
                        ClassicAssert.AreEqual(value.value + 1, output.value.value);
                    else
                        ClassicAssert.AreEqual(value.value, output.value.value);
                }
            }
        }
    }
}
