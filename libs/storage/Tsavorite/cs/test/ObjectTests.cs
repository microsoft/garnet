// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using ClassAllocator = GenericAllocator<MyKey, MyValue, StoreFunctions<MyKey, MyValue, MyKey.Comparer, DefaultRecordDisposer<MyKey, MyValue>>>;
    using ClassStoreFunctions = StoreFunctions<MyKey, MyValue, MyKey.Comparer, DefaultRecordDisposer<MyKey, MyValue>>;

    [AllureNUnit]
    [TestFixture]
    internal class ObjectTests : AllureTestBase
    {
        private TsavoriteKV<MyKey, MyValue, ClassStoreFunctions, ClassAllocator> store;
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
            DeleteDirectory(MethodTestDir);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ObjectInMemWriteRead()
        {
            using var session = store.NewSession<MyInput, MyOutput, Empty, MyFunctions>(new MyFunctions());
            var bContext = session.BasicContext;

            MyKey key1 = new() { key = 9999999 };
            MyValue value = new() { value = 23 };

            MyInput input = null;
            MyOutput output = new();

            _ = bContext.Upsert(ref key1, ref value, Empty.Default);
            _ = bContext.Read(ref key1, ref input, ref output, Empty.Default);
            ClassicAssert.AreEqual(value.value, output.value.value);
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjectInMemWriteRead2()
        {
            using var session = store.NewSession<MyInput, MyOutput, Empty, MyFunctions>(new MyFunctions());
            var bContext = session.BasicContext;

            MyKey key1 = new() { key = 8999998 };
            MyInput input1 = new() { value = 23 };
            MyOutput output = new();

            _ = bContext.RMW(ref key1, ref input1, Empty.Default);

            MyKey key2 = new() { key = 8999999 };
            MyInput input2 = new() { value = 24 };
            _ = bContext.RMW(ref key2, ref input2, Empty.Default);

            _ = bContext.Read(ref key1, ref input1, ref output, Empty.Default);

            ClassicAssert.AreEqual(input1.value, output.value.value);

            _ = bContext.Read(ref key2, ref input2, ref output, Empty.Default);
            ClassicAssert.AreEqual(input2.value, output.value.value);

        }


        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ObjectDiskWriteRead()
        {
            using var session = store.NewSession<MyInput, MyOutput, Empty, MyFunctions>(new MyFunctions());
            var bContext = session.BasicContext;

            for (int i = 0; i < 2000; i++)
            {
                var key = new MyKey { key = i };
                var value = new MyValue { value = i };
                _ = bContext.Upsert(ref key, ref value, Empty.Default);
                // store.ShiftReadOnlyAddress(store.LogTailAddress);
            }

            MyKey key2 = new() { key = 23 };
            MyInput input = new();
            MyOutput g1 = new();
            var status = bContext.Read(ref key2, ref input, ref g1, Empty.Default);

            if (status.IsPending)
            {
                _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                (status, g1) = GetSinglePendingResult(outputs);
            }

            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(23, g1.value.value);

            key2 = new MyKey { key = 99999 };
            status = bContext.Read(ref key2, ref input, ref g1, Empty.Default);

            if (status.IsPending)
                (status, _) = bContext.GetSinglePendingResult();
            ClassicAssert.IsFalse(status.Found);

            // Update last 100 using RMW in memory
            for (int i = 1900; i < 2000; i++)
            {
                var key = new MyKey { key = i };
                input = new MyInput { value = 1 };
                status = bContext.RMW(ref key, ref input, Empty.Default);
                ClassicAssert.IsFalse(status.IsPending, "Expected RMW to complete in-memory");
            }

            // Update first 100 using RMW from storage
            for (int i = 0; i < 100; i++)
            {
                var key1 = new MyKey { key = i };
                input = new MyInput { value = 1 };
                status = bContext.RMW(ref key1, ref input, Empty.Default);
                if (status.IsPending)
                    _ = bContext.CompletePending(true);
            }

            for (int i = 0; i < 2000; i++)
            {
                var output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                status = bContext.Read(ref key1, ref input, ref output, Empty.Default);
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