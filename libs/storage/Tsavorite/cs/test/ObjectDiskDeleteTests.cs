// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if LOGRECORD_TODO

using System.IO;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>;

    [AllureNUnit]
    [TestFixture]
    internal class ObjectDiskDeleteTests : AllureTestBase
    {
        private TsavoriteKV<ClassStoreFunctions, ClassAllocator> store;
        private ClientSession<TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete, ClassStoreFunctions, ClassAllocator> session;
        private BasicContext<TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete, ClassStoreFunctions, ClassAllocator> bContext;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectDiskDeleteTests.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectDiskDeleteTests.obj.log"), deleteOnClose: true);

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                MemorySize = 1L << 14,
                PageSize = 1L << 9
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
            session = store.NewSession<TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete());
            bContext = session.BasicContext;
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
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

        public void DiskDeleteBasicTest1()
        {
            const int totalRecords = 2000;
            var start = store.Log.TailAddress;
            for (int i = 0; i < totalRecords; i++)
            {
                var _key = new TestObjectKey { key = i };
                var _value = new TestObjectValue { value = i };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref _key), _value, 0);
            }

            for (int i = 0; i < totalRecords; i++)
            {
                var input = new TestObjectInput();
                var output = new TestObjectOutput();
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };

                if (bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, 0).IsPending)
                    _ = bContext.CompletePending(true);
                else
                    ClassicAssert.AreEqual(value.value, output.value.value);
            }

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new TestObjectKey { key = i };
                _ = bContext.Delete(SpanByte.FromPinnedVariable(ref key1));
            }

            for (int i = 0; i < totalRecords; i++)
            {
                var input = new TestObjectInput();
                var output = new TestObjectOutput();
                var key1 = new TestObjectKey { key = i };

                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, 1);

                if (status.IsPending)
                {
                    _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, _) = GetSinglePendingResult(outputs);
                }
                ClassicAssert.IsFalse(status.Found);
            }


            using var iter = store.Log.Scan(start, store.Log.TailAddress, DiskScanBufferingMode.SinglePageBuffering);
            int val = 0;
            while (iter.GetNext())
            {
                if (iter.Info.Tombstone)
                    val++;
            }
            ClassicAssert.AreEqual(val, totalRecords);
        }


        [Test]
        [Category("TsavoriteKV")]
        public void DiskDeleteBasicTest2()
        {
            const int totalRecords = 2000;
            for (int i = 0; i < totalRecords; i++)
            {
                var _key = new TestObjectKey { key = i };
                var _value = new TestObjectValue { value = i };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref _key), _value, 0);
            }

            var key100 = new TestObjectKey { key = 100 };
            var value100 = new TestObjectValue { value = 100 };
            var key200 = new TestObjectKey { key = 200 };

            _ = bContext.Delete(SpanByte.FromPinnedVariable(ref key100));

            var input = new TestObjectInput { value = 1000 };
            var output = new TestObjectOutput();
            var status = bContext.Read(SpanByte.FromPinnedVariable(ref key100), ref input, ref output, 1);
            ClassicAssert.IsFalse(status.Found, status.ToString());

            status = bContext.Upsert(SpanByte.FromPinnedVariable(ref key100), value100, 0);
            ClassicAssert.IsTrue(!status.Found, status.ToString());

            status = bContext.Read(SpanByte.FromPinnedVariable(ref key100), ref input, ref output, 0);
            ClassicAssert.IsTrue(status.Found, status.ToString());
            ClassicAssert.AreEqual(value100.value, output.value.value);

            _ = bContext.Delete(SpanByte.FromPinnedVariable(ref key100));
            _ = bContext.Delete(SpanByte.FromPinnedVariable(ref key200));

            // This RMW should create new initial value, since item is deleted
            status = bContext.RMW(SpanByte.FromPinnedVariable(ref key200), ref input, 1);
            ClassicAssert.IsFalse(status.Found);

            status = bContext.Read(SpanByte.FromPinnedVariable(ref key200), ref input, ref output, 0);
            ClassicAssert.IsTrue(status.Found, status.ToString());
            ClassicAssert.AreEqual(input.value, output.value.value);

            // Delete key 200 again
            _ = bContext.Delete(SpanByte.FromPinnedVariable(ref key200));

            // Eliminate all records from memory
            for (int i = 201; i < 2000; i++)
            {
                var _key = new TestObjectKey { key = i };
                var _value = new TestObjectValue { value = i };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref _key), _value, 0);
            }
            status = bContext.Read(SpanByte.FromPinnedVariable(ref key100), ref input, ref output, 1);
            ClassicAssert.IsTrue(status.IsPending);
            _ = bContext.CompletePending(true);

            // This RMW should create new initial value, since item is deleted
            status = bContext.RMW(SpanByte.FromPinnedVariable(ref key200), ref input, 1);
            ClassicAssert.IsTrue(status.IsPending);
            _ = bContext.CompletePending(true);

            status = bContext.Read(SpanByte.FromPinnedVariable(ref key200), ref input, ref output, 0);
            ClassicAssert.IsTrue(status.Found, status.ToString());
            ClassicAssert.AreEqual(input.value, output.value.value);
        }
    }
}

#endif // LOGRECORD_TODO
