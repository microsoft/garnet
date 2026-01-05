// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
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
    internal class GenericIterationTests : AllureTestBase
    {
        private TsavoriteKV<MyKey, MyValue, ClassStoreFunctions, ClassAllocator> store;
        private ClientSession<MyKey, MyValue, MyInput, MyOutput, int, MyFunctionsDelete, ClassStoreFunctions, ClassAllocator> session;
        private BasicContext<MyKey, MyValue, MyInput, MyOutput, int, MyFunctionsDelete, ClassStoreFunctions, ClassAllocator> bContext;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            // Tests call InternalSetup()
        }

        private void InternalSetup(bool largeMemory)
        {
            // Broke this out as we have different requirements by test.
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "GenericIterationTests.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "GenericIterationTests.obj.log"), deleteOnClose: true);

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                MemorySize = 1L << (largeMemory ? 25 : 14),
                PageSize = 1L << (largeMemory ? 20 : 9)
            }, StoreFunctions<MyKey, MyValue>.Create(new MyKey.Comparer(), () => new MyKeySerializer(), () => new MyValueSerializer(), DefaultRecordDisposer<MyKey, MyValue>.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
            session = store.NewSession<MyInput, MyOutput, int, MyFunctionsDelete>(new MyFunctionsDelete());
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

        internal struct GenericPushIterationTestFunctions : IScanIteratorFunctions<MyKey, MyValue>
        {
            internal int keyMultToValue;
            internal long numRecords;
            internal int stopAt;

            public bool SingleReader(ref MyKey key, ref MyValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                if (keyMultToValue > 0)
                    ClassicAssert.AreEqual(key.key * keyMultToValue, value.value);
                return stopAt != ++numRecords;
            }

            public bool ConcurrentReader(ref MyKey key, ref MyValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);
            public readonly bool OnStart(long beginAddress, long endAddress) => true;
            public readonly void OnException(Exception exception, long numberOfRecords) { }
            public readonly void OnStop(bool completed, long numberOfRecords) { }
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]

        public void GenericIterationBasicTest([Values] ScanIteratorType scanIteratorType)
        {
            InternalSetup(largeMemory: false);
            GenericPushIterationTestFunctions scanIteratorFunctions = new();

            const int totalRecords = 2000;

            void iterateAndVerify(int keyMultToValue, int expectedRecs)
            {
                scanIteratorFunctions.keyMultToValue = keyMultToValue;
                scanIteratorFunctions.numRecords = 0;

                if (scanIteratorType == ScanIteratorType.Pull)
                {
                    using var iter = session.Iterate();
                    while (iter.GetNext(out var recordInfo))
                        _ = scanIteratorFunctions.SingleReader(ref iter.GetKey(), ref iter.GetValue(), default, default, out _);
                }
                else
                    ClassicAssert.IsTrue(session.Iterate(ref scanIteratorFunctions), $"Failed to complete push iteration; numRecords = {scanIteratorFunctions.numRecords}");

                ClassicAssert.AreEqual(expectedRecs, scanIteratorFunctions.numRecords);
            }

            // Initial population
            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                _ = bContext.Upsert(ref key1, ref value);
            }
            iterateAndVerify(1, totalRecords);

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = 2 * i };
                _ = bContext.Upsert(ref key1, ref value);
            }
            iterateAndVerify(2, totalRecords);

            for (int i = totalRecords / 2; i < totalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                _ = bContext.Upsert(ref key1, ref value);
            }
            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                _ = bContext.Upsert(ref key1, ref value);
            }
            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                var key1 = new MyKey { key = i };
                _ = bContext.Delete(ref key1);
            }
            iterateAndVerify(0, totalRecords / 2);

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = 3 * i };
                _ = bContext.Upsert(ref key1, ref value);
            }
            iterateAndVerify(3, totalRecords);

            store.Log.FlushAndEvict(wait: true);
            iterateAndVerify(3, totalRecords);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]

        public void GenericIterationPushStopTest()
        {
            InternalSetup(largeMemory: false);
            GenericPushIterationTestFunctions scanIteratorFunctions = new();

            const int totalRecords = 2000;
            var start = store.Log.TailAddress;

            void scanAndVerify(int stopAt, bool useScan)
            {
                scanIteratorFunctions.numRecords = 0;
                scanIteratorFunctions.stopAt = stopAt;
                if (useScan)
                    ClassicAssert.IsFalse(store.Log.Scan(ref scanIteratorFunctions, start, store.Log.TailAddress), $"Failed to terminate push iteration early; numRecords = {scanIteratorFunctions.numRecords}");
                else
                    ClassicAssert.IsFalse(session.Iterate(ref scanIteratorFunctions), $"Failed to terminate push iteration early; numRecords = {scanIteratorFunctions.numRecords}");
                ClassicAssert.AreEqual(stopAt, scanIteratorFunctions.numRecords);
            }

            // Initial population
            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                _ = bContext.Upsert(ref key1, ref value);
            }

            scanAndVerify(42, useScan: true);
            scanAndVerify(42, useScan: false);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void GenericIterationPushLockTest([Values(1, 4)] int scanThreads, [Values(1, 4)] int updateThreads, [Values] ScanMode scanMode)
        {
            InternalSetup(largeMemory: true);

            const int totalRecords = 2000;
            var start = store.Log.TailAddress;

            void LocalScan(int i)
            {
                using var session = store.NewSession<MyInput, MyOutput, int, MyFunctionsDelete>(new MyFunctionsDelete());
                GenericPushIterationTestFunctions scanIteratorFunctions = new();

                if (scanMode == ScanMode.Scan)
                    ClassicAssert.IsTrue(store.Log.Scan(ref scanIteratorFunctions, start, store.Log.TailAddress), $"Failed to complete push scan; numRecords = {scanIteratorFunctions.numRecords}");
                else
                    ClassicAssert.IsTrue(session.Iterate(ref scanIteratorFunctions), $"Failed to complete push iteration; numRecords = {scanIteratorFunctions.numRecords}");
                ClassicAssert.AreEqual(totalRecords, scanIteratorFunctions.numRecords);
            }

            void LocalUpdate(int tid)
            {
                using var session = store.NewSession<MyInput, MyOutput, int, MyFunctionsDelete>(new MyFunctionsDelete());
                for (int i = 0; i < totalRecords; i++)
                {
                    var key1 = new MyKey { key = i };
                    var value = new MyValue { value = (tid + 1) * i };
                    _ = bContext.Upsert(ref key1, ref value);
                }
            }

            { // Initial population
                for (int i = 0; i < totalRecords; i++)
                {
                    var key1 = new MyKey { key = i };
                    var value = new MyValue { value = i };
                    _ = bContext.Upsert(ref key1, ref value);
                }
            }

            List<Task> tasks = [];   // Task rather than Thread for propagation of exception.
            var numThreads = scanThreads + updateThreads;
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                if (t < scanThreads)
                    tasks.Add(Task.Factory.StartNew(() => LocalScan(tid)));
                else
                    tasks.Add(Task.Factory.StartNew(() => LocalUpdate(tid)));
            }
            Task.WaitAll([.. tasks]);
        }
    }
}