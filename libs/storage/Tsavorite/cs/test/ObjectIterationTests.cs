// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>;

    [TestFixture]
    internal class ObjectIterationTests
    {
        private TsavoriteKV<ClassStoreFunctions, ClassAllocator> store;
        private ClientSession<TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete, ClassStoreFunctions, ClassAllocator> session;
        private BasicContext<TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete, ClassStoreFunctions, ClassAllocator> bContext;
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
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectIterationTests.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectIterationTests.obj.log"), deleteOnClose: true);

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                MemorySize = 1L << (largeMemory ? 25 : 14),
                PageSize = 1L << (largeMemory ? 20 : 9)
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer(), DefaultRecordDisposer.Instance)
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

        internal struct ObjectPushIterationTestFunctions : IScanIteratorFunctions
        {
            internal int keyMultToValue;
            internal long numRecords;
            internal int stopAt;

            public bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                if (keyMultToValue > 0)
                    ClassicAssert.AreEqual(logRecord.Key.AsRef<TestObjectKey>().key * keyMultToValue, ((TestObjectValue)logRecord.ValueObject).value);
                return stopAt != ++numRecords;
            }

            public readonly bool OnStart(long beginAddress, long endAddress) => true;
            public readonly void OnException(Exception exception, long numberOfRecords) { }
            public readonly void OnStop(bool completed, long numberOfRecords) { }
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]

        public void ObjectIterationBasicTest([Values] ScanIteratorType scanIteratorType)
        {
            InternalSetup(largeMemory: false);
            ObjectPushIterationTestFunctions scanIteratorFunctions = new();

            const int totalRecords = 2000;

            void iterateAndVerify(int keyMultToValue, int expectedRecs)
            {
                scanIteratorFunctions.keyMultToValue = keyMultToValue;
                scanIteratorFunctions.numRecords = 0;

                if (scanIteratorType == ScanIteratorType.Pull)
                {
                    using var iter = session.Iterate();
                    while (iter.GetNext())
                        _ = scanIteratorFunctions.Reader(in iter, default, default, out _);
                }
                else
                    ClassicAssert.IsTrue(session.Iterate(ref scanIteratorFunctions), $"Failed to complete push iteration; numRecords = {scanIteratorFunctions.numRecords}");

                ClassicAssert.AreEqual(expectedRecs, scanIteratorFunctions.numRecords);
            }

            // Initial population
            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), value);
            }
            iterateAndVerify(1, totalRecords);

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = 2 * i };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), value);
            }
            iterateAndVerify(2, totalRecords);

            for (int i = totalRecords / 2; i < totalRecords; i++)
            {
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), value);
            }
            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), value);
            }
            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                var key1 = new TestObjectKey { key = i };
                _ = bContext.Delete(SpanByte.FromPinnedVariable(ref key1));
            }
            iterateAndVerify(0, totalRecords / 2);

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = 3 * i };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), value);
            }
            iterateAndVerify(3, totalRecords);

            store.Log.FlushAndEvict(wait: true);
            iterateAndVerify(3, totalRecords);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]

        public void ObjectIterationPushStopTest()
        {
            InternalSetup(largeMemory: false);
            ObjectPushIterationTestFunctions scanIteratorFunctions = new();

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
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), value);
            }

            scanAndVerify(42, useScan: true);
            scanAndVerify(42, useScan: false);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]
        //[Repeat(3000)]
        public void ObjectIterationPushLockTest([Values(1, 2, 4, 8)] int scanThreads, [Values(0, 1, 4)] int updateThreads, [Values] ScanMode scanMode, [Values] bool largeMemory)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1}, name = {TestContext.CurrentContext.Test.Name} ***");

            InternalSetup(largeMemory);

            const int totalRecords = 2000;
            var start = store.Log.TailAddress;

            void LocalScan(int i)
            {
                using var session = store.NewSession<TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete());
                ObjectPushIterationTestFunctions scanIteratorFunctions = new();

                var end = store.Log.TailAddress;
                if (scanMode == ScanMode.Scan)
                    Assert.That(store.Log.Scan(ref scanIteratorFunctions, start, end), Is.True, $"Failed to complete push scan; numRecords = {scanIteratorFunctions.numRecords}, start = {start}, end = {end}");
                else
                    Assert.That(session.Iterate(ref scanIteratorFunctions), Is.True, $"Failed to complete push iteration; numRecords = {scanIteratorFunctions.numRecords}, start = {start}, end = {end}");

                // If we are doing Scan with updates and without largeMemory, there will be records appended at the log tail due to not 
                // being able to do IPU, so the scan count may be > totalRecords.
                if (scanMode == ScanMode.Scan && !largeMemory && updateThreads > 0)
                    Assert.That(scanIteratorFunctions.numRecords, Is.GreaterThanOrEqualTo(totalRecords));
                else
                    Assert.That(scanIteratorFunctions.numRecords, Is.EqualTo(totalRecords));
            }

            const int keyTag = 0x420000;

            void LocalUpdate(int tid)
            {
                using var session = store.NewSession<TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete());
                for (int i = 0; i < totalRecords; i++)
                {
                    var key1 = new TestObjectKey { key = i + keyTag };
                    var value = new TestObjectValue { value = (tid + 1) * i };
                    _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), value);
                }
            }

            { // Initial population
                for (int i = 0; i < totalRecords; i++)
                {
                    var key1 = new TestObjectKey { key = i + keyTag };
                    var value = new TestObjectValue { value = i + 340000 };
                    _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), value);
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