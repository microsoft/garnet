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
    using StructStoreFunctions = StoreFunctions<KeyStruct, ValueStruct, KeyStruct.Comparer, DefaultRecordDisposer<KeyStruct, ValueStruct>>;

    [AllureNUnit]
    [TestFixture]
    internal class BlittableIterationTests : AllureTestBase
    {
        private TsavoriteKV<KeyStruct, ValueStruct, StructStoreFunctions, BlittableAllocator<KeyStruct, ValueStruct, StructStoreFunctions>> store;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            DeleteDirectory(MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            OnTearDown();
        }

        internal struct BlittablePushIterationTestFunctions : IScanIteratorFunctions<KeyStruct, ValueStruct>
        {
            internal int keyMultToValue;
            internal long numRecords;
            internal int stopAt;

            public bool SingleReader(ref KeyStruct key, ref ValueStruct value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                if (keyMultToValue > 0)
                    ClassicAssert.AreEqual(key.kfield1 * keyMultToValue, value.vfield1);
                return stopAt != ++numRecords;
            }

            public bool ConcurrentReader(ref KeyStruct key, ref ValueStruct value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);
            public readonly bool OnStart(long beginAddress, long endAddress) => true;
            public readonly void OnException(Exception exception, long numberOfRecords) { }
            public readonly void OnStop(bool completed, long numberOfRecords) { }
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]
        public void BlittableIterationBasicTest([Values] TestDeviceType deviceType, [Values] ScanIteratorType scanIteratorType)
        {
            log = CreateTestDevice(deviceType, Path.Join(MethodTestDir, $"{deviceType}.log"));

            store = new(
                new()
                {
                    IndexSize = 1L << 26,
                    LogDevice = log,
                    MemorySize = 1L << 15,
                    PageSize = 1L << 9,
                    SegmentSize = 1L << 22
                }, StoreFunctions<KeyStruct, ValueStruct>.Create(KeyStruct.Comparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<InputStruct, OutputStruct, int, FunctionsCompaction>(new FunctionsCompaction());
            var bContext = session.BasicContext;

            BlittablePushIterationTestFunctions scanIteratorFunctions = new();

            const int totalRecords = 500;

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
            for (var i = 0; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                _ = bContext.Upsert(ref key1, ref value);
            }
            iterateAndVerify(1, totalRecords);

            for (var i = 0; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = 2 * i, vfield2 = i + 1 };
                _ = bContext.Upsert(ref key1, ref value);
            }
            iterateAndVerify(2, totalRecords);

            for (var i = totalRecords / 2; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                _ = bContext.Upsert(ref key1, ref value);
            }
            iterateAndVerify(0, totalRecords);

            for (var i = 0; i < totalRecords; i += 2)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                _ = bContext.Upsert(ref key1, ref value);
            }
            iterateAndVerify(0, totalRecords);

            for (var i = 0; i < totalRecords; i += 2)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                _ = bContext.Delete(ref key1);
            }
            iterateAndVerify(0, totalRecords / 2);

            for (var i = 0; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = 3 * i, vfield2 = i + 1 };
                _ = bContext.Upsert(ref key1, ref value);
            }
            iterateAndVerify(3, totalRecords);

            store.Log.FlushAndEvict(wait: true);
            iterateAndVerify(3, totalRecords);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]
        public void BlittableIterationPushStopTest()
        {
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "stop_test.log"));

            store = new(
                new()
                {
                    IndexSize = 1L << 26,
                    LogDevice = log,
                    MemorySize = 1L << 15,
                    PageSize = 1L << 9,
                    SegmentSize = 1L << 22
                }, StoreFunctions<KeyStruct, ValueStruct>.Create(KeyStruct.Comparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<InputStruct, OutputStruct, int, FunctionsCompaction>(new FunctionsCompaction());
            var bContext = session.BasicContext;
            BlittablePushIterationTestFunctions scanIteratorFunctions = new();

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
            for (var i = 0; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                _ = bContext.Upsert(ref key1, ref value);
            }

            scanAndVerify(42, useScan: true);
            scanAndVerify(42, useScan: false);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void BlittableIterationPushLockTest([Values(1, 4)] int scanThreads, [Values(1, 4)] int updateThreads, [Values] ScanMode scanMode)
        {
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "lock_test.log"));

            // Must be large enough to contain all records in memory to exercise locking
            store = new(
                new()
                {
                    IndexSize = 1L << 26,
                    LogDevice = log,
                    MemorySize = 1L << 25,
                    PageSize = 1L << 20,
                    SegmentSize = 1L << 22
                }, StoreFunctions<KeyStruct, ValueStruct>.Create(KeyStruct.Comparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            const int totalRecords = 2000;
            var start = store.Log.TailAddress;

            void LocalScan(int i)
            {
                using var session = store.NewSession<InputStruct, OutputStruct, int, FunctionsCompaction>(new FunctionsCompaction());
                BlittablePushIterationTestFunctions scanIteratorFunctions = new();
                if (scanMode == ScanMode.Scan)
                    ClassicAssert.IsTrue(store.Log.Scan(ref scanIteratorFunctions, start, store.Log.TailAddress), $"Failed to complete push scan; numRecords = {scanIteratorFunctions.numRecords}");
                else
                    ClassicAssert.IsTrue(session.Iterate(ref scanIteratorFunctions), $"Failed to complete push iteration; numRecords = {scanIteratorFunctions.numRecords}");
                ClassicAssert.AreEqual(totalRecords, scanIteratorFunctions.numRecords);
            }

            void LocalUpdate(int tid)
            {
                using var session = store.NewSession<InputStruct, OutputStruct, int, FunctionsCompaction>(new FunctionsCompaction());
                var bContext = session.BasicContext;
                for (var iteration = 0; iteration < 2; ++iteration)
                {
                    for (var i = 0; i < totalRecords; i++)
                    {
                        var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                        var value = new ValueStruct { vfield1 = (tid + 1) * i, vfield2 = i + 1 };
                        _ = bContext.Upsert(ref key1, ref value, 0);
                    }
                }
            }

            { // Initial population
                using var session = store.NewSession<InputStruct, OutputStruct, int, FunctionsCompaction>(new FunctionsCompaction());
                var bContext = session.BasicContext;
                for (var i = 0; i < totalRecords; i++)
                {
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                    _ = bContext.Upsert(ref key1, ref value);
                }
            }

            List<Task> tasks = [];   // Task rather than Thread for propagation of exception.
            var numThreads = scanThreads + updateThreads;
            for (var t = 0; t < numThreads; t++)
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