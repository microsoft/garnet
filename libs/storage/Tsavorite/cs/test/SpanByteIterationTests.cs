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
    using SpanByteStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    [AllureNUnit]
    [TestFixture]
    internal class SpanByteIterationTests : AllureTestBase
    {
        private TsavoriteKV<SpanByte, SpanByte, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store;
        private IDevice log;

        // Note: We always set value.length to 2, which includes both VLValue members; we are not exercising the "Variable Length" aspect here.
        private const int ValueLength = 2;

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

        internal struct SpanBytePushIterationTestFunctions : IScanIteratorFunctions<SpanByte, SpanByte>
        {
            internal int keyMultToValue;
            internal long numRecords;
            internal int stopAt;

            public unsafe bool SingleReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                if (keyMultToValue > 0)
                {
                    var keyItem = key.AsSpan<long>()[0];
                    var valueItem = value.AsSpan<int>()[0];
                    ClassicAssert.AreEqual(keyItem * keyMultToValue, valueItem);
                }
                return stopAt != ++numRecords;
            }

            public bool ConcurrentReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

            public readonly bool OnStart(long beginAddress, long endAddress) => true;
            public readonly void OnException(Exception exception, long numberOfRecords) { }
            public readonly void OnStop(bool completed, long numberOfRecords) { }
        }

        internal struct IterationCollisionTestFunctions : IScanIteratorFunctions<SpanByte, SpanByte>
        {
            internal List<long> keys;
            public IterationCollisionTestFunctions() => keys = new();

            public unsafe bool SingleReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                keys.Add(*(long*)key.ToPointer());
                cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                return true;
            }

            public bool ConcurrentReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

            public readonly bool OnStart(long beginAddress, long endAddress) => true;
            public readonly void OnException(Exception exception, long numberOfRecords) { }
            public readonly void OnStop(bool completed, long numberOfRecords) { }
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void SpanByteIterationBasicTest([Values] TestDeviceType deviceType, [Values] ScanIteratorType scanIteratorType)
        {
            log = CreateTestDevice(deviceType, $"{MethodTestDir}{deviceType}.log");
            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                MemorySize = 1L << 15,
                PageSize = 1L << 9,
                SegmentSize = 1L << 22
            }, StoreFunctions<SpanByte, SpanByte>.Create()
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<SpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;

            SpanBytePushIterationTestFunctions scanIteratorFunctions = new();

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

            // Note: We only have a single value element; we are not exercising the "Variable Length" aspect here.
            Span<long> keySpan = stackalloc long[1];
            Span<int> valueSpan = stackalloc int[1];
            var key = keySpan.AsSpanByte();
            var value = valueSpan.AsSpanByte();

            // Initial population
            for (int i = 0; i < totalRecords; i++)
            {
                keySpan[0] = i;
                valueSpan[0] = i;
                _ = bContext.Upsert(ref key, ref value);
            }
            iterateAndVerify(1, totalRecords);

            for (int i = 0; i < totalRecords; i++)
            {
                keySpan[0] = i;
                valueSpan[0] = i * 2;
                _ = bContext.Upsert(ref key, ref value);
            }
            iterateAndVerify(2, totalRecords);

            for (int i = totalRecords / 2; i < totalRecords; i++)
            {
                keySpan[0] = i;
                valueSpan[0] = i;
                _ = bContext.Upsert(ref key, ref value);
            }
            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                keySpan[0] = i;
                valueSpan[0] = i;
                _ = bContext.Upsert(ref key, ref value);
            }
            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                keySpan[0] = i;
                _ = bContext.Delete(ref key);
            }
            iterateAndVerify(0, totalRecords / 2);

            for (int i = 0; i < totalRecords; i++)
            {
                keySpan[0] = i;
                valueSpan[0] = i * 3;
                _ = bContext.Upsert(ref key, ref value);
            }
            iterateAndVerify(3, totalRecords);

            store.Log.FlushAndEvict(wait: true);
            iterateAndVerify(3, totalRecords);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]
        public void SpanByteIterationPushStopTest([Values] TestDeviceType deviceType)
        {
            log = CreateTestDevice(deviceType, Path.Join(MethodTestDir, $"{deviceType}.log"));
            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                MemorySize = 1L << 15,
                PageSize = 1L << 9,
                SegmentSize = 1L << 22
            }, StoreFunctions<SpanByte, SpanByte>.Create()
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<SpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;
            SpanBytePushIterationTestFunctions scanIteratorFunctions = new();

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

            // Note: We only have a single value element; we are not exercising the "Variable Length" aspect here.
            Span<long> keySpan = stackalloc long[1];
            Span<int> valueSpan = stackalloc int[1];
            var key = keySpan.AsSpanByte();
            var value = valueSpan.AsSpanByte();

            // Initial population
            for (int i = 0; i < totalRecords; i++)
            {
                keySpan[0] = i;
                valueSpan[0] = i;
                _ = bContext.Upsert(ref key, ref value);
            }

            scanAndVerify(42, useScan: true);
            scanAndVerify(42, useScan: false);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void SpanByteIterationPushLockTest([Values(1, 4)] int scanThreads, [Values(1, 4)] int updateThreads, [Values] ScanMode scanMode)
        {
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "lock_test.log"));

            // Must be large enough to contain all records in memory to exercise locking
            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                MemorySize = 1L << 25,
                PageSize = 1L << 19,
                SegmentSize = 1L << 22
            }, StoreFunctions<SpanByte, SpanByte>.Create()
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            const int totalRecords = 2000;
            var start = store.Log.TailAddress;

            void LocalScan(int i)
            {
                using var session = store.NewSession<SpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
                SpanBytePushIterationTestFunctions scanIteratorFunctions = new();
                if (scanMode == ScanMode.Scan)
                    ClassicAssert.IsTrue(store.Log.Scan(ref scanIteratorFunctions, start, store.Log.TailAddress), $"Failed to complete push scan; numRecords = {scanIteratorFunctions.numRecords}");
                else
                    ClassicAssert.IsTrue(session.Iterate(ref scanIteratorFunctions), $"Failed to complete push iteration; numRecords = {scanIteratorFunctions.numRecords}");
                ClassicAssert.AreEqual(totalRecords, scanIteratorFunctions.numRecords);
            }

            void LocalUpdate(int tid)
            {
                // Note: We only have a single value element; we are not exercising the "Variable Length" aspect here.
                Span<long> keySpan = stackalloc long[1];
                Span<int> valueSpan = stackalloc int[1];
                var key = keySpan.AsSpanByte();
                var value = valueSpan.AsSpanByte();

                using var session = store.NewSession<SpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
                var bContext = session.BasicContext;
                for (int i = 0; i < totalRecords; i++)
                {
                    keySpan[0] = i;
                    valueSpan[0] = i * (tid + 1);
                    _ = bContext.Upsert(ref key, ref value);
                }
            }

            {
                // Initial population
                // Note: We only have a single value element; we are not exercising the "Variable Length" aspect here.
                Span<long> keySpan = stackalloc long[1];
                Span<int> valueSpan = stackalloc int[1];
                var key = keySpan.AsSpanByte();
                var value = valueSpan.AsSpanByte();

                using var session = store.NewSession<SpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
                var bContext = session.BasicContext;
                for (int i = 0; i < totalRecords; i++)
                {
                    keySpan[0] = i;
                    valueSpan[0] = i;
                    _ = bContext.Upsert(ref key, ref value);
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