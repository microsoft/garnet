// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    [TestFixture]
    internal class SpanByteIterationTests
    {
        private TsavoriteKV<SpanByte, SpanByte> store;
        private IDevice log;
        private string path;

        // Note: We always set value.length to 2, which includes both VLValue members; we are not exercising the "Variable Length" aspect here.
        private const int ValueLength = 2;

        [SetUp]
        public void Setup()
        {
            path = MethodTestDir + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            DeleteDirectory(path, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            DeleteDirectory(path);
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
                    Assert.AreEqual(keyItem * keyMultToValue, valueItem);
                }
                return stopAt != ++numRecords;
            }

            public bool ConcurrentReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

            public bool OnStart(long beginAddress, long endAddress) => true;
            public void OnException(Exception exception, long numberOfRecords) { }
            public void OnStop(bool completed, long numberOfRecords) { }
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void SpanByteIterationBasicTest([Values] DeviceType deviceType, [Values] ScanIteratorType scanIteratorType)
        {
            log = CreateTestDevice(deviceType, $"{path}{deviceType}.log");
            store = new TsavoriteKV<SpanByte, SpanByte>
                 (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 9, SegmentSizeBits = 22 },
                 concurrencyControlMode: scanIteratorType == ScanIteratorType.Pull ? ConcurrencyControlMode.None : ConcurrencyControlMode.LockTable);

            using var session = store.NewSession<SpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
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
                        scanIteratorFunctions.SingleReader(ref iter.GetKey(), ref iter.GetValue(), default, default, out _);
                }
                else
                    Assert.IsTrue(session.Iterate(ref scanIteratorFunctions), $"Failed to complete push iteration; numRecords = {scanIteratorFunctions.numRecords}");

                Assert.AreEqual(expectedRecs, scanIteratorFunctions.numRecords);
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
                session.Upsert(ref key, ref value);
            }
            iterateAndVerify(1, totalRecords);

            for (int i = 0; i < totalRecords; i++)
            {
                keySpan[0] = i;
                valueSpan[0] = i * 2;
                session.Upsert(ref key, ref value);
            }
            iterateAndVerify(2, totalRecords);

            for (int i = totalRecords / 2; i < totalRecords; i++)
            {
                keySpan[0] = i;
                valueSpan[0] = i;
                session.Upsert(ref key, ref value);
            }
            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                keySpan[0] = i;
                valueSpan[0] = i;
                session.Upsert(ref key, ref value);
            }
            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                keySpan[0] = i;
                session.Delete(ref key);
            }
            iterateAndVerify(0, totalRecords / 2);

            for (int i = 0; i < totalRecords; i++)
            {
                keySpan[0] = i;
                valueSpan[0] = i * 3;
                session.Upsert(ref key, ref value);
            }
            iterateAndVerify(3, totalRecords);

            store.Log.FlushAndEvict(wait: true);
            iterateAndVerify(3, totalRecords);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]
        public void SpanByteIterationPushStopTest([Values] DeviceType deviceType)
        {
            log = CreateTestDevice(deviceType, $"{path}{deviceType}.log");
            store = new TsavoriteKV<SpanByte, SpanByte>
                 (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 9, SegmentSizeBits = 22 });

            using var session = store.NewSession<SpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            SpanBytePushIterationTestFunctions scanIteratorFunctions = new();

            const int totalRecords = 2000;
            var start = store.Log.TailAddress;

            void scanAndVerify(int stopAt, bool useScan)
            {
                scanIteratorFunctions.numRecords = 0;
                scanIteratorFunctions.stopAt = stopAt;
                if (useScan)
                    Assert.IsFalse(store.Log.Scan(ref scanIteratorFunctions, start, store.Log.TailAddress), $"Failed to terminate push iteration early; numRecords = {scanIteratorFunctions.numRecords}");
                else
                    Assert.IsFalse(session.Iterate(ref scanIteratorFunctions), $"Failed to terminate push iteration early; numRecords = {scanIteratorFunctions.numRecords}");
                Assert.AreEqual(stopAt, scanIteratorFunctions.numRecords);
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
                session.Upsert(ref key, ref value);
            }

            scanAndVerify(42, useScan: true);
            scanAndVerify(42, useScan: false);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void SpanByteIterationPushLockTest([Values(1, 4)] int scanThreads, [Values(1, 4)] int updateThreads, [Values] ScanMode scanMode)
        {
            log = Devices.CreateLogDevice($"{path}lock_test.log");
            // Must be large enough to contain all records in memory to exercise locking
            store = new TsavoriteKV<SpanByte, SpanByte>
                 (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 25, PageSizeBits = 19, SegmentSizeBits = 22 });

            const int totalRecords = 2000;
            var start = store.Log.TailAddress;

            void LocalScan(int i)
            {
                using var session = store.NewSession<SpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
                SpanBytePushIterationTestFunctions scanIteratorFunctions = new();
                if (scanMode == ScanMode.Scan)
                    Assert.IsTrue(store.Log.Scan(ref scanIteratorFunctions, start, store.Log.TailAddress), $"Failed to complete push scan; numRecords = {scanIteratorFunctions.numRecords}");
                else
                    Assert.IsTrue(session.Iterate(ref scanIteratorFunctions), $"Failed to complete push iteration; numRecords = {scanIteratorFunctions.numRecords}");
                Assert.AreEqual(totalRecords, scanIteratorFunctions.numRecords);
            }

            void LocalUpdate(int tid)
            {
                // Note: We only have a single value element; we are not exercising the "Variable Length" aspect here.
                Span<long> keySpan = stackalloc long[1];
                Span<int> valueSpan = stackalloc int[1];
                var key = keySpan.AsSpanByte();
                var value = valueSpan.AsSpanByte();

                using var session = store.NewSession<SpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
                for (int i = 0; i < totalRecords; i++)
                {
                    keySpan[0] = i;
                    valueSpan[0] = i * (tid + 1);
                    session.Upsert(ref key, ref value);
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
                for (int i = 0; i < totalRecords; i++)
                {
                    keySpan[0] = i;
                    valueSpan[0] = i;
                    session.Upsert(ref key, ref value);
                }
            }

            List<Task> tasks = new();   // Task rather than Thread for propagation of exception.
            var numThreads = scanThreads + updateThreads;
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                if (t < scanThreads)
                    tasks.Add(Task.Factory.StartNew(() => LocalScan(tid)));
                else
                    tasks.Add(Task.Factory.StartNew(() => LocalUpdate(tid)));
            }
            Task.WaitAll(tasks.ToArray());
        }
    }
}