// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using SpanByteStoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordTriggers>;

    [AllureNUnit]
    [TestFixture]
    internal class SpanByteIterationTests : AllureTestBase
    {
        private TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store;
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

        internal struct SpanBytePushIterationTestFunctions : IScanIteratorFunctions
        {
            internal int keyMultToValue;
            internal long numRecords;
            internal int stopAt;

            public bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                if (keyMultToValue > 0)
                {
                    var keyItem = MemoryMarshal.Cast<byte, long>(logRecord.Key)[0];
                    var valueItem = MemoryMarshal.Cast<byte, int>(logRecord.ValueSpan)[0];
                    ClassicAssert.AreEqual(keyItem * keyMultToValue, valueItem);
                }
                return stopAt != ++numRecords;
            }

            public readonly bool OnStart(long beginAddress, long endAddress) => true;
            public readonly void OnException(Exception exception, long numberOfRecords) { }
            public readonly void OnStop(bool completed, long numberOfRecords) { }
        }

        internal struct IterationCollisionTestFunctions : IScanIteratorFunctions
        {
            internal List<long> keys;
            public IterationCollisionTestFunctions() => keys = [];

            public readonly bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                keys.Add(MemoryMarshal.Cast<byte, long>(logRecord.Key)[0]);
                cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                return true;
            }

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
                LogMemorySize = 1L << 15,
                PageSize = 1L << 9,
                SegmentSize = 1L << 22
            }, StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
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
                    while (iter.GetNext())
                        _ = scanIteratorFunctions.Reader(in iter, default, default, out _);
                }
                else
                    ClassicAssert.IsTrue(session.Iterate(ref scanIteratorFunctions), $"Failed to complete push iteration; numRecords = {scanIteratorFunctions.numRecords}");

                ClassicAssert.AreEqual(expectedRecs, scanIteratorFunctions.numRecords);
            }

            // Note: We only have a single value element; we are not exercising the "Variable Length" aspect here.
            Span<long> keySpan = stackalloc long[1];
            Span<int> valueSpan = stackalloc int[1];
            var key = TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<long, byte>(keySpan));
            var value = MemoryMarshal.Cast<int, byte>(valueSpan);

            // Initial population
            for (int i = 0; i < totalRecords; i++)
            {
                keySpan[0] = i;
                valueSpan[0] = i;
                _ = bContext.Upsert(key, value);
            }
            iterateAndVerify(1, totalRecords);

            for (int i = 0; i < totalRecords; i++)
            {
                keySpan[0] = i;
                valueSpan[0] = i * 2;
                _ = bContext.Upsert(key, value);
            }
            iterateAndVerify(2, totalRecords);

            for (int i = totalRecords / 2; i < totalRecords; i++)
            {
                keySpan[0] = i;
                valueSpan[0] = i;
                _ = bContext.Upsert(key, value);
            }
            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                keySpan[0] = i;
                valueSpan[0] = i;
                _ = bContext.Upsert(key, value);
            }
            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                keySpan[0] = i;
                _ = bContext.Delete(key);
            }
            iterateAndVerify(0, totalRecords / 2);

            for (int i = 0; i < totalRecords; i++)
            {
                keySpan[0] = i;
                valueSpan[0] = i * 3;
                _ = bContext.Upsert(key, value);
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
                LogMemorySize = 1L << 15,
                PageSize = 1L << 9,
                SegmentSize = 1L << 22
            }, StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
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
            var key = TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<long, byte>(keySpan));
            var value = MemoryMarshal.Cast<int, byte>(valueSpan);

            // Initial population
            for (int i = 0; i < totalRecords; i++)
            {
                keySpan[0] = i;
                valueSpan[0] = i;
                _ = bContext.Upsert(key, value);
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
                LogMemorySize = 1L << 25,
                PageSize = 1L << 19,
                SegmentSize = 1L << 22
            }, StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            const int totalRecords = 2000;
            var start = store.Log.TailAddress;

            void LocalScan(int i)
            {
                using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
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
                var key = TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<long, byte>(keySpan));
                var value = MemoryMarshal.Cast<int, byte>(valueSpan);

                using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
                var bContext = session.BasicContext;
                for (int i = 0; i < totalRecords; i++)
                {
                    keySpan[0] = i;
                    valueSpan[0] = i * (tid + 1);
                    _ = bContext.Upsert(key, value);
                }
            }

            {
                // Initial population
                // Note: We only have a single value element; we are not exercising the "Variable Length" aspect here.
                Span<long> keySpan = stackalloc long[1];
                Span<int> valueSpan = stackalloc int[1];
                var key = TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<long, byte>(keySpan));
                var value = MemoryMarshal.Cast<int, byte>(valueSpan);

                using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
                var bContext = session.BasicContext;
                for (int i = 0; i < totalRecords; i++)
                {
                    keySpan[0] = i;
                    valueSpan[0] = i;
                    _ = bContext.Upsert(key, value);
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

        /// <summary>
        /// Basic correctness test for <c>IterateLookupSnapshot</c>: after a series of RCU updates
        /// to multiple keys (each older record gets sealed when its replacement is upserted), the
        /// snapshot variant emits each unique live key exactly once and returns the latest value
        /// in the snapshot.
        /// <para/>
        /// Note: the rule-S2 protection (snapshot vs. <c>maxAddress = long.MaxValue</c> default)
        /// only manifests during an in-flight RCU race window where the older record has not yet
        /// been sealed. That race window is too narrow to reproduce deterministically without
        /// test-only instrumentation hooks; the parameter pattern this API uses internally
        /// (<c>untilAddress = maxAddress = capturedTail</c>) is exercised by
        /// <c>MigrateOperation.Scan</c> in the cluster code path.
        /// </summary>
        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void SpanByteIterateLookupSnapshotBasicCorrectness()
        {
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "snapshot_basic.log"));
            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                LogMemorySize = 1L << 25,
                PageSize = 1L << 19,
                SegmentSize = 1L << 22
            }, StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;

            Span<long> keySpan = stackalloc long[1];
            Span<int> valueSpan = stackalloc int[1];
            var key = TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<long, byte>(keySpan));
            var value = MemoryMarshal.Cast<int, byte>(valueSpan);

            const int totalRecords = 200;

            // Phase 1: insert keys [0, totalRecords) with value = key * 1.
            for (int i = 0; i < totalRecords; i++)
            {
                keySpan[0] = i;
                valueSpan[0] = i;
                _ = bContext.Upsert(key, value);
            }

            // Phase 2: RCU each key to a new value (key * 10). Force RCU by flushing to read-only
            // first so the upsert can't update in place.
            store.Log.Flush(wait: true);
            for (int i = 0; i < totalRecords; i++)
            {
                keySpan[0] = i;
                valueSpan[0] = i * 10;
                _ = bContext.Upsert(key, value);
            }

            // IterateLookupSnapshot should emit each unique live key exactly once, with the
            // post-RCU value (key * 10).
            var fns = new SnapshotProbeAllFunctions { observed = new Dictionary<long, int>() };
            _ = session.IterateLookupSnapshot(ref fns);

            ClassicAssert.AreEqual(totalRecords, fns.observed.Count,
                "IterateLookupSnapshot should emit each unique live key exactly once");
            for (int i = 0; i < totalRecords; i++)
            {
                ClassicAssert.IsTrue(fns.observed.TryGetValue(i, out var emittedValue),
                    $"Key {i} not emitted by IterateLookupSnapshot");
                ClassicAssert.AreEqual(i * 10, emittedValue,
                    $"Key {i}: expected snapshot to expose post-RCU value but got {emittedValue}");
            }

            // Stable: a second IterateLookupSnapshot call returns the same set.
            var fns2 = new SnapshotProbeAllFunctions { observed = new Dictionary<long, int>() };
            _ = session.IterateLookupSnapshot(ref fns2);
            ClassicAssert.AreEqual(totalRecords, fns2.observed.Count,
                "Second IterateLookupSnapshot should emit the same number of records (snapshot is stable across calls)");
        }

        private struct SnapshotProbeAllFunctions : IScanIteratorFunctions
        {
            // NOTE: Must hold a reference type because the struct is boxed when stored in
            // ScanCursorState.functions (interface field) — mutations on the boxed copy land
            // there, not on the caller's struct. The Dictionary reference itself is shared.
            public Dictionary<long, int> observed;

            public readonly bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                cursorRecordResult = CursorRecordResult.Accept;
                var k = MemoryMarshal.Cast<byte, long>(logRecord.Key)[0];
                var v = MemoryMarshal.Cast<byte, int>(logRecord.ValueSpan)[0];
                ClassicAssert.IsFalse(observed.ContainsKey(k),
                    $"IterateLookupSnapshot emitted key {k} more than once (snapshot semantics violated)");
                observed[k] = v;
                return true;
            }

            public readonly bool OnStart(long beginAddress, long endAddress) => true;
            public readonly void OnException(Exception exception, long numberOfRecords) { }
            public readonly void OnStop(bool completed, long numberOfRecords) { }
        }
    }
}