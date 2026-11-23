// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery.objects
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// Recovery undoes the records a checkpoint's next version (v+1) wrote in its fuzzy region, but their objects stay in the main object log. A transaction
    /// held in version v keeps writing version-v records after them, so the kept records' objects on that main-log page have the undone records' objects
    /// between them. The object-log reader reads a page's objects in one pass from its first kept record to the end of its last one, and must skip such a gap
    /// however large it is: within a read buffer, across read buffers, and across object-log segments. This happens when recovery loads the page's objects,
    /// and again when a scan reads the page from disk.
    /// </summary>
    [TestFixture]
    public class ObjectLogReadGapTests : TestBase
    {
        /// <summary>The number of records written before the checkpoint</summary>
        const int NumRecords = 20;

        /// <summary>The number of version-v+1 records, which recovery undoes; their objects are the gap</summary>
        const int NumUndoneRecords = 3;

        /// <summary>The first key of the version-v+1 records</summary>
        const int FirstUndoneKey = 1000;

        /// <summary>The number of version-v records the transaction writes after the version-v+1 records</summary>
        const int NumTransactionRecords = 3;

        /// <summary>The first key the transaction writes</summary>
        const int FirstTransactionKey = 2000;

        /// <summary>The object size of every record that recovery keeps</summary>
        const int KeptObjectSize = 100;

        /// <summary>The object-log segment size: the smallest allowed, so that a gap of a few MB crosses a segment boundary</summary>
        const long ObjectLogSegmentSize = 1L << LogSettings.kMinObjectLogSegmentSizeBits;

        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        // undoneObjectSize: 2,000 bytes gives a gap of a few sectors within one read buffer. 6 MB gives an 18 MB gap, longer than the reader's four 4 MB
        // read buffers together, so the reader moves through all of them and wraps around to the first, which it refilled when it left it; the gap also
        // crosses four 4 MB object-log segment boundaries.
        // For Snapshot, the main log is flushed during the checkpoint, so the undone and kept records are below its flushedLogicalAddress and are read from
        // the main log; recovery's object load for them is deferred until after the snapshot phase.
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void RecoveryAndScanSkipObjectsOfUndoneRecords(
            [Values(CheckpointType.FoldOver, CheckpointType.Snapshot)] CheckpointType checkpointType,
            [Values(2000, 6 << 20)] int undoneObjectSize)
        {
            Prepare(out var log, out var objlog, out var store);
            Guid token;
            try
            {
                Upsert(store, 0, NumRecords, KeptObjectSize);
                token = CheckpointWithUndoneRecordsBeforeKeptOnes(store, checkpointType, undoneObjectSize);
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            Prepare(out log, out objlog, out store);
            try
            {
                // Recovery loads the objects of the kept records, skipping the undone records' objects.
                _ = store.RecoverAsync(default, token).AsTask().GetAwaiter().GetResult();
                VerifyKeys(store, "resident");

                // Reading each record back from disk uses its own object-log position; a scan reads each page's objects in one pass, skipping the gap again.
                store.Log.FlushAndEvict(wait: true);
                VerifyKeys(store, "read from disk");

                ScanFunctions scanFunctions = new() { scanned = [] };
                ClassicAssert.IsTrue(store.Log.Scan(ref scanFunctions, store.Log.BeginAddress, store.Log.TailAddress), "the scan did not complete");
                ClassicAssert.AreEqual(NumRecords + NumTransactionRecords, scanFunctions.scanned.Count, "the scan must return exactly the kept records");
                for (var key = 0; key < NumRecords; key++)
                    ClassicAssert.IsTrue(scanFunctions.scanned.Contains(key), $"key {key} not scanned");
                for (var key = FirstTransactionKey; key < FirstTransactionKey + NumTransactionRecords; key++)
                    ClassicAssert.IsTrue(scanFunctions.scanned.Contains(key), $"key {key} not scanned");
            }
            finally
            {
                Destroy(log, objlog, store);
            }
        }

        // Takes a checkpoint while a transaction holds version v open, so that in its fuzzy region the version-v+1 records come before the version-v
        // records the transaction writes, all on the same main-log page.
        private static Guid CheckpointWithUndoneRecordsBeforeKeptOnes(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, CheckpointType checkpointType,
            int undoneObjectSize)
        {
            using var txnSession = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
            var tContext = txnSession.TransactionalContext;
            var txnKeys = GC.AllocateArray<int>(NumTransactionRecords, pinned: true);
            var lockKeys = new FixedLengthTransactionalKeyStruct[NumTransactionRecords];
            for (var ii = 0; ii < NumTransactionRecords; ii++)
            {
                txnKeys[ii] = FirstTransactionKey + ii;
                lockKeys[ii] = new(SpanByte.FromPinnedVariable(ref txnKeys[ii]), LockType.Exclusive, tContext);
            }

            var txnVersion = store.stateMachineDriver.AcquireTransactionVersion();
            tContext.BeginTransaction();
            tContext.Lock<FixedLengthTransactionalKeyStruct>(lockKeys);
            txnVersion = store.stateMachineDriver.VerifyTransactionVersion(txnVersion);
            tContext.LocksAcquired(txnVersion);

            Guid token;
            try
            {
                // The open version-v transaction holds the checkpoint in IN_PROGRESS.
                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, checkpointType));
                while (store.stateMachineDriver.SystemState.Phase != Phase.IN_PROGRESS)
                    _ = Thread.Yield();

                Upsert(store, FirstUndoneKey, NumUndoneRecords, undoneObjectSize);
                for (var ii = 0; ii < NumTransactionRecords; ii++)
                    _ = tContext.Upsert(new TestObjectKey { key = txnKeys[ii] }, CreateValue(txnKeys[ii], KeptObjectSize));

                if (checkpointType == CheckpointType.Snapshot)
                    store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            }
            finally
            {
                tContext.Unlock<FixedLengthTransactionalKeyStruct>(lockKeys);
                tContext.EndTransaction();
                store.stateMachineDriver.EndTransaction(txnVersion);
            }

            store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            return token;
        }

        // Every kept key must be found with its own value, and no undone key may be found.
        private static void VerifyKeys(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, string where)
        {
            using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
            var bContext = session.BasicContext;
            foreach (var (firstKey, count, expectFound) in new[] { (0, NumRecords, true), (FirstUndoneKey, NumUndoneRecords, false), (FirstTransactionKey, NumTransactionRecords, true) })
            {
                for (var key = firstKey; key < firstKey + count; key++)
                {
                    TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Object };
                    TestLargeObjectOutput output = new();
                    var status = bContext.Read(new TestObjectKey { key = key }, ref input, ref output);
                    if (status.IsPending)
                    {
                        Assert.That(bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                        (status, output) = GetSinglePendingResult(completedOutputs);
                    }

                    if (expectFound)
                    {
                        ClassicAssert.IsTrue(status.Found, $"key {key} not found ({where})");
                        ClassicAssert.IsTrue(IsValueOf(key, output.valueObject), $"key {key} wrong value ({where})");
                    }
                    else
                        ClassicAssert.IsFalse(status.Found, $"undone key {key} found ({where})");
                }
            }
        }

        // Checks each scanned record's value and collects its key.
        private struct ScanFunctions : IScanIteratorFunctions
        {
            /// <summary>The keys scanned so far</summary>
            internal HashSet<int> scanned;

            public readonly bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                cursorRecordResult = CursorRecordResult.Accept;
                var key = logRecord.Key.AsRef<TestObjectKey>().key;
                ClassicAssert.IsTrue(IsValueOf(key, (TestLargeObjectValue)logRecord.ValueObject), $"scanned key {key} has the wrong value");
                ClassicAssert.IsTrue(scanned.Add(key), $"key {key} scanned twice");
                return true;
            }

            public readonly bool OnStart(long beginAddress, long endAddress) => true;
            public readonly void OnException(Exception exception, long numberOfRecords) { }
            public readonly void OnStop(bool completed, long numberOfRecords) { }
        }

        private static void Upsert(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, int firstKey, int count, int objectSize)
        {
            using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
            for (var key = firstKey; key < firstKey + count; key++)
                _ = session.BasicContext.Upsert(new TestObjectKey { key = key }, CreateValue(key, objectSize));
        }

        // The value's size identifies whether it is a kept record's, and its first bytes identify its key.
        private static TestLargeObjectValue CreateValue(int key, int objectSize)
        {
            var value = new TestLargeObjectValue(objectSize);
            value.value[0] = (byte)key;
            value.value[1] = (byte)(key >> 8);
            return value;
        }

        private static bool IsValueOf(int key, TestLargeObjectValue value)
            => value is not null && value.value.Length == KeptObjectSize && value.value[0] == (byte)key && value.value[1] == (byte)(key >> 8);

        private static void Prepare(out IDevice log, out IDevice objlog, out TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "readgap.log"));
            objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "readgap.obj.log"));
            store = new(new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                ObjectLogDevice = objlog,
                SegmentSize = 1L << 20,
                ObjectLogSegmentSize = ObjectLogSegmentSize,
                LogMemorySize = 32L * MinKvLogPageSize,
                PageSize = MinKvLogPageSize,
                CheckpointDir = Path.Combine(MethodTestDir, "check-points")
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer(), DefaultRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
        }

        private static void Destroy(IDevice log, IDevice objlog, TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            store.Dispose();
            log.Dispose();
            objlog.Dispose();
        }
    }
}