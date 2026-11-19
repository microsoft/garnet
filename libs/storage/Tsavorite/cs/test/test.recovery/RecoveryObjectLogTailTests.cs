// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
    /// When a checkpoint's main-log region ends with records that recovery undoes (v+1 records), their objects lie between the kept records' objects and the
    /// checkpoint's object-log tail. Recovery resumes writing the main object log right after the kept objects instead, so new objects reuse that space, and
    /// new records written after the kept ones on the same page have their objects right after the kept ones.
    /// </summary>
    [TestFixture]
    public class RecoveryObjectLogTailTests : TestBase
    {
        /// <summary>The number of records written before the checkpoint</summary>
        const int NumRecords = 20;

        /// <summary>The number of version-v+1 records, which recovery undoes; their objects are the space reused</summary>
        const int NumUndoneRecords = 3;

        /// <summary>The first key of the version-v+1 records</summary>
        const int FirstUndoneKey = 1000;

        /// <summary>For Snapshot, the number of version-v records a transaction writes after the main log is flushed, which the snapshot file holds</summary>
        const int NumSnapshotRecords = 3;

        /// <summary>The first key of the records the snapshot file holds</summary>
        const int FirstSnapshotKey = 2000;

        /// <summary>The number of records written after recovery</summary>
        const int NumNewRecords = 5;

        /// <summary>The first key written after recovery</summary>
        const int FirstNewKey = 3000;

        /// <summary>The object size of every record that recovery keeps, and of the records written after recovery</summary>
        const int KeptObjectSize = 100;

        /// <summary>The bytes each such object takes in the object log: <see cref="TestLargeObjectValue.Serializer"/> writes its length, then its bytes</summary>
        const int SerializedObjectSize = sizeof(int) + KeptObjectSize;

        /// <summary>The object-log segment size the tests use: the smallest allowed</summary>
        const long ObjectLogSegmentSize = 1L << LogSettings.kMinObjectLogSegmentSizeBits;

        /// <summary>For the segment-crossing test, the bytes the crossing object takes in the object log; it starts in one segment and ends in the next</summary>
        const int CrossingSerializedSize = 2 << 20;

        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        // undoneObjectSize: 2,000 bytes, or 6 MB, so that the space reused spans several 4 MB object-log segments.
        // For Snapshot, the main log is flushed while the v+1 records are the last ones, so they are below the checkpoint's flushedLogicalAddress, and a
        // transaction held in version v then writes records that the snapshot file holds, on the same page. Those records' objects are written to the main
        // object log after recovery, when the page is flushed.
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void RecoveryResumesObjectLogAfterKeptObjects(
            [Values(CheckpointType.FoldOver, CheckpointType.Snapshot)] CheckpointType checkpointType,
            [Values(2000, 6 << 20)] int undoneObjectSize)
        {
            var numSnapshotRecords = checkpointType == CheckpointType.Snapshot ? NumSnapshotRecords : 0;

            Prepare(out var log, out var objlog, out var store);
            Guid token;
            ulong checkpointObjectLogTail;
            try
            {
                Upsert(store, 0, NumRecords, KeptObjectSize);
                token = CheckpointEndingWithUndoneRecords(store, checkpointType, undoneObjectSize);
                checkpointObjectLogTail = store.hlogBase.GetObjectLogTail().CurrentAddress;
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            Guid token2;
            Prepare(out log, out objlog, out store);
            try
            {
                // One flush wrote the kept records' objects from the start of the object log, then the v+1 records' objects.
                var sectorSize = (long)objlog.SectorSize;
                var keptObjectsEnd = (ulong)Utility.RoundUp((long)NumRecords * SerializedObjectSize, sectorSize);
                ClassicAssert.Less(keptObjectsEnd, checkpointObjectLogTail, "the checkpoint's object-log tail must be past the undone records' objects");

                _ = store.RecoverAsync(default, token).AsTask().GetAwaiter().GetResult();
                ClassicAssert.AreEqual(keptObjectsEnd, store.hlogBase.GetObjectLogTail().CurrentAddress, "the object-log tail must resume right after the kept objects");

                // The next flush writes the snapshot file's records and the new ones, starting right after the kept objects.
                Upsert(store, FirstNewKey, NumNewRecords, KeptObjectSize);
                store.Log.FlushAndEvict(wait: true);
                ClassicAssert.AreEqual((ulong)Utility.RoundUp((long)keptObjectsEnd + (numSnapshotRecords + NumNewRecords) * SerializedObjectSize, sectorSize),
                    store.hlogBase.GetObjectLogTail().CurrentAddress, "the flush must write its objects right after the kept objects");
                VerifyKeys(store, numSnapshotRecords, "read from disk");

                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token2, CheckpointType.FoldOver));
                store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            // Recovery loads the page's objects in one pass: the kept ones, then the ones written over the undone records' objects.
            Prepare(out log, out objlog, out store);
            try
            {
                _ = store.RecoverAsync(default, token2).AsTask().GetAwaiter().GetResult();
                VerifyKeys(store, numSnapshotRecords, "after the second recovery");
            }
            finally
            {
                Destroy(log, objlog, store);
            }
        }

        // The last kept object starts in one object-log segment and ends in the next, and recovery must resume right after its true end. firstSerializedSize,
        // the size of the object before it, sets where it starts: 2.5 MB into the first 4 MB segment, for a 2 MB object.
        [TestCase(5 << 19)]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void RecoveryResumesObjectLogAfterKeptObjectCrossingSegment(int firstSerializedSize)
        {
            var keptObjectsEnd = (ulong)(firstSerializedSize + CrossingSerializedSize);
            Assert.That((ulong)firstSerializedSize < (ulong)ObjectLogSegmentSize && keptObjectsEnd > (ulong)ObjectLogSegmentSize, "the second object must cross the segment boundary");
            var keptSizes = new[] { firstSerializedSize - sizeof(int), CrossingSerializedSize - sizeof(int) };

            Prepare(out var log, out var objlog, out var store);
            Guid token;
            try
            {
                for (var key = 0; key < keptSizes.Length; key++)
                    Upsert(store, key, 1, keptSizes[key]);
                token = CheckpointEndingWithUndoneRecords(store, CheckpointType.FoldOver, undoneObjectSize: 1 << 20);
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            Guid token2;
            Prepare(out log, out objlog, out store);
            try
            {
                var sectorSize = (long)objlog.SectorSize;
                _ = store.RecoverAsync(default, token).AsTask().GetAwaiter().GetResult();
                ClassicAssert.AreEqual((ulong)Utility.RoundUp((long)keptObjectsEnd, sectorSize), store.hlogBase.GetObjectLogTail().CurrentAddress,
                    "the object-log tail must resume right after the end of the object that crosses the segment boundary");

                Upsert(store, FirstNewKey, 1, KeptObjectSize);
                store.Log.FlushAndEvict(wait: true);
                VerifyWholeObjects(store, keptSizes, "read from disk");

                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token2, CheckpointType.FoldOver));
                store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            Prepare(out log, out objlog, out store);
            try
            {
                _ = store.RecoverAsync(default, token2).AsTask().GetAwaiter().GetResult();
                VerifyWholeObjects(store, keptSizes, "after the second recovery");
            }
            finally
            {
                Destroy(log, objlog, store);
            }
        }

        // Each kept key (0 up to keptSizes.Length) must have its whole object intact, every byte, so that an object partly written over is detected; the
        // key written after recovery must be found too.
        private static void VerifyWholeObjects(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, int[] keptSizes, string where)
        {
            using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
            var bContext = session.BasicContext;
            for (var key = 0; key < keptSizes.Length; key++)
            {
                var value = ReadValue(bContext, key);
                ClassicAssert.IsNotNull(value, $"key {key} not found ({where})");
                ClassicAssert.AreEqual(keptSizes[key], value.value.Length, $"key {key} wrong object size ({where})");
                var expected = CreateValue(key, keptSizes[key]).value;
                ClassicAssert.IsTrue(expected.AsSpan().SequenceEqual(value.value), $"key {key} object content differs ({where})");
            }
            ClassicAssert.IsNotNull(ReadValue(bContext, FirstNewKey), $"key {FirstNewKey} not found ({where})");
        }

        private static TestLargeObjectValue ReadValue(BasicContext<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions, ClassStoreFunctions, ClassAllocator> bContext,
            int key)
        {
            TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Object };
            TestLargeObjectOutput output = new();
            var status = bContext.Read(new TestObjectKey { key = key }, ref input, ref output);
            if (status.IsPending)
            {
                Assert.That(bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                (status, output) = GetSinglePendingResult(completedOutputs);
            }
            return status.Found ? output.valueObject : null;
        }

        // Takes a checkpoint while a transaction holds version v open, so the v+1 records written meanwhile are in its fuzzy region. For Snapshot, the main log
        // is flushed after them and the transaction then writes version-v records, which only the snapshot file holds.
        private static Guid CheckpointEndingWithUndoneRecords(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, CheckpointType checkpointType, int undoneObjectSize)
        {
            using var txnSession = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
            var tContext = txnSession.TransactionalContext;
            var txnKeys = GC.AllocateArray<int>(NumSnapshotRecords, pinned: true);
            var lockKeys = new FixedLengthTransactionalKeyStruct[NumSnapshotRecords];
            for (var ii = 0; ii < NumSnapshotRecords; ii++)
            {
                txnKeys[ii] = FirstSnapshotKey + ii;
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
                if (checkpointType == CheckpointType.Snapshot)
                {
                    store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                    for (var ii = 0; ii < NumSnapshotRecords; ii++)
                        _ = tContext.Upsert(new TestObjectKey { key = txnKeys[ii] }, CreateValue(txnKeys[ii], KeptObjectSize));
                }
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

        // Every kept or new key must be found with its own value, and no undone key may be found.
        private static void VerifyKeys(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, int numSnapshotRecords, string where)
        {
            using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
            var bContext = session.BasicContext;
            foreach (var (firstKey, count, expectFound) in new[] { (0, NumRecords, true), (FirstUndoneKey, NumUndoneRecords, false),
                (FirstSnapshotKey, numSnapshotRecords, true), (FirstNewKey, NumNewRecords, true) })
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
                        var value = output.valueObject;
                        ClassicAssert.IsTrue(value is not null && value.value.Length == KeptObjectSize && value.value[0] == (byte)key && value.value[1] == (byte)(key >> 8),
                            $"key {key} wrong value ({where})");
                    }
                    else
                        ClassicAssert.IsFalse(status.Found, $"undone key {key} found ({where})");
                }
            }
        }

        private static void Upsert(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, int firstKey, int count, int objectSize)
        {
            using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
            for (var key = firstKey; key < firstKey + count; key++)
                _ = session.BasicContext.Upsert(new TestObjectKey { key = key }, CreateValue(key, objectSize));
        }

        // The value's first bytes identify its key.
        private static TestLargeObjectValue CreateValue(int key, int objectSize)
        {
            var value = new TestLargeObjectValue(objectSize);
            value.value[0] = (byte)key;
            value.value[1] = (byte)(key >> 8);
            return value;
        }

        private static void Prepare(out IDevice log, out IDevice objlog, out TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "objtail.log"));
            objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "objtail.obj.log"));
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