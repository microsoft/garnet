// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery.objects
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// A snapshot checkpoint takes its tail (finalLogicalAddress) at WAIT_FLUSH and its flushedLogicalAddress later, at PERSISTENCE_CALLBACK, so writes
    /// during the checkpoint can carry the main log's flush past the tail. Recovery restores TailAddress to the tail, so it must not treat the log as flushed
    /// above it: records written after recovery at those addresses must be flushed like any others, and survive the next checkpoint and recovery. The records
    /// above the tail are discarded, so the object log resumes after the kept records' objects, reusing the space of theirs. Such a recovery leaves the log
    /// flushed only to the tail, usually mid-page, so the next snapshot checkpoint can have a mid-page flushedLogicalAddress.
    /// </summary>
    [TestFixture]
    public class SnapshotRecoveryFlushedPastTailTests : TestBase
    {
        /// <summary>The number of records the checkpoint holds</summary>
        const int NumRecords = 1000;

        /// <summary>The number of records written after the checkpoint takes its tail, which the checkpoint does not hold</summary>
        const int NumPostCheckpointRecords = 100;

        /// <summary>The first key written after recovery</summary>
        const int FirstNewKey = 5000;

        /// <summary>The number of keys written after recovery; enough to run past the first checkpoint's flushedLogicalAddress</summary>
        const int NumNewKeys = 200;

        /// <summary>Pauses the checkpoint state machine on entry to a chosen phase so the test can act in that window.</summary>
        private sealed class PauseAtPhase(Phase pauseAt) : IStateMachineCallback
        {
            /// <summary>Set when the state machine is about to enter the chosen phase</summary>
            internal readonly ManualResetEventSlim Reached = new(false);

            /// <summary>Set by the test to let the state machine enter the chosen phase</summary>
            internal readonly ManualResetEventSlim Release = new(false);

            public void BeforeEnteringState(SystemState next)
            {
                if (next.Phase != pauseAt)
                    return;
                Reached.Set();
                _ = Release.Wait(TimeSpan.FromSeconds(30));
            }
        }

        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task RecoveryAfterFlushPastCheckpointTailKeepsNewRecords()
        {
            Prepare(out var log, out var objlog, out var store);
            Guid token;
            ulong keptObjectsEnd;
            try
            {
                Upsert(store, 0, NumRecords);

                // Flush them all, so their objects end the object log, at a sector boundary; recovery must resume the object log there.
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                keptObjectsEnd = store.hlogBase.GetObjectLogTail().CurrentAddress;

                (token, _) = await CheckpointFlushedPastTailAsync(store).ConfigureAwait(false);

                // The checkpoint records the object log's tail when it completes, after the objects of the records written after its tail.
                var checkpointObjectLogTail = store.hlogBase.GetObjectLogTail().CurrentAddress;
                ClassicAssert.Greater(checkpointObjectLogTail, keptObjectsEnd, "the checkpoint's object-log tail must be past the kept records' objects");
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            Guid token2;
            Prepare(out log, out objlog, out store);
            try
            {
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                ClassicAssert.LessOrEqual(store.Log.ReadOnlyAddress, store.Log.TailAddress, "ReadOnlyAddress must not be above the tail after recovery");
                ClassicAssert.LessOrEqual(store.Log.FlushedUntilAddress, store.Log.TailAddress, "FlushedUntilAddress must not be above the tail after recovery");
                ClassicAssert.AreEqual(keptObjectsEnd, store.hlogBase.GetObjectLogTail().CurrentAddress,
                    "the object log must resume right after the kept records' objects, reusing the space of the discarded records' objects");

                Upsert(store, FirstNewKey, NumNewKeys);
                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token2, CheckpointType.FoldOver));
                await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            Prepare(out log, out objlog, out store);
            try
            {
                _ = await store.RecoverAsync(default, token2).ConfigureAwait(false);
                VerifyAllKeys(store, NumNewKeys, "after the second recovery");
            }
            finally
            {
                Destroy(log, objlog, store);
            }
        }

        // When recovery caps the main-log region at the checkpoint's tail, it leaves ReadOnlyAddress and FlushedUntilAddress there, mid-page, with that page in
        // memory. A snapshot checkpoint taken before the read-only address moves then has that mid-page address as its flushedLogicalAddress, and recovering
        // from it keeps the page's main-log records below that address while taking the rest of the page from the snapshot file. The main log on disk still
        // holds, on that page above the first checkpoint's tail, the records written after that tail, which the first recovery discarded; they must not come
        // back. newRecords is the number of records written between the two checkpoints: with 0 the second snapshot holds no page, with 20 it holds the rest of
        // that page, and with 1000 it also holds enough later pages that a 4-page recovery budget evicts that page, flushing it to the main log from the
        // mid-page address.
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task SnapshotCheckpointAfterRecoveryCappedAtTail([Values(0, 20, 1000)] int newRecords, [Values(0, 4)] int recoveryTargetPageCount)
        {
            Prepare(out var log, out var objlog, out var store);
            Guid token;
            long checkpointTail;
            try
            {
                Upsert(store, 0, NumRecords);
                (token, checkpointTail) = await CheckpointFlushedPastTailAsync(store).ConfigureAwait(false);
                ClassicAssert.Greater(checkpointTail % MinKvLogPageSize, PageHeader.Size, "the checkpoint's tail must be past the first record on its page");
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            Guid token2;
            Prepare(out log, out objlog, out store);
            try
            {
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                ClassicAssert.AreEqual(checkpointTail, store.Log.FlushedUntilAddress, "recovery must leave FlushedUntilAddress at the checkpoint's tail");
                ClassicAssert.Less(store.Log.HeadAddress, checkpointTail, "the tail's page must stay resident");

                Upsert(store, FirstNewKey, newRecords);
                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token2, CheckpointType.Snapshot));
                await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);
                ClassicAssert.AreEqual(checkpointTail, store.GetLogFileSize(token2).hybridLogFileEndAddress,
                    "the second checkpoint's flushedLogicalAddress must be the first checkpoint's tail");
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            Prepare(out log, out objlog, out store);
            try
            {
                if (recoveryTargetPageCount > 0)
                {
                    var targetSize = (long)recoveryTargetPageCount * MinKvLogPageSize;
                    store.Log.SetLogSizeTracker(new LogSizeTracker<ClassStoreFunctions, ClassAllocator>(store.Log, targetSize, targetSize / 8, targetSize / 16, logger: null));
                }
                _ = await store.RecoverAsync(default, token2).ConfigureAwait(false);
                if (recoveryTargetPageCount > 0 && newRecords >= 1000)
                {
                    ClassicAssert.GreaterOrEqual(store.Log.HeadAddress, checkpointTail - checkpointTail % MinKvLogPageSize + MinKvLogPageSize,
                        "recovery must evict the tail's page");
                }
                VerifyAllKeys(store, newRecords, "after recovery");

                // Read every record from disk. Unless recovery evicted it, the tail's page is flushed from the mid-page address, over the discarded records.
                store.Log.FlushAndEvict(wait: true);
                VerifyAllKeys(store, newRecords, "read from disk");
            }
            finally
            {
                Destroy(log, objlog, store);
            }
        }

        // Takes a snapshot checkpoint whose flushedLogicalAddress is above its tail. The checkpoint has taken its tail when it is about to enter WAIT_FLUSH, so
        // the records written there are not in the checkpoint, and flushing the main log past them carries flushedLogicalAddress past the tail. Returns the
        // checkpoint's token and tail.
        private static async Task<(Guid token, long checkpointTail)> CheckpointFlushedPastTailAsync(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            var pause = new PauseAtPhase(Phase.WAIT_FLUSH);
            store.stateMachineDriver.UnsafeRegisterCallback(pause);
            ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out var token, CheckpointType.Snapshot));
            ClassicAssert.IsTrue(pause.Reached.Wait(TimeSpan.FromSeconds(30)), "the checkpoint did not reach WAIT_FLUSH");
            var checkpointTail = store.Log.TailAddress;
            try
            {
                Upsert(store, NumRecords, NumPostCheckpointRecords);
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            }
            finally
            {
                pause.Release.Set();
            }

            await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);
            ClassicAssert.Greater(store.GetLogFileSize(token).hybridLogFileEndAddress, checkpointTail, "the checkpoint's flushedLogicalAddress must be above its tail");
            return (token, checkpointTail);
        }

        private static void Upsert(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, int firstKey, int count)
        {
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;
            for (var key = firstKey; key < firstKey + count; key++)
                _ = bContext.Upsert(new TestObjectKey { key = key }, new TestObjectValue { value = key });
        }

        // The checkpoint's keys and the keys written after recovery must be found with their own values, and the keys written after the checkpoint took its
        // tail must not be found.
        private static void VerifyAllKeys(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, int numNewKeys, string where)
        {
            VerifyKeys(store, 0, NumRecords, expectFound: true, where);
            VerifyKeys(store, NumRecords, NumPostCheckpointRecords, expectFound: false, where);
            VerifyKeys(store, FirstNewKey, numNewKeys, expectFound: true, where);
        }

        // Each key in the range must be found with its own value, or, with expectFound false, not found.
        private static void VerifyKeys(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, int firstKey, int count, bool expectFound, string where)
        {
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;
            for (var key = firstKey; key < firstKey + count; key++)
            {
                TestObjectInput input = default;
                TestObjectOutput output = new();
                var status = bContext.Read(new TestObjectKey { key = key }, ref input, ref output);
                if (status.IsPending)
                {
                    Assert.That(bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }

                if (expectFound)
                {
                    ClassicAssert.IsTrue(status.Found, $"key {key} not found ({where})");
                    ClassicAssert.AreEqual(key, output.value.value, $"key {key} wrong value ({where})");
                }
                else
                    ClassicAssert.IsFalse(status.Found, $"key {key}, written after the first checkpoint took its tail, found ({where})");
            }
        }

        private static void Prepare(out IDevice log, out IDevice objlog, out TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "pasttail.log"));
            objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "pasttail.obj.log"));
            store = new(new()
            {
                IndexSize = 1L << 22,
                LogDevice = log,
                ObjectLogDevice = objlog,
                SegmentSize = 1L << 20,
                LogMemorySize = 32L * MinKvLogPageSize,
                PageSize = MinKvLogPageSize,
                CheckpointDir = Path.Combine(MethodTestDir, "check-points")
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer())
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