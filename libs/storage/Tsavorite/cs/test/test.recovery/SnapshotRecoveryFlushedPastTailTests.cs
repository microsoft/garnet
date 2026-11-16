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
    /// above the tail are discarded, so the object log resumes after the kept records' objects, reusing the space of theirs.
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

                // The checkpoint has taken its tail when it is about to enter WAIT_FLUSH. Write more records there and flush the main log past that tail.
                var pause = new PauseAtPhase(Phase.WAIT_FLUSH);
                store.stateMachineDriver.UnsafeRegisterCallback(pause);
                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.Snapshot));
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
                VerifyKeys(store, 0, NumRecords, expectFound: true);
                VerifyKeys(store, NumRecords, NumPostCheckpointRecords, expectFound: false);
                VerifyKeys(store, FirstNewKey, NumNewKeys, expectFound: true);
            }
            finally
            {
                Destroy(log, objlog, store);
            }
        }

        private static void Upsert(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, int firstKey, int count)
        {
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;
            for (var key = firstKey; key < firstKey + count; key++)
                _ = bContext.Upsert(new TestObjectKey { key = key }, new TestObjectValue { value = key });
        }

        // Each key in the range must be found with its own value, or, with expectFound false, not found.
        private static void VerifyKeys(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, int firstKey, int count, bool expectFound)
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
                    ClassicAssert.IsTrue(status.Found, $"key {key} not found");
                    ClassicAssert.AreEqual(key, output.value.value, $"key {key} wrong value");
                }
                else
                    ClassicAssert.IsFalse(status.Found, $"key {key}, written after the first checkpoint took its tail, found");
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