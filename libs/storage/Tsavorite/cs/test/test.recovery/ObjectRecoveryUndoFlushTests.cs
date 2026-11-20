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
    /// Recovery undoes the records that a checkpoint's next version (v+1) wrote in the checkpoint's fuzzy region, then writes each main-log page it changed
    /// back to the main log. That write happens before the page's objects are loaded, so every record on the page must keep the object-log position it was
    /// read with. These tests hold a checkpoint in IN_PROGRESS with an open version-v transaction while writing v+1 records, recover, and check every record
    /// both while resident and, after evicting the whole log, as read back from disk.
    /// </summary>
    [TestFixture]
    public class ObjectRecoveryUndoFlushTests : TestBase
    {
        /// <summary>The number of version-v records when the hybrid-log scan starts at the start of the log</summary>
        const int NumRecords = 1000;

        /// <summary>For a mid-page scan start, the number of version-v records written before evicting the whole log, which leaves HeadAddress mid-page</summary>
        const int NumRecordsBeforeEvict = 510;

        /// <summary>For a mid-page scan start, the total number of version-v records; few enough that the tail stays on the page holding HeadAddress</summary>
        const int NumRecordsMidPage = 530;

        /// <summary>The number of keys first written in version v+1, which recovery must remove</summary>
        const int NumNewKeys = 100;

        /// <summary>The number of version-v keys rewritten in version v+1, which recovery must return to their version-v values</summary>
        const int NumUpdatedKeys = 10;

        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        // scanStartsMidPage: evicting the whole log partway through leaves HeadAddress mid-page, and the full checkpoint's index starts above it, so the
        // hybrid-log scan starts at HeadAddress; the v+1 records follow on the same page, so the undo write of that page starts mid-page as well.
        // For Snapshot, the main log is flushed past the v+1 records while the checkpoint is held, so the hybrid-log phase reads and undoes them.
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task RecoveryUndoFlushKeepsObjectLogPositions(
            [Values(CheckpointType.FoldOver, CheckpointType.Snapshot)] CheckpointType checkpointType,
            [Values] bool scanStartsMidPage)
        {
            var numRecords = scanStartsMidPage ? NumRecordsMidPage : NumRecords;

            Prepare(out var log, out var objlog, out var store);
            try
            {
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var i = 0; i < numRecords; i++)
                    {
                        if (scanStartsMidPage && i == NumRecordsBeforeEvict)
                            store.Log.FlushAndEvict(wait: true);
                        _ = bContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = i });
                    }
                }

                if (scanStartsMidPage)
                {
                    ClassicAssert.Greater(store.Log.HeadAddress % MinKvLogPageSize, PageHeader.Size, "HeadAddress must be past the first record on its page");
                    ClassicAssert.AreEqual(store.Log.HeadAddress / MinKvLogPageSize, store.Log.TailAddress / MinKvLogPageSize,
                        "the v+1 records must start on the page holding HeadAddress");
                }

                // An open version-v transaction holds the checkpoint in IN_PROGRESS, so the records written meanwhile are v+1 records in its fuzzy region.
                Guid token;
                var txnVersion = store.stateMachineDriver.AcquireTransactionVersion();
                try
                {
                    ClassicAssert.IsTrue(store.TryInitiateFullCheckpoint(out token, checkpointType));
                    while (store.stateMachineDriver.SystemState.Phase != Phase.IN_PROGRESS)
                        _ = Thread.Yield();

                    using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions()))
                    {
                        var bContext = session.BasicContext;
                        for (var i = numRecords; i < numRecords + NumNewKeys; i++)
                            _ = bContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = i });
                        for (var i = numRecords - NumUpdatedKeys; i < numRecords; i++)
                            _ = bContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = -i });
                    }

                    if (checkpointType == CheckpointType.Snapshot)
                        store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                }
                finally
                {
                    store.stateMachineDriver.EndTransaction(txnVersion);
                }

                await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);
                Destroy(log, objlog, store);

                Prepare(out log, out objlog, out store);
                _ = await store.RecoverAsync(token).ConfigureAwait(false);
                VerifyRecords(store, numRecords, "resident");

                store.Log.FlushAndEvict(wait: true);
                VerifyRecords(store, numRecords, "read from disk");
            }
            finally
            {
                Destroy(log, objlog, store);
            }
        }

        // Each version-v key must have its version-v value, and no key first written in version v+1 may be found.
        private static void VerifyRecords(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, int numRecords, string where)
        {
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;
            for (var i = 0; i < numRecords + NumNewKeys; i++)
            {
                TestObjectInput input = default;
                TestObjectOutput output = new();
                var status = bContext.Read(new TestObjectKey { key = i }, ref input, ref output);
                if (status.IsPending)
                {
                    Assert.That(bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }

                if (i < numRecords)
                {
                    ClassicAssert.IsTrue(status.Found, $"key {i} not found ({where})");
                    ClassicAssert.AreEqual(i, output.value.value, $"key {i} wrong value ({where})");
                }
                else
                    ClassicAssert.IsFalse(status.Found, $"key {i}, first written in version v+1, found ({where})");
            }
        }

        private static void Prepare(out IDevice log, out IDevice objlog, out TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "undoflush.log"));
            objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "undoflush.obj.log"));
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