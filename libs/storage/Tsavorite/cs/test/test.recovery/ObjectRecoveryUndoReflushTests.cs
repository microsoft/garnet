// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery.objects
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// Regression tests for the Pass1 recovery undo-reflush of a FoldOver checkpoint's fuzzy boundary page.
    ///
    /// The bug (fixed in <c>ObjectAllocatorImpl.WriteAsync</c>'s hybrid-log-region recovery branch): a FoldOver checkpoint
    /// keeps its whole fuzzy region in the main log (no snapshot file), so on recovery the fuzzy v+1 records are undone by
    /// <c>RecoverHybridLogAsync</c> -> <c>ProcessReadPageAndFlush</c> -> <c>AsyncFlushPagesForRecovery</c> and any page
    /// carrying an undone (touched) record is re-flushed. For the still-valid object records on that boundary page, the
    /// recovery flush used to call <c>SetRecoveredObjectLogRecordStartPosition</c>, which reads the record's object-log
    /// position slot AS A LENGTH. But Pass1 never deserializes (it only SetInvalid + rehashes), so the slot still holds the
    /// original POSITION; consuming it as a length advanced the running page position and stamped garbage positions/length
    /// hints into the on-disk record image. The recovering run reads the correct live in-memory page (so the corruption is
    /// MASKED that run), but a later recovery that reads the page from disk gets the garbage — a cross-restart corruption.
    /// The fix writes a v2.2 record VERBATIM here (its object bytes are already durable in the main object-log and its
    /// position/hints are already correct; only the SetInvalid captured by the flush copy is needed).
    ///
    /// <see cref="FoldOverFuzzyUndoReflushSurvivesSecondRecovery"/> recovers TWICE from the same FoldOver token/log and
    /// asserts the keepers survive the second recovery; <see cref="FoldOverNoFuzzyDoubleRecoveryControl"/> is the no-fuzzy
    /// control that isolates the corruption to the undo-reflush (it always passed, before and after the fix).
    /// </summary>
    [TestFixture]
    public class ObjectRecoveryUndoReflushTests : TestBase
    {
        // Enough stable records to span many pages so the boundary page is a real main-log page that gets touched/reflushed.
        const int NumStable = 6000;

        // Fuzzy v+1 records written during IN_PROGRESS; a few hundred guarantees the boundary page fills and (with FoldOver)
        // is flushed to the main log, and that at least one fuzzy record shares the last stable page.
        const int NumFuzzy = 512;

        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task SnapshotDoesNotAdvanceReadOnlyAddress()
        {
            var logMemorySize = 64L * MinKvLogPageSize;
            Guid token;
            long flushedObservedDuringCheckpoint;
            long readOnlyBeforeSnapshotFlush;

            Prepare(logMemorySize, out var log, out var objlog, out var store);
            try
            {
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var i = 0; i < NumStable; i++)
                        _ = bContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = i });

                    ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.Snapshot), "failed to initiate Snapshot checkpoint");

                    var guard = 0;
                    while (store.SystemState.Phase != Phase.IN_PROGRESS)
                    {
                        bContext.Refresh();
                        if (++guard > 1_000_000)
                        {
                            Assert.Fail($"state machine never reached IN_PROGRESS (stuck at {store.SystemState.Phase})");
                            return;
                        }
                    }

                    for (var i = NumStable; i < NumStable + NumFuzzy; i++)
                        _ = bContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = i });
                    flushedObservedDuringCheckpoint = store.Log.FlushedUntilAddress;
                    readOnlyBeforeSnapshotFlush = store.Log.ReadOnlyAddress;
                }

                await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);
                Assert.That(store.Log.ReadOnlyAddress, Is.EqualTo(readOnlyBeforeSnapshotFlush));
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            var checkpointInfo = default(HybridLogRecoveryInfo);
            checkpointInfo.Recover(token,
                new DeviceLogCommitCheckpointManager(
                    new LocalStorageNamedDeviceFactoryCreator(),
                    new DefaultCheckpointNamingScheme(new DirectoryInfo(Path.Combine(MethodTestDir, "check-points")).FullName)));
            Assert.That(checkpointInfo.snapshotFileLogicalStartAddress, Is.LessThanOrEqualTo(flushedObservedDuringCheckpoint));

            Prepare(logMemorySize, out log, out objlog, out store);
            try
            {
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);

                using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
                var bContext = session.BasicContext;
                for (var i = 0; i < NumStable; i++)
                {
                    var found = TryReadValue(bContext, i, out var value);
                    ClassicAssert.IsTrue(found, $"stable key {i} not found");
                    ClassicAssert.AreEqual(i, value, $"stable key {i} has wrong value");
                }

                for (var i = NumStable; i < NumStable + NumFuzzy; i++)
                    ClassicAssert.IsFalse(TryReadValue(bContext, i, out _), $"fuzzy key {i} was not undone");
            }
            finally
            {
                Destroy(log, objlog, store);
            }
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task SnapshotRecoveryMergesSuffixAtFlushedBoundary()
        {
            const int candidateStartRecord = 5000;
            const int numRecords = 8000;
            var logMemorySize = 2048L * MinKvLogPageSize;
            long snapshotFileLogicalStartAddress = 0, mainLogRecoveryEndAddress = 0;
            long snapshotStartPage = 0;
            uint sectorSize = 0;
            Guid token;

            Prepare(logMemorySize, out var log, out var objlog, out var store, throttleCheckpointFlushDelayMs: 5);
            try
            {
                using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
                var bContext = session.BasicContext;

                for (var i = 0; i < numRecords; i++)
                {
                    _ = bContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = i });
                    var tail = store.Log.TailAddress;
                    var offset = store.hlogBase.GetOffsetOnPage(tail);

                    if (i >= candidateStartRecord && snapshotFileLogicalStartAddress == 0 && tail > store.Log.ReadOnlyAddress
                        && offset > log.SectorSize && offset < MinKvLogPageSize - (2 * log.SectorSize)
                        && offset % log.SectorSize != 0)
                    {
                        snapshotFileLogicalStartAddress = tail;
                    }
                    else if (snapshotFileLogicalStartAddress != 0 && mainLogRecoveryEndAddress == 0
                        && store.hlogBase.GetPage(tail) == store.hlogBase.GetPage(snapshotFileLogicalStartAddress)
                        && tail > snapshotFileLogicalStartAddress
                        && offset < MinKvLogPageSize - log.SectorSize && offset % log.SectorSize != 0)
                    {
                        mainLogRecoveryEndAddress = tail;
                    }
                }

                Assert.That(snapshotFileLogicalStartAddress, Is.GreaterThan(0),
                    "failed to find an unaligned snapshotFileLogicalStartAddress record boundary");
                Assert.That(mainLogRecoveryEndAddress, Is.GreaterThan(snapshotFileLogicalStartAddress),
                    "failed to find a later unaligned mainLogRecoveryEndAddress record boundary on the same page");
                snapshotStartPage = store.hlogBase.GetPage(snapshotFileLogicalStartAddress);
                sectorSize = log.SectorSize;
                store.Log.ShiftReadOnlyAddress(snapshotFileLogicalStartAddress, wait: true);

                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.Snapshot),
                    "failed to initiate Snapshot checkpoint");

                var guard = 0;
                while (store.SystemState.Phase != Phase.WAIT_FLUSH)
                {
                    bContext.Refresh();
                    if (++guard > 1_000_000)
                    {
                        Assert.Fail($"state machine never reached WAIT_FLUSH (stuck at {store.SystemState.Phase})");
                        return;
                    }
                }

                store.Log.ShiftReadOnlyAddress(mainLogRecoveryEndAddress, wait: true);
                await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            var checkpointInfo = default(HybridLogRecoveryInfo);
            checkpointInfo.Recover(token,
                new DeviceLogCommitCheckpointManager(
                    new LocalStorageNamedDeviceFactoryCreator(),
                    new DefaultCheckpointNamingScheme(new DirectoryInfo(Path.Combine(MethodTestDir, "check-points")).FullName)));
            Assert.Multiple(() =>
            {
                Assert.That(checkpointInfo.snapshotFileLogicalStartAddress, Is.EqualTo(snapshotFileLogicalStartAddress));
                Assert.That(checkpointInfo.mainLogRecoveryEndAddress, Is.EqualTo(mainLogRecoveryEndAddress));
                Assert.That(checkpointInfo.mainLogRecoveryEndAddress % sectorSize, Is.Not.Zero);
                Assert.That(checkpointInfo.mainLogRecoveryEndAddress / MinKvLogPageSize, Is.EqualTo(snapshotStartPage));
            });

            Prepare(logMemorySize, out log, out objlog, out store);
            try
            {
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);

                using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
                var bContext = session.BasicContext;
                for (var i = 0; i < numRecords; i++)
                {
                    var found = TryReadValue(bContext, i, out var value);
                    ClassicAssert.IsTrue(found, $"recovered key {i} not found");
                    ClassicAssert.AreEqual(i, value, $"recovered key {i} has wrong value");
                }
            }
            finally
            {
                Destroy(log, objlog, store);
            }
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task NullDeviceSnapshotKeepsActivePageResident()
        {
            const int stableCount = 200;
            const int fuzzyCount = 3000;
            var logMemorySize = 8L * MinKvLogPageSize;
            Guid token;

            Prepare(logMemorySize, out var log, out var objlog, out var store,
                throttleCheckpointFlushDelayMs: 5, useNullMainDevices: true);
            try
            {
                using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
                var bContext = session.BasicContext;
                for (var i = 0; i < stableCount; i++)
                    _ = bContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = i });

                Assert.That(store.hlogBase.HeadAddress, Is.LessThanOrEqualTo(store.hlogBase.GetFirstValidLogicalAddressOnPage(0)),
                    "stable setup evicted records before Snapshot began");
                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.Snapshot),
                    "failed to initiate Snapshot checkpoint");

                var guard = 0;
                while (store.SystemState.Phase != Phase.WAIT_FLUSH)
                {
                    bContext.Refresh();
                    if (++guard > 1_000_000)
                    {
                        Assert.Fail($"state machine never reached WAIT_FLUSH (stuck at {store.SystemState.Phase})");
                        return;
                    }
                }

                for (var i = stableCount; i < stableCount + fuzzyCount; i++)
                    _ = bContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = i });

                await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            Prepare(logMemorySize, out log, out objlog, out store, useNullMainDevices: true);
            try
            {
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);

                using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
                var bContext = session.BasicContext;
                for (var i = 0; i < stableCount; i++)
                {
                    var found = TryReadValue(bContext, i, out var value);
                    ClassicAssert.IsTrue(found, $"stable key {i} not found");
                    ClassicAssert.AreEqual(i, value, $"stable key {i} has wrong value");
                }
                for (var i = stableCount; i < stableCount + fuzzyCount; i++)
                    ClassicAssert.IsFalse(TryReadValue(bContext, i, out _), $"fuzzy key {i} was not undone");
            }
            finally
            {
                Destroy(log, objlog, store);
            }
        }

        /// <summary>
        /// Build a FoldOver checkpoint with a fuzzy region (v+1 records undone on recovery) whose boundary page also carries
        /// valid v object records, then recover from it TWICE. The undo-reflush must NOT corrupt the on-disk object-log
        /// position of the valid records: every keeper correct after the first recovery must still be correct after a second
        /// recovery from the same token/log (and the second recovery must not throw while reading the object-log).
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task FoldOverFuzzyUndoReflushSurvivesSecondRecovery()
        {
            var logMemorySize = 64L * MinKvLogPageSize;
            Guid token;

            // Phase 1: build the fuzzy FoldOver checkpoint.
            Prepare(logMemorySize, out var log, out var objlog, out var store);
            try
            {
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions()))
                {
                    var bContext = session.BasicContext;

                    // Stable (v) keepers: value == key.
                    for (var i = 0; i < NumStable; i++)
                        _ = bContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = i });

                    // Start a FoldOver checkpoint and pump the state machine to IN_PROGRESS (version bumps v -> v+1).
                    ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.FoldOver), "failed to initiate FoldOver checkpoint");

                    var guard = 0;
                    while (store.SystemState.Phase != Phase.IN_PROGRESS)
                    {
                        bContext.Refresh();
                        if (++guard > 1_000_000)
                        {
                            Assert.Fail($"state machine never reached IN_PROGRESS (stuck at {store.SystemState.Phase})");
                            return;
                        }
                    }

                    // Fuzzy (v+1) records written during IN_PROGRESS: NEW keys so that, on recovery, an undone record is
                    // NotFound (a clean detector that the undo path fired). These land below the FoldOver untilAddress.
                    for (var i = NumStable; i < NumStable + NumFuzzy; i++)
                        _ = bContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = i });
                }

                await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            // Run 1 recovery: the undo-reflush touches the boundary page; the recovering run reads the correct live page.
            Prepare(logMemorySize, out log, out objlog, out store);
            var run1Found = new bool[NumStable];
            var run1Value = new int[NumStable];
            int undoneFuzzyCount;
            try
            {
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);

                using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
                var bContext = session.BasicContext;

                for (var i = 0; i < NumStable; i++)
                {
                    run1Found[i] = TryReadValue(bContext, i, out run1Value[i]);
                    ClassicAssert.IsTrue(run1Found[i], $"run1: keeper key {i} not found");
                    ClassicAssert.AreEqual(i, run1Value[i], $"run1: keeper key {i} wrong value");
                }

                // Confirm the undo path was actually exercised: fuzzy (v+1) NEW keys must have been undone (NotFound). If this
                // ever stops firing, the test is no longer guarding the undo-reflush and should be revisited.
                undoneFuzzyCount = 0;
                for (var i = NumStable; i < NumStable + NumFuzzy; i++)
                {
                    if (!TryReadValue(bContext, i, out _))
                        undoneFuzzyCount++;
                }
            }
            finally
            {
                // NOTE: AllocatorBase.Dispose does not flush dirty pages, so run 1's on-disk image survives for run 2 to read.
                Destroy(log, objlog, store);
            }

            TestContext.WriteLine($"[undo-reflush] undone fuzzy keys after run 1: {undoneFuzzyCount} / {NumFuzzy}");
            ClassicAssert.Greater(undoneFuzzyCount, 0, "the fuzzy undo path was not exercised (no v+1 records were undone); this test no longer guards the undo-reflush");

            // Run 2 recovery: reads run 1's on-disk image. With the fix this recovers cleanly and every keeper still matches
            // run 1; a throw or a keeper regression here is the undo-reflush corruption reappearing.
            Prepare(logMemorySize, out log, out objlog, out store);
            var regressions = 0;
            var firstRegression = -1;
            try
            {
                try
                {
                    _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Assert.Fail($"run2: RecoverAsync threw ({ex.GetType().Name}: {ex.Message}) reading the image left by run 1 — undo-reflush corruption regression.");
                    return;
                }

                using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
                var bContext = session.BasicContext;

                for (var i = 0; i < NumStable; i++)
                {
                    // Only keepers that were correct in run 1 can prove an on-disk regression in run 2.
                    if (!run1Found[i])
                        continue;

                    bool found;
                    int value = 0;
                    try
                    {
                        found = TryReadValue(bContext, i, out value);
                    }
                    catch (Exception ex)
                    {
                        regressions++;
                        if (firstRegression < 0)
                            firstRegression = i;
                        TestContext.WriteLine($"[undo-reflush] run2 key {i}: read threw {ex.GetType().Name}: {ex.Message}");
                        continue;
                    }

                    if (!found || value != run1Value[i])
                    {
                        regressions++;
                        if (firstRegression < 0)
                            firstRegression = i;
                        if (regressions <= 20)
                            TestContext.WriteLine($"[undo-reflush] run2 key {i}: found={found} value={value} (run1 value={run1Value[i]})");
                    }
                }
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            TestContext.WriteLine($"[undo-reflush] keeper regressions run1 -> run2: {regressions} (first at key {firstRegression})");

            ClassicAssert.AreEqual(0, regressions,
                $"undo-reflush corruption regression: {regressions} keeper record(s) were correct after the first recovery but wrong/missing after a second recovery from the same FoldOver token (first regression at key {firstRegression}). " +
                "The undo-reflush is re-deriving the object-log position of valid records on the fuzzy boundary page instead of writing them verbatim.");
        }

        /// <summary>
        /// Control for <see cref="FoldOverFuzzyUndoReflushSurvivesSecondRecovery"/>: the SAME two-instance,
        /// double-recovery-from-one-FoldOver-token shape, but WITHOUT any fuzzy (v+1) writes, so no page is touched and no
        /// undo-reflush runs. Both recoveries must succeed and read every keeper correctly. This isolates the boundary-page
        /// corruption in the sibling test to the undo-reflush (rather than to double-recovery itself).
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task FoldOverNoFuzzyDoubleRecoveryControl()
        {
            var logMemorySize = 64L * MinKvLogPageSize;
            Guid token;

            Prepare(logMemorySize, out var log, out var objlog, out var store);
            try
            {
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var i = 0; i < NumStable; i++)
                        _ = bContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = i });
                }

                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.FoldOver), "failed to initiate FoldOver checkpoint");
                await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            // Recover twice from the same token; both runs must read every keeper correctly.
            for (var run = 1; run <= 2; run++)
            {
                Prepare(logMemorySize, out log, out objlog, out store);
                try
                {
                    _ = await store.RecoverAsync(default, token).ConfigureAwait(false);

                    using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
                    var bContext = session.BasicContext;
                    for (var i = 0; i < NumStable; i++)
                    {
                        var found = TryReadValue(bContext, i, out var value);
                        ClassicAssert.IsTrue(found, $"control run{run}: keeper key {i} not found");
                        ClassicAssert.AreEqual(i, value, $"control run{run}: keeper key {i} wrong value");
                    }
                }
                finally
                {
                    Destroy(log, objlog, store);
                }
            }
        }

        private static bool TryReadValue(BasicContext<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions, ClassStoreFunctions, ClassAllocator> bContext, int key, out int value)
        {
            TestObjectInput input = default;
            TestObjectOutput output = new();
            var status = bContext.Read(new TestObjectKey { key = key }, ref input, ref output);
            if (status.IsPending)
            {
                Assert.That(bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                (status, output) = GetSinglePendingResult(completedOutputs);
            }

            value = output.value?.value ?? 0;
            return status.Found;
        }

        private static void Prepare(long logMemorySize, out IDevice log, out IDevice objlog,
            out TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, int throttleCheckpointFlushDelayMs = -1,
            bool useNullMainDevices = false)
        {
            log = useNullMainDevices ? new NullDevice() : Devices.CreateLogDevice(Path.Combine(MethodTestDir, "undoreflush.log"));
            objlog = useNullMainDevices ? new NullDevice() : Devices.CreateLogDevice(Path.Combine(MethodTestDir, "undoreflush.obj.log"));
            store = new(new()
            {
                IndexSize = 1L << 22,
                LogDevice = log,
                ObjectLogDevice = objlog,
                SegmentSize = 1L << 20,
                LogMemorySize = logMemorySize,
                PageSize = MinKvLogPageSize,
                CheckpointDir = Path.Combine(MethodTestDir, "check-points"),
                ThrottleCheckpointFlushDelayMs = throttleCheckpointFlushDelayMs
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