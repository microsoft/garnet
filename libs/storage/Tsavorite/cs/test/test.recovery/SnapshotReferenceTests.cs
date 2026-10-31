// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;
using SharedListFunctions = Tsavorite.test.recovery.CheckpointSharedObjectMutationTests.SharedListFunctions;
using SharedListHeapObject = Tsavorite.test.recovery.CheckpointSharedObjectMutationTests.SharedListHeapObject;
using SharedListSerializer = Tsavorite.test.recovery.CheckpointSharedObjectMutationTests.SharedListSerializer;

namespace Tsavorite.test.recovery
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// A (v+1) CopyUpdate during a Snapshot checkpoint captures the (v) value it supersedes into the checkpoint's object log, unless the
    /// value's main-log image is already durable. These tests order the CopyUpdate against the Checkpoint Flush and the ReadOnly flush of the
    /// source's page, and check that recovery returns the (v) value from whichever log it reads the source from.
    /// </summary>
    [TestFixture]
    internal class SnapshotReferenceTests : TestBase
    {
        const int InitialCount = 8;
        const int RmwValue = 99;
        const long MainLogSegmentSize = 1L << 20;
        const long ObjectLogSegmentSize = 1L << 30;

        /// <summary>When the (v+1) RMW runs relative to the flushes that write the source.</summary>
        public enum RmwOrder
        {
            /// <summary>Before the Checkpoint Flush walks the source; the ReadOnly flush then writes the source's page before the checkpoint
            /// samples the main log's recovery end, so recovery reads the source from the main log.</summary>
            BeforeCheckpointFlush,

            /// <summary>After the Checkpoint Flush serializes the source, before its page write completes; the ReadOnly flush then writes the
            /// source's page before the checkpoint samples the main log's recovery end.</summary>
            DuringCheckpointFlush,

            /// <summary>After the ReadOnly flush serializes the source, before its page write completes.</summary>
            DuringReadOnlyFlush,

            /// <summary>After the ReadOnly flush has written the source's page, so its image is durable and nothing is captured.</summary>
            AfterReadOnlyFlush,

            /// <summary>Before the Checkpoint Flush walks the source, and no ReadOnly flush writes it before the checkpoint completes, so
            /// recovery reads the source from the snapshot.</summary>
            BeforeCheckpointFlushWithoutReadOnlyFlush,
        }

        string dir;
        IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            dir = MethodTestDir;
            _ = Directory.CreateDirectory(dir);
            log = Devices.CreateLogDevice(Path.Join(dir, "Capture.log"), deleteOnClose: false);
            objlog = Devices.CreateLogDevice(Path.Join(dir, "Capture.obj.log"), deleteOnClose: false);
        }

        [TearDown]
        public void TearDown()
        {
            ObjectFlushInjection.Reset();
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;
            OnTearDown();
        }

        TsavoriteKV<ClassStoreFunctions, ClassAllocator> CreateStore(bool overflowKeys = false)
            => CreateStore(overflowKeys, log, objlog, dir);

        static TsavoriteKV<ClassStoreFunctions, ClassAllocator> CreateStore(bool overflowKeys, IDevice log, IDevice objlog, string dir)
            => new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.9,
                LogMemorySize = 1L << 16,
                PageSize = 1L << 13,
                SegmentSize = MainLogSegmentSize,
                ObjectLogSegmentSize = ObjectLogSegmentSize,
                MaxInlineKeySize = overflowKeys ? 0 : KVSettings.DefaultMaxInlineKeySize,
                CheckpointDir = dir
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new SharedListSerializer())
             , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        [Test]
        public void SnapshotOwnershipSurvivesClearingTransientReleaseState()
        {
            RecordInfo info = default;
            info.InitializeForNewRecord();
            info.HasSnapshotObjectLogReference = true;
            info.DeferredCopyUpdate = true;
            info.DeferredDispose = true;
            Assert.That(info.TryTakeDeferredDispose(out var copyUpdate), Is.True);
            Assert.That(copyUpdate, Is.True);
            Assert.That(info.TryTakeDeferredDispose(out _), Is.False, "A release must be claimed only once");
            Assert.That(info.HasSnapshotObjectLogReference, Is.True);

            info.DeferredCopyUpdate = true;
            info.DeferredDispose = true;
            info.ClearBitsForDiskImages();
            Assert.That(info.HasSnapshotObjectLogReference, Is.True);
            Assert.That(info.DeferredCopyUpdate, Is.False);
            Assert.That(info.DeferredDispose, Is.False);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async Task RecoveryReadsTheSupersededValue([Values] RmwOrder order, [Values] bool overflowKeys)
        {
            if (!ObjectFlushInjection.IsAvailable)
                Assert.Ignore("ObjectFlushInjection is compiled out in Release, so the flushes cannot be held.");

            var key = new TestObjectKey { key = 1 };
            var shared = new List<int>();
            for (var ii = 1; ii <= InitialCount; ++ii)
                shared.Add(ii);
            var sourceObject = new SharedListHeapObject(shared);

            var rmwBeforeCheckpointFlush = order is RmwOrder.BeforeCheckpointFlush or RmwOrder.BeforeCheckpointFlushWithoutReadOnlyFlush;
            var readOnlyFlushesTheSource = order != RmwOrder.BeforeCheckpointFlushWithoutReadOnlyFlush;
            var expectCapture = order != RmwOrder.AfterReadOnlyFlush;
            var expectMainReference = expectCapture && readOnlyFlushesTheSource && order != RmwOrder.DuringReadOnlyFlush;

            using var checkpointFlushAtSource = new ManualResetEventSlim(false);
            using var releaseCheckpointFlushAtSource = new ManualResetEventSlim(false);
            using var checkpointFlushAtNextPage = new ManualResetEventSlim(false);
            using var releaseCheckpointFlushAtNextPage = new ManualResetEventSlim(false);
            using var readOnlyFlushAtSource = new ManualResetEventSlim(false);
            using var releaseReadOnlyFlushAtSource = new ManualResetEventSlim(false);

            Guid token;
            long sourceAddress, sourceTail;
            int[] serializedCounts;
            var store = CreateStore(overflowKeys);
            try
            {
                long earlierAddress, nextPageAddress;
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
                {
                    earlierAddress = store.Log.TailAddress;
                    _ = session.BasicContext.Upsert(new TestObjectKey { key = 2 }, new SharedListHeapObject([0]), Empty.Default);
                    sourceAddress = store.Log.TailAddress;
                    _ = session.BasicContext.Upsert(key, sourceObject, Empty.Default);
                    sourceTail = store.Log.TailAddress;

                    // Fill the source's page, then put one object record on the next page for the Checkpoint Flush to be held on.
                    var nextKey = 3;
                    while (store.hlogBase.GetPage(store.Log.TailAddress) == store.hlogBase.GetPage(sourceAddress))
                        _ = session.BasicContext.Upsert(new TestObjectKey { key = nextKey++ }, new SharedListHeapObject([nextKey]), Empty.Default);
                    nextPageAddress = store.Log.TailAddress;
                    _ = session.BasicContext.Upsert(new TestObjectKey { key = nextKey }, new SharedListHeapObject([nextKey]), Empty.Default);
                }

                var sourcePage = store.hlogBase.GetPage(sourceAddress);
                var sourcePageEnd = store.hlogBase.GetLogicalAddressOfStartOfPage(sourcePage + 1);
                Assert.That(store.hlogBase.GetPage(earlierAddress), Is.EqualTo(sourcePage), "The earlier record must share the source's page");
                Assert.That(store.hlogBase.GetPage(nextPageAddress), Is.EqualTo(sourcePage + 1));
                Assert.That(store.Log.ReadOnlyAddress, Is.LessThanOrEqualTo(sourceAddress), "The source must start in the mutable region");
                Assert.That(store.hlogBase._wrapper.CreateLogRecord(sourceAddress).DataHeader.KeyIsOverflow, Is.EqualTo(overflowKeys));

                // Until the Checkpoint Flush is held on the next page, no ReadOnly flush runs, so every hold before that is the Checkpoint
                // Flush's. After it, the Checkpoint Flush has finished the source's page, so a hold at the source is the ReadOnly flush's.
                var checkpointFlushHoldAddress = order switch
                {
                    RmwOrder.BeforeCheckpointFlush or RmwOrder.BeforeCheckpointFlushWithoutReadOnlyFlush => earlierAddress,
                    RmwOrder.DuringCheckpointFlush => sourceAddress,
                    _ => -1,
                };
                var nextPage = sourcePage + 1;
                ObjectFlushInjection.Hook = (phase, logicalAddress) =>
                {
                    if (phase != ObjectFlushPhase.AfterRecordWritten)
                        return;
                    if (logicalAddress == checkpointFlushHoldAddress && !checkpointFlushAtSource.IsSet)
                    {
                        checkpointFlushAtSource.Set();
                        _ = releaseCheckpointFlushAtSource.Wait(TimeSpan.FromSeconds(30));
                    }
                    else if (store.hlogBase.GetPage(logicalAddress) == nextPage && !checkpointFlushAtNextPage.IsSet)
                    {
                        checkpointFlushAtNextPage.Set();
                        _ = releaseCheckpointFlushAtNextPage.Wait(TimeSpan.FromSeconds(30));
                    }
                    else if (order == RmwOrder.DuringReadOnlyFlush && logicalAddress == sourceAddress && checkpointFlushAtNextPage.IsSet
                        && !readOnlyFlushAtSource.IsSet)
                    {
                        readOnlyFlushAtSource.Set();
                        _ = releaseReadOnlyFlushAtSource.Wait(TimeSpan.FromSeconds(30));
                    }
                };

                Assert.That(store.TryInitiateFullCheckpoint(out token, CheckpointType.Snapshot), Is.True);

                if (checkpointFlushHoldAddress >= 0)
                {
                    Assert.That(checkpointFlushAtSource.Wait(TimeSpan.FromSeconds(30)), Is.True, "The Checkpoint Flush never reached its hold point");
                    if (rmwBeforeCheckpointFlush || order == RmwOrder.DuringCheckpointFlush)
                        Rmw(store, sourceAddress, expectCapture);
                    releaseCheckpointFlushAtSource.Set();
                }

                Assert.That(checkpointFlushAtNextPage.Wait(TimeSpan.FromSeconds(30)), Is.True, "The Checkpoint Flush never reached the next page");
                var coordination = store._hybridLogCheckpoint.snapshotFlushCoordination;
                Assert.That(coordination, Is.Not.Null, "Snapshot coordination was not created");
                var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
                while (coordination.ReadOnlyFlushPageLimit <= sourcePage && DateTime.UtcNow < deadline)
                    Thread.Sleep(1);
                Assert.That(coordination.ReadOnlyFlushPageLimit, Is.GreaterThan(sourcePage), "The Checkpoint Flush did not complete the source's page");

                switch (order)
                {
                    case RmwOrder.BeforeCheckpointFlush:
                    case RmwOrder.DuringCheckpointFlush:
                        store.Log.ShiftReadOnlyAddress(sourcePageEnd, wait: true);
                        break;

                    case RmwOrder.DuringReadOnlyFlush:
                        var readOnlyShift = Task.Run(() => store.Log.ShiftReadOnlyAddress(sourcePageEnd, wait: true));
                        Assert.That(readOnlyFlushAtSource.Wait(TimeSpan.FromSeconds(30)), Is.True, "The ReadOnly flush never reached the source");
                        Rmw(store, sourceAddress, expectCapture);
                        releaseReadOnlyFlushAtSource.Set();
                        Assert.That(readOnlyShift.Wait(TimeSpan.FromSeconds(30)), Is.True, "The ReadOnly shift did not complete");
                        break;

                    case RmwOrder.AfterReadOnlyFlush:
                        store.Log.ShiftReadOnlyAddress(sourcePageEnd, wait: true);
                        Assert.That(store.Log.FlushedUntilAddress, Is.GreaterThanOrEqualTo(sourcePageEnd));
                        Rmw(store, sourceAddress, expectCapture);
                        break;
                }
                if (readOnlyFlushesTheSource)
                {
                    Assert.That(store.Log.FlushedUntilAddress, Is.GreaterThanOrEqualTo(sourcePageEnd), "The ReadOnly flush did not write the source's page");
                    Assert.That(() => store.hlogBase._wrapper.CreateLogRecord(sourceAddress).DataHeader.ValueIsObject, Is.False.After(5000, 10),
                        "A superseded object's heap must be released after its page is durable, without waiting for eviction");
                }

                releaseCheckpointFlushAtNextPage.Set();
                await store.CompleteCheckpointAsync().ConfigureAwait(false);

                HybridLogCheckpointInfo persisted = default;
                try
                {
                    persisted.Recover(token, store.checkpointManager);
                    Assert.That(persisted.info.mainLogRecoveryEndAddress > sourceAddress, Is.EqualTo(readOnlyFlushesTheSource),
                        "Recovery must read the source from the main log exactly when the ReadOnly flush wrote it before the checkpoint ended");
                    Assert.That(persisted.info.snapshotObjectLogStartAddress < persisted.info.mainLogRecoveryEndAddress, Is.EqualTo(expectMainReference),
                        "Only a main-log image that references the snapshot extends its ownership range");
                }
                finally
                {
                    persisted.Dispose();
                }

                lock (sourceObject.SerializedItemCounts)
                    serializedCounts = [.. sourceObject.SerializedItemCounts];
            }
            finally
            {
                releaseCheckpointFlushAtSource.Set();
                releaseCheckpointFlushAtNextPage.Set();
                releaseReadOnlyFlushAtSource.Set();
                ObjectFlushInjection.Reset();
                store.Dispose();
            }

            Assert.That(serializedCounts, Is.Not.Empty, "The (v) object was never serialized");
            Assert.That(serializedCounts, Has.All.EqualTo(InitialCount),
                $"A serialization of the (v) object saw edited content; item counts: [{string.Join(", ", serializedCounts)}]");

            // Recover twice. Before the second recovery, the first recovered store appends to both logs, which the second must discard.
            for (var recovery = 1; recovery <= 2; recovery++)
            {
                store = CreateStore(overflowKeys);
                try
                {
                    _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                    ReadSourceVersion(store, key, sourceAddress, $"recovery {recovery}, in memory");

                    store.Log.FlushAndEvict(wait: true);
                    ReadSourceVersion(store, key, sourceAddress, $"recovery {recovery}, after eviction");

                    if (recovery == 1)
                    {
                        using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions());
                        for (var ii = 0; ii < 64; ii++)
                            _ = session.BasicContext.Upsert(new TestObjectKey { key = 1000 + ii }, new SharedListHeapObject([ii, ii, ii]), Empty.Default);
                        store.Log.FlushAndEvict(wait: true);
                    }
                }
                finally
                {
                    store.Dispose();
                }
            }
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task ReferencesBelowSnapshotStartAreRepairedOutsideTheResidentRange([Values] bool overflowKeys, [Values] bool useIndex)
        {
            var sourceObject = new SharedListHeapObject([1, 2, 3, 4, 5, 6, 7, 8]);
            var key = new TestObjectKey { key = 1 };
            Guid token;
            long sourceAddress;
            var store = CreateStore(overflowKeys);
            try
            {
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
                {
                    sourceAddress = store.Log.TailAddress;
                    _ = session.BasicContext.Upsert(key, sourceObject, Empty.Default);
                    var nextKey = 2;
                    while (store.hlogBase.GetPage(store.Log.TailAddress) < 2)
                        _ = session.BasicContext.Upsert(new TestObjectKey { key = nextKey++ }, new SharedListHeapObject([nextKey]), Empty.Default);
                    _ = session.BasicContext.Upsert(new TestObjectKey { key = nextKey }, new SharedListHeapObject([nextKey]), Empty.Default);
                }

                var pause = new PauseAtPhase(Phase.WAIT_INDEX_CHECKPOINT);
                store.stateMachineDriver.UnsafeRegisterCallback(pause);
                try
                {
                    Assert.That(store.TryInitiateFullCheckpoint(out token, CheckpointType.Snapshot), Is.True);
                    Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True);
                    Rmw(store, sourceAddress, expectCapture: true);
                    store.Log.ShiftReadOnlyAddress(store.hlogBase.GetLogicalAddressOfStartOfPage(1), wait: true);
                    using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions());
                    for (var ii = 0; ii < 2000; ii++)
                        _ = session.BasicContext.Upsert(new TestObjectKey { key = 10000 + ii }, new SharedListHeapObject([ii]), Empty.Default);
                }
                finally
                {
                    pause.Release.Set();
                }
                await store.CompleteCheckpointAsync().ConfigureAwait(false);
                HybridLogCheckpointInfo checkpoint = default;
                try
                {
                    checkpoint.Recover(token, store.checkpointManager);
                    Assert.That(checkpoint.info.snapshotObjectLogStartAddress, Is.EqualTo(sourceAddress));
                    Assert.That(checkpoint.info.snapshotFileLogicalStartAddress, Is.GreaterThan(sourceAddress));
                    Assert.That(checkpoint.info.headAddress, Is.GreaterThan(sourceAddress));
                }
                finally
                {
                    checkpoint.Dispose();
                }
            }
            finally
            {
                store.Dispose();
            }

            for (var recovery = 0; recovery < 2; recovery++)
            {
                store = CreateStore(overflowKeys);
                try
                {
                    _ = await store.RecoverAsync(useIndex ? token : default, token).ConfigureAwait(false);
                    Assert.That(store.Log.HeadAddress, Is.GreaterThan(sourceAddress));
                    ReadSourceVersion(store, key, sourceAddress, $"disk-only repair, recovery {recovery}");
                    var ranges = store.GetLogFileSize(token);
                    Assert.That(ranges.hybridLogObjectFileEndAddress, Is.GreaterThanOrEqualTo(store.hlogBase.ObjectLogFlushedUntilAddress),
                        "A transfer must include durable repair copies beyond the original metadata tail");
                    if (recovery == 0)
                        await VerifyTransferredCheckpoint(overflowKeys, token, ranges, key, sourceAddress).ConfigureAwait(false);
                    if (recovery == 1)
                    {
                        Assert.That(store.TryInitiateFullCheckpoint(out var nextToken, CheckpointType.Snapshot), Is.True);
                        await store.CompleteCheckpointAsync().ConfigureAwait(false);
                        Directory.Delete(Path.Combine(dir, "cpr-checkpoints", token.ToString()), recursive: true);
                        token = nextToken;
                    }
                }
                finally
                {
                    store.Dispose();
                }
            }

            store = CreateStore(overflowKeys);
            try
            {
                _ = await store.RecoverAsync(useIndex ? token : default, token).ConfigureAwait(false);
                ReadSourceVersion(store, key, sourceAddress, "checkpoint after recovery, with the original snapshot deleted");
            }
            finally
            {
                store.Dispose();
            }
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task InterruptedReferenceRepairCanBeRetried([Values(1, 2)] int failedRepairPage, [Values] bool overflowKeys)
        {
            var controlled = new ControlledReadFailureDevice(log);
            log = controlled;
            Guid token;
            long firstSource, secondSource;
            var store = CreateStore(overflowKeys);
            try
            {
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
                {
                    firstSource = store.Log.TailAddress;
                    _ = session.BasicContext.Upsert(new TestObjectKey { key = 1 }, new SharedListHeapObject([1, 2, 3, 4, 5, 6, 7, 8]), Empty.Default);
                    var filler = 100;
                    while (store.hlogBase.GetPage(store.Log.TailAddress) < 1)
                        _ = session.BasicContext.Upsert(new TestObjectKey { key = filler++ }, new SharedListHeapObject([filler]), Empty.Default);
                    _ = session.BasicContext.Upsert(new TestObjectKey { key = filler++ }, new SharedListHeapObject([filler]), Empty.Default);
                    secondSource = store.Log.TailAddress;
                    _ = session.BasicContext.Upsert(new TestObjectKey { key = 2 }, new SharedListHeapObject([1, 2, 3, 4, 5, 6, 7, 8]), Empty.Default);
                    while (store.hlogBase.GetPage(store.Log.TailAddress) < 3)
                        _ = session.BasicContext.Upsert(new TestObjectKey { key = filler++ }, new SharedListHeapObject([filler]), Empty.Default);
                    _ = session.BasicContext.Upsert(new TestObjectKey { key = filler }, new SharedListHeapObject([filler]), Empty.Default);
                }

                var pause = new PauseAtPhase(Phase.WAIT_INDEX_CHECKPOINT);
                store.stateMachineDriver.UnsafeRegisterCallback(pause);
                try
                {
                    Assert.That(store.TryInitiateFullCheckpoint(out token, CheckpointType.Snapshot), Is.True);
                    Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True);
                    Rmw(store, firstSource, expectCapture: true);
                    Rmw(store, secondSource, expectCapture: true, key: 2);
                    store.Log.ShiftReadOnlyAddress(store.hlogBase.GetLogicalAddressOfStartOfPage(2), wait: true);
                }
                finally
                {
                    pause.Release.Set();
                }
                await store.CompleteCheckpointAsync().ConfigureAwait(false);
            }
            finally
            {
                store.Dispose();
            }

            store = CreateStore(overflowKeys);
            try
            {
                controlled.FailWriteNumber(controlled.WriteCount + failedRepairPage);
                _ = Assert.CatchAsync<TsavoriteException>(async () => await store.RecoverAsync(default, token).ConfigureAwait(false));
                Assert.That(controlled.FailedWriteCount, Is.EqualTo(1));
                Assert.That(controlled.LastFailedWriteAddress, Is.EqualTo((ulong)((failedRepairPage - 1) * (1L << 13))),
                    "The interruption must occur while persisting a repaired page, after its object bytes are durable");
            }
            finally
            {
                store.Dispose();
            }

            for (var recovery = 0; recovery < 2; recovery++)
            {
                store = CreateStore(overflowKeys);
                try
                {
                    _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                    ReadSourceVersion(store, new TestObjectKey { key = 1 }, firstSource, $"interrupted repair retry {recovery}");
                    ReadSourceVersion(store, new TestObjectKey { key = 2 }, secondSource, $"interrupted repair retry {recovery}");
                }
                finally
                {
                    store.Dispose();
                }
            }
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task LaterCheckpointRejectsObsoleteSnapshotReferences([Values] bool overflowKeys)
        {
            Guid firstToken, nextToken;
            var store = CreateStore(overflowKeys);
            try
            {
                var sourceAddress = store.Log.TailAddress;
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
                    _ = session.BasicContext.Upsert(new TestObjectKey { key = 1 }, new SharedListHeapObject([1, 2, 3, 4, 5, 6, 7, 8]), Empty.Default);

                var pause = new PauseAtPhase(Phase.WAIT_INDEX_CHECKPOINT);
                store.stateMachineDriver.UnsafeRegisterCallback(pause);
                try
                {
                    Assert.That(store.TryInitiateFullCheckpoint(out firstToken, CheckpointType.Snapshot), Is.True);
                    Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True);
                    Rmw(store, sourceAddress, expectCapture: true);
                    store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                }
                finally
                {
                    pause.Release.Set();
                }
                await store.CompleteCheckpointAsync().ConfigureAwait(false);
                Assert.That(store.TryInitiateFullCheckpoint(out nextToken, CheckpointType.Snapshot), Is.True);
                await store.CompleteCheckpointAsync().ConfigureAwait(false);
                Directory.Delete(Path.Combine(dir, "cpr-checkpoints", firstToken.ToString()), recursive: true);
            }
            finally
            {
                store.Dispose();
            }

            store = CreateStore(overflowKeys);
            try
            {
                _ = await store.RecoverAsync(default, nextToken).ConfigureAwait(false);
                var functions = new SharedListFunctions();
                using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(functions);
                TestObjectInput input = new();
                TestObjectOutput output = new();
                var status = session.BasicContext.Read(new TestObjectKey { key = 1 }, ref input, ref output);
                if (status.IsPending)
                    _ = session.BasicContext.CompletePending(wait: true);
                Assert.That(functions.LastRead, Is.Not.Null);
                Assert.That(functions.LastRead.Items, Has.Count.EqualTo(InitialCount + 1));
            }
            finally
            {
                store.Dispose();
            }
        }

        async Task VerifyTransferredCheckpoint(bool overflowKeys, Guid token, LogFileInfo ranges, TestObjectKey key, long sourceAddress)
        {
            var target = Path.Combine(dir, "transferred");
            var sourceCheckpoint = Path.Combine(dir, "cpr-checkpoints", token.ToString());
            var targetCheckpoint = Path.Combine(target, "cpr-checkpoints", token.ToString());
            _ = Directory.CreateDirectory(targetCheckpoint);
            foreach (var metadata in Directory.GetFiles(sourceCheckpoint, "info.dat*"))
                File.Copy(metadata, Path.Combine(targetCheckpoint, Path.GetFileName(metadata)));

            objects.ObjectLogCheckpointTransferTests.CopyRange(Path.Combine(dir, "Capture.log"), Path.Combine(target, "Capture.log"),
                ranges.hybridLogFileStartAddress, ranges.hybridLogFileEndAddress, MainLogSegmentSize);
            objects.ObjectLogCheckpointTransferTests.CopyRange(Path.Combine(dir, "Capture.obj.log"), Path.Combine(target, "Capture.obj.log"),
                ranges.hybridLogObjectFileStartAddress, ranges.hybridLogObjectFileEndAddress, ObjectLogSegmentSize);
            objects.ObjectLogCheckpointTransferTests.CopyRange(Path.Combine(sourceCheckpoint, "snapshot.dat"), Path.Combine(targetCheckpoint, "snapshot.dat"),
                0, ranges.snapshotFileEndAddress, MainLogSegmentSize);
            objects.ObjectLogCheckpointTransferTests.CopyRange(Path.Combine(sourceCheckpoint, "snapshot.obj.dat"), Path.Combine(targetCheckpoint, "snapshot.obj.dat"),
                0, ranges.snapshotObjectFileEndAddress, ObjectLogSegmentSize);

            using var targetLog = Devices.CreateLogDevice(Path.Combine(target, "Capture.log"), deleteOnClose: false);
            using var targetObjects = Devices.CreateLogDevice(Path.Combine(target, "Capture.obj.log"), deleteOnClose: false);
            using var targetStore = CreateStore(overflowKeys, targetLog, targetObjects, target);
            _ = await targetStore.RecoverAsync(default, token).ConfigureAwait(false);
            ReadSourceVersion(targetStore, key, sourceAddress, "transferred checkpoint after repair");
        }

        /// <summary>A failed capture fails the checkpoint, and the store's next checkpoint recovers the value that superseded it.</summary>
        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async Task FailedCaptureFailsTheCheckpoint()
        {
            var key = new TestObjectKey { key = 1 };
            var shared = new List<int>();
            for (var ii = 1; ii <= InitialCount; ++ii)
                shared.Add(ii);
            var sourceObject = new ThrowingListHeapObject(shared);

            var store = CreateStore();
            try
            {
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
                    _ = session.BasicContext.Upsert(key, sourceObject, Empty.Default);

                var pause = new PauseAtPhase(Phase.WAIT_INDEX_CHECKPOINT);
                store.stateMachineDriver.UnsafeRegisterCallback(pause);
                try
                {
                    Assert.That(store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot), Is.True);
                    Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True, "The checkpoint did not reach IN_PROGRESS");

                    sourceObject.ThrowOnSerialize = true;
                    using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
                    {
                        TestObjectInput input = new() { value = RmwValue };
                        TestObjectOutput output = new();
                        var status = session.BasicContext.RMW(key, ref input, ref output);
                        Assert.That(status.IsCompletedSuccessfully, Is.True, "A failed capture must not fail the RMW");
                    }
                    sourceObject.ThrowOnSerialize = false;
                    Assert.That(shared, Has.Count.EqualTo(InitialCount + 1), "The RMW did not take the CopyUpdate path");
                    Assert.That(sourceObject.SerializeAttempts, Is.GreaterThan(0), "The RMW did not attempt a capture");
                }
                finally
                {
                    pause.Release.Set();
                }
                _ = Assert.CatchAsync<TsavoriteException>(async () => await store.CompleteCheckpointAsync().ConfigureAwait(false),
                    "A checkpoint whose capture failed must not complete");

                // The next checkpoint has the (v+1) record, which needs no capture.
                Assert.That(store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot), Is.True);
                await store.CompleteCheckpointAsync().ConfigureAwait(false);
            }
            finally
            {
                store.Dispose();
            }

            store = CreateStore();
            try
            {
                _ = await store.RecoverAsync().ConfigureAwait(false);
                var functions = new SharedListFunctions();
                using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(functions);
                TestObjectInput input = new();
                TestObjectOutput output = new();
                var status = session.BasicContext.Read(key, ref input, ref output);
                if (status.IsPending)
                    _ = session.BasicContext.CompletePending(wait: true);
                Assert.That(functions.LastRead, Is.Not.Null, "The key's object did not survive recovery");
                Assert.That(functions.LastRead.Items, Has.Count.EqualTo(InitialCount + 1), "Recovery did not return the (v+1) value");
            }
            finally
            {
                store.Dispose();
            }
        }

        static void Rmw(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, long sourceAddress, bool expectCapture, int key = 1)
        {
            using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
            {
                var newRecordAddress = store.Log.TailAddress;
                TestObjectInput input = new() { value = RmwValue };
                TestObjectOutput output = new();
                var status = session.BasicContext.RMW(new TestObjectKey { key = key }, ref input, ref output);
                if (status.IsPending)
                    _ = session.BasicContext.CompletePending(wait: true);
                Assert.That(store.hlogBase._wrapper.CreateLogRecord(newRecordAddress).Info.IsInNewVersion, Is.True, "The RMW was not a (v+1) CopyUpdate");
            }

            var captures = store.hlogBase.ActiveSnapshotObjectLog;
            Assert.That(captures, Is.Not.Null, "The Snapshot checkpoint is not capturing");
            Assert.That(store.hlogBase._wrapper.CreateLogRecord(sourceAddress).Info.HasSnapshotObjectLogReference, Is.EqualTo(expectCapture),
                expectCapture ? "The RMW did not capture the source's (v) value" : "The RMW captured a source whose image was durable");

            // Releasing a source rewrites its value field as inline. A captured source must keep its object, because the flushes that stamp or
            // write its record may still walk it; a durable source must be released, as it would be outside a checkpoint.
            Assert.That(store.hlogBase._wrapper.CreateLogRecord(sourceAddress).DataHeader.ValueIsObject, Is.EqualTo(expectCapture),
                expectCapture ? "The captured source released its value object" : "The durable source kept its value object");
        }

        static void ReadSourceVersion(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, TestObjectKey key, long sourceAddress, string when)
        {
            var functions = new SharedListFunctions();
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(functions);
            TestObjectInput input = new();
            TestObjectOutput output = new();
            var status = session.BasicContext.Read(key, ref input, ref output);
            if (status.IsPending)
                _ = session.BasicContext.CompletePending(wait: true);

            Assert.That(functions.LastReadAddress, Is.EqualTo(sourceAddress), $"Recovery should resolve the key to the (v) record ({when})");
            Assert.That(functions.LastRead, Is.Not.Null, $"The recovered (v) record holds no object value ({when})");
            Assert.That(functions.LastRead.Items, Has.Count.EqualTo(InitialCount),
                $"The recovered (v) record carries {functions.LastRead.Items.Count} items rather than its own {InitialCount} ({when})");
        }

        /// <summary>A <see cref="SharedListHeapObject"/>-compatible object whose serialization can be made to fail.</summary>
        sealed class ThrowingListHeapObject(List<int> items) : HeapObjectBase
        {
            internal readonly List<int> Items = items;
            internal volatile bool ThrowOnSerialize;
            internal int SerializeAttempts;

            public override IHeapObject Clone() => new SharedListHeapObject(Items);

            public override void Dispose() { }

            public override void DoSerialize(BinaryWriter writer)
            {
                _ = Interlocked.Increment(ref SerializeAttempts);
                if (ThrowOnSerialize)
                    throw new InvalidOperationException("Injected serialization failure");
                writer.Write(Items.Count);
                foreach (var item in Items)
                    writer.Write(item);
            }

            public override void WriteType(BinaryWriter writer, bool isNull) => writer.Write(isNull);
        }

        /// <summary>Pauses the checkpoint state machine on entry to a chosen phase so the test can act in that window.</summary>
        sealed class PauseAtPhase(Phase pauseAt) : IStateMachineCallback
        {
            internal readonly ManualResetEventSlim Reached = new(false);
            internal readonly ManualResetEventSlim Release = new(false);

            public void BeforeEnteringState(SystemState next)
            {
                if (next.Phase != pauseAt)
                    return;
                Reached.Set();
                _ = Release.Wait(TimeSpan.FromSeconds(30));
            }
        }
    }
}