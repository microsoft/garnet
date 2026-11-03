// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
    /// A (v+1) CopyUpdate during a Snapshot checkpoint captures the (v) value it supersedes into the checkpoint's object log, unless a
    /// ReadOnly flush has already written the value's main-log image. These tests order the CopyUpdate against the Checkpoint Flush and the
    /// ReadOnly flush of the source's page, and check that recovery returns the (v) value from whichever log it reads the source from.
    /// </summary>
    [TestFixture]
    internal partial class SnapshotReferenceTests : TestBase
    {
        const int InitialCount = 8;
        const int RmwValue = 99;
        const long MainLogSegmentSize = 1L << 20;
        const long ObjectLogSegmentSize = 1L << 30;

        /// <summary>When the (v+1) RMW runs relative to the flushes that write the source.</summary>
        public enum RmwOrder
        {
            /// <summary>Before the Checkpoint Flush walks the source, so the capture writes the (v) value; the ReadOnly flush then writes the
            /// source's page before the checkpoint samples the main log's recovery end, so recovery reads the source from the main log.</summary>
            BeforeCheckpointFlush,

            /// <summary>After the Checkpoint Flush serializes the source, before its page write completes, so the capture only marks the
            /// record; the ReadOnly flush then writes the source's page before the checkpoint samples the main log's recovery end.</summary>
            DuringCheckpointFlush,

            /// <summary>While the ReadOnly flush walks the source's page, before it reaches the source. The Checkpoint Flush has written the
            /// source, so the capture only marks the record, and the ReadOnly flush keeps the reference.</summary>
            DuringReadOnlyFlushBeforeSource,

            /// <summary>After the ReadOnly flush serializes the source, before its page write completes. That write holds the (v) value, so
            /// nothing is captured, and the source is released at once without changing its bytes.</summary>
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
            info.DeferredDispose = true;
            Assert.That(info.TryTakeDeferredDispose(), Is.True);
            Assert.That(info.TryTakeDeferredDispose(), Is.False, "A disposal must be claimed only once");
            Assert.That(info.DeferredDispose, Is.False, "Claiming a disposal must clear its mark");
            Assert.That(info.HasSnapshotObjectLogReference, Is.True);

            info.DeferredDispose = true;
            info.ClearBitsForDiskImages();
            Assert.That(info.DeferredDispose, Is.False, "A disk image carries no disposal mark");
            Assert.That(info.HasSnapshotObjectLogReference, Is.True);

            // The bit above the mark is unused: a CopyUpdate's release is never deferred.
            info.Unused8 = true;
            Assert.That(info.DeferredDispose, Is.False, "Unused8 must not read as a disposal mark");
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
            var rmwDuringReadOnlyFlush = order is RmwOrder.DuringReadOnlyFlushBeforeSource or RmwOrder.DuringReadOnlyFlush;
            var expectCapture = order is not (RmwOrder.AfterReadOnlyFlush or RmwOrder.DuringReadOnlyFlush);
            var expectMainReference = expectCapture && readOnlyFlushesTheSource;

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

                // The ReadOnly flush is held at the source before it claims the source's object (AfterCapture), or after it has written the
                // object and stamped the record (AfterRecordWritten).
                var readOnlyFlushHoldPhase = order == RmwOrder.DuringReadOnlyFlushBeforeSource ? ObjectFlushPhase.AfterCapture : ObjectFlushPhase.AfterRecordWritten;
                ObjectFlushInjection.Hook = (phase, logicalAddress) =>
                {
                    if (rmwDuringReadOnlyFlush && phase == readOnlyFlushHoldPhase && logicalAddress == sourceAddress && checkpointFlushAtNextPage.IsSet
                        && !readOnlyFlushAtSource.IsSet)
                    {
                        readOnlyFlushAtSource.Set();
                        _ = releaseReadOnlyFlushAtSource.Wait(TimeSpan.FromSeconds(30));
                        return;
                    }
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
                };

                Assert.That(store.TryInitiateFullCheckpoint(out token, CheckpointType.Snapshot), Is.True);

                if (checkpointFlushHoldAddress >= 0)
                {
                    Assert.That(checkpointFlushAtSource.Wait(TimeSpan.FromSeconds(30)), Is.True, "The Checkpoint Flush never reached its hold point");
                    if (rmwBeforeCheckpointFlush || order == RmwOrder.DuringCheckpointFlush)
                        Rmw(store, sourceAddress, expectCapture, expectStampUnchanged: order == RmwOrder.DuringCheckpointFlush);
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

                    case RmwOrder.DuringReadOnlyFlushBeforeSource:
                    case RmwOrder.DuringReadOnlyFlush:
                        var readOnlyShift = Task.Run(() => store.Log.ShiftReadOnlyAddress(sourcePageEnd, wait: true));
                        Assert.That(readOnlyFlushAtSource.Wait(TimeSpan.FromSeconds(30)), Is.True, "The ReadOnly flush never reached the source");

                        // The Checkpoint Flush has published the source, and here the ReadOnly flush is walking its page, so the capture must
                        // not rewrite the stamp either flush may be writing. The source's release does not wait for the write.
                        Rmw(store, sourceAddress, expectCapture, expectStampUnchanged: true);
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
                    var flushedSource = store.hlogBase._wrapper.CreateLogRecord(sourceAddress);
                    Assert.That(flushedSource.DataHeader.ValueIsObject && flushedSource.ValueObject is null, Is.True,
                        "The superseded source must stay released, as an object record, after its page is durable");
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

        /// <summary>
        /// A runtime read that reaches a marked main-log image skips it, because only recovery reads the snapshot object log that holds its
        /// components, and it frees no transient ObjectIdMap slot it did not allocate. A read whose overflow key does not match the record it
        /// reads frees none either, and key-version iteration does not report the skipped image.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task RuntimeReadsSkipAMarkedImageWithoutFreeingItsSlots([Values] bool overflowKeys)
        {
            var key = new TestObjectKey { key = 1 };
            long sourceAddress, otherKeyAddress;
            var store = CreateStore(overflowKeys);
            try
            {
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
                {
                    sourceAddress = store.Log.TailAddress;
                    _ = session.BasicContext.Upsert(key, new SharedListHeapObject([1, 2, 3]), Empty.Default);
                    otherKeyAddress = store.Log.TailAddress;
                    var nextKey = 2;
                    while (store.hlogBase.GetPage(store.Log.TailAddress) < 2)
                        _ = session.BasicContext.Upsert(new TestObjectKey { key = nextKey++ }, new SharedListHeapObject([nextKey]), Empty.Default);
                    _ = session.BasicContext.Upsert(new TestObjectKey { key = nextKey }, new SharedListHeapObject([nextKey]), Empty.Default);
                }

                // The capture marks the source, and the ReadOnly flush during the checkpoint writes the marked image.
                var pause = new PauseAtPhase(Phase.WAIT_INDEX_CHECKPOINT);
                store.stateMachineDriver.UnsafeRegisterCallback(pause);
                try
                {
                    Assert.That(store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot), Is.True);
                    Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True);
                    Rmw(store, sourceAddress, expectCapture: true);
                    store.Log.ShiftReadOnlyAddress(store.hlogBase.GetLogicalAddressOfStartOfPage(1), wait: true);
                }
                finally
                {
                    pause.Release.Set();
                }
                await store.CompleteCheckpointAsync().ConfigureAwait(false);
                store.Log.FlushAndEvict(wait: true);

                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
                {
                    Assert.That(ReadAtAddress(session, sourceAddress).Found, Is.False, "A read returned the marked image");
                    if (overflowKeys)
                        Assert.That(ReadAtAddress(session, otherKeyAddress).Found, Is.False, "A read returned a record with another overflow key");
                }

                ImmutableRegionScanTests.ReportingScan scan = new();
                _ = store.Log.IterateKeyVersions(ref scan, key);
                Assert.That(scan.Failure, Is.Null, $"Key-version iteration failed: {scan.Failure}");

                // The CopyUpdate's list holds the source's three items and the one the RMW added.
                (int, string)[] expectedVersions = [(key.key, "list:4")];
                Assert.That(scan.Records.Select(r => (r.Key, r.Value)), Is.EqualTo(expectedVersions),
                    "Key-version iteration must report only the CopyUpdate's record");

                // Every pending read has completed, so each slot the transient map has handed out is free again, exactly once.
                var transientMap = store.hlogBase._wrapper.TransientObjectIdMap;
                List<int> freeSlots = [];
                while (transientMap.freeSlots.TryPop(out var slot))
                    freeSlots.Add(slot);
                Assert.That(freeSlots, Has.All.LessThan(transientMap.Count), "A read freed a slot the transient map never handed out");
                Assert.That(freeSlots, Is.Unique, "A read freed a transient slot twice");
            }
            finally
            {
                store.Dispose();
            }

            static Status ReadAtAddress(ClientSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions, ClassStoreFunctions, ClassAllocator> session,
                long address)
            {
                TestObjectInput input = default;
                TestObjectOutput output = new();
                ReadOptions readOptions = default;
                var status = session.BasicContext.ReadAtAddress(address, new TestObjectKey { key = 1 }, ref input, ref output, ref readOptions, out _);
                if (status.IsPending)
                {
                    Assert.That(session.BasicContext.CompletePendingWithOutputs(out var outputs, wait: true), Is.True);
                    (status, _) = GetSinglePendingResult(outputs);
                }
                return status;
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

        /// <summary>
        /// Recovery invalidates some records without reading their components: the capturing checkpoint's (v+1) record, and, recovering a later
        /// checkpoint, the source's main-log image that references the capturing checkpoint's object log. Their slots must still index their
        /// page's map, holding empty components, because a scan that includes closed records, such as a bounded ScanCursor, resolves them; and a
        /// bounded ScanCursor must not report them, since neither was ever the live version of its key in the recovered store.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task RecoveryGivesInvalidRecordsEmptyComponents([Values] bool fromCapturingCheckpoint, [Values] bool overflowKeys)
        {
            var key = new TestObjectKey { key = 1 };
            Guid capturingToken, laterToken;
            long sourceAddress, newAddress;
            var store = CreateStore(overflowKeys);
            try
            {
                sourceAddress = UpsertSource(store, key);

                // The ReadOnly flush writes the source's page while the capturing checkpoint is active, so its image keeps the reference.
                var pause = new PauseAtPhase(Phase.WAIT_INDEX_CHECKPOINT);
                store.stateMachineDriver.UnsafeRegisterCallback(pause);
                try
                {
                    Assert.That(store.TryInitiateFullCheckpoint(out capturingToken, CheckpointType.Snapshot), Is.True);
                    Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True);
                    newAddress = store.Log.TailAddress;
                    Rmw(store, sourceAddress, expectCapture: true);
                    store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                }
                finally
                {
                    pause.Release.Set();
                }
                await store.CompleteCheckpointAsync().ConfigureAwait(false);
                Assert.That(store.TryInitiateFullCheckpoint(out laterToken, CheckpointType.Snapshot), Is.True);
                await store.CompleteCheckpointAsync().ConfigureAwait(false);
            }
            finally
            {
                store.Dispose();
            }

            store = CreateStore(overflowKeys);
            try
            {
                _ = await store.RecoverAsync(default, fromCapturingCheckpoint ? capturingToken : laterToken).ConfigureAwait(false);
                var (liveAddress, invalidAddress, liveCount) = fromCapturingCheckpoint
                    ? (sourceAddress, newAddress, InitialCount)
                    : (newAddress, sourceAddress, InitialCount + 1);

                var invalid = store.hlogBase._wrapper.CreateLogRecord(invalidAddress);
                Assert.That(invalid.Info.Invalid, Is.True, "Recovery did not invalidate the record");
                Assert.That(invalid.ValueObject, Is.Null, "The invalid record's value slot does not hold an empty component");
                if (overflowKeys)
                    Assert.That(invalid.Key.Length, Is.Zero, "The invalid record's key slot does not hold an empty component");

                var scan = new ReportedRecordScan();
                using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions());
                _ = session.IterateLookupSnapshot(ref scan);
                (long address, int count)[] expected = [(liveAddress, liveCount)];
                Assert.That(scan.Reported, Is.EqualTo(expected), "The scan did not report exactly the live record and its value");
            }
            finally
            {
                store.Dispose();
            }
        }

        /// <summary>Collects the address and item count of each record a scan reports; an invalid record's count is -1.</summary>
        sealed class ReportedRecordScan : IScanIteratorFunctions
        {
            internal readonly List<(long address, int count)> Reported = [];

            public bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                cursorRecordResult = CursorRecordResult.Accept;
                Reported.Add((recordMetadata.Address, logRecord.Info.Invalid ? -1 : ((SharedListHeapObject)logRecord.ValueObject).Items.Count));
                return true;
            }

            public void OnException(Exception exception, long numberOfRecords) => Assert.Fail($"The scan failed after {numberOfRecords} records: {exception}");

            public bool OnStart(long beginAddress, long endAddress) => true;

            public void OnStop(bool completed, long numberOfRecords) { }
        }

        /// <summary>
        /// A capture outlives its checkpoint while its source stays unflushed. The source's next flush, by a ReadOnly flush with no checkpoint
        /// active or by the next checkpoint's Snapshot flush, writes the superseded value and clears the marker rather than keep a reference into
        /// the completed checkpoint's object log, which the next checkpoint's recovery would resolve in its own object log.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task CaptureIsNotKeptAfterItsCheckpoint([Values] bool readOnlyFlushFirst, [Values] bool overflowKeys)
        {
            var key = new TestObjectKey { key = 1 };
            Guid firstToken, secondToken;
            long sourceAddress, newAddress;
            var store = CreateStore(overflowKeys);
            try
            {
                sourceAddress = UpsertSource(store, key);

                // The first checkpoint captures the source, and completes with the source unflushed.
                var pause = new PauseAtPhase(Phase.WAIT_INDEX_CHECKPOINT);
                store.stateMachineDriver.UnsafeRegisterCallback(pause);
                try
                {
                    Assert.That(store.TryInitiateFullCheckpoint(out firstToken, CheckpointType.Snapshot), Is.True);
                    Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True);
                    newAddress = store.Log.TailAddress;
                    Rmw(store, sourceAddress, expectCapture: true);
                }
                finally
                {
                    pause.Release.Set();
                }
                await store.CompleteCheckpointAsync().ConfigureAwait(false);
                Assert.That(store.Log.FlushedUntilAddress, Is.LessThanOrEqualTo(sourceAddress), "The source must still be unflushed");
                Assert.That(SourceIsMarked(store, sourceAddress), Is.True, "The marker should outlive its checkpoint while the source is unflushed");

                if (readOnlyFlushFirst)
                {
                    store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                    Assert.That(SourceIsMarked(store, sourceAddress), Is.False, "A ReadOnly flush kept the capture of a completed checkpoint");
                }

                Assert.That(store.TryInitiateFullCheckpoint(out secondToken, CheckpointType.Snapshot), Is.True);
                await store.CompleteCheckpointAsync().ConfigureAwait(false);
                Assert.That(SourceIsMarked(store, sourceAddress), Is.False, "The next checkpoint's Snapshot flush kept the capture of a completed checkpoint");
            }
            finally
            {
                store.Dispose();
            }

            // The second checkpoint recovers the CopyUpdate's value, and the first still recovers the captured (v) value.
            store = CreateStore(overflowKeys);
            try
            {
                _ = await store.RecoverAsync(default, secondToken).ConfigureAwait(false);
                ReadSourceVersion(store, key, newAddress, "second checkpoint", expectedCount: InitialCount + 1);
            }
            finally
            {
                store.Dispose();
            }

            store = CreateStore(overflowKeys);
            try
            {
                _ = await store.RecoverAsync(default, firstToken).ConfigureAwait(false);
                ReadSourceVersion(store, key, sourceAddress, "first checkpoint");
            }
            finally
            {
                store.Dispose();
            }
        }

        /// <summary>
        /// A Snapshot flush's stamp outlives its checkpoint while the record stays mutable, and an in-place update then changes the value it
        /// recorded. The next checkpoint's capture must write the updated value rather than take the earlier checkpoint's stamp as its own.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task CaptureAfterAnInPlaceUpdateWritesTheUpdatedValue([Values] bool overflowKeys)
        {
            var key = new TestObjectKey { key = 1 };
            Guid token;
            long sourceAddress;
            var store = CreateStore(overflowKeys);
            try
            {
                sourceAddress = UpsertSource(store, key);

                // The first checkpoint's Snapshot flush writes the source and stamps it.
                Assert.That(store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot), Is.True);
                await store.CompleteCheckpointAsync().ConfigureAwait(false);
                var firstStamp = store.hlogBase._wrapper.CreateLogRecord(sourceAddress).GetObjectLogRecordRef();

                // With no checkpoint active, the mutable source is updated in place, which leaves that stamp.
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
                {
                    var tail = store.Log.TailAddress;
                    TestObjectInput input = new() { value = RmwValue };
                    TestObjectOutput output = new();
                    _ = session.BasicContext.RMW(key, ref input, ref output);
                    Assert.That(store.Log.TailAddress, Is.EqualTo(tail), "The RMW did not update the source in place");
                }
                Assert.That(store.hlogBase._wrapper.CreateLogRecord(sourceAddress).GetObjectLogRecordRef(), Is.EqualTo(firstStamp));

                // The next checkpoint's CopyUpdate captures the updated value.
                var pause = new PauseAtPhase(Phase.WAIT_INDEX_CHECKPOINT);
                store.stateMachineDriver.UnsafeRegisterCallback(pause);
                try
                {
                    Assert.That(store.TryInitiateFullCheckpoint(out token, CheckpointType.Snapshot), Is.True);
                    Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True);
                    Rmw(store, sourceAddress, expectCapture: true);
                    Assert.That(store.hlogBase._wrapper.CreateLogRecord(sourceAddress).GetObjectLogRecordRef(), Is.Not.EqualTo(firstStamp),
                        "The capture took the earlier checkpoint's stamp as its own");
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
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                ReadSourceVersion(store, key, sourceAddress, "capture after an in-place update", expectedCount: InitialCount + 1);
            }
            finally
            {
                store.Dispose();
            }
        }

        /// <summary>
        /// A NullDevice main log holds no (v) image, so a (v+1) CopyUpdate captures its source even below FlushedUntilAddress, and releases the
        /// captured source's value at once, leaving its checkpoint's placeholder. With the Checkpoint Flush already past the source, the capture
        /// only marks it.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task NullDeviceMainLogRecoversTheCapturedValue([Values] bool afterCheckpointFlush, [Values] bool overflowKeys)
        {
            if (afterCheckpointFlush && !ObjectFlushInjection.IsAvailable)
                Assert.Ignore("ObjectFlushInjection is compiled out in Release, so the Checkpoint Flush cannot be held.");

            var key = new TestObjectKey { key = 1 };
            Guid token;
            long sourceAddress;
            using var checkpointFlushAtSource = new ManualResetEventSlim(false);
            using var releaseCheckpointFlushAtSource = new ManualResetEventSlim(false);
            using (var nullLog = new NullDevice())
            using (var nullObjects = new NullDevice())
            {
                var store = CreateStore(overflowKeys, nullLog, nullObjects, dir);
                try
                {
                    sourceAddress = UpsertSource(store, key);
                    if (afterCheckpointFlush)
                    {
                        ObjectFlushInjection.Hook = (phase, logicalAddress) =>
                        {
                            if (phase == ObjectFlushPhase.AfterRecordWritten && logicalAddress == sourceAddress && !checkpointFlushAtSource.IsSet)
                            {
                                checkpointFlushAtSource.Set();
                                _ = releaseCheckpointFlushAtSource.Wait(TimeSpan.FromSeconds(30));
                            }
                        };
                        Assert.That(store.TryInitiateFullCheckpoint(out token, CheckpointType.Snapshot), Is.True);
                        Assert.That(checkpointFlushAtSource.Wait(TimeSpan.FromSeconds(30)), Is.True, "The Checkpoint Flush never reached the source");
                        Rmw(store, sourceAddress, expectCapture: true, expectStampUnchanged: true);
                        releaseCheckpointFlushAtSource.Set();
                    }
                    else
                    {
                        // A NullDevice main log advances FlushedUntilAddress past the source without writing it.
                        store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                        Assert.That(store.Log.FlushedUntilAddress, Is.GreaterThan(sourceAddress));
                        var pause = new PauseAtPhase(Phase.WAIT_INDEX_CHECKPOINT);
                        store.stateMachineDriver.UnsafeRegisterCallback(pause);
                        try
                        {
                            Assert.That(store.TryInitiateFullCheckpoint(out token, CheckpointType.Snapshot), Is.True);
                            Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True);
                            Rmw(store, sourceAddress, expectCapture: true);
                        }
                        finally
                        {
                            pause.Release.Set();
                        }
                    }
                    await store.CompleteCheckpointAsync().ConfigureAwait(false);
                    _ = store.hlogBase._wrapper.CreateLogRecord(sourceAddress).TryGetOutOfLineComponents(out _, out _, out var slotObject);
                    Assert.That(slotObject is SupersededObjectValue { CapturingSnapshot: not null }, Is.True,
                        "A captured source on a NullDevice main log must be released at once, leaving its checkpoint's placeholder");
                }
                finally
                {
                    releaseCheckpointFlushAtSource.Set();
                    ObjectFlushInjection.Reset();
                    store.Dispose();
                }
            }

            using var recoveryLog = new NullDevice();
            using var recoveryObjects = new NullDevice();
            var recovered = CreateStore(overflowKeys, recoveryLog, recoveryObjects, dir);
            try
            {
                _ = await recovered.RecoverAsync(default, token).ConfigureAwait(false);
                ReadSourceVersion(recovered, key, sourceAddress, "NullDevice main log");
            }
            finally
            {
                recovered.Dispose();
            }
        }

        /// <summary>
        /// A checkpoint can start while a ReadOnly flush walks the source's page, and capture the source before the flush reaches it. The flush
        /// must find that checkpoint when it reaches the source, and keep the capture: the checkpoint recovers the source from the main log.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task ReadOnlyFlushKeepsTheCaptureOfACheckpointThatStartsDuringItsWalk([Values] bool overflowKeys)
        {
            if (!ObjectFlushInjection.IsAvailable)
                Assert.Ignore("ObjectFlushInjection is compiled out in Release, so the ReadOnly flush cannot be held.");

            var key = new TestObjectKey { key = 1 };
            Guid token;
            long sourceAddress;
            using var readOnlyFlushAtSource = new ManualResetEventSlim(false);
            using var releaseReadOnlyFlushAtSource = new ManualResetEventSlim(false);
            var store = CreateStore(overflowKeys);
            try
            {
                sourceAddress = UpsertSource(store, key);

                // Hold the ReadOnly flush at the source before it claims the source's object.
                ObjectFlushInjection.Hook = (phase, logicalAddress) =>
                {
                    if (phase == ObjectFlushPhase.AfterCapture && logicalAddress == sourceAddress && !readOnlyFlushAtSource.IsSet)
                    {
                        readOnlyFlushAtSource.Set();
                        _ = releaseReadOnlyFlushAtSource.Wait(TimeSpan.FromSeconds(30));
                    }
                };
                var readOnlyShift = Task.Run(() => store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true));
                Assert.That(readOnlyFlushAtSource.Wait(TimeSpan.FromSeconds(30)), Is.True, "The ReadOnly flush never reached the source");
                Assert.That(store.hlogBase.ActiveSnapshotObjectLog, Is.Null, "The ReadOnly flush must start its walk with no checkpoint active");

                var pause = new PauseAtPhase(Phase.WAIT_INDEX_CHECKPOINT);
                store.stateMachineDriver.UnsafeRegisterCallback(pause);
                try
                {
                    Assert.That(store.TryInitiateFullCheckpoint(out token, CheckpointType.Snapshot), Is.True);
                    Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True);
                    Rmw(store, sourceAddress, expectCapture: true);
                    releaseReadOnlyFlushAtSource.Set();
                    Assert.That(readOnlyShift.Wait(TimeSpan.FromSeconds(30)), Is.True, "The ReadOnly shift did not complete");
                    Assert.That(SourceIsMarked(store, sourceAddress), Is.True,
                        "The ReadOnly flush replaced the capture of the checkpoint that started during its walk");
                }
                finally
                {
                    releaseReadOnlyFlushAtSource.Set();
                    pause.Release.Set();
                }
                await store.CompleteCheckpointAsync().ConfigureAwait(false);

                HybridLogCheckpointInfo persisted = default;
                try
                {
                    persisted.Recover(token, store.checkpointManager);
                    Assert.That(persisted.info.mainLogRecoveryEndAddress, Is.GreaterThan(sourceAddress), "The checkpoint must recover the source from the main log");
                    Assert.That(persisted.info.snapshotObjectLogStartAddress, Is.EqualTo(sourceAddress),
                        "Recovery must repoint the source's main-log image at the capture");
                }
                finally
                {
                    persisted.Dispose();
                }
            }
            finally
            {
                releaseReadOnlyFlushAtSource.Set();
                ObjectFlushInjection.Reset();
                store.Dispose();
            }

            store = CreateStore(overflowKeys);
            try
            {
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                ReadSourceVersion(store, key, sourceAddress, "capture by a checkpoint that started during the ReadOnly flush's walk");
            }
            finally
            {
                store.Dispose();
            }
        }

        /// <summary>
        /// A flush holds the claim on the source's object from its object-log write through its stamp, so a capture cannot interleave with
        /// that stamp. The capture waits for the claim, then finds the source published by the Snapshot flush and only marks it.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task CaptureWaitsForTheFlushThatHoldsTheClaim([Values] bool overflowKeys)
        {
            if (!ObjectFlushInjection.IsAvailable)
                Assert.Ignore("ObjectFlushInjection is compiled out in Release, so the Checkpoint Flush cannot be held.");

            var key = new TestObjectKey { key = 1 };
            Guid token;
            long sourceAddress;
            using var flushHoldsClaim = new ManualResetEventSlim(false);
            using var releaseFlush = new ManualResetEventSlim(false);
            var store = CreateStore(overflowKeys);
            try
            {
                sourceAddress = UpsertSource(store, key);
                ObjectFlushInjection.Hook = (phase, logicalAddress) =>
                {
                    if (phase == ObjectFlushPhase.ClaimHeld && logicalAddress == sourceAddress && !flushHoldsClaim.IsSet)
                    {
                        flushHoldsClaim.Set();
                        _ = releaseFlush.Wait(TimeSpan.FromSeconds(30));
                    }
                };
                Assert.That(store.TryInitiateFullCheckpoint(out token, CheckpointType.Snapshot), Is.True);
                Assert.That(flushHoldsClaim.Wait(TimeSpan.FromSeconds(30)), Is.True, "The Checkpoint Flush never claimed the source");

                // The CopyUpdate blocks in its capture until the flush releases the claim, so a short wait times out.
                var rmw = Task.Run(() => Rmw(store, sourceAddress, expectCapture: true));
                Assert.That(rmw.Wait(TimeSpan.FromMilliseconds(250)), Is.False, "The capture did not wait for the claim of the flush publishing the source");
                var stampBeforeRelease = store.hlogBase._wrapper.CreateLogRecord(sourceAddress).GetObjectLogRecordRef();
                releaseFlush.Set();
                Assert.That(rmw.Wait(TimeSpan.FromSeconds(30)), Is.True, "The capture did not complete after the flush released its claim");
                Assert.That(store.hlogBase._wrapper.CreateLogRecord(sourceAddress).GetObjectLogRecordRef(), Is.Not.EqualTo(stampBeforeRelease),
                    "The flush did not stamp the source");
                Assert.That(SourceIsMarked(store, sourceAddress), Is.True, "The flush's stamp cleared the capture's marker");
                await store.CompleteCheckpointAsync().ConfigureAwait(false);
            }
            finally
            {
                releaseFlush.Set();
                ObjectFlushInjection.Reset();
                store.Dispose();
            }

            store = CreateStore(overflowKeys);
            try
            {
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                ReadSourceVersion(store, key, sourceAddress, "capture that waited for the flush's claim");
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
                ValueTask completion;
                try
                {
                    Assert.That(store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot), Is.True);
                    Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True, "The checkpoint did not reach IN_PROGRESS");

                    sourceObject.ThrowOnSerialize = true;
                    var tailBeforeRmw = store.Log.TailAddress;
                    using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
                    {
                        TestObjectInput input = new() { value = RmwValue };
                        TestObjectOutput output = new();
                        var status = session.BasicContext.RMW(key, ref input, ref output);
                        Assert.That(status.IsCompletedSuccessfully, Is.True, "A failed capture must not fail the RMW");
                    }
                    sourceObject.ThrowOnSerialize = false;
                    Assert.That(store.Log.TailAddress, Is.GreaterThan(tailBeforeRmw), "The RMW did not take the CopyUpdate path");
                    Assert.That(shared, Has.Count.EqualTo(InitialCount + 1), "The RMW did not apply its change");
                    Assert.That(sourceObject.SerializeAttempts, Is.GreaterThan(0), "The RMW did not attempt a capture");

                    // Observe the checkpoint before releasing it: once it has aborted, CompleteCheckpointAsync has no checkpoint to report on.
                    completion = store.CompleteCheckpointAsync();
                }
                finally
                {
                    pause.Release.Set();
                }
                _ = Assert.CatchAsync<TsavoriteException>(async () => await completion.ConfigureAwait(false),
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

        /// <summary>Run a (v+1) CopyUpdate of the source at <paramref name="sourceAddress"/>, and check what it left in the source record.</summary>
        /// <param name="store">The store</param>
        /// <param name="sourceAddress">The source record's address</param>
        /// <param name="expectCapture">Whether the CopyUpdate captures the source, which marks the record</param>
        /// <param name="key">The source's key</param>
        /// <param name="expectStampUnchanged">Whether a flush of this checkpoint already published the source, so the CopyUpdate must leave its
        /// stamp alone</param>
        static void Rmw(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, long sourceAddress, bool expectCapture, int key = 1,
            bool expectStampUnchanged = false)
        {
            var priorStamp = store.hlogBase._wrapper.CreateLogRecord(sourceAddress).GetObjectLogRecordRef();
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
                expectCapture ? "The RMW did not capture the source's (v) value" : "The RMW captured a source whose (v) value a ReadOnly flush had written");
            if (expectStampUnchanged)
                Assert.That(store.hlogBase._wrapper.CreateLogRecord(sourceAddress).GetObjectLogRecordRef(), Is.EqualTo(priorStamp),
                    "The RMW restamped a source that a flush had already published");

            // A CopyUpdate releases its source at once, captured or not, without changing the record's bytes, which a flush may still be
            // writing: the source stays an object record, and its value slot gets a placeholder. A captured source's placeholder is its
            // checkpoint's, through which the flushes keep the capture.
            var source = store.hlogBase._wrapper.CreateLogRecord(sourceAddress);
            Assert.That(source.DataHeader.ValueIsObject, Is.True, "The release must leave the source an object record");
            Assert.That(source.ValueObject, Is.Null, "The source kept its value object");
            _ = source.TryGetOutOfLineComponents(out _, out _, out var slotObject);
            Assert.That(slotObject, Is.SameAs(expectCapture ? captures.ObjectSentinel : SupersededObjectValue.Released),
                expectCapture ? "A captured source's slot must hold its checkpoint's placeholder" : "A released source's slot must hold the released placeholder");
        }

        static void ReadSourceVersion(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, TestObjectKey key, long sourceAddress, string when,
            int expectedCount = InitialCount)
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
            Assert.That(functions.LastRead.Items, Has.Count.EqualTo(expectedCount),
                $"The recovered (v) record carries {functions.LastRead.Items.Count} items rather than its own {expectedCount} ({when})");
        }

        static bool SourceIsMarked(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, long sourceAddress)
            => store.hlogBase._wrapper.CreateLogRecord(sourceAddress).Info.HasSnapshotObjectLogReference;

        /// <summary>
        /// CopyUpdates on several threads capture many sources during one Snapshot checkpoint, through the object-log writers the checkpoint
        /// keeps between captures, and recovery returns every key's (v) value.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task ConcurrentCapturesRecoverEveryValue([Values] bool overflowKeys)
        {
            const int KeyCount = 64;
            const int ThreadCount = 4;
            Guid token;
            var sourceAddresses = new long[KeyCount];
            var store = CreateStore(overflowKeys);
            try
            {
                // A first record, so that each source's address is the tail its upsert reads.
                _ = UpsertSource(store, new TestObjectKey { key = KeyCount });
                for (var k = 0; k < KeyCount; k++)
                    sourceAddresses[k] = UpsertSource(store, new TestObjectKey { key = k });

                var pause = new PauseAtPhase(Phase.WAIT_INDEX_CHECKPOINT);
                store.stateMachineDriver.UnsafeRegisterCallback(pause);
                try
                {
                    Assert.That(store.TryInitiateFullCheckpoint(out token, CheckpointType.Snapshot), Is.True);
                    Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True, "The checkpoint did not reach IN_PROGRESS");

                    var tasks = new Task[ThreadCount];
                    for (var t = 0; t < ThreadCount; t++)
                    {
                        var first = t;
                        tasks[t] = Task.Run(() =>
                        {
                            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions());
                            for (var k = first; k < KeyCount; k += ThreadCount)
                            {
                                TestObjectInput input = new() { value = RmwValue };
                                TestObjectOutput output = new();
                                if (session.BasicContext.RMW(new TestObjectKey { key = k }, ref input, ref output).IsPending)
                                    _ = session.BasicContext.CompletePending(wait: true);
                            }
                        });
                    }
                    Assert.That(Task.WaitAll(tasks, TimeSpan.FromSeconds(60)), Is.True, "The CopyUpdates did not complete");
                    for (var k = 0; k < KeyCount; k++)
                        Assert.That(SourceIsMarked(store, sourceAddresses[k]), Is.True, $"The CopyUpdate of key {k} did not capture its source");
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
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                for (var k = 0; k < KeyCount; k++)
                    ReadSourceVersion(store, new TestObjectKey { key = k }, sourceAddresses[k], $"key {k}");
            }
            finally
            {
                store.Dispose();
            }
        }

        static long UpsertSource(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, TestObjectKey key)
        {
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions());
            var sourceAddress = store.Log.TailAddress;
            List<int> items = [];
            for (var ii = 1; ii <= InitialCount; ++ii)
                items.Add(ii);
            _ = session.BasicContext.Upsert(key, new SharedListHeapObject(items), Empty.Default);
            return sourceAddress;
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