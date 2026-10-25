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
    internal class SnapshotCaptureTests : TestBase
    {
        const int InitialCount = 8;
        const int RmwValue = 99;

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
            => new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.9,
                LogMemorySize = 1L << 16,
                PageSize = 1L << 13,
                MaxInlineKeySize = overflowKeys ? 0 : KVSettings.DefaultMaxInlineKeySize,
                CheckpointDir = dir
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new SharedListSerializer())
             , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

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
            var expectIndexEntry = expectCapture && readOnlyFlushesTheSource;

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
                    Assert.That(store.Log.FlushedUntilAddress, Is.GreaterThanOrEqualTo(sourcePageEnd), "The ReadOnly flush did not write the source's page");

                releaseCheckpointFlushAtNextPage.Set();
                await store.CompleteCheckpointAsync().ConfigureAwait(false);

                HybridLogCheckpointInfo persisted = default;
                try
                {
                    persisted.Recover(token, store.checkpointManager);
                    Assert.That(persisted.info.mainLogRecoveryEndAddress > sourceAddress, Is.EqualTo(readOnlyFlushesTheSource),
                        "Recovery must read the source from the main log exactly when the ReadOnly flush wrote it before the checkpoint ended");
                    Assert.That(persisted.info.captureIndexCount, Is.EqualTo(expectIndexEntry ? 1 : 0),
                        "Only a capture of a record that recovery reads from the main log is indexed");
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

        static void Rmw(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, long sourceAddress, bool expectCapture)
        {
            using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
            {
                var newRecordAddress = store.Log.TailAddress;
                TestObjectInput input = new() { value = RmwValue };
                TestObjectOutput output = new();
                var status = session.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, ref output);
                if (status.IsPending)
                    _ = session.BasicContext.CompletePending(wait: true);
                Assert.That(store.hlogBase._wrapper.CreateLogRecord(newRecordAddress).Info.IsInNewVersion, Is.True, "The RMW was not a (v+1) CopyUpdate");
            }

            var captures = store.hlogBase.ActiveSnapshotCaptures;
            Assert.That(captures, Is.Not.Null, "The Snapshot checkpoint is not capturing");
            Assert.That(captures.TryGet(sourceAddress, out _), Is.EqualTo(expectCapture),
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