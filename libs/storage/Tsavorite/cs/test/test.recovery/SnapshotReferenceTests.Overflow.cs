// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;
using SharedListSerializer = Tsavorite.test.recovery.CheckpointSharedObjectMutationTests.SharedListSerializer;

namespace Tsavorite.test.recovery
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// The overflow-value counterparts of the object tests. A (v+1) CopyUpdate that keeps an overflow value's length gives the new record the
    /// source's array, and its PostCopyUpdater then changes that array, so a Snapshot checkpoint captures the source's (v) value first and
    /// points the source's value slot at its sentinel.
    /// </summary>
    internal partial class SnapshotReferenceTests
    {
        const int OverflowValueLength = 1000;
        const byte InitialFill = 1;
        const byte CapturedFill = 2;
        const byte LaterFill = 3;

        /// <summary>RMW input: the byte every byte of the value becomes.</summary>
        internal struct FillInput
        {
            internal byte fill;
        }

        /// <summary>Read output: the value, and the address of the record it came from.</summary>
        internal sealed class FillOutput
        {
            internal byte[] value;
            internal long address;
        }

        /// <summary>
        /// RMW fills the value with <see cref="FillInput.fill"/>, keeping its length. As Garnet's SETBIT, BITFIELD and SETRANGE do, CopyUpdater
        /// only copies the source, which shares its overflow array with the new record, and PostCopyUpdater makes the change.
        /// </summary>
        internal sealed class FillFunctions : SessionFunctionsBase<FillInput, FillOutput, Empty>
        {
            public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref FillInput input, ref FillOutput output, ref ReadInfo readInfo)
            {
                output.value = srcLogRecord.ValueSpan.ToArray();
                output.address = readInfo.Address;
                return true;
            }

            public override bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref FillInput input, ref FillOutput output, ref RMWInfo rmwInfo)
            {
                logRecord.ValueSpan.Fill(input.fill);
                return true;
            }

            public override bool InPlaceUpdater(ref LogRecord logRecord, ref FillInput input, ref FillOutput output, ref RMWInfo rmwInfo)
            {
                logRecord.ValueSpan.Fill(input.fill);
                return true;
            }

            public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo,
                    ref FillInput input, ref FillOutput output, ref RMWInfo rmwInfo)
                => dstLogRecord.TryCopyFrom(in srcLogRecord, in sizeInfo);

            public override bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo,
                ref FillInput input, ref FillOutput output, ref RMWInfo rmwInfo)
            {
                dstLogRecord.ValueSpan.Fill(input.fill);
                return true;
            }

            public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref FillInput input)
                => new() { KeySize = srcLogRecord.Key.Length, ValueSize = srcLogRecord.ValueSpan.Length };

            public override RecordFieldInfo GetRMWInitialFieldInfo<TKey>(TKey key, ref FillInput input)
                => new() { KeySize = key.KeyBytes.Length, ValueSize = OverflowValueLength };
        }

        TsavoriteKV<ClassStoreFunctions, ClassAllocator> CreateOverflowStore(bool overflowKeys = false)
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
                MaxInlineValueSize = 0,
                CheckpointDir = dir
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new SharedListSerializer())
             , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        static long UpsertOverflow(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, int key, byte fill)
        {
            using var session = store.NewSession<TestObjectKey, FillInput, FillOutput, Empty, FillFunctions>(new FillFunctions());
            var address = store.Log.TailAddress;
            var value = new byte[OverflowValueLength];
            value.AsSpan().Fill(fill);
            _ = session.BasicContext.Upsert(new TestObjectKey { key = key }, value.AsSpan(), Empty.Default);
            return address;
        }

        static void FillRmw(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, int key, byte fill)
        {
            using var session = store.NewSession<TestObjectKey, FillInput, FillOutput, Empty, FillFunctions>(new FillFunctions());
            FillInput input = new() { fill = fill };
            FillOutput output = new();
            var status = session.BasicContext.RMW(new TestObjectKey { key = key }, ref input, ref output);
            if (status.IsPending)
                _ = session.BasicContext.CompletePending(wait: true);
        }

        static OverflowByteArray GetValueOverflow(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, long address)
        {
            Assert.That(store.hlogBase._wrapper.CreateLogRecord(address).TryGetOutOfLineComponents(out _, out var valueOverflow, out _), Is.True,
                $"The record at {address} has no resolvable value");
            return valueOverflow;
        }

        /// <summary>Run a (v+1) CopyUpdate of the overflow source at <paramref name="sourceAddress"/>, and check what it left in the source.</summary>
        /// <param name="store">The store</param>
        /// <param name="sourceAddress">The source record's address</param>
        /// <param name="expectCapture">Whether the CopyUpdate captures the source, which marks it and detaches it from the shared array</param>
        /// <param name="key">The source's key</param>
        /// <param name="expectStampUnchanged">Whether a flush of this checkpoint already published the source, so the CopyUpdate must leave its
        /// stamp alone</param>
        static void RmwOverflowSource(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, long sourceAddress, bool expectCapture, int key = 1,
            bool expectStampUnchanged = false)
        {
            var priorStamp = store.hlogBase._wrapper.CreateLogRecord(sourceAddress).GetObjectLogRecordRef();
            var newRecordAddress = store.Log.TailAddress;
            FillRmw(store, key, CapturedFill);
            Assert.That(store.hlogBase._wrapper.CreateLogRecord(newRecordAddress).Info.IsInNewVersion, Is.True, "The RMW was not a (v+1) CopyUpdate");

            Assert.That(store.hlogBase.ActiveSnapshotObjectLog, Is.Not.Null, "The Snapshot checkpoint is not capturing");
            Assert.That(SourceIsMarked(store, sourceAddress), Is.EqualTo(expectCapture),
                expectCapture ? "The RMW did not capture the source's (v) value" : "The RMW captured a source whose (v) value a ReadOnly flush had written");
            if (expectStampUnchanged)
                Assert.That(store.hlogBase._wrapper.CreateLogRecord(sourceAddress).GetObjectLogRecordRef(), Is.EqualTo(priorStamp),
                    "The RMW restamped a source that a flush had already published");

            // A captured source is detached from the array its new record changes; any other source still shares it.
            var sourceValue = GetValueOverflow(store, sourceAddress);
            var newValue = GetValueOverflow(store, newRecordAddress);
            if (expectCapture)
            {
                Assert.That(sourceValue.IsSentinel, Is.True, "The captured source still references the array its new record changes");
                Assert.That(ReferenceEquals(sourceValue.Array, store.hlogBase.ActiveSnapshotObjectLog.OverflowSentinel.Array), Is.True,
                    "The captured source references another checkpoint's sentinel");
            }
            else
                Assert.That(ReferenceEquals(sourceValue.Array, newValue.Array), Is.True, "The CopyUpdate did not share the source's array");
        }

        static void ReadOverflowVersion(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, int key, long expectedAddress, byte expectedFill, string when)
        {
            using var session = store.NewSession<TestObjectKey, FillInput, FillOutput, Empty, FillFunctions>(new FillFunctions());
            FillInput input = new();
            FillOutput output = new();
            var status = session.BasicContext.Read(new TestObjectKey { key = key }, ref input, ref output);
            if (status.IsPending)
            {
                Assert.That(session.BasicContext.CompletePendingWithOutputs(out var completed, wait: true), Is.True);
                (status, output) = GetSinglePendingResult(completed);
            }
            Assert.That(status.Found, Is.True, $"The key was not found ({when})");
            Assert.That(output.address, Is.EqualTo(expectedAddress), $"Recovery resolved the key to the wrong record ({when})");
            Assert.That(output.value, Has.Length.EqualTo(OverflowValueLength), $"The recovered value has the wrong length ({when})");
            Assert.That(output.value, Has.All.EqualTo(expectedFill), $"The recovered value holds the wrong version ({when})");
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async Task RecoveryReadsTheSupersededOverflowValue([Values] RmwOrder order, [Values] bool overflowKeys)
        {
            if (!ObjectFlushInjection.IsAvailable)
                Assert.Ignore("ObjectFlushInjection is compiled out in Release, so the flushes cannot be held.");

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
            long sourceAddress;
            var store = CreateOverflowStore(overflowKeys);
            try
            {
                var earlierAddress = UpsertOverflow(store, 2, InitialFill);
                sourceAddress = UpsertOverflow(store, 1, InitialFill);

                // Fill the source's page, then put one record on the next page for the Checkpoint Flush to be held on.
                var nextKey = 3;
                while (store.hlogBase.GetPage(store.Log.TailAddress) == store.hlogBase.GetPage(sourceAddress))
                    _ = UpsertOverflow(store, nextKey++, InitialFill);
                var nextPageAddress = UpsertOverflow(store, nextKey, InitialFill);

                var sourcePage = store.hlogBase.GetPage(sourceAddress);
                var sourcePageEnd = store.hlogBase.GetLogicalAddressOfStartOfPage(sourcePage + 1);
                Assert.That(store.hlogBase.GetPage(earlierAddress), Is.EqualTo(sourcePage), "The earlier record must share the source's page");
                Assert.That(store.hlogBase.GetPage(nextPageAddress), Is.EqualTo(sourcePage + 1));
                Assert.That(store.Log.ReadOnlyAddress, Is.LessThanOrEqualTo(sourceAddress), "The source must start in the mutable region");
                Assert.That(store.hlogBase._wrapper.CreateLogRecord(sourceAddress).DataHeader.ValueIsOverflow, Is.True);
                Assert.That(store.hlogBase._wrapper.CreateLogRecord(sourceAddress).DataHeader.KeyIsOverflow, Is.EqualTo(overflowKeys));

                // The same holds as in the object test: until the Checkpoint Flush is held on the next page no ReadOnly flush runs.
                var checkpointFlushHoldAddress = order switch
                {
                    RmwOrder.BeforeCheckpointFlush or RmwOrder.BeforeCheckpointFlushWithoutReadOnlyFlush => earlierAddress,
                    RmwOrder.DuringCheckpointFlush => sourceAddress,
                    _ => -1,
                };
                var nextPage = sourcePage + 1;
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
                        RmwOverflowSource(store, sourceAddress, expectCapture, expectStampUnchanged: order == RmwOrder.DuringCheckpointFlush);
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

                        // Before the ReadOnly flush claims the value, the walk has already read the source's slot; the capture detaches the
                        // source, and the flush must find the sentinel when it checks the slot again.
                        RmwOverflowSource(store, sourceAddress, expectCapture, expectStampUnchanged: true);
                        releaseReadOnlyFlushAtSource.Set();
                        Assert.That(readOnlyShift.Wait(TimeSpan.FromSeconds(30)), Is.True, "The ReadOnly shift did not complete");
                        break;

                    case RmwOrder.AfterReadOnlyFlush:
                        store.Log.ShiftReadOnlyAddress(sourcePageEnd, wait: true);
                        Assert.That(store.Log.FlushedUntilAddress, Is.GreaterThanOrEqualTo(sourcePageEnd));
                        RmwOverflowSource(store, sourceAddress, expectCapture);
                        break;
                }
                if (readOnlyFlushesTheSource)
                    Assert.That(store.Log.FlushedUntilAddress, Is.GreaterThanOrEqualTo(sourcePageEnd), "The ReadOnly flush did not write the source's page");
                Assert.That(SourceIsMarked(store, sourceAddress), Is.EqualTo(expectCapture), "A flush replaced the capture of the active checkpoint");

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
            }
            finally
            {
                releaseCheckpointFlushAtSource.Set();
                releaseCheckpointFlushAtNextPage.Set();
                releaseReadOnlyFlushAtSource.Set();
                ObjectFlushInjection.Reset();
                store.Dispose();
            }

            // Recover twice. Before the second recovery, the first recovered store appends to both logs, which the second must discard.
            for (var recovery = 1; recovery <= 2; recovery++)
            {
                store = CreateOverflowStore(overflowKeys);
                try
                {
                    _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                    ReadOverflowVersion(store, 1, sourceAddress, InitialFill, $"recovery {recovery}, in memory");

                    store.Log.FlushAndEvict(wait: true);
                    ReadOverflowVersion(store, 1, sourceAddress, InitialFill, $"recovery {recovery}, after eviction");

                    if (recovery == 1)
                    {
                        for (var ii = 0; ii < 64; ii++)
                            _ = UpsertOverflow(store, 1000 + ii, LaterFill);
                        store.Log.FlushAndEvict(wait: true);
                    }
                }
                finally
                {
                    store.Dispose();
                }
            }
        }

        /// <summary>
        /// An overflow array's publication phase describes only the record that published it. Here a ReadOnly flush publishes the source, and a
        /// CopyUpdate outside a checkpoint then gives a new record the source's array; the source, durable and alone in its chain, is elided,
        /// and the array keeps the source's phase. A checkpoint's CopyUpdate of the new record must capture it: nothing has written it.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task CaptureIgnoresThePublicationOfAnotherRecordSharingTheValue([Values] bool overflowKeys)
        {
            Guid token;
            long sharedAddress;
            var store = CreateOverflowStore(overflowKeys);
            try
            {
                _ = UpsertOverflow(store, 1, InitialFill);

                // The ReadOnly flush publishes the first record, and the CopyUpdate of the now read-only record takes its array.
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                sharedAddress = store.Log.TailAddress;
                FillRmw(store, 1, CapturedFill);
                Assert.That(store.hlogBase._wrapper.CreateLogRecord(sharedAddress).DataHeader.ValueIsOverflow, Is.True,
                    "The CopyUpdate did not create the record expected");

                var pause = new PauseAtPhase(Phase.WAIT_INDEX_CHECKPOINT);
                store.stateMachineDriver.UnsafeRegisterCallback(pause);
                try
                {
                    Assert.That(store.TryInitiateFullCheckpoint(out token, CheckpointType.Snapshot), Is.True);
                    Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True);
                    var newAddress = store.Log.TailAddress;
                    FillRmw(store, 1, LaterFill);
                    Assert.That(store.hlogBase._wrapper.CreateLogRecord(newAddress).Info.IsInNewVersion, Is.True, "The RMW was not a (v+1) CopyUpdate");
                    Assert.That(SourceIsMarked(store, sharedAddress), Is.True, "The capture took the first record's ReadOnly publication as its source's");
                    Assert.That(GetValueOverflow(store, sharedAddress).IsSentinel, Is.True, "The captured source still references the array its new record changes");
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

            store = CreateOverflowStore(overflowKeys);
            try
            {
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                ReadOverflowVersion(store, 1, sharedAddress, CapturedFill, "capture of a record sharing a published array");
            }
            finally
            {
                store.Dispose();
            }
        }

        /// <summary>
        /// A Snapshot flush's publication of an overflow value outlives its checkpoint while the record stays mutable, and an in-place update then
        /// changes the value. The next checkpoint's capture must write the updated value rather than take the earlier checkpoint's stamp.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task CaptureAfterAnInPlaceUpdateWritesTheUpdatedOverflowValue([Values] bool overflowKeys)
        {
            Guid token;
            long sourceAddress;
            var store = CreateOverflowStore(overflowKeys);
            try
            {
                // Another record ahead of the source, so the first checkpoint's object log holds the source at a position the second
                // checkpoint's capture, which writes first to its own object log, does not reproduce.
                _ = UpsertOverflow(store, 2, InitialFill);
                sourceAddress = UpsertOverflow(store, 1, InitialFill);

                Assert.That(store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot), Is.True);
                await store.CompleteCheckpointAsync().ConfigureAwait(false);
                var firstStamp = store.hlogBase._wrapper.CreateLogRecord(sourceAddress).GetObjectLogRecordRef();

                var tail = store.Log.TailAddress;
                FillRmw(store, 1, CapturedFill);
                Assert.That(store.Log.TailAddress, Is.EqualTo(tail), "The RMW did not update the source in place");

                var pause = new PauseAtPhase(Phase.WAIT_INDEX_CHECKPOINT);
                store.stateMachineDriver.UnsafeRegisterCallback(pause);
                try
                {
                    Assert.That(store.TryInitiateFullCheckpoint(out token, CheckpointType.Snapshot), Is.True);
                    Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True);
                    FillRmw(store, 1, LaterFill);
                    Assert.That(SourceIsMarked(store, sourceAddress), Is.True, "The RMW did not capture the source");
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

            store = CreateOverflowStore(overflowKeys);
            try
            {
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                ReadOverflowVersion(store, 1, sourceAddress, CapturedFill, "capture after an in-place update");
            }
            finally
            {
                store.Dispose();
            }
        }

        /// <summary>
        /// A capture acts on the publication its claim takes over. As in <see cref="CaptureIgnoresThePublicationOfAnotherRecordSharingTheValue"/>,
        /// the shared array carries an older version's ReadOnly publication. Here the ReadOnly flush publishes the shared record after its capture
        /// reads the array's phase word and before its CAS. The word has the same value both times, so the CAS succeeds, and the capture must see
        /// that the ReadOnly flush published its source: it captures nothing and leaves the stamp the flush's page write carries, and recovery
        /// reads the source's (v) value from the main log. Acting on the older version's publication instead, it would restamp and mark the
        /// source, and recovery would invalidate the marked image and return the older version's value.
        /// <para>The source must be below <c>S</c>, so the ReadOnly flush that publishes it is issued before <c>WAIT_FLUSH</c>. Above <c>S</c>,
        /// the Snapshot flush publishes it first, which changes the word.</para>
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task CaptureSkipsASourceTheReadOnlyFlushPublishesBeforeTheClaim([Values] bool overflowKeys)
        {
            if (!ObjectFlushInjection.IsAvailable)
                Assert.Ignore("ObjectFlushInjection is compiled out in Release, so the flush and the capture cannot be interleaved.");

            using var flushAtSource = new ManualResetEventSlim(false);
            using var releaseFlushAtSource = new ManualResetEventSlim(false);
            using var captureSampledWord = new ManualResetEventSlim(false);
            using var releaseCapture = new ManualResetEventSlim(false);
            using var flushPublishedSource = new ManualResetEventSlim(false);
            using var releaseFlushAfterSource = new ManualResetEventSlim(false);
            var rmwThreadId = -1;

            Guid token;
            long sharedAddress;
            var store = CreateOverflowStore(overflowKeys);
            var pause = new PauseAtPhase(Phase.WAIT_INDEX_CHECKPOINT);
            try
            {
                // The ReadOnly flush publishes the first record, and the CopyUpdate of the now read-only record takes its array.
                _ = UpsertOverflow(store, 1, InitialFill);
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                sharedAddress = store.Log.TailAddress;
                FillRmw(store, 1, CapturedFill);
                Assert.That(store.hlogBase._wrapper.CreateLogRecord(sharedAddress).DataHeader.ValueIsOverflow, Is.True,
                    "The CopyUpdate did not create the record expected");

                ObjectFlushInjection.Hook = (phase, logicalAddress) =>
                {
                    // The capture's claim runs on the RMW's thread; the flush's claim, on a flush thread, passes.
                    if (phase == ObjectFlushPhase.PublicationClaimSampled)
                    {
                        if (Environment.CurrentManagedThreadId == Volatile.Read(ref rmwThreadId) && !captureSampledWord.IsSet)
                        {
                            captureSampledWord.Set();
                            _ = releaseCapture.Wait(TimeSpan.FromSeconds(30));
                        }
                        return;
                    }
                    if (logicalAddress != sharedAddress)
                        return;
                    if (phase == ObjectFlushPhase.AfterCapture && !flushAtSource.IsSet)
                    {
                        flushAtSource.Set();
                        _ = releaseFlushAtSource.Wait(TimeSpan.FromSeconds(30));
                    }
                    else if (phase == ObjectFlushPhase.AfterRecordWritten && !flushPublishedSource.IsSet)
                    {
                        flushPublishedSource.Set();
                        _ = releaseFlushAfterSource.Wait(TimeSpan.FromSeconds(30));
                    }
                };

                store.stateMachineDriver.UnsafeRegisterCallback(pause);
                Assert.That(store.TryInitiateFullCheckpoint(out token, CheckpointType.Snapshot), Is.True);
                Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True);

                // Issued before WAIT_FLUSH, the ReadOnly flush of the shared record keeps it below S. Hold it before it claims the array.
                var readOnlyShift = Task.Run(() => store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true));
                Assert.That(flushAtSource.Wait(TimeSpan.FromSeconds(30)), Is.True, "The ReadOnly flush never reached the shared record");

                // The (v+1) CopyUpdate's capture reads the array's phase word, the first record's ReadOnly publication, and is held before its CAS.
                var newAddress = store.Log.TailAddress;
                var rmw = Task.Run(() =>
                {
                    Volatile.Write(ref rmwThreadId, Environment.CurrentManagedThreadId);
                    FillRmw(store, 1, LaterFill);
                });
                Assert.That(captureSampledWord.Wait(TimeSpan.FromSeconds(30)), Is.True, "The CopyUpdate did not try to capture the shared record");

                // The ReadOnly flush publishes the shared record, which leaves the same phase word with the shared record as its owner.
                releaseFlushAtSource.Set();
                Assert.That(flushPublishedSource.Wait(TimeSpan.FromSeconds(30)), Is.True, "The ReadOnly flush did not publish the shared record");
                var publishedStamp = store.hlogBase._wrapper.CreateLogRecord(sharedAddress).GetObjectLogRecordRef();

                releaseCapture.Set();
                Assert.That(rmw.Wait(TimeSpan.FromSeconds(30)), Is.True, "The CopyUpdate did not complete");
                Assert.That(store.hlogBase._wrapper.CreateLogRecord(newAddress).Info.IsInNewVersion, Is.True, "The RMW was not a (v+1) CopyUpdate");
                Assert.That(SourceIsMarked(store, sharedAddress), Is.False,
                    "The capture marked a source that the ReadOnly flush published before the capture's claim");
                Assert.That(store.hlogBase._wrapper.CreateLogRecord(sharedAddress).GetObjectLogRecordRef(), Is.EqualTo(publishedStamp),
                    "The capture restamped a source that the ReadOnly flush had published");

                releaseFlushAfterSource.Set();
                Assert.That(readOnlyShift.Wait(TimeSpan.FromSeconds(30)), Is.True, "The ReadOnly shift did not complete");
                pause.Release.Set();
                await store.CompleteCheckpointAsync().ConfigureAwait(false);

                HybridLogCheckpointInfo checkpoint = default;
                try
                {
                    checkpoint.Recover(token, store.checkpointManager);
                    Assert.That(checkpoint.info.snapshotFileLogicalStartAddress, Is.GreaterThan(sharedAddress),
                        "The shared record must be below S, where only the ReadOnly flush writes it");
                }
                finally
                {
                    checkpoint.Dispose();
                }
            }
            finally
            {
                releaseFlushAtSource.Set();
                releaseCapture.Set();
                releaseFlushAfterSource.Set();
                pause.Release.Set();
                ObjectFlushInjection.Reset();
                store.Dispose();
            }

            store = CreateOverflowStore(overflowKeys);
            try
            {
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                ReadOverflowVersion(store, 1, sharedAddress, CapturedFill, "after the ReadOnly flush published the source during its capture's claim");
            }
            finally
            {
                store.Dispose();
            }
        }
    }
}