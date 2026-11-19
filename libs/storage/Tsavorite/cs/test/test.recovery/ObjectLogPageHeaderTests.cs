// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
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
    /// A main-log page's header holds its object-log floor: the main object log's tail before the page's first flush wrote any of its records. Truncating
    /// the main log reads it from disk to decide which object-log segments it can remove. On disk, each page with object records must hold a floor at or
    /// below its first record's objects, and above the previous such page's first objects, since the main object log is written in page order: a lower
    /// floor keeps segments that could be removed, and 0 keeps all of them.
    /// </summary>
    [TestFixture]
    public class ObjectLogPageHeaderTests : TestBase
    {
        /// <summary>How the pages reach the main log</summary>
        public enum Scenario
        {
            /// <summary>Without a size tracker, the log runs past its page buffer, so later pages reuse slots that earlier pages were cleared from</summary>
            ReusedSlotWithoutSizeTracker,

            /// <summary>A snapshot checkpoint writes pages to its snapshot file before the main log flushes them</summary>
            SnapshotThenFlush,

            /// <summary>Recovery from such a checkpoint keeps the snapshot file's pages in memory, and they are flushed afterwards</summary>
            SnapshotRecoveryKeepsPages,

            /// <summary>Recovery from such a checkpoint evicts the snapshot file's pages, flushing them and copying their objects to the main object log</summary>
            SnapshotRecoveryEvictsPages,
        }

        /// <summary>The number of pages the log holds in memory</summary>
        const int NumMemoryPages = 32;

        /// <summary>The number of records written first: more pages than <see cref="NumMemoryPages"/></summary>
        const int NumRecords = 6000;

        /// <summary>For <see cref="Scenario.SnapshotThenFlush"/>, the number of records written after the checkpoint, so that the main log flushes the pages
        /// the snapshot holds</summary>
        const int NumRecordsAfterCheckpoint = 3000;

        /// <summary>For <see cref="Scenario.SnapshotRecoveryEvictsPages"/>, recovery's memory budget in pages: too small for the pages the snapshot holds</summary>
        const int RecoveryBudgetPages = 8;

        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        // The stores that take a snapshot write with a size tracker, so cleared pages go back to a pool and are initialized when reused. Recovery that keeps
        // the snapshot's pages in memory runs without one; every page it holds, header included, is read from the main log or the snapshot file.
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task PageHeadersHoldLowestObjectLogPosition([Values] Scenario scenario)
        {
            var takesSnapshot = scenario != Scenario.ReusedSlotWithoutSizeTracker;
            var recovers = scenario is Scenario.SnapshotRecoveryKeepsPages or Scenario.SnapshotRecoveryEvictsPages;

            Prepare(out var log, out var objlog, out var store);
            Guid token = default;
            long flushedLogicalAddress = 0, tailAddress;
            try
            {
                if (takesSnapshot)
                    SetSizeTracker(store, NumMemoryPages);
                Upsert(store, 0, NumRecords);

                if (takesSnapshot)
                {
                    ClassicAssert.GreaterOrEqual(store.Log.TailAddress - store.Log.FlushedUntilAddress, 2L * MinKvLogPageSize,
                        "the main log must not have flushed the last pages, so the snapshot holds them");
                    ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.Snapshot));
                    await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);
                    flushedLogicalAddress = store.GetLogFileSize(token).hybridLogFileEndAddress;
                    if (scenario == Scenario.SnapshotThenFlush)
                        Upsert(store, NumRecords, NumRecordsAfterCheckpoint);
                }

                if (!recovers)
                    store.Log.FlushAndEvict(wait: true);
                tailAddress = store.Log.TailAddress;
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            if (recovers)
            {
                Prepare(out log, out objlog, out store);
                try
                {
                    var evictsPages = scenario == Scenario.SnapshotRecoveryEvictsPages;
                    if (evictsPages)
                        SetSizeTracker(store, RecoveryBudgetPages);
                    _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                    if (evictsPages)
                        ClassicAssert.Greater(store.Log.HeadAddress, flushedLogicalAddress + MinKvLogPageSize, "recovery must evict pages the snapshot holds");
                    else
                        ClassicAssert.LessOrEqual(store.Log.HeadAddress, flushedLogicalAddress, "recovery must keep the pages the snapshot holds in memory");
                    store.Log.FlushAndEvict(wait: true);
                    tailAddress = store.Log.TailAddress;
                }
                finally
                {
                    Destroy(log, objlog, store);
                }
            }

            VerifyPageHeaders(Path.Combine(MethodTestDir, "hdr.log.0"), tailAddress);
        }

        // Reads the main log's first segment, which holds all of the test's records, and checks each page that holds records with objects.
        private static unsafe void VerifyPageHeaders(string mainLogSegmentPath, long tailAddress)
        {
            var bytes = File.ReadAllBytes(mainLogSegmentPath);
            ClassicAssert.GreaterOrEqual(bytes.Length, tailAddress, "the main log's first segment must hold all of the records");

            var numPagesChecked = 0;
            var previousFirstObjectPosition = -1L;
            List<string> mismatches = [];
            fixed (byte* segmentStart = bytes)
            {
                for (var page = 0L; page * MinKvLogPageSize < tailAddress; page++)
                {
                    var pageStart = segmentStart + page * MinKvLogPageSize;

                    // The page's first valid record with objects; every record in these tests has an object. The segment starts at address 0, and the
                    // records end at the tail.
                    var recordsEnd = (long)segmentStart + Math.Min((page + 1) * MinKvLogPageSize, tailAddress);
                    var firstObjectPosition = -1L;
                    for (var address = (long)pageStart + PageHeader.Size; address < recordsEnd;)
                    {
                        var logRecord = new LogRecord(address);
                        if (logRecord.Info.IsNull)
                            break;
                        if (logRecord.Info.Valid && logRecord.DataHeader.RecordHasObjects && !logRecord.Info.HasSnapshotObjectLogReference
                            && !ObjectLogPositionInfo.WordIsUnstamped(logRecord.RawObjectLogPositionWord))
                        {
                            firstObjectPosition = (long)logRecord.GetObjectLogRecordStartPositionAndLengths(out _, out _, HybridLogRecoveryInfo.CheckpointVersion);
                            break;
                        }
                        var allocatedSize = logRecord.AllocatedSize;
                        Assert.That(allocatedSize, Is.GreaterThan(0), $"page {page}: record at offset {address - (long)pageStart} has no size");
                        address += allocatedSize;
                    }
                    if (firstObjectPosition < 0)
                        continue;

                    numPagesChecked++;
                    var floor = ((PageHeader*)pageStart)->GetLowestObjectLogAddress();
                    if (floor < LogAddress.FirstValidAddress || floor > firstObjectPosition || floor <= previousFirstObjectPosition)
                    {
                        var header = floor < 0 ? "unstamped" : floor.ToString("N0");
                        mismatches.Add($"page {page}: floor {header}, first object at {firstObjectPosition:N0}, previous page's first object at {previousFirstObjectPosition:N0}");
                    }
                    previousFirstObjectPosition = firstObjectPosition;
                }
            }

            ClassicAssert.Greater(numPagesChecked, NumMemoryPages, "the records must fill more pages than the log holds in memory");
            ClassicAssert.IsEmpty(mismatches, $"{mismatches.Count} of {numPagesChecked} page headers do not hold a floor between the previous page's first object and their own: {string.Join("; ", mismatches.GetRange(0, Math.Min(5, mismatches.Count)))}");
        }

        private static void SetSizeTracker(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, int numPages)
        {
            var targetSize = (long)numPages * MinKvLogPageSize;
            store.Log.SetLogSizeTracker(new LogSizeTracker<ClassStoreFunctions, ClassAllocator>(store.Log, targetSize, targetSize / 8, targetSize / 16, logger: null));
        }

        private static void Upsert(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, int firstKey, int count)
        {
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;
            for (var key = firstKey; key < firstKey + count; key++)
                _ = bContext.Upsert(new TestObjectKey { key = key }, new TestObjectValue { value = key });
        }

        private static void Prepare(out IDevice log, out IDevice objlog, out TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "hdr.log"));
            objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "hdr.obj.log"));
            store = new(new()
            {
                IndexSize = 1L << 22,
                LogDevice = log,
                ObjectLogDevice = objlog,
                SegmentSize = 1L << 20,
                LogMemorySize = NumMemoryPages * MinKvLogPageSize,
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