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
    /// Verifies that the byte ranges reported by <see cref="TsavoriteKV{TStoreFunctions, TAllocator}.GetLogFileSize(Guid)"/>
    /// are sufficient to recover a Snapshot checkpoint on a different node. Cluster replication full sync transfers only
    /// those ranges of the main log, the main object log, the snapshot and the snapshot object log; the target node then
    /// recovers from them, so a range that stops short of the data referenced by the transferred main log leaves the
    /// target unable to deserialize its objects.
    /// </summary>
    [TestFixture]
    public class ObjectLogCheckpointTransferTests : TestBase
    {
        const int NumRecords = 6000;
        const long MainLogSegmentSize = 1L << 20;
        const long ObjectLogSegmentSize = 1L << 30;
        const int ThrottleCheckpointFlushDelayMs = 20;
        const string LogBaseName = "xfer.log";
        const string ObjectLogBaseName = "xfer.obj.log";
        const string CheckpointDirName = "check-points";

        string sourceDir;
        string targetDir;

        [SetUp]
        public void Setup()
        {
            RecreateDirectory(MethodTestDir);
            sourceDir = Path.Combine(MethodTestDir, "source");
            targetDir = Path.Combine(MethodTestDir, "target");
        }

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        // flushMainLogDuringCheckpoint models a concurrent main-log ReadOnly flush - the shift of
        // ReadOnlyAddress that flushes the pages it makes immutable - starting after the checkpoint has
        // captured its snapshot-start object-log position: it advances both FlushedUntilAddress and the
        // main object-log tail before the checkpoint records them at PERSISTENCE_CALLBACK.
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task TransferredSnapshotCheckpointRecoversAllObjects([Values] bool flushMainLogDuringCheckpoint)
        {
            Prepare(sourceDir, out var log, out var objlog, out var store);
            Guid token;
            LogFileInfo logFileInfo;
            try
            {
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var ii = 0; ii < NumRecords; ii++)
                        _ = bContext.Upsert(new TestObjectKey { key = ii }, new TestObjectValue { value = ii });
                }

                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.Snapshot));

                Task concurrentFlush = null;
                if (flushMainLogDuringCheckpoint)
                {
                    concurrentFlush = Task.Run(() =>
                    {
                        // The snapshot object log is created immediately before the snapshot flush is issued, so this waits until the
                        // checkpoint is inside its (throttled) snapshot flush.
                        while (store._hybridLogCheckpoint.snapshotObjectLog is null)
                            Thread.Yield();
                        // Page-aligned, as produced by AllocatorBase.CalculateReadOnlyAddress for automatic and size-tracker shifts.
                        var readOnlyAddress = store.Log.TailAddress & ~(MinKvLogPageSize - 1);
                        store.Log.ShiftReadOnlyAddress(readOnlyAddress, wait: true);
                    });
                }

                await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);
                if (concurrentFlush is not null)
                    await concurrentFlush.ConfigureAwait(false);

                logFileInfo = store.GetLogFileSize(token);
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            TransferCheckpoint(token, logFileInfo);
            await RecoverTargetAndVerifyAllObjects(token).ConfigureAwait(false);
        }

        // A Snapshot whose snapshot region holds no objects -- every object record was flushed before the checkpoint, and only
        // tombstones were written since -- still has main-log records that point into the main object log, so the main object log's
        // range must be transferred even though the snapshot object log is empty.
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task TransferredSnapshotWithoutSnapshotObjectsRecoversAllObjects()
        {
            const int DeletedKeyCount = 16;
            Prepare(sourceDir, out var log, out var objlog, out var store);
            Guid token;
            LogFileInfo logFileInfo;
            try
            {
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var ii = 0; ii < NumRecords; ii++)
                        _ = bContext.Upsert(new TestObjectKey { key = ii }, new TestObjectValue { value = ii });

                    // Deleting a key that was flushed appends a tombstone, which has no object.
                    store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                    for (var ii = 0; ii < DeletedKeyCount; ii++)
                        _ = bContext.Delete(new TestObjectKey { key = ii });
                }

                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.Snapshot));
                await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);

                logFileInfo = store.GetLogFileSize(token);
                Assert.That(logFileInfo.snapshotObjectFileEndAddress, Is.LessThanOrEqualTo(LogAddress.FirstValidAddress),
                    "the snapshot object log must be empty, or this test proves nothing");
                Assert.That(File.Exists(Path.Combine(sourceDir, CheckpointDirName, "cpr-checkpoints", token.ToString(), "snapshot.dat.0")), Is.True,
                    "the checkpoint must have written a snapshot region, or this test proves nothing");
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            TransferCheckpoint(token, logFileInfo);
            await RecoverTargetAndVerifyAllObjects(token, DeletedKeyCount).ConfigureAwait(false);
        }

        // A checkpoint records where the main object log resumes after recovery. A ReadOnly flush of a page past the checkpoint may have
        // appended object bytes it has not yet flushed when the checkpoint samples the log, and a crash loses them. The checkpoint must record
        // the log's durable end: a replica's transfer would otherwise read bytes the file does not hold, and recovery would read their page back.
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task CheckpointRecordsTheMainObjectLogsDurableEnd()
        {
            Prepare(sourceDir, out var log, out var objlog, out var store);
            Guid token;
            LogFileInfo logFileInfo;
            try
            {
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var ii = 0; ii < NumRecords; ii++)
                        _ = bContext.Upsert(new TestObjectKey { key = ii }, new TestObjectValue { value = ii });
                }
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

                // Stand in for an in-flight ReadOnly flush of a later page: append to the main object log, onto a page past its durable end,
                // without flushing.
                var mainObjectLog = store.hlogBase.RecoveryFlushObjectLog;
                var durableEnd = mainObjectLog.FlushedUntilAddress;
                var scope = mainObjectLog.BeginScope();
                try
                {
                    var restOfPage = (int)(mainObjectLog.PageSize - (mainObjectLog.TailAddress & (mainObjectLog.PageSize - 1)));
                    _ = mainObjectLog.Allocate(restOfPage);
                    _ = mainObjectLog.Allocate(4096);
                }
                finally
                {
                    mainObjectLog.EndScope(scope);
                }
                Assert.That(mainObjectLog.TailAddress, Is.GreaterThan(durableEnd));

                // Records after the append form the checkpoint's snapshot region; their objects go to the checkpoint's own object log.
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var ii = NumRecords; ii < NumRecords + 200; ii++)
                        _ = bContext.Upsert(new TestObjectKey { key = ii }, new TestObjectValue { value = ii });
                }

                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.Snapshot));
                await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);

                logFileInfo = store.GetLogFileSize(token);
                Assert.That(logFileInfo.hybridLogObjectFileEndAddress, Is.LessThanOrEqualTo(mainObjectLog.FlushedUntilAddress),
                    "the checkpoint must record the main object log's durable end, not its tail");
                Assert.That(mainObjectLog.TailAddress, Is.GreaterThan(mainObjectLog.FlushedUntilAddress),
                    "the appended bytes must still be unflushed at the checkpoint, or this test proves nothing");
            }
            finally
            {
                // A crash: the appended bytes never reach the device.
                Destroy(log, objlog, store);
            }

            TransferCheckpoint(token, logFileInfo);
            await RecoverTargetAndVerifyAllObjects(token).ConfigureAwait(false);
        }

        // A store whose records are all inline creates its main object log at its first ReadOnly flush, but never writes to it. When that flush
        // runs within a checkpoint -- here a FoldOver's own flush; under replication, a ReadOnly flush during a Snapshot -- the checkpoint records
        // a begin address of zero, taken at PREPARE before the log existed, and the empty log's end at its first valid address. The main object
        // log's range must still be empty: the log's file was never written, so a transfer that read it would fail.
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task EmptyMainObjectLogCreatedDuringCheckpointHasAnEmptyRange()
        {
            const int InlineRecords = 1000;
            const int InlineValueLength = 16;
            static byte[] MakeValue(int key) { var value = new byte[InlineValueLength]; value.AsSpan().Fill((byte)key); return value; }

            Prepare(sourceDir, out var log, out var objlog, out var store);
            Guid token;
            try
            {
                using (var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var ii = 0; ii < InlineRecords; ii++)
                        _ = bContext.Upsert(new TestObjectKey { key = ii }, MakeValue(ii).AsSpan(), Empty.Default);
                }
                Assert.That(store.hlogBase.ObjectLogBeginAddress, Is.Zero, "the main object log must not exist before the checkpoint, or this test proves nothing");

                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.FoldOver));
                await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);
                Assert.That(store.hlogBase.ObjectLogFlushedUntilAddress, Is.EqualTo(LogAddress.FirstValidAddress),
                    "the checkpoint's flush must create the main object log and write nothing to it, or this test proves nothing");

                var logFileInfo = store.GetLogFileSize(token);
                Assert.That(logFileInfo.hybridLogObjectFileEndAddress, Is.EqualTo(logFileInfo.hybridLogObjectFileStartAddress),
                    "an empty main object log must have an empty range");
                Assert.That(logFileInfo.hasSnapshotObjects, Is.False, "a checkpoint with no object-log data must report none");
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            // Recovery restores the empty main object log without reading anything.
            Prepare(sourceDir, out log, out objlog, out store);
            try
            {
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);

                using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
                var bContext = session.BasicContext;
                for (var ii = 0; ii < InlineRecords; ii++)
                {
                    TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Inline, expectedSpanLength = InlineValueLength };
                    TestLargeObjectOutput output = new();
                    var status = bContext.Read(new TestObjectKey { key = ii }, ref input, ref output, Empty.Default);
                    if (status.IsPending)
                    {
                        Assert.That(bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                        (status, output) = GetSinglePendingResult(completedOutputs);
                    }

                    ClassicAssert.IsTrue(status.Found, $"key {ii} not found after recovery");
                    Assert.That(output.valueArray, Is.EqualTo(MakeValue(ii)), $"key {ii} has the wrong value after recovery");
                }
            }
            finally
            {
                Destroy(log, objlog, store);
            }
        }

        // Recovers the transferred checkpoint on the target and reads every object back; keys below deletedKeyCount must be absent.
        private async Task RecoverTargetAndVerifyAllObjects(Guid token, int deletedKeyCount = 0)
        {
            Prepare(targetDir, out var log, out var objlog, out var store);
            try
            {
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);

                using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
                var bContext = session.BasicContext;
                for (var ii = 0; ii < NumRecords; ii++)
                {
                    var key = new TestObjectKey { key = ii };
                    TestObjectInput input = default;
                    TestObjectOutput output = new();
                    var status = bContext.Read(key, ref input, ref output);
                    if (status.IsPending)
                    {
                        Assert.That(bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                        (status, output) = GetSinglePendingResult(completedOutputs);
                    }

                    if (ii < deletedKeyCount)
                    {
                        ClassicAssert.IsFalse(status.Found, $"deleted key {ii} found on the transfer target");
                        continue;
                    }
                    ClassicAssert.IsTrue(status.Found, $"key {ii} not found on the transfer target");
                    ClassicAssert.AreEqual(ii, output.value.value, $"key {ii} has the wrong value on the transfer target");
                }
            }
            finally
            {
                Destroy(log, objlog, store);
            }
        }

        // A snapshot-region page that recovery flushes to the main log has its objects copied into the MAIN object log and its records
        // repointed there, but it arrives carrying the header it was checkpointed with, which holds no main object-log floor.
        // The page header's floor bounds main object-log truncation, so a floor from another address space truncates against it.
        // Every resident page must therefore hold either no position or one at/after where the main object log ended when the
        // checkpoint was taken, which is where recovery began appending.
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task SnapshotPageHeadersUseMainObjectLog()
        {
            Prepare(sourceDir, out var log, out var objlog, out var store);
            Guid token;
            ulong mainObjectLogEndAtCheckpoint;
            long snapshotRegionStartAddress;
            try
            {
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var ii = 0; ii < NumRecords; ii++)
                        _ = bContext.Upsert(new TestObjectKey { key = ii }, new TestObjectValue { value = ii });
                }

                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.Snapshot));
                await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);

                // The main log file ends at the hybrid-log/snapshot boundary: records below it are already durable on the main log and
                // their pages legitimately carry main object-log positions from before the checkpoint; records at or above it live only
                // in the snapshot and are what recovery copies into the main object log.
                var logFileInfo = store.GetLogFileSize(token);
                mainObjectLogEndAtCheckpoint = (ulong)logFileInfo.hybridLogObjectFileEndAddress;
                snapshotRegionStartAddress = logFileInfo.hybridLogFileEndAddress;
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            Prepare(sourceDir, out log, out objlog, out store);
            long recoveredTailAddress;
            try
            {
                // Recovery only flushes snapshot pages to the main log when it has to evict, which requires a size-tracker budget.
                // Without one TrimLogPages returns immediately, every page stays resident, and the path under test never runs.
                const long recoveryTargetSize = 8L * MinKvLogPageSize;
                var tracker = new LogSizeTracker<ClassStoreFunctions, ClassAllocator>(store.Log, recoveryTargetSize,
                    recoveryTargetSize / 8, recoveryTargetSize / 16, logger: null);
                store.Log.SetLogSizeTracker(tracker);

                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                recoveredTailAddress = store.Log.TailAddress;
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            // Snapshot-region pages are flushed to the MAIN log during recovery and then evicted, so the stamp has to be read back from
            // the main log file rather than from a resident page. A page that stayed resident instead was returned to the freshly-allocated
            // state (NotSet) for a later flush to stamp, which reads the same here.
            var stamped = 0;
            var scanned = 0;
            var firstSnapshotPage = (snapshotRegionStartAddress + MinKvLogPageSize - 1) / MinKvLogPageSize;
            for (var page = firstSnapshotPage; page <= recoveredTailAddress / MinKvLogPageSize; page++)
            {
                ++scanned;
                var word = ReadPageHeaderObjectLogWord(Path.Combine(sourceDir, LogBaseName), page * MinKvLogPageSize);
                if (ObjectLogPositionInfo.WordIsUnstamped(word))
                    continue;
                ++stamped;
                Assert.That(word & ObjectLogPositionInfo.AddressMask, Is.GreaterThanOrEqualTo(mainObjectLogEndAtCheckpoint),
                    $"snapshot-region page {page} header holds object-log position {word & ObjectLogPositionInfo.AddressMask}, which "
                    + $"precedes the main object log's end at checkpoint ({mainObjectLogEndAtCheckpoint}); that is a snapshot-object-log position, not a main one");
            }

            Assert.That(stamped, Is.GreaterThan(0),
                $"no snapshot-region page carried an object-log position, so this test proves nothing: scanned {scanned} pages from {firstSnapshotPage} "
                + $"(snapshotRegionStart {snapshotRegionStartAddress}, recoveredTail {recoveredTailAddress}, mainObjEnd {mainObjectLogEndAtCheckpoint})");
        }

        // A Snapshot checkpoint writes a page to its own object log before the ReadOnly flush writes that page to the main log, and both write
        // the same live page. Main object-log truncation reads a page's floor from the main log, so the header the ReadOnly flush persists
        // must hold a main object-log address. The checkpoint's object log starts again at its first address, so a floor from it would hold
        // truncation back indefinitely, or let truncation pass live records.
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task ReadOnlyFlushAfterSnapshotStampsMainObjectLogFloor()
        {
            // Flushed to the main object log before the checkpoint, so its end is well above any address in the checkpoint's object log.
            const int FlushedRecords = 4500;

            // Written after that flush, so the checkpoint's Snapshot writes their pages before the ReadOnly flush does.
            const int SnapshotRecords = 1500;

            Prepare(sourceDir, out var log, out var objlog, out var store);
            long mainObjectLogEndBeforeCheckpoint, snapshotRegionStartAddress, flushedUntilAddress;
            try
            {
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var ii = 0; ii < FlushedRecords; ii++)
                        _ = bContext.Upsert(new TestObjectKey { key = ii }, new TestObjectValue { value = ii });
                    store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                    mainObjectLogEndBeforeCheckpoint = store.hlogBase.ObjectLogFlushedUntilAddress;
                    snapshotRegionStartAddress = store.Log.TailAddress;

                    for (var ii = FlushedRecords; ii < FlushedRecords + SnapshotRecords; ii++)
                        _ = bContext.Upsert(new TestObjectKey { key = ii }, new TestObjectValue { value = ii });
                }

                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out var token, CheckpointType.Snapshot));
                await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);
                Assert.That(store.GetLogFileSize(token).snapshotObjectFileEndAddress, Is.GreaterThan(LogAddress.FirstValidAddress),
                    "the Snapshot must have written objects to its own object log, or this test proves nothing");

                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                flushedUntilAddress = store.Log.FlushedUntilAddress;
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            // Pages wholly in the snapshot region reached the main log only through the ReadOnly flush after the checkpoint.
            var checkedPages = 0;
            var firstPageStart = (snapshotRegionStartAddress + MinKvLogPageSize - 1) / MinKvLogPageSize * MinKvLogPageSize;
            for (var pageStart = firstPageStart; pageStart + PageHeader.Size < flushedUntilAddress; pageStart += MinKvLogPageSize)
            {
                var word = ReadPageHeaderObjectLogWord(Path.Combine(sourceDir, LogBaseName), pageStart);
                Assert.That(ObjectLogPositionInfo.WordIsUnstamped(word), Is.False, $"the page at {pageStart} has no object-log floor");
                Assert.That((long)(word & ObjectLogPositionInfo.AddressMask), Is.GreaterThanOrEqualTo(mainObjectLogEndBeforeCheckpoint),
                    $"the page at {pageStart} has floor {word & ObjectLogPositionInfo.AddressMask}, below the main object log's end before the "
                    + $"checkpoint ({mainObjectLogEndBeforeCheckpoint}); that is an address in the checkpoint's object log");
                ++checkedPages;
            }
            Assert.That(checkedPages, Is.GreaterThan(0), "no page lay wholly in the snapshot region, so this test proves nothing");
        }

        /// <summary>Read a main-log page's <see cref="PageHeader.objectLogLowestPositionWord"/> straight from the log file.</summary>
        private static unsafe ulong ReadPageHeaderObjectLogWord(string logBaseName, long pageStartAddress)
        {
            var path = $"{logBaseName}.{pageStartAddress / MainLogSegmentSize}";
            if (!File.Exists(path))
                return ObjectLogPositionInfo.NotSet;

            var offsetInSegment = pageStartAddress % MainLogSegmentSize;
            using var stream = File.OpenRead(path);
            if (offsetInSegment + PageHeader.Size > stream.Length)
                return ObjectLogPositionInfo.NotSet;

            _ = stream.Seek(offsetInSegment, SeekOrigin.Begin);
            var bytes = new byte[PageHeader.Size];
            stream.ReadExactly(bytes);
            fixed (byte* headerPtr = bytes)
                return ((PageHeader*)headerPtr)->objectLogLowestPositionWord;
        }

        // Copies exactly the file ranges that TsavoriteSnapshotReader sends to a replica during full sync.
        private void TransferCheckpoint(Guid token, LogFileInfo logFileInfo)
        {
            var sourceCheckpointDir = Path.Combine(sourceDir, CheckpointDirName, "cpr-checkpoints", token.ToString());
            var targetCheckpointDir = Path.Combine(targetDir, CheckpointDirName, "cpr-checkpoints", token.ToString());
            _ = Directory.CreateDirectory(targetCheckpointDir);
            foreach (var metadataFile in Directory.GetFiles(sourceCheckpointDir, "info.dat*"))
                File.Copy(metadataFile, Path.Combine(targetCheckpointDir, Path.GetFileName(metadataFile)));

            if (logFileInfo.hybridLogFileEndAddress > PageHeader.Size)
            {
                CopyRange(Path.Combine(sourceDir, LogBaseName), Path.Combine(targetDir, LogBaseName),
                    logFileInfo.hybridLogFileStartAddress, logFileInfo.hybridLogFileEndAddress, MainLogSegmentSize);
                if (logFileInfo.hasSnapshotObjects)
                    CopyRange(Path.Combine(sourceDir, ObjectLogBaseName), Path.Combine(targetDir, ObjectLogBaseName),
                        logFileInfo.hybridLogObjectFileStartAddress, logFileInfo.hybridLogObjectFileEndAddress, ObjectLogSegmentSize);
            }

            if (logFileInfo.snapshotFileEndAddress > PageHeader.Size)
            {
                CopyRange(Path.Combine(sourceCheckpointDir, "snapshot.dat"), Path.Combine(targetCheckpointDir, "snapshot.dat"),
                    0, logFileInfo.snapshotFileEndAddress, MainLogSegmentSize);
                if (logFileInfo.hasSnapshotObjects)
                    CopyRange(Path.Combine(sourceCheckpointDir, "snapshot.obj.dat"), Path.Combine(targetCheckpointDir, "snapshot.obj.dat"),
                        0, logFileInfo.snapshotObjectFileEndAddress, ObjectLogSegmentSize);
            }
        }

        internal static void CopyRange(string sourceBaseName, string targetBaseName, long startAddress, long endAddress, long segmentSize)
        {
            for (var segment = startAddress / segmentSize; startAddress < endAddress && segment <= (endAddress - 1) / segmentSize; segment++)
            {
                var segmentStartAddress = segment * segmentSize;
                var from = Math.Max(startAddress, segmentStartAddress) - segmentStartAddress;
                var to = Math.Min(endAddress, segmentStartAddress + segmentSize) - segmentStartAddress;

                var buffer = new byte[to - from];
                using (var sourceStream = new FileStream($"{sourceBaseName}.{segment}", FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    _ = sourceStream.Seek(from, SeekOrigin.Begin);
                    sourceStream.ReadExactly(buffer);
                }

                using var targetStream = new FileStream($"{targetBaseName}.{segment}", FileMode.Create, FileAccess.Write);
                _ = targetStream.Seek(from, SeekOrigin.Begin);
                targetStream.Write(buffer);
            }
        }

        private static void Prepare(string dir, out IDevice log, out IDevice objlog, out TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            _ = Directory.CreateDirectory(dir);
            log = Devices.CreateLogDevice(Path.Combine(dir, LogBaseName));
            objlog = Devices.CreateLogDevice(Path.Combine(dir, ObjectLogBaseName));
            store = new(new()
            {
                IndexSize = 1L << 22,
                LogDevice = log,
                ObjectLogDevice = objlog,
                SegmentSize = MainLogSegmentSize,
                ObjectLogSegmentSize = ObjectLogSegmentSize,
                LogMemorySize = 32 * MinKvLogPageSize,
                PageSize = MinKvLogPageSize,
                ThrottleCheckpointFlushDelayMs = ThrottleCheckpointFlushDelayMs,
                CheckpointDir = Path.Combine(dir, CheckpointDirName)
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