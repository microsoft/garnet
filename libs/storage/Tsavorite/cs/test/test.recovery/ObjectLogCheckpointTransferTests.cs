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
                        // The flush buffers are created immediately after the snapshot-start object-log position is captured,
                        // so this waits until the checkpoint is inside its (throttled) snapshot flush.
                        while (store._hybridLogCheckpoint.objectLogFlushBuffers is null)
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

            Prepare(targetDir, out log, out objlog, out store);
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
        // repointed there, but it arrives carrying the header it was checkpointed with, whose position is in the SNAPSHOT object log.
        // GetLowestObjectLogSegmentInUse feeds that word to the main device's TruncateUntilSegment, so a stale stamp truncates against an
        // unrelated address space. Every resident page must therefore hold either no position or one at/after where the main object log
        // ended when the checkpoint was taken, which is where recovery began appending.
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
                if (word == ObjectLogFilePositionInfo.NotSet)
                    continue;
                ++stamped;
                Assert.That(word & ObjectLogFilePositionInfo.SegmentAndOffsetMask, Is.GreaterThanOrEqualTo(mainObjectLogEndAtCheckpoint),
                    $"snapshot-region page {page} header holds object-log position {word & ObjectLogFilePositionInfo.SegmentAndOffsetMask}, which "
                    + $"precedes the main object log's end at checkpoint ({mainObjectLogEndAtCheckpoint}); that is a snapshot-object-log position, not a main one");
            }

            Assert.That(stamped, Is.GreaterThan(0),
                $"no snapshot-region page carried an object-log position, so this test proves nothing: scanned {scanned} pages from {firstSnapshotPage} "
                + $"(snapshotRegionStart {snapshotRegionStartAddress}, recoveredTail {recoveredTailAddress}, mainObjEnd {mainObjectLogEndAtCheckpoint})");
        }

        /// <summary>Read a main-log page's <see cref="PageHeader.objectLogLowestPositionWord"/> straight from the log file.</summary>
        private static unsafe ulong ReadPageHeaderObjectLogWord(string logBaseName, long pageStartAddress)
        {
            var path = $"{logBaseName}.{pageStartAddress / MainLogSegmentSize}";
            if (!File.Exists(path))
                return ObjectLogFilePositionInfo.NotSet;

            var offsetInSegment = pageStartAddress % MainLogSegmentSize;
            using var stream = File.OpenRead(path);
            if (offsetInSegment + PageHeader.Size > stream.Length)
                return ObjectLogFilePositionInfo.NotSet;

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

        private static void CopyRange(string sourceBaseName, string targetBaseName, long startAddress, long endAddress, long segmentSize)
        {
            for (var segment = startAddress / segmentSize; startAddress < endAddress && segment <= (endAddress - 1) / segmentSize; segment++)
            {
                var segmentStartAddress = segment * segmentSize;
                var from = Math.Max(startAddress, segmentStartAddress) - segmentStartAddress;
                var to = Math.Min(endAddress, segmentStartAddress + segmentSize) - segmentStartAddress;

                var buffer = new byte[to - from];
                using (var sourceStream = File.OpenRead($"{sourceBaseName}.{segment}"))
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