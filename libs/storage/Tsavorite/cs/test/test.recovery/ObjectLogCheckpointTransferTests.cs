// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
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

        // flushMainLogDuringCheckpoint models a concurrent main-log flush (as issued by LogSizeTracker eviction) that
        // starts after the checkpoint has captured its snapshot-start object-log position: it advances both
        // FlushedUntilAddress and the main object-log tail before the checkpoint records them at PERSISTENCE_CALLBACK.
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