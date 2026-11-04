// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// Tests for trimming an object store's logs to the segments that recovery to a checkpoint needs, as VerifyRecoveryInfo does when
    /// asked to trim the log
    /// </summary>
    [TestFixture]
    internal class TrimLogTests : TestBase
    {
        const int PageSizeBits = LogSettings.kMinPageSizeBits;

        // 4 pages per main-log segment, so each phase below writes several segments
        const int SegmentSizeBits = PageSizeBits + 2;

        // Each phase writes about 8 MB of object data: two or three object-log segments
        const int ObjectSize = 1 << 13;
        const int RecordsPerPhase = 1000;

        string logPath, objectLogPath;

        [SetUp]
        public void Setup()
        {
            RecreateDirectory(MethodTestDir);
            logPath = Path.Join(MethodTestDir, "trim.log");
            objectLogPath = Path.Join(MethodTestDir, "trim.obj.log");
        }

        [TearDown]
        public void TearDown()
        {
            DeleteDirectory(MethodTestDir);
            OnTearDown();
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async Task TrimLogRemovesTheSegmentsAboveTheCheckpointFromBothLogs([Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType)
        {
            Guid token;
            using (var log = CreateDevice(logPath))
            using (var objectLog = CreateDevice(objectLogPath))
            using (var store = CreateStore(log, objectLog))
            {
                using (var session = NewSession(store))
                {
                    Upsert(session, 0, RecordsPerPhase);
                    _ = store.TryInitiateFullCheckpoint(out token, checkpointType);
                    await store.CompleteCheckpointAsync().ConfigureAwait(false);

                    // Write past the checkpoint, so that both logs gain segments above its data
                    Upsert(session, RecordsPerPhase, RecordsPerPhase);
                    store.Log.FlushAndEvict(wait: true);
                }

                var info = new HybridLogCheckpointInfo();
                info.Recover(token, store.CheckpointManager);
                var validUntil = checkpointType == CheckpointType.Snapshot ? info.info.flushedLogicalAddress : info.info.finalLogicalAddress;
                var lastValidSegment = (int)((validUntil - 1) >> SegmentSizeBits);
                var lastValidObjectLogSegment = info.info.hlogEndObjectLogTail.SegmentId;
                Assert.That(Segments(logPath).Max(), Is.GreaterThan(lastValidSegment), "the main log should have segments above the checkpoint's");
                Assert.That(Segments(objectLogPath).Max(), Is.GreaterThan(lastValidObjectLogSegment), "the object log should have segments above the checkpoint's");

                store.hlogBase.VerifyRecoveryInfo(info, trimLog: true);

                Assert.That(Segments(logPath), Is.EqualTo(Enumerable.Range(0, lastValidSegment + 1)), "the main log should keep exactly the checkpoint's segments");
                Assert.That(Segments(objectLogPath), Is.EqualTo(Enumerable.Range(0, lastValidObjectLogSegment + 1)), "the object log should keep exactly the checkpoint's segments");
            }

            // A store over the trimmed logs recovers the checkpoint, objects included
            using (var log = CreateDevice(logPath))
            using (var objectLog = CreateDevice(objectLogPath))
            using (var store = CreateStore(log, objectLog))
            {
                _ = await store.RecoverAsync(token).ConfigureAwait(false);
                using var session = NewSession(store);
                var context = session.BasicContext;
                var input = new TestLargeObjectInput { wantValueStyle = TestValueStyle.Object };
                for (var key = 0; key < RecordsPerPhase; key++)
                {
                    var output = new TestLargeObjectOutput();
                    var status = context.Read(new TestObjectKey { key = key }, ref input, ref output, Empty.Default);
                    if (status.IsPending)
                        (status, output) = context.GetSinglePendingResult();
                    Assert.That(status.Found, Is.True, $"key {key} should be recovered");
                    ClassicAssert.AreEqual(ObjectSize, output.valueObject.value.Length, $"key {key}");
                    ClassicAssert.AreEqual(unchecked((byte)(ObjectSize * 2 - 1)), output.valueObject.value[^1], $"key {key}");
                }
            }
        }

        static IDevice CreateDevice(string path) => new RandomAccessLocalStorageDevice(path, deleteOnClose: false);

        static TsavoriteKV<ClassStoreFunctions, ClassAllocator> CreateStore(IDevice log, IDevice objectLog)
            => new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objectLog,
                MutableFraction = 0.1,
                PageSize = 1L << PageSizeBits,
                LogMemorySize = 1L << (PageSizeBits + 3),
                SegmentSize = 1L << SegmentSizeBits,
                ObjectLogSegmentSize = 1L << LogSettings.kMinObjectLogSegmentSizeBits,
                CheckpointDir = MethodTestDir,
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer()),
                (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        static ClientSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions, ClassStoreFunctions, ClassAllocator> NewSession(
            TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
            => store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());

        static void Upsert(ClientSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions, ClassStoreFunctions, ClassAllocator> session,
            int firstKey, int count)
        {
            var context = session.BasicContext;
            for (var key = firstKey; key < firstKey + count; key++)
                _ = context.Upsert(new TestObjectKey { key = key }, new TestLargeObjectValue(ObjectSize), Empty.Default);
        }

        /// <summary>The segments whose files the log at <paramref name="path"/> has, in ascending order</summary>
        static List<int> Segments(string path)
            => [.. Directory.GetFiles(Path.GetDirectoryName(path), Path.GetFileName(path) + ".*")
                .Select(file => int.Parse(Path.GetExtension(file).AsSpan(1)))
                .Order()];
    }
}