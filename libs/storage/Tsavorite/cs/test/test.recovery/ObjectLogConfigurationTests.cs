// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery
{
    using ObjAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ObjStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// Verifies that <see cref="KVSettings.ObjectLogMemorySize"/>, <see cref="KVSettings.ObjectLogPageSize"/>, and
    /// <see cref="KVSettings.ObjectLogSegmentSize"/> shape every object log a store opens, and that a value out of range fails store
    /// construction rather than the first object-log write.
    /// </summary>
    [TestFixture]
    public class ObjectLogConfigurationTests : TestBase
    {
        const string MainLogName = "config.log";
        const string ObjectLogName = "config.obj.log";

        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => OnTearDown();

        static TsavoriteKV<ObjStoreFunctions, ObjAllocator> CreateObjectStore(IDevice log, IDevice objlog, long objectLogSegmentSize,
                long objectLogPageSize = 0, long objectLogMemorySize = 0)
            => new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                PageSize = 1L << 13,
                LogMemorySize = 1L << 16,
                SegmentSize = 1L << 17,
                ObjectLogSegmentSize = objectLogSegmentSize,
                ObjectLogPageSize = objectLogPageSize,
                ObjectLogMemorySize = objectLogMemorySize,
                CheckpointDir = MethodTestDir,
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer(), DefaultRecordTriggers.Instance),
               (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        TsavoriteException AssertConstructionFails(long objectLogSegmentSize, long objectLogPageSize, long objectLogMemorySize = 0)
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, MainLogName));
            using var objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, ObjectLogName));
            return Assert.Throws<TsavoriteException>(() =>
            {
                using var store = CreateObjectStore(log, objlog, objectLogSegmentSize, objectLogPageSize, objectLogMemorySize);
            });
        }

        /// <summary>
        /// The memory size sets the page count of the main object log and of a Snapshot checkpoint's object log alike; zero takes the
        /// default count.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjectLogMemorySizeSetsThePageCountOfEveryObjectLog([Values(0, KVSettings.MinObjectLogPageCount, 16)] int pageCount)
        {
            const int PageSizeBits = 16;
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, MainLogName));
            using var objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, ObjectLogName));
            using var snapshotObjlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "config.snapshot.obj.log"));
            using var store = CreateObjectStore(log, objlog, objectLogSegmentSize: 1L << 20, objectLogPageSize: 1L << PageSizeBits,
                objectLogMemorySize: (long)pageCount << PageSizeBits);

            var expectedPageCount = pageCount == 0 ? ObjectLog.DefaultPageCount : pageCount;

            var mainObjectLog = store.hlogBase.RecoveryFlushObjectLog;
            ClassicAssert.AreEqual(1 << PageSizeBits, mainObjectLog.PageSize, "main object log page size");
            ClassicAssert.AreEqual(expectedPageCount, mainObjectLog.PageCount, "main object log page count");

            using var snapshotObjectLog = store.hlogBase.CreateSnapshotObjectLog(snapshotObjlog);
            ClassicAssert.AreEqual(1 << PageSizeBits, snapshotObjectLog.PageSize, "snapshot object log page size");
            ClassicAssert.AreEqual(expectedPageCount, snapshotObjectLog.PageCount, "snapshot object log page count");
        }

        /// <summary>
        /// A memory size holding fewer than <see cref="KVSettings.MinObjectLogPageCount"/> pages, or more than the page-number headroom
        /// allows, fails store construction.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjectLogMemorySizeOutsideThePageCountRangeFailsConstruction([Values(0, 1, 31)] int pageCountBits)
        {
            const int PageSizeBits = 12;
            var ex = AssertConstructionFails(objectLogSegmentSize: 1L << 20, objectLogPageSize: 1L << PageSizeBits,
                objectLogMemorySize: 1L << (PageSizeBits + pageCountBits));
            StringAssert.Contains(nameof(KVSettings.ObjectLogMemorySize), ex.Message);
        }

        /// <summary>
        /// A page must be between 4 KB and 128 MB, and no larger than a segment.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        [TestCase(LogSettings.kMinPageSizeBits - 1, 20, TestName = "PageBelowMinimum")]
        [TestCase(LogSettings.kMaxPageSizeBits + 1, 30, TestName = "PageAboveMaximum")]
        [TestCase(21, 20, TestName = "PageLargerThanSegment")]
        public void ObjectLogPageSizeOutOfRangeFailsConstruction(int pageSizeBits, int segmentSizeBits)
        {
            var ex = AssertConstructionFails(objectLogSegmentSize: 1L << segmentSizeBits, objectLogPageSize: 1L << pageSizeBits);
            StringAssert.Contains(nameof(KVSettings.ObjectLogPageSize), ex.Message);
        }

        /// <summary>
        /// Object-log segments far below the 4 MB the object log once required still carry objects through eviction, a checkpoint, and
        /// recovery. The page defaults to the segment size, so a value larger than a page is written in several chunks, which span
        /// segments. Half the objects are evicted to the main object log first; the rest are still in memory at the checkpoint, so a
        /// Snapshot writes them to the checkpoint's object log, also in small segments.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task ObjectsRoundTripThroughSmallObjectLogSegments([Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType)
        {
            const long ObjectLogSegmentSize = 1L << 16;
            const int NumObjects = 40;
            const int MaxObjectSize = 200 * 1024;

            var rng = new Random(101);
            var sizes = new int[NumObjects];
            for (var key = 0; key < NumObjects; key++)
                sizes[key] = 1 + rng.Next(MaxObjectSize);

            Guid token;
            using (var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, MainLogName)))
            using (var objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, ObjectLogName)))
            using (var store = CreateObjectStore(log, objlog, ObjectLogSegmentSize))
            using (var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
            {
                var bContext = session.BasicContext;
                for (var key = 0; key < NumObjects / 2; key++)
                    _ = bContext.Upsert(new TestObjectKey { key = key }, new TestLargeObjectValue(sizes[key]), Empty.Default);
                store.Log.FlushAndEvict(wait: true);
                Assert.That(store.hlogBase.ObjectLogFlushedUntilAddress, Is.GreaterThan(4 * ObjectLogSegmentSize),
                    "The evicted objects must span several object-log segments");

                for (var key = NumObjects / 2; key < NumObjects; key++)
                    _ = bContext.Upsert(new TestObjectKey { key = key }, new TestLargeObjectValue(sizes[key]), Empty.Default);
                VerifyObjects(session, sizes);

                ClassicAssert.IsTrue(store.TryInitiateFullCheckpoint(out token, checkpointType), "failed to initiate checkpoint");
                await store.CompleteCheckpointAsync().ConfigureAwait(false);
            }

            using (var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, MainLogName)))
            using (var objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, ObjectLogName)))
            using (var store = CreateObjectStore(log, objlog, ObjectLogSegmentSize))
            {
                _ = await store.RecoverAsync(token).ConfigureAwait(false);
                using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
                VerifyObjects(session, sizes);
            }
        }

        static void VerifyObjects(ClientSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions, ObjStoreFunctions, ObjAllocator> session,
            int[] sizes)
        {
            var bContext = session.BasicContext;
            TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Object };
            for (var key = 0; key < sizes.Length; key++)
            {
                TestLargeObjectOutput output = new();
                var status = bContext.Read(new TestObjectKey { key = key }, ref input, ref output, Empty.Default);
                if (status.IsPending)
                    (status, output) = bContext.GetSinglePendingResult();
                ClassicAssert.IsTrue(status.Found, $"key {key}: {status}");

                var value = output.valueObject.value;
                ClassicAssert.AreEqual(sizes[key], value.Length, $"key {key} length");
                for (var i = 0; i < value.Length; i++)
                {
                    if (value[i] != (byte)(value.Length + i))
                        Assert.Fail($"key {key}: byte {i} is {value[i]}, expected {(byte)(value.Length + i)}");
                }
            }
        }
    }
}