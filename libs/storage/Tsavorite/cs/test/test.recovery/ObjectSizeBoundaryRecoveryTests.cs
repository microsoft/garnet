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
    using LargeObjAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using LargeObjStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;
    using SbKeyAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordTriggers>>;
    using SbKeyStoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordTriggers>;

    /// <summary>
    /// Sweeps out-of-line VALUE sizes across every object-log serialization boundary (exact-size cutoff, sector, 4 KB page, 64 KB,
    /// saturated value hint, and multi-chunk sizes) for BOTH object and overflow values, checkpointing (Snapshot and
    /// FoldOver) and recovering, then reading every record back byte-for-byte. This gives explicit coverage of the size
    /// boundaries the object-log length encoding pivots on (headerless exact-size vs length-prefixed vs multi-chunk with a
    /// directory). It complements the fixed-size recovery tests (which cover a couple of points) with a systematic sweep.
    ///
    /// The size sweep uses a large object-log segment so no single value spans a segment boundary; a separate pair of tests
    /// (<see cref="RecoverObjectValueSpanningObjectLogSegmentBoundary"/> and
    /// <see cref="RecoverOvfValueSpanningObjLogSegment"/>) exercises the segment-crossing case explicitly.
    /// </summary>
    [TestFixture]
    public class ObjectSizeBoundaryRecoveryTests : TestBase
    {
        // Sizes straddling every object-log encoding boundary:
        //   1, 100                : tiny headerless
        //   511, 512, 513         : exact-size cutoff (kOutOfLineExactSizeCutoff = 511) / sub-sector / sector boundary (512 B)
        //   1023, 1024            : length-prefixed, around 1 KB
        //   4095, 4096            : 4 KB page boundary
        //   65535, 65536          : 64 KB boundary
        //   262144                : 256 KB (one chunk)
        //   2 MB, 3 MB            : saturated value hint -- an extent of 511 or more 4 KB pages hints ObjectLogExtent.LargeValueHint, so
        //                           the first read is a full chunk plus its prefix, clamped to the object-log page and tail, rather than
        //                           the value's length. 3 MB * 6 records writes ~18 MB in a 1 GB segment.
        //   5 MB                  : more than one 4 MB chunk, so the record has a directory
        static readonly int[] BoundarySizes =
        [
            1, 100, 510, 511, 512, 513, 1023, 1024, 4095, 4096, 65535, 65536,
            131071, 131072, 131073, 262144,
            // Bracket the value hint's saturation seam: the objectId hint stops being an exact 4 KB page count and saturates to
            // ObjectLogExtent.LargeValueHint at page count 511. An overflow value's extent adds exactly the 4-byte length prefix, so
            // these give extents of exactly 509, 510, and 511 pages; an object's serializer framing shifts each slightly higher.
            (509 * RecordDataHeader.kFlushPageSize) - ObjectLogExtent.LengthPrefixSize,
            (510 * RecordDataHeader.kFlushPageSize) - ObjectLogExtent.LengthPrefixSize,
            (511 * RecordDataHeader.kFlushPageSize) - ObjectLogExtent.LengthPrefixSize,
            2 * 1024 * 1024, 3 * 1024 * 1024,
            (4 * 1024 * 1024) - 1, 4 * 1024 * 1024, (4 * 1024 * 1024) + 1, 5 * 1024 * 1024
        ];

        // Large object-log segment: the size sweep never crosses a segment boundary (isolating pure size-boundary behavior).
        const long NoSplitObjectLogSegmentSize = 1L << 30;      // 1 GB

        // Small object-log segment used by the segment-crossing tests: with 5 MB values, records straddle the 16 MB boundary.
        const long SplitObjectLogSegmentSize = 1L << 24;        // 16 MB

        // Tight recovery memory budget: when attached to the recovery store, the size tracker's total (main-log bytes + object
        // heap) exceeds this budget while loading boundary-size objects, forcing the deferred-object-load eviction path
        // (FindHeadAddressCutoffOnPage -> FlushSnapshotPageForRecovery -> read objects back from the copied main object-log)
        // rather than keeping everything resident. 8 pages = 32 KB, comfortably above the 4-page tracker minimum.
        const long RecoveryEvictTargetSize = 8L * MinKvLogPageSize;

        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => OnTearDown();

        // Keep total IO bounded: fewer records for the larger sizes.
        static int RecordCountForSize(int size) => size <= 65536 ? 64 : (size <= 262144 ? 16 : 6);

        // Object-value store: heap objects (TestLargeObjectValue) serialized to the object log.
        static TsavoriteKV<LargeObjStoreFunctions, LargeObjAllocator> CreateObjectStore(IDevice log, IDevice objlog, string checkpointDir, long memorySize, long objectLogSegmentSize)
            => new(new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.9,
                PageSize = MinKvLogPageSize,
                LogMemorySize = memorySize,
                SegmentSize = 1L << 20,
                ObjectLogSegmentSize = objectLogSegmentSize,
                CheckpointDir = checkpointDir,
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer(), DefaultRecordTriggers.Instance),
               (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        // Overflow-value store: raw byte[] values forced out of line (MaxInlineValueSize = 0) into the object log.
        static TsavoriteKV<LargeObjStoreFunctions, LargeObjAllocator> CreateOverflowStore(IDevice log, IDevice objlog, string checkpointDir, long memorySize, long objectLogSegmentSize)
            => new(new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.9,
                PageSize = MinKvLogPageSize,
                LogMemorySize = memorySize,
                SegmentSize = 1L << 20,
                ObjectLogSegmentSize = objectLogSegmentSize,
                MaxInlineValueSize = 0,             // store raw byte[] values as overflow (not objects)
                CheckpointDir = checkpointDir,
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer(), DefaultRecordTriggers.Instance),
               (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        // Deterministic, per-key-identifiable payload: bytes are (byte)(key*31 + i), so a swapped/torn record is detectable.
        static byte[] MakePayload(int key, int size)
        {
            var v = new byte[size];
            for (var i = 0; i < size; i++)
                v[i] = (byte)((key * 31) + i);
            return v;
        }

        static void VerifyPayload(int key, int size, byte[] actual)
        {
            ClassicAssert.IsNotNull(actual, $"key {key} (size {size}) read back null");
            ClassicAssert.AreEqual(size, actual.Length, $"key {key} wrong length");

            // Compare EVERY byte. SequenceEqual is vectorized, so a full compare costs far less than the per-byte
            // assertions it replaces, and a torn or swapped interior that a first/middle/last spot-check would miss
            // is exactly the failure these boundary tests exist to catch.
            var expected = MakePayload(key, size);
            if (actual.AsSpan().SequenceEqual(expected))
                return;

            // Report the first divergence rather than dumping the whole payload.
            var firstDiff = actual.AsSpan().CommonPrefixLength(expected);
            Assert.Fail($"key {key} (size {size}) payload mismatch at byte {firstDiff}: expected {expected[firstDiff]}, got {actual[firstDiff]}");
        }

        // Write numRecords object values of the given size, checkpoint, recover into a fresh store, and read every record back byte-for-byte.
        // When recoveryTargetSize > 0, a LogSizeTracker with that budget is attached to the recovery store so the deferred object
        // load must evict during recovery (exercising the snapshot-page-flush / read-from-main-object-log path under pressure).
        async Task RunObjectValueRecovery(CheckpointType checkpointType, int valueSize, int numRecords, long objectLogSegmentSize, long recoveryTargetSize = 0)
        {
            var checkpointDir = Path.Combine(MethodTestDir, "checkpoints");
            IDevice log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "obj.log"), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "obj.obj.log"), deleteOnClose: false);
            try
            {
                Guid token;
                using (var store1 = CreateObjectStore(log, objlog, checkpointDir, 1L << 20, objectLogSegmentSize))
                {
                    using (var session = store1.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
                    {
                        var bContext = session.BasicContext;
                        for (var key = 0; key < numRecords; key++)
                            _ = bContext.Upsert(new TestObjectKey { key = key }, new TestLargeObjectValue() { value = MakePayload(key, valueSize) });
                    }
                    var (success, checkpointToken) = await store1.TakeFullCheckpointAsync(checkpointType).ConfigureAwait(false);
                    ClassicAssert.IsTrue(success, "checkpoint failed");
                    token = checkpointToken;
                }

                using (var store2 = CreateObjectStore(log, objlog, checkpointDir, 1L << 20, objectLogSegmentSize))
                {
                    if (recoveryTargetSize > 0)
                    {
                        var tracker = new LogSizeTracker<LargeObjStoreFunctions, LargeObjAllocator>(store2.Log, recoveryTargetSize, recoveryTargetSize / 8, recoveryTargetSize / 16, logger: null);
                        store2.Log.SetLogSizeTracker(tracker);
                    }
                    _ = await store2.RecoverAsync(default, token).ConfigureAwait(false);
                    using var session = store2.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
                    var bContext = session.BasicContext;
                    for (var key = 0; key < numRecords; key++)
                    {
                        TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Object };
                        TestLargeObjectOutput output = new();
                        var status = bContext.Read(new TestObjectKey { key = key }, ref input, ref output);
                        if (status.IsPending)
                        {
                            ClassicAssert.IsTrue(bContext.CompletePendingWithOutputs(out var completed, wait: true));
                            (status, output) = GetSinglePendingResult(completed);
                        }
                        ClassicAssert.IsTrue(status.Found, $"key {key} not found (size {valueSize}, {checkpointType})");
                        VerifyPayload(key, valueSize, output.valueObject?.value);
                    }
                }
            }
            finally
            {
                log.Dispose();
                objlog.Dispose();
            }
        }

        // Write numRecords overflow (raw byte[]) values of the given size, checkpoint, recover into a fresh store, and read every record back byte-for-byte.
        // When recoveryTargetSize > 0, a LogSizeTracker with that budget is attached to the recovery store so the deferred object
        // load must evict during recovery (exercising the snapshot-page-flush / read-from-main-object-log path under pressure).
        async Task RunOverflowValueRecovery(CheckpointType checkpointType, int valueSize, int numRecords, long objectLogSegmentSize, long recoveryTargetSize = 0)
        {
            var checkpointDir = Path.Combine(MethodTestDir, "checkpoints");
            IDevice log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "ovf.log"), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "ovf.obj.log"), deleteOnClose: false);
            try
            {
                Guid token;
                using (var store1 = CreateOverflowStore(log, objlog, checkpointDir, 1L << 20, objectLogSegmentSize))
                {
                    using (var session = store1.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
                    {
                        var bContext = session.BasicContext;
                        for (var key = 0; key < numRecords; key++)
                            _ = bContext.Upsert(new TestObjectKey { key = key }, MakePayload(key, valueSize).AsSpan(), Empty.Default);
                    }
                    var (success, checkpointToken) = await store1.TakeFullCheckpointAsync(checkpointType).ConfigureAwait(false);
                    ClassicAssert.IsTrue(success, "checkpoint failed");
                    token = checkpointToken;
                }

                using (var store2 = CreateOverflowStore(log, objlog, checkpointDir, 1L << 20, objectLogSegmentSize))
                {
                    if (recoveryTargetSize > 0)
                    {
                        var tracker = new LogSizeTracker<LargeObjStoreFunctions, LargeObjAllocator>(store2.Log, recoveryTargetSize, recoveryTargetSize / 8, recoveryTargetSize / 16, logger: null);
                        store2.Log.SetLogSizeTracker(tracker);
                    }
                    _ = await store2.RecoverAsync(default, token).ConfigureAwait(false);
                    using var session = store2.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
                    var bContext = session.BasicContext;
                    for (var key = 0; key < numRecords; key++)
                    {
                        TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Overflow, expectedSpanLength = valueSize };
                        TestLargeObjectOutput output = new();
                        var status = bContext.Read(new TestObjectKey { key = key }, ref input, ref output, Empty.Default);
                        if (status.IsPending)
                        {
                            ClassicAssert.IsTrue(bContext.CompletePendingWithOutputs(out var completed, wait: true));
                            (status, output) = GetSinglePendingResult(completed);
                        }
                        ClassicAssert.IsTrue(status.Found, $"key {key} not found (size {valueSize}, {checkpointType})");
                        VerifyPayload(key, valueSize, output.valueArray);
                    }
                }
            }
            finally
            {
                log.Dispose();
                objlog.Dispose();
            }
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public Task RecoverObjectValueAcrossSizeBoundaries(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [ValueSource(nameof(BoundarySizes))] int valueSize)
            => RunObjectValueRecovery(checkpointType, valueSize, RecordCountForSize(valueSize), NoSplitObjectLogSegmentSize);

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public Task RecoverOverflowValueAcrossSizeBoundaries(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [ValueSource(nameof(BoundarySizes))] int valueSize)
            => RunOverflowValueRecovery(checkpointType, valueSize, RecordCountForSize(valueSize), NoSplitObjectLogSegmentSize);

        // Small-memory recovery: same size sweep, but recover under a tight LogSizeTracker budget so the deferred object load
        // must evict during recovery. For the larger sizes the object heap alone exceeds the budget, forcing the per-page cutoff
        // (FindHeadAddressCutoffOnPage) and snapshot-page flush (FlushSnapshotPageForRecovery) to run and the evicted records'
        // objects to be read back from the main object log they were copied to -- exercising the snapshot-to-main record copy and
        // the hint decode for every exact, length-prefixed, saturated-hint, and directory size under memory pressure, not just tiny
        // fixed-size objects. Includes the 5 MB point, whose record has a directory, so the copy must follow the directory to copy
        // every chunk rather than truncating the value.
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public Task RecoverObjectValueLowMemBoundaries(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [ValueSource(nameof(BoundarySizes))] int valueSize)
            => RunObjectValueRecovery(checkpointType, valueSize, RecordCountForSize(valueSize), NoSplitObjectLogSegmentSize, RecoveryEvictTargetSize);

        // Small-memory recovery counterpart for OVERFLOW (raw byte[]) values across the same size sweep.
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public Task RecoverOverflowValueLowMemBoundaries(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [ValueSource(nameof(BoundarySizes))] int valueSize)
            => RunOverflowValueRecovery(checkpointType, valueSize, RecordCountForSize(valueSize), NoSplitObjectLogSegmentSize, RecoveryEvictTargetSize);

        // Anchor test that GUARANTEES head-advancing eviction during recovery of chunked objects (the sweep above validates
        // correctness under pressure but does not assert that eviction fired). 512 x 64 KB chunked object values give ~32 MB of
        // object heap -- far over the 32 KB recovery budget -- across ~512 main-log records spanning enough pages that recovery
        // evicts whole pages and advances HeadAddress above BeginAddress. Every record must still read back byte-for-byte from
        // the main object-log its snapshot page was copied into during eviction.
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task RecoverChunkedObjLowMemEvicts(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType)
        {
            const int numRecords = 512;
            const int valueSize = 65536;
            var checkpointDir = Path.Combine(MethodTestDir, "checkpoints");
            IDevice log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "evict.log"), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "evict.obj.log"), deleteOnClose: false);
            try
            {
                Guid token;
                using (var store1 = CreateObjectStore(log, objlog, checkpointDir, 1L << 20, NoSplitObjectLogSegmentSize))
                {
                    using (var session = store1.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
                    {
                        var bContext = session.BasicContext;
                        for (var key = 0; key < numRecords; key++)
                            _ = bContext.Upsert(new TestObjectKey { key = key }, new TestLargeObjectValue() { value = MakePayload(key, valueSize) });
                    }
                    var (success, checkpointToken) = await store1.TakeFullCheckpointAsync(checkpointType).ConfigureAwait(false);
                    ClassicAssert.IsTrue(success, "checkpoint failed");
                    token = checkpointToken;
                }

                using (var store2 = CreateObjectStore(log, objlog, checkpointDir, 1L << 20, NoSplitObjectLogSegmentSize))
                {
                    var tracker = new LogSizeTracker<LargeObjStoreFunctions, LargeObjAllocator>(store2.Log, RecoveryEvictTargetSize, RecoveryEvictTargetSize / 8, RecoveryEvictTargetSize / 16, logger: null);
                    store2.Log.SetLogSizeTracker(tracker);
                    _ = await store2.RecoverAsync(default, token).ConfigureAwait(false);

                    ClassicAssert.Greater(store2.Log.HeadAddress, store2.Log.BeginAddress, "expected recovery eviction to advance HeadAddress above BeginAddress");

                    using var session = store2.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
                    var bContext = session.BasicContext;
                    for (var key = 0; key < numRecords; key++)
                    {
                        TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Object };
                        TestLargeObjectOutput output = new();
                        var status = bContext.Read(new TestObjectKey { key = key }, ref input, ref output);
                        if (status.IsPending)
                        {
                            ClassicAssert.IsTrue(bContext.CompletePendingWithOutputs(out var completed, wait: true));
                            (status, output) = GetSinglePendingResult(completed);
                        }
                        ClassicAssert.IsTrue(status.Found, $"key {key} not found ({checkpointType})");
                        VerifyPayload(key, valueSize, output.valueObject?.value);
                    }
                }
            }
            finally
            {
                log.Dispose();
                objlog.Dispose();
            }
        }

        // Explicit segment-crossing coverage: 6 x 5 MB object values in 16 MB object-log segments, so some record's object-log
        // data spans a segment boundary during recovery.
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public Task RecoverObjectValueSpanningObjectLogSegmentBoundary(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType)
            => RunObjectValueRecovery(checkpointType, 5 * 1024 * 1024, numRecords: 6, SplitObjectLogSegmentSize);

        // Same segment-crossing scenario for OVERFLOW values. (Short method name to stay under the Windows MAX_PATH limit for the
        // deep Snapshot checkpoint device path.)
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public Task RecoverOvfValueSpanningObjLogSegment(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType)
            => RunOverflowValueRecovery(checkpointType, 5 * 1024 * 1024, numRecords: 6, SplitObjectLogSegmentSize);

        // ----- Overflow KEY recovery: the checkpoint/recover counterpart of ObjectSizeBoundaryDiskIOTests.ReadOverflowKeyFromDiskAcrossSizeBoundaries. -----

        const int OverflowKeyInlineCutoff = 16;                 // keys longer than this go out of line
        const int OverflowKeyCompanionValueSize = 50;           // small value paired with each overflow key

        // Overflow-key store: SpanByte keys forced out of line (MaxInlineKeySize small). With an overflow value the record has a
        // directory, whose top record the key slot's hint sizes; with an inline value the key is the record's only extent, so the key
        // slot's hint is the key's own size, including the page-count seam.
        static TsavoriteKV<SbKeyStoreFunctions, SbKeyAllocator> CreateOverflowKeyStore(IDevice log, IDevice objlog, string checkpointDir, long memorySize,
                bool inlineValue)
            => new(new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.9,
                PageSize = MinKvLogPageSize,
                LogMemorySize = memorySize,
                SegmentSize = 1L << 20,
                ObjectLogSegmentSize = NoSplitObjectLogSegmentSize,
                MaxInlineKeySize = OverflowKeyInlineCutoff,
                MaxInlineValueSize = inlineValue ? 1 << 10 : 0,
                CheckpointDir = checkpointDir,
            }, StoreFunctions.Create(new SpanByteComparer(), () => new TestObjectValue.Serializer(), DefaultRecordTriggers.Instance),
               (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        // Deterministic key of the given size: first 4 bytes carry the record index (uniqueness + regeneration), the rest is a fill.
        static byte[] MakeKey(int recordIndex, int size)
        {
            var k = new byte[size];
            for (var i = 0; i < size; i++)
                k[i] = (byte)((recordIndex * 7) + i + 1);
            _ = BitConverter.TryWriteBytes(k, recordIndex);
            return k;
        }

        // Write numRecords overflow-key records, checkpoint, recover into a fresh store, and read every record back by its overflow key.
        // A mis-decoded key length/position after recovery fails the on-disk key comparison and the lookup would not find the record.
        // When recoveryTargetSize > 0, the recovery store is put under LogSizeTracker memory pressure (small-memory recovery).
        async Task RunOverflowKeyRecovery(CheckpointType checkpointType, int keySize, int numRecords, bool inlineValue, long recoveryTargetSize = 0)
        {
            var checkpointDir = Path.Combine(MethodTestDir, "checkpoints");
            IDevice log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "ovfkey.log"), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "ovfkey.obj.log"), deleteOnClose: false);
            try
            {
                Guid token;
                using (var store1 = CreateOverflowKeyStore(log, objlog, checkpointDir, 1L << 20, inlineValue))
                {
                    using (var session = store1.NewSession<TestSpanByteKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
                    {
                        var bContext = session.BasicContext;
                        for (var rec = 0; rec < numRecords; rec++)
                            _ = bContext.Upsert(TestSpanByteKey.FromArray(MakeKey(rec, keySize)), MakePayload(rec, OverflowKeyCompanionValueSize).AsSpan(), Empty.Default);
                    }
                    var (success, checkpointToken) = await store1.TakeFullCheckpointAsync(checkpointType).ConfigureAwait(false);
                    ClassicAssert.IsTrue(success, "checkpoint failed");
                    token = checkpointToken;
                }

                using (var store2 = CreateOverflowKeyStore(log, objlog, checkpointDir, 1L << 20, inlineValue))
                {
                    if (recoveryTargetSize > 0)
                    {
                        var tracker = new LogSizeTracker<SbKeyStoreFunctions, SbKeyAllocator>(store2.Log, recoveryTargetSize, recoveryTargetSize / 8, recoveryTargetSize / 16, logger: null);
                        store2.Log.SetLogSizeTracker(tracker);
                    }
                    _ = await store2.RecoverAsync(default, token).ConfigureAwait(false);
                    using var session = store2.NewSession<TestSpanByteKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
                    var bContext = session.BasicContext;
                    for (var rec = 0; rec < numRecords; rec++)
                    {
                        TestLargeObjectInput input = new()
                        {
                            wantValueStyle = inlineValue ? TestValueStyle.Inline : TestValueStyle.Overflow,
                            expectedSpanLength = OverflowKeyCompanionValueSize
                        };
                        TestLargeObjectOutput output = new();
                        var status = bContext.Read(TestSpanByteKey.FromArray(MakeKey(rec, keySize)), ref input, ref output, Empty.Default);
                        if (status.IsPending)
                        {
                            ClassicAssert.IsTrue(bContext.CompletePendingWithOutputs(out var completed, wait: true));
                            (status, output) = GetSinglePendingResult(completed);
                        }
                        ClassicAssert.IsTrue(status.Found, $"record {rec} (keySize {keySize}, {checkpointType}) not found — overflow key failed to round-trip");
                        VerifyPayload(rec, OverflowKeyCompanionValueSize, output.valueArray);
                    }
                }
            }
            finally
            {
                log.Dispose();
                objlog.Dispose();
            }
        }

        // Recovering a record whose KEY is out of line (overflow). During the recovery index-build pass, RecoverFromPage (Recovery.cs)
        // hashes every record's key. An overflow key's bytes live in the object log and are not in the transient objectIdMap during that
        // pass, so the hash is computed by reading the key bytes on demand from the object log (ComputeRecoveryOverflowKeyHash) — the main
        // object log for FoldOver/hybrid-log pages, the snapshot object log for snapshot pages. Inline-key recovery is unaffected.
        //
        // The sizes cross both encoding seams of ComputeOverflowKeySizeHint. 512 bytes is the first size past kOutOfLineExactSizeCutoff,
        // where the objectId hint stops being an exact byte count and becomes a 4 KB page count. The second seam is page count 512, the
        // first count too large for the 9-bit objectId hint, whose high bits then move into the raw RDH KeyLength field. A key's extent
        // adds exactly the 4-byte length prefix, so 510 and 511 pages' worth give extents of exactly that many pages, and 2 MB is above
        // the seam. ObjectIdSlotAndPositionFlagsTests pins the exact transition at the encoder. Only with an inline value is the key the
        // record's only extent, so that its hint is the key's own size.
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public Task RecoverOverflowKey(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [Values(512,
                    (510 * RecordDataHeader.kFlushPageSize) - ObjectLogExtent.LengthPrefixSize,
                    (511 * RecordDataHeader.kFlushPageSize) - ObjectLogExtent.LengthPrefixSize,
                    2 * 1024 * 1024,
                    5 * 1024 * 1024)] int keySize,
            [Values] bool inlineValue)
            => RunOverflowKeyRecovery(checkpointType, keySize, numRecords: RecordCountForSize(keySize), inlineValue);
    }
}