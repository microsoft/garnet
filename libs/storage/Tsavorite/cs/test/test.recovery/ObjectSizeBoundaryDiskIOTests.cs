// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using Garnet.test;
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
    /// Companion to <see cref="ObjectSizeBoundaryRecoveryTests"/> for the NORMAL disk-IO path (no checkpoint/recovery): sweeps
    /// out-of-line VALUE sizes across every object-log serialization boundary for BOTH object and overflow values, flushes and
    /// evicts the whole log so every record's objects live only on disk, then issues a pending <c>Read</c> that must fault each
    /// record's key/value back through the <see cref="ObjectLogReader{TStoreFunctions}"/> and verify it byte-for-byte. Where the
    /// recovery sweep exercises the checkpoint/recover reader path, this exercises the runtime pending-IO reader path over the
    /// same size boundaries (headerless exact-size vs a leading ChunkHeader vs chunked continuation). Black-box by construction,
    /// so it survives a change to the on-disk length encoding.
    /// </summary>
    [TestFixture]
    public class ObjectSizeBoundaryDiskIOTests : TestBase
    {
        // Sizes straddling every object-log encoding boundary (see ObjectSizeBoundaryRecoveryTests for the rationale of each point).
        // 2 MB / 3 MB land in the objectId size-hint sentinel window (extent >= ~2 MB saturates the 9-bit objectId page count to its
        // sentinel; the single-record read-ahead then issues a 4 MB initial read that OVERSHOOTS
        // these sub-4 MB values, including the last record near the object-log tail (short read past the written data).
        static readonly int[] BoundarySizes =
        [
            1, 100, 510, 511, 512, 513, 1023, 1024, 4095, 4096, 65535, 65536,
            131071, 131072, 131073, 262144, 2 * 1024 * 1024, 3 * 1024 * 1024,
            (4 * 1024 * 1024) - 1, 4 * 1024 * 1024, (4 * 1024 * 1024) + 1, 5 * 1024 * 1024
        ];

        // Large object-log segment: no single value spans a segment boundary (isolating pure size-boundary behavior).
        const long NoSplitObjectLogSegmentSize = 1L << 30;      // 1 GB

        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => OnTearDown();

        // Keep total IO bounded: fewer records for the larger sizes.
        static int RecordCountForSize(int size) => size <= 65536 ? 64 : (size <= 262144 ? 16 : 6);

        static TsavoriteKV<LargeObjStoreFunctions, LargeObjAllocator> CreateObjectStore(IDevice log, IDevice objlog, long objectLogSegmentSize)
            => new(new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.9,
                PageSize = MinKvLogPageSize,
                LogMemorySize = 1L << 20,
                SegmentSize = 1L << 20,
                ObjectLogSegmentSize = objectLogSegmentSize,
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer(), DefaultRecordTriggers.Instance),
               (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        static TsavoriteKV<LargeObjStoreFunctions, LargeObjAllocator> CreateOverflowStore(IDevice log, IDevice objlog, long objectLogSegmentSize)
            => new(new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.9,
                PageSize = MinKvLogPageSize,
                LogMemorySize = 1L << 20,
                SegmentSize = 1L << 20,
                ObjectLogSegmentSize = objectLogSegmentSize,
                MaxInlineValueSize = 0,             // store raw byte[] values as overflow (not objects)
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
            // Spot-check first, middle, and last bytes (a full compare is unnecessary given the deterministic fill).
            ClassicAssert.AreEqual((byte)((key * 31) + 0), actual[0], $"key {key} byte[0] mismatch");
            var mid = size / 2;
            ClassicAssert.AreEqual((byte)((key * 31) + mid), actual[mid], $"key {key} byte[{mid}] mismatch");
            ClassicAssert.AreEqual((byte)((key * 31) + (size - 1)), actual[size - 1], $"key {key} byte[{size - 1}] mismatch");
        }

        [Test]
        [Category("TsavoriteKV"), Category("ObjectIdMap")]
        public void ReadObjectValueFromDiskAcrossSizeBoundaries([ValueSource(nameof(BoundarySizes))] int valueSize)
        {
            var numRecords = RecordCountForSize(valueSize);
            IDevice log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "obj.log"), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "obj.obj.log"), deleteOnClose: false);
            try
            {
                using var store = CreateObjectStore(log, objlog, NoSplitObjectLogSegmentSize);
                using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
                var bContext = session.BasicContext;

                for (var key = 0; key < numRecords; key++)
                    _ = bContext.Upsert(new TestObjectKey { key = key }, new TestLargeObjectValue() { value = MakePayload(key, valueSize) });

                // Force every record's objects onto disk so the read below must fault them back through the object-log reader.
                store.Log.FlushAndEvict(wait: true);

                for (var key = 0; key < numRecords; key++)
                {
                    TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Object };
                    TestLargeObjectOutput output = new();
                    var status = bContext.Read(new TestObjectKey { key = key }, ref input, ref output);
                    ClassicAssert.IsTrue(status.IsPending, $"key {key} (size {valueSize}) expected to fault from disk");
                    (status, output) = bContext.GetSinglePendingResult();
                    ClassicAssert.IsTrue(status.Found, $"key {key} not found (size {valueSize})");
                    VerifyPayload(key, valueSize, output.valueObject?.value);
                }
            }
            finally
            {
                log.Dispose();
                objlog.Dispose();
            }
        }

        [Test]
        [Category("TsavoriteKV"), Category("ObjectIdMap")]
        public void ReadOverflowValueFromDiskAcrossSizeBoundaries([ValueSource(nameof(BoundarySizes))] int valueSize)
        {
            var numRecords = RecordCountForSize(valueSize);
            IDevice log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "ovf.log"), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "ovf.obj.log"), deleteOnClose: false);
            try
            {
                using var store = CreateOverflowStore(log, objlog, NoSplitObjectLogSegmentSize);
                using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
                var bContext = session.BasicContext;

                for (var key = 0; key < numRecords; key++)
                    _ = bContext.Upsert(new TestObjectKey { key = key }, MakePayload(key, valueSize).AsSpan(), Empty.Default);

                store.Log.FlushAndEvict(wait: true);

                for (var key = 0; key < numRecords; key++)
                {
                    TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Overflow, expectedSpanLength = valueSize };
                    TestLargeObjectOutput output = new();
                    var status = bContext.Read(new TestObjectKey { key = key }, ref input, ref output, Empty.Default);
                    ClassicAssert.IsTrue(status.IsPending, $"key {key} (size {valueSize}) expected to fault from disk");
                    (status, output) = bContext.GetSinglePendingResult();
                    ClassicAssert.IsTrue(status.Found, $"key {key} not found (size {valueSize})");
                    VerifyPayload(key, valueSize, output.valueArray);
                }
            }
            finally
            {
                log.Dispose();
                objlog.Dispose();
            }
        }

        // Overflow KEY sizes across the same encoding boundaries. All are well above the small MaxInlineKeySize below, so every key
        // is forced out of line into the object log. (The default inline-key cutoff is 1022, so a dedicated small cutoff is required
        // to exercise the sub-1 KB boundaries as overflow keys.)
        static readonly int[] KeyBoundarySizes =
        [
            17, 100, 510, 511, 512, 513, 1023, 1024, 4095, 4096, 65535, 65536, 131071, 131072, 131073,
            (2 * 1024 * 1024) - 1, 2 * 1024 * 1024, (2 * 1024 * 1024) + 1, 5 * 1024 * 1024
        ];

        const int OverflowKeyInlineCutoff = 16;                 // keys longer than this go out of line
        const int OverflowKeyCompanionValueSize = 50;           // small headerless overflow value paired with each overflow key

        static TsavoriteKV<SbKeyStoreFunctions, SbKeyAllocator> CreateOverflowKeyStore(IDevice log, IDevice objlog, long objectLogSegmentSize)
            => new(new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.9,
                PageSize = MinKvLogPageSize,
                LogMemorySize = 1L << 20,
                SegmentSize = 1L << 20,
                ObjectLogSegmentSize = objectLogSegmentSize,
                MaxInlineKeySize = OverflowKeyInlineCutoff,     // force keys out of line
                MaxInlineValueSize = 0,                         // pair with an overflow (raw byte[]) value
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

        [Test]
        [Category("TsavoriteKV"), Category("ObjectIdMap")]
        public void ReadOverflowKeyFromDiskAcrossSizeBoundaries([ValueSource(nameof(KeyBoundarySizes))] int keySize)
        {
            var numRecords = RecordCountForSize(keySize);
            IDevice log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "ovfkey.log"), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "ovfkey.obj.log"), deleteOnClose: false);
            try
            {
                using var store = CreateOverflowKeyStore(log, objlog, NoSplitObjectLogSegmentSize);
                using var session = store.NewSession<TestSpanByteKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
                var bContext = session.BasicContext;

                for (var rec = 0; rec < numRecords; rec++)
                    _ = bContext.Upsert(TestSpanByteKey.FromArray(MakeKey(rec, keySize)), MakePayload(rec, OverflowKeyCompanionValueSize).AsSpan(), Empty.Default);

                // Evict so both the overflow key and its value must fault back from the object log; a mis-decoded key length/position
                // would fail the on-disk key comparison and the lookup would not find the record.
                store.Log.FlushAndEvict(wait: true);

                for (var rec = 0; rec < numRecords; rec++)
                {
                    TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Overflow, expectedSpanLength = OverflowKeyCompanionValueSize };
                    TestLargeObjectOutput output = new();
                    var status = bContext.Read(TestSpanByteKey.FromArray(MakeKey(rec, keySize)), ref input, ref output, Empty.Default);
                    ClassicAssert.IsTrue(status.IsPending, $"record {rec} (keySize {keySize}) expected to fault from disk");
                    (status, output) = bContext.GetSinglePendingResult();
                    ClassicAssert.IsTrue(status.Found, $"record {rec} (keySize {keySize}) not found — overflow key failed to round-trip");
                    VerifyPayload(rec, OverflowKeyCompanionValueSize, output.valueArray);
                }
            }
            finally
            {
                log.Dispose();
                objlog.Dispose();
            }
        }
    }
}