// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery
{
    using ObjAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ObjStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;
    using SbKeyAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordTriggers>>;
    using SbKeyStoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordTriggers>;

    /// <summary>
    /// Backward-compatibility tests that recover a downlevel "v7" (checkpoint version 7) object-store checkpoint with the current
    /// reader and verify every value byte-for-byte. The only released downlevel format is v7, whose object log uses the dense
    /// split-length encoding decoded by <c>LogRecord.GetObjectLogRecordStartPositionAndLengths_v21</c>; recovery selects that decode
    /// from the checkpoint metadata version (not a per-record position-word flag).
    ///
    /// There is deliberately NO v7 page-generation code in production. This fixture synthesizes a v7 checkpoint as a TEST UTILITY: it
    /// takes a real current-format FoldOver checkpoint of small (headerless) records — whose object-log bytes are already dense and
    /// therefore byte-identical to the v7 dense encoding — and then rewrites, on disk, only what distinguishes v7 from the current
    /// format:
    ///   * each non-inline record's length encoding is re-stamped into the v7 split form (RDH low bits + objectId-slot high bits) and
    ///     its ObjectLogPosition word gets the ReuseObjectIdForSize flag (bit 63) with the current size-hint flags cleared, and
    ///   * the checkpoint metadata is re-serialized through <see cref="HybridLogRecoveryInfo.ToByteArray(int)"/> at target version 7.
    /// The object-log bytes and record positions are left untouched, so no v7 object-log writer is needed.
    /// </summary>
    [TestFixture]
    public class V7DownlevelRecoveryTests : TestBase
    {
        const string MainLogName = "v7main.log";
        const string ObjectLogName = "v7main.obj.log";
        const string CheckpointDirName = "checkpoints";

        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => OnTearDown();

        static TsavoriteKV<ObjStoreFunctions, ObjAllocator> CreateObjectStore(IDevice log, IDevice objlog, string checkpointDir, long memorySize)
            => new(new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.9,
                PageSize = MinKvLogPageSize,
                LogMemorySize = memorySize,
                SegmentSize = 1L << 20,
                ObjectLogSegmentSize = 1L << 22,
                CheckpointDir = checkpointDir,
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer(), DefaultRecordTriggers.Instance),
               (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        const int OverflowKeyInlineCutoff = 16;     // keys longer than this go out of line (into the object log)

        // Overflow-key store: SpanByte keys forced out of line (small MaxInlineKeySize), with small overflow (byte[]) values.
        static TsavoriteKV<SbKeyStoreFunctions, SbKeyAllocator> CreateOverflowKeyStore(IDevice log, IDevice objlog, string checkpointDir, long memorySize)
            => new(new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.9,
                PageSize = MinKvLogPageSize,
                LogMemorySize = memorySize,
                SegmentSize = 1L << 20,
                ObjectLogSegmentSize = 1L << 22,
                MaxInlineKeySize = OverflowKeyInlineCutoff,
                MaxInlineValueSize = 0,             // store raw byte[] values as overflow (not objects)
                CheckpointDir = checkpointDir,
            }, StoreFunctions.Create(new SpanByteComparer(), () => new TestLargeObjectValue.Serializer(), DefaultRecordTriggers.Instance),
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
            for (var i = 0; i < size; i++)
                ClassicAssert.AreEqual((byte)((key * 31) + i), actual[i], $"key {key} byte[{i}] mismatch");
        }

        // Build a current-format FoldOver checkpoint of numRecords small object-value records, then transform it on disk into a v7
        // (checkpoint version 7) checkpoint. Returns the checkpoint token.
        Guid BuildV7FoldOverFixture(int numRecords, int valueSize)
        {
            var checkpointDir = Path.Combine(MethodTestDir, CheckpointDirName);
            Guid token;

            IDevice log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, MainLogName), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, ObjectLogName), deleteOnClose: false);
            try
            {
                using var store = CreateObjectStore(log, objlog, checkpointDir, 1L << 20);
                using (var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var key = 0; key < numRecords; key++)
                        _ = bContext.Upsert(new TestObjectKey { key = key }, new TestLargeObjectValue() { value = MakePayload(key, valueSize) });
                }

                // Log-only FoldOver checkpoint: no index checkpoint, so recovery rebuilds the index from the (transformed) log.
                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.FoldOver), "failed to initiate FoldOver checkpoint");
                store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            }
            finally
            {
                log.Dispose();
                objlog.Dispose();
            }

            TransformCheckpointToV7(checkpointDir, MethodTestDir, token);
            return token;
        }

        // Build a current-format FoldOver checkpoint of numRecords overflow-key + overflow-value records, then transform it on disk into
        // a v7 checkpoint. Keys and values are small (headerless) so the object-log bytes are already dense.
        Guid BuildV7OverflowKeyFoldOverFixture(int numRecords, int keySize, int valueSize)
        {
            var checkpointDir = Path.Combine(MethodTestDir, CheckpointDirName);
            Guid token;

            IDevice log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, MainLogName), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, ObjectLogName), deleteOnClose: false);
            try
            {
                using var store = CreateOverflowKeyStore(log, objlog, checkpointDir, 1L << 20);
                using (var session = store.NewSession<TestSpanByteKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var rec = 0; rec < numRecords; rec++)
                        _ = bContext.Upsert(TestSpanByteKey.FromArray(MakeKey(rec, keySize)), MakePayload(rec, valueSize).AsSpan(), Empty.Default);
                }

                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.FoldOver), "failed to initiate FoldOver checkpoint");
                store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            }
            finally
            {
                log.Dispose();
                objlog.Dispose();
            }

            TransformCheckpointToV7(checkpointDir, MethodTestDir, token);
            return token;
        }

        // Inline-key + overflow-value store (raw byte[] values forced out of line into the object log).
        static TsavoriteKV<ObjStoreFunctions, ObjAllocator> CreateOverflowValueStore(IDevice log, IDevice objlog, string checkpointDir, long memorySize)
            => new(new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.9,
                PageSize = MinKvLogPageSize,
                LogMemorySize = memorySize,
                SegmentSize = 1L << 20,
                ObjectLogSegmentSize = 1L << 22,
                MaxInlineValueSize = 0,
                CheckpointDir = checkpointDir,
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer(), DefaultRecordTriggers.Instance),
               (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        // Overflow-key + inline-value store: SpanByte keys forced out of line, values small enough to stay inline in the record.
        static TsavoriteKV<SbKeyStoreFunctions, SbKeyAllocator> CreateOverflowKeyInlineValueStore(IDevice log, IDevice objlog, string checkpointDir, long memorySize)
            => new(new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.9,
                PageSize = MinKvLogPageSize,
                LogMemorySize = memorySize,
                SegmentSize = 1L << 20,
                ObjectLogSegmentSize = 1L << 22,
                MaxInlineKeySize = OverflowKeyInlineCutoff,
                MaxInlineValueSize = 256,
                CheckpointDir = checkpointDir,
            }, StoreFunctions.Create(new SpanByteComparer(), () => new TestLargeObjectValue.Serializer(), DefaultRecordTriggers.Instance),
               (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        // Build a FoldOver checkpoint of overflow-key + inline-value records, transformed to v7 (keeps the current object log; the
        // headerless overflow-key bytes are already dense). The value is inline, so only the key is out of line.
        Guid BuildV7OverflowKeyInlineValueFixture(int numRecords, int keySize, int valueSize)
        {
            var checkpointDir = Path.Combine(MethodTestDir, CheckpointDirName);
            Guid token;

            IDevice log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, MainLogName), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, ObjectLogName), deleteOnClose: false);
            try
            {
                using var store = CreateOverflowKeyInlineValueStore(log, objlog, checkpointDir, 1L << 20);
                using (var session = store.NewSession<TestSpanByteKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var rec = 0; rec < numRecords; rec++)
                        _ = bContext.Upsert(TestSpanByteKey.FromArray(MakeKey(rec, keySize)), MakePayload(rec, valueSize).AsSpan(), Empty.Default);
                }
                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.FoldOver), "failed to initiate FoldOver checkpoint");
                store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            }
            finally
            {
                log.Dispose();
                objlog.Dispose();
            }

            TransformCheckpointToV7(checkpointDir, MethodTestDir, token);
            return token;
        }

        // Build a FoldOver checkpoint of object-value records, transformed to a DENSE v7 object log so LARGE values (headered in the
        // current format, dense in v7) are exercised.
        Guid BuildV7DenseObjectValueFixture(int numRecords, int valueSize)
        {
            var checkpointDir = Path.Combine(MethodTestDir, CheckpointDirName);
            Guid token;

            IDevice log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, MainLogName), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, ObjectLogName), deleteOnClose: false);
            try
            {
                using var store = CreateObjectStore(log, objlog, checkpointDir, 1L << 20);
                using (var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var key = 0; key < numRecords; key++)
                        _ = bContext.Upsert(new TestObjectKey { key = key }, new TestLargeObjectValue() { value = MakePayload(key, valueSize) });
                }
                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.FoldOver), "failed to initiate FoldOver checkpoint");
                store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            }
            finally
            {
                log.Dispose();
                objlog.Dispose();
            }

            // Record i (page order) is key i; its dense object-log bytes are the serialized object value.
            TransformCheckpointToV7Dense(checkpointDir, MethodTestDir, token, i => (null, SerializeLargeObject(MakePayload(i, valueSize))));
            return token;
        }

        // Build a FoldOver checkpoint of inline-key + overflow-value records, transformed to a DENSE v7 object log so LARGE overflow
        // values are exercised.
        Guid BuildV7DenseOverflowValueFixture(int numRecords, int valueSize)
        {
            var checkpointDir = Path.Combine(MethodTestDir, CheckpointDirName);
            Guid token;

            IDevice log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, MainLogName), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, ObjectLogName), deleteOnClose: false);
            try
            {
                using var store = CreateOverflowValueStore(log, objlog, checkpointDir, 1L << 20);
                using (var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var key = 0; key < numRecords; key++)
                        _ = bContext.Upsert(new TestObjectKey { key = key }, MakePayload(key, valueSize).AsSpan(), Empty.Default);
                }
                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.FoldOver), "failed to initiate FoldOver checkpoint");
                store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            }
            finally
            {
                log.Dispose();
                objlog.Dispose();
            }

            TransformCheckpointToV7Dense(checkpointDir, MethodTestDir, token, i => (null, MakePayload(i, valueSize)));
            return token;
        }

        // Rewrite the checkpoint's metadata and main-log records in place so the checkpoint reads as version 7 (keeps the current
        // object log; only valid for headerless records whose current object bytes are already dense).
        static unsafe void TransformCheckpointToV7(string checkpointDir, string logDir, Guid token)
        {
            var namingScheme = new DefaultCheckpointNamingScheme(new DirectoryInfo(checkpointDir).FullName);
            using var checkpointManager = new DeviceLogCommitCheckpointManager(new LocalStorageNamedDeviceFactoryCreator(), namingScheme, removeOutdated: false);

            var info = new HybridLogRecoveryInfo();
            info.Recover(token, checkpointManager);
            ClassicAssert.AreEqual(HybridLogRecoveryInfo.CheckpointVersion, info.hybridLogRecoveryVersion, "expected a current-format checkpoint before transform");
            ClassicAssert.AreEqual(0, info.useSnapshotFile, "expected a FoldOver checkpoint");

            var beginAddress = info.beginAddress;
            var tailAddress = info.recoveredTailAddress;
            ClassicAssert.LessOrEqual(tailAddress, (long)MinKvLogPageSize, "v7 fixture must fit on a single main-log page (single-page transform)");

            // Rewrite the main-log page records (all on page 0 for this small fixture) into the v7 split-length encoding.
            var mainLogSegment = FindLogSegmentZero(logDir, MainLogName);
            var pageBytes = File.ReadAllBytes(mainLogSegment);
            fixed (byte* pagePtr = pageBytes)
            {
                var pageBase = (long)pagePtr;   // logical address == file offset on page 0
                var offset = beginAddress;
                while (offset < tailAddress)
                {
                    var logRecord = new LogRecord(pageBase + offset);
                    if (logRecord.Info.IsNull)
                    {
                        offset += RecordInfo.Size;
                        continue;
                    }
                    var allocatedSize = logRecord.AllocatedSize;
                    if (logRecord.Info.Valid && !logRecord.DataHeader.RecordIsInline)
                        StampRecordAsV7(pageBase + offset);
                    offset += allocatedSize;
                }
            }
            File.WriteAllBytes(mainLogSegment, pageBytes);

            // Re-serialize the metadata at target version 7 and overwrite info.dat. The object-log tail and record positions are
            // unchanged, so the metadata's object-log tail fields remain valid; ToByteArray(7) emits the v7 layout and checksum.
            checkpointManager.CommitLogCheckpointMetadata(token, info.ToByteArray(targetVersion: 7));
        }

        // Convert a single record from the current objectId-hint encoding to the v7 split-length encoding. The object-log bytes and the
        // record's object-log position (segment+offset) are left untouched; the current headerless bytes are byte-identical to v7 dense.
        static unsafe void StampRecordAsV7(long physicalAddress)
        {
            var logRecord = new LogRecord(physicalAddress);
            var dataHeader = logRecord.DataHeader;

            // Field addresses use the physical (objectId-slot) field lengths, which are independent of the raw length bits, so they are
            // stable across the stamp. Read the current exact lengths and the position word BEFORE mutating the length fields.
            var (_, keyAddress) = dataHeader.GetKeyFieldInfo(physicalAddress);
            var (_, valueAddress) = dataHeader.GetValueFieldInfo(physicalAddress);
            var objectLogPositionPtr = (ulong*)logRecord.GetObjectLogPositionAddress(logRecord.GetOptionalStartAddress());
            var positionWord = *objectLogPositionPtr;
            _ = logRecord.GetObjectLogRecordStartPositionAndLengths(out var keyLength, out var valueLength, HybridLogRecoveryInfo.CheckpointVersion);

            if (dataHeader.KeyIsOverflow)
            {
                *(int*)keyAddress = (int)((uint)keyLength >> RecordDataHeader.kKeyLengthBits);
                dataHeader.KeyLength = keyLength & (int)RecordDataHeader.kKeyLengthLowBitsMask;
            }
            if (!dataHeader.ValueIsInline)
            {
                *(int*)valueAddress = (int)(valueLength >> RecordDataHeader.kValueLengthBits);
                dataHeader.ValueLength = (int)(valueLength & RecordDataHeader.kValueLengthLowBitsMask);
            }
            logRecord.SetDataHeader(dataHeader);

            // Keep the segment+offset; set the ReuseObjectIdForSize flag (bit 63); clear the current size-hint flags (bits 60-62).
            *objectLogPositionPtr = (positionWord & ObjectLogFilePositionInfo.SegmentAndOffsetMask) | ObjectLogFilePositionInfo.kReuseObjectIdForSizeMask;
        }

        static string FindLogSegmentZero(string logDir, string logName)
        {
            var candidates = Directory.GetFiles(logDir, logName + ".*")
                .Where(f => !Path.GetFileName(f).StartsWith(logName + ".obj.", StringComparison.Ordinal))
                .OrderBy(f => f, StringComparer.Ordinal)
                .ToArray();
            ClassicAssert.IsNotEmpty(candidates, $"no main-log segment file for {logName} in {logDir}");
            return candidates[0];
        }

        // The dense v7 object log starts at a nonzero offset so no record's object-log position word is 0 (a zero word reads as "unset").
        const long DenseObjectLogStart = 512;

        // General v7 transform that WRITES its own dense (headerless) object log, so records of ANY size can be synthesized in the v7
        // encoding (unlike TransformCheckpointToV7, which keeps the current object log and therefore only works for headerless records
        // whose current bytes are already dense). getRecordBytes(i) supplies the raw object-log bytes for the i-th out-of-line record in
        // page order: the overflow key bytes (or null for an inline key) and the value bytes — the raw overflow byte[] for an overflow
        // value, or the serialized-object bytes for an object value (or null for an inline value).
        static unsafe void TransformCheckpointToV7Dense(string checkpointDir, string logDir, Guid token, Func<int, (byte[] keyBytes, byte[] valueBytes)> getRecordBytes)
        {
            var namingScheme = new DefaultCheckpointNamingScheme(new DirectoryInfo(checkpointDir).FullName);
            using var checkpointManager = new DeviceLogCommitCheckpointManager(new LocalStorageNamedDeviceFactoryCreator(), namingScheme, removeOutdated: false);

            var info = new HybridLogRecoveryInfo();
            info.Recover(token, checkpointManager);
            ClassicAssert.AreEqual(HybridLogRecoveryInfo.CheckpointVersion, info.hybridLogRecoveryVersion, "expected a current-format checkpoint before transform");
            ClassicAssert.AreEqual(0, info.useSnapshotFile, "expected a FoldOver checkpoint");
            var beginAddress = info.beginAddress;
            var tailAddress = info.recoveredTailAddress;
            ClassicAssert.LessOrEqual(tailAddress, (long)MinKvLogPageSize, "v7 fixture must fit on a single main-log page (single-page transform)");
            var segmentSizeBits = info.hlogEndObjectLogTail.SegmentSizeBits;

            var mainLogSegment = FindLogSegmentZero(logDir, MainLogName);
            var pageBytes = File.ReadAllBytes(mainLogSegment);

            using var denseObjectLog = new MemoryStream();
            denseObjectLog.Write(new byte[DenseObjectLogStart], 0, (int)DenseObjectLogStart);

            fixed (byte* pagePtr = pageBytes)
            {
                var pageBase = (long)pagePtr;
                var offset = beginAddress;
                var recordIndex = 0;
                while (offset < tailAddress)
                {
                    var logRecord = new LogRecord(pageBase + offset);
                    if (logRecord.Info.IsNull)
                    {
                        offset += RecordInfo.Size;
                        continue;
                    }
                    var allocatedSize = logRecord.AllocatedSize;
                    if (logRecord.Info.Valid && !logRecord.DataHeader.RecordIsInline)
                    {
                        var dataHeader = logRecord.DataHeader;
                        var (keyBytes, valueBytes) = getRecordBytes(recordIndex);
                        var denseStart = denseObjectLog.Position;

                        var keyLen = 0;
                        if (dataHeader.KeyIsOverflow)
                        {
                            ClassicAssert.IsNotNull(keyBytes, $"record {recordIndex} has an overflow key but no key bytes were supplied");
                            denseObjectLog.Write(keyBytes, 0, keyBytes.Length);
                            keyLen = keyBytes.Length;
                        }
                        var valLen = 0;
                        if (!dataHeader.ValueIsInline)
                        {
                            ClassicAssert.IsNotNull(valueBytes, $"record {recordIndex} has a non-inline value but no value bytes were supplied");
                            denseObjectLog.Write(valueBytes, 0, valueBytes.Length);
                            valLen = valueBytes.Length;
                        }

                        StampRecordDenseV7(pageBase + offset, (ulong)denseStart, keyLen, valLen);
                        if (recordIndex == 0)
                            ((PageHeader*)pagePtr)->objectLogLowestPositionWord = (ulong)denseStart;
                        ++recordIndex;
                    }
                    offset += allocatedSize;
                }
            }
            File.WriteAllBytes(mainLogSegment, pageBytes);

            // Write the dense object log, zero-padded up to a device sector so a sector-aligned read of the last record does not hit EOF.
            var objectLogSegment = FindLogSegmentZero(logDir, ObjectLogName);
            var denseEnd = denseObjectLog.Position;
            var denseBytes = denseObjectLog.ToArray();
            const int sector = 4096;
            var paddedLength = (int)(((denseBytes.Length + sector - 1) / sector) * sector);
            if (paddedLength != denseBytes.Length)
                Array.Resize(ref denseBytes, paddedLength);
            File.WriteAllBytes(objectLogSegment, denseBytes);

            // Point the object-log tail fields at the dense end and re-serialize the metadata at target version 7.
            info.hlogEndObjectLogTail = new ObjectLogFilePositionInfo((ulong)denseEnd, segmentSizeBits);
            info.snapshotStartObjectLogTail = info.hlogEndObjectLogTail;
            info.snapshotEndObjectLogTail = info.hlogEndObjectLogTail;
            info.beginAddressObjectLogSegment = 0;
            checkpointManager.CommitLogCheckpointMetadata(token, info.ToByteArray(targetVersion: 7));
        }

        // Stamp a record into the v7 split-length encoding pointing at a dense object-log position (segment 0, offset denseOffset).
        static unsafe void StampRecordDenseV7(long physicalAddress, ulong denseOffset, int keyLen, int valLen)
        {
            var logRecord = new LogRecord(physicalAddress);
            var dataHeader = logRecord.DataHeader;
            var (_, keyAddress) = dataHeader.GetKeyFieldInfo(physicalAddress);
            var (_, valueAddress) = dataHeader.GetValueFieldInfo(physicalAddress);
            var objectLogPositionPtr = (ulong*)logRecord.GetObjectLogPositionAddress(logRecord.GetOptionalStartAddress());

            if (dataHeader.KeyIsOverflow)
            {
                *(int*)keyAddress = (int)((uint)keyLen >> RecordDataHeader.kKeyLengthBits);
                dataHeader.KeyLength = keyLen & (int)RecordDataHeader.kKeyLengthLowBitsMask;
            }
            if (!dataHeader.ValueIsInline)
            {
                *(int*)valueAddress = (int)((uint)valLen >> RecordDataHeader.kValueLengthBits);
                dataHeader.ValueLength = (int)((uint)valLen & (uint)RecordDataHeader.kValueLengthLowBitsMask);
            }
            logRecord.SetDataHeader(dataHeader);

            *objectLogPositionPtr = (denseOffset & ObjectLogFilePositionInfo.SegmentAndOffsetMask) | ObjectLogFilePositionInfo.kReuseObjectIdForSizeMask;
        }

        // The bytes TestLargeObjectValue.Serializer writes: a 4-byte little-endian length followed by the payload.
        static byte[] SerializeLargeObject(byte[] payload)
        {
            var bytes = new byte[sizeof(int) + payload.Length];
            BitConverter.TryWriteBytes(bytes, payload.Length);
            payload.CopyTo(bytes, sizeof(int));
            return bytes;
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore"), Category("Smoke")]
        public async Task RecoverV7ObjectValueFoldOver([Values(8, 32)] int numRecords, [Values(1, 100, 507)] int valueSize)
        {
            var token = BuildV7FoldOverFixture(numRecords, valueSize);

            var checkpointDir = Path.Combine(MethodTestDir, CheckpointDirName);
            IDevice log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, MainLogName), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, ObjectLogName), deleteOnClose: false);
            try
            {
                using var store = CreateObjectStore(log, objlog, checkpointDir, 1L << 20);
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);

                using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
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
                    ClassicAssert.IsTrue(status.Found, $"key {key} not found after v7 recovery");
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
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task RecoverV7LargeObjectValueFoldOver([Values(513, 1024, 4096, 20000)] int valueSize)
        {
            // valueSize > 507 makes the serialized object exceed the 511-byte exact-size cutoff, so the current format would frame it with a
            // leading ChunkHeader. v7 stored it dense; the dense fixture writer reproduces that. Verifies the current reader decodes a large
            // v7 (headerless/dense) object value.
            const int numRecords = 6;
            var token = BuildV7DenseObjectValueFixture(numRecords, valueSize);

            var checkpointDir = Path.Combine(MethodTestDir, CheckpointDirName);
            IDevice log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, MainLogName), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, ObjectLogName), deleteOnClose: false);
            try
            {
                using var store = CreateObjectStore(log, objlog, checkpointDir, 1L << 20);
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);

                using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
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
                    ClassicAssert.IsTrue(status.Found, $"key {key} not found after large v7 recovery (size {valueSize})");
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
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task RecoverV7LargeOverflowValueFoldOver([Values(512, 1024, 4096, 20000)] int valueSize)
        {
            const int numRecords = 6;
            var token = BuildV7DenseOverflowValueFixture(numRecords, valueSize);

            var checkpointDir = Path.Combine(MethodTestDir, CheckpointDirName);
            IDevice log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, MainLogName), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, ObjectLogName), deleteOnClose: false);
            try
            {
                using var store = CreateOverflowValueStore(log, objlog, checkpointDir, 1L << 20);
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);

                using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
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
                    ClassicAssert.IsTrue(status.Found, $"key {key} not found after large v7 overflow-value recovery (size {valueSize})");
                    VerifyPayload(key, valueSize, output.valueArray);
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
        public async Task RecoverV7ObjectValueFoldOverLowMem()
        {
            // Recover a v7 checkpoint into a store under a tight memory budget so recovery evicts records to the main log. A record evicted
            // during a downlevel recovery is NOT up-converted, so it stays on the main log in the v7 encoding; the later runtime read of it
            // must still decode correctly (the per-record ReuseObjectIdForSize flag selects the v7 decode even though the store is now current).
            const int numRecords = 40;
            const int valueSize = 400;
            var token = BuildV7FoldOverFixture(numRecords, valueSize);

            var checkpointDir = Path.Combine(MethodTestDir, CheckpointDirName);
            IDevice log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, MainLogName), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, ObjectLogName), deleteOnClose: false);
            try
            {
                using var store = CreateObjectStore(log, objlog, checkpointDir, 1L << 20);
                var target = 4L * MinKvLogPageSize;
                var tracker = new LogSizeTracker<ObjStoreFunctions, ObjAllocator>(store.Log, target, target / 8, target / 16, logger: null);
                store.Log.SetLogSizeTracker(tracker);
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);

                using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
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
                    ClassicAssert.IsTrue(status.Found, $"key {key} not found after low-mem v7 recovery");
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
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task RecoverV7OverflowKeyFoldOver([Values(8, 16)] int numRecords, [Values(50, 300)] int keySize)
        {
            const int valueSize = 50;
            var token = BuildV7OverflowKeyFoldOverFixture(numRecords, keySize, valueSize);

            var checkpointDir = Path.Combine(MethodTestDir, CheckpointDirName);
            IDevice log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, MainLogName), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, ObjectLogName), deleteOnClose: false);
            try
            {
                using var store = CreateOverflowKeyStore(log, objlog, checkpointDir, 1L << 20);
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);

                using var session = store.NewSession<TestSpanByteKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
                var bContext = session.BasicContext;
                for (var rec = 0; rec < numRecords; rec++)
                {
                    // Reading by the overflow key proves its bytes round-tripped: the Pass-1 index build hashes the key by reading it
                    // from the object log via the v7 decode, so a mis-decoded key length/position would fail this lookup.
                    TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Overflow, expectedSpanLength = valueSize };
                    TestLargeObjectOutput output = new();
                    var status = bContext.Read(TestSpanByteKey.FromArray(MakeKey(rec, keySize)), ref input, ref output, Empty.Default);
                    if (status.IsPending)
                    {
                        ClassicAssert.IsTrue(bContext.CompletePendingWithOutputs(out var completed, wait: true));
                        (status, output) = GetSinglePendingResult(completed);
                    }
                    ClassicAssert.IsTrue(status.Found, $"record {rec} (keySize {keySize}) not found — overflow key failed to round-trip through v7 recovery");
                    VerifyPayload(rec, valueSize, output.valueArray);
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
        public async Task RecoverV7OverflowKeyInlineValueFoldOver([Values(8, 16)] int numRecords, [Values(50, 300)] int keySize)
        {
            const int valueSize = 40;
            var token = BuildV7OverflowKeyInlineValueFixture(numRecords, keySize, valueSize);

            var checkpointDir = Path.Combine(MethodTestDir, CheckpointDirName);
            IDevice log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, MainLogName), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, ObjectLogName), deleteOnClose: false);
            try
            {
                using var store = CreateOverflowKeyInlineValueStore(log, objlog, checkpointDir, 1L << 20);
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);

                using var session = store.NewSession<TestSpanByteKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
                var bContext = session.BasicContext;
                for (var rec = 0; rec < numRecords; rec++)
                {
                    TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Inline, expectedSpanLength = valueSize };
                    TestLargeObjectOutput output = new();
                    var status = bContext.Read(TestSpanByteKey.FromArray(MakeKey(rec, keySize)), ref input, ref output, Empty.Default);
                    if (status.IsPending)
                    {
                        ClassicAssert.IsTrue(bContext.CompletePendingWithOutputs(out var completed, wait: true));
                        (status, output) = GetSinglePendingResult(completed);
                    }
                    ClassicAssert.IsTrue(status.Found, $"record {rec} (keySize {keySize}) not found — overflow key + inline value failed to round-trip through v7 recovery");
                    VerifyPayload(rec, valueSize, output.valueArray);
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
        public unsafe void V7FixtureHasDownlevelEncodingOnDisk()
        {
            const int numRecords = 8;
            const int valueSize = 100;
            var token = BuildV7FoldOverFixture(numRecords, valueSize);

            var namingScheme = new DefaultCheckpointNamingScheme(new DirectoryInfo(Path.Combine(MethodTestDir, CheckpointDirName)).FullName);
            using var checkpointManager = new DeviceLogCommitCheckpointManager(new LocalStorageNamedDeviceFactoryCreator(), namingScheme, removeOutdated: false);

            // Metadata line 1 is the checkpoint version. Read the raw metadata bytes (GetLogCheckpointMetadata only strips the 4-byte
            // length prefix) and parse the first line ourselves, without invoking the production metadata decoder.
            var metadata = checkpointManager.GetLogCheckpointMetadata(token);
            using (var reader = new StreamReader(new MemoryStream(metadata)))
                ClassicAssert.AreEqual("7", reader.ReadLine(), "metadata checkpoint version should be 7");

            var info = new HybridLogRecoveryInfo();
            info.Recover(token, checkpointManager);
            ClassicAssert.AreEqual(7, info.hybridLogRecoveryVersion, "recovered metadata version should be 7");

            // Every out-of-line record on the main-log page must have the ReuseObjectIdForSize flag (bit 63) set and none of the
            // current size-hint flags (bits 60-62). Read the raw position word directly rather than via the production decoder.
            var mainLogSegment = FindLogSegmentZero(MethodTestDir, MainLogName);
            var pageBytes = File.ReadAllBytes(mainLogSegment);
            var objectRecordCount = 0;
            fixed (byte* pagePtr = pageBytes)
            {
                var pageBase = (long)pagePtr;
                var offset = info.beginAddress;
                while (offset < info.recoveredTailAddress)
                {
                    var logRecord = new LogRecord(pageBase + offset);
                    if (logRecord.Info.IsNull)
                    {
                        offset += RecordInfo.Size;
                        continue;
                    }
                    if (logRecord.Info.Valid && !logRecord.DataHeader.RecordIsInline)
                    {
                        var word = *(ulong*)logRecord.GetObjectLogPositionAddress(logRecord.GetOptionalStartAddress());
                        ClassicAssert.AreNotEqual(0UL, word & ObjectLogFilePositionInfo.kReuseObjectIdForSizeMask, "ReuseObjectIdForSize (bit 63) should be set on a v7 record");
                        ClassicAssert.AreEqual(0UL, word & ObjectLogFilePositionInfo.kKeyIsExactSizeMask, "KeyIsExactSize should be clear on a v7 record");
                        ClassicAssert.AreEqual(0UL, word & ObjectLogFilePositionInfo.kValueIsExactSizeMask, "ValueIsExactSize should be clear on a v7 record");
                        ClassicAssert.AreEqual(0UL, word & ObjectLogFilePositionInfo.kKeyHasExtendedSizeHintMask, "KeyHasExtendedSizeHint should be clear on a v7 record");
                        ++objectRecordCount;
                    }
                    offset += logRecord.AllocatedSize;
                }
            }
            ClassicAssert.AreEqual(numRecords, objectRecordCount, "expected one out-of-line object record per key");
        }
    }
}