// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.Objects
{
    using static TestUtils;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>A pass-through device that counts reads, so a test can check how many IOs a record read takes.</summary>
    internal sealed class ReadCountingDevice : StorageDeviceBase
    {
        readonly IDevice underlying;
        int readCount;
        int readsToFail;

        public ReadCountingDevice(IDevice underlying) : base(underlying.FileName, underlying.SectorSize, underlying.Capacity)
            => this.underlying = underlying;

        /// <summary>Reads issued since the last <see cref="ResetReadCount"/>.</summary>
        public int ReadCount => Volatile.Read(ref readCount);

        public void ResetReadCount() => Volatile.Write(ref readCount, 0);

        /// <summary>Complete the next <paramref name="count"/> reads with an error, without reading.</summary>
        public void FailNextReads(int count) => Volatile.Write(ref readsToFail, count);

        public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
            underlying.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
        }

        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
            => underlying.RemoveSegmentAsync(segment, callback, result);

        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
            DeviceIOCompletionCallback callback, object context)
            => underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);

        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
            DeviceIOCompletionCallback callback, object context)
        {
            _ = Interlocked.Increment(ref readCount);
            if (Volatile.Read(ref readsToFail) > 0 && Interlocked.Decrement(ref readsToFail) >= 0)
            {
                callback(errorCode: 38, numBytes: 0, context, ioException: null);
                return;
            }
            underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);
        }

        public override void Dispose() => underlying.Dispose();
    }

    /// <summary>
    /// Covers the object-log format written by <see cref="ObjectLogWriter{TStoreFunctions}"/> and read by
    /// <see cref="ObjectLogReader{TStoreFunctions}"/>: single extents, exact and length-prefixed; directories, including mixed
    /// key/value chunks, page cuts, combined final data, and spines; zero-length components; copying between logs; IO counts; and
    /// rejection of corrupt structure.
    /// </summary>
    [TestFixture]
    internal class ObjectLogChunkFormatTests : TestBase
    {
        // A 4 KB page and 1 KB chunks reach page cuts and multi-chunk records with small data.
        const int PageSizeBits = 12;
        const int PageSize = 1 << PageSizeBits;
        const int ChunkSize = 1024;
        const int MinPartialBytes = 64;

        IDevice device;
        ReadCountingDevice countingDevice;
        ObjectLog objectLog;
        ObjectLogWriter<ClassStoreFunctions> writer;
        ObjectLogReader<ClassStoreFunctions> reader;
        ClassStoreFunctions storeFunctions;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            storeFunctions = StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer());
            OpenLog();
        }

        [TearDown]
        public void TearDown()
        {
            CloseLog();
            DeleteDirectory(MethodTestDir);
            OnTearDown();
        }

        void OpenLog(int maxDirectoryRecordSize = ObjectLogWriterSettings.DefaultMaxDirectoryRecordSize, long restoreTail = 0)
        {
            device = Devices.CreateLogDevice(Path.Join(MethodTestDir, "chunks.obj.log"), deleteOnClose: false);
            countingDevice = new ReadCountingDevice(device);
            objectLog = new ObjectLog(new ObjectLogSettings
            {
                LogDevice = countingDevice,
                PageSizeBits = PageSizeBits,
                PageCount = 8,
                SegmentSizeBits = 16
            });
            if (restoreTail > 0)
                objectLog.RestoreTail(LogAddress.FirstValidAddress, restoreTail);
            writer = new(objectLog, storeFunctions, new ObjectLogWriterSettings
            {
                ChunkSize = ChunkSize,
                MinPartialBytes = MinPartialBytes,
                MaxDirectoryRecordSize = maxDirectoryRecordSize
            });
            reader = new(objectLog, storeFunctions);
        }

        void CloseLog()
        {
            reader?.Dispose();
            reader = null;
            writer?.Dispose();
            writer = null;
            objectLog?.Dispose();
            objectLog = null;
            countingDevice?.Dispose();
            countingDevice = null;
            device = null;
        }

        /// <summary>Make everything durable and reopen the log at its tail, so earlier extents are read from the device.</summary>
        void ReopenFromDevice(int maxDirectoryRecordSize = ObjectLogWriterSettings.DefaultMaxDirectoryRecordSize)
        {
            var tail = objectLog.FlushTail(wait: true);
            CloseLog();
            OpenLog(maxDirectoryRecordSize, restoreTail: tail);
        }

        static byte[] Pattern(int length, byte seed)
        {
            var bytes = new byte[length];
            for (var ii = 0; ii < length; ++ii)
                bytes[ii] = (byte)(seed + (ii * 7));
            return bytes;
        }

        static OverflowByteArray ToOverflow(byte[] bytes)
        {
            var overflow = OverflowByteArray.AllocateData(bytes.Length);
            bytes.CopyTo(overflow.Span);
            return overflow;
        }

        public enum ValueKind { None, Overflow, Object }

        /// <summary>A record to write: an optional overflow key, and an optional overflow or object value.</summary>
        sealed class TestRecord(int keyLength, ValueKind valueKind, int valueLength)
        {
            internal readonly byte[] Key = keyLength > 0 ? Pattern(keyLength, 11) : null;
            internal readonly ValueKind ValueKind = valueKind;
            internal readonly byte[] ValueBytes = valueKind == ValueKind.Overflow ? Pattern(valueLength, 23) : null;
            internal readonly TestLargeObjectValue ValueObject = valueKind == ValueKind.Object && valueLength > 0 ? new TestLargeObjectValue(valueLength) : null;
            internal ObjectLogWriteResult Result;

            /// <summary>The bytes the value serializes to: a 4-byte size and the payload.</summary>
            internal long SerializedValueLength => ValueKind switch
            {
                ValueKind.Overflow => ValueBytes.Length,
                ValueKind.Object => ValueObject is null ? 0 : sizeof(int) + ValueObject.value.Length,
                _ => 0
            };

            internal ObjectLogRecordRef Ref => ObjectLogRecordRef.FromWriteResult(Result, Key is not null,
                ValueKind == ValueKind.Overflow, ValueKind == ValueKind.Object);
        }

        void Write(TestRecord record)
        {
            var key = record.Key is null ? default : ToOverflow(record.Key);
            var value = record.ValueBytes is null ? default : ToOverflow(record.ValueBytes);
            record.Result = writer.WriteRecordObjects(key, value, record.ValueKind == ValueKind.Object, record.ValueObject);
        }

        void Verify(TestRecord record, string what)
        {
            reader.BeginRecord(record.Ref);
            try
            {
                Assert.That(reader.KeyLength, Is.EqualTo(record.Key?.Length ?? 0), $"{what}: key length");
                Assert.That(reader.ValueLength, Is.EqualTo(record.SerializedValueLength), $"{what}: value length");
                if (record.Key is not null)
                    Assert.That(reader.ReadKey().ReadOnlySpan.ToArray(), Is.EqualTo(record.Key), $"{what}: key bytes");
                if (record.ValueKind == ValueKind.Overflow)
                    Assert.That(reader.ReadValueOverflow().ReadOnlySpan.ToArray(), Is.EqualTo(record.ValueBytes), $"{what}: value bytes");
                else if (record.ValueKind == ValueKind.Object)
                {
                    var valueObject = (TestLargeObjectValue)reader.ReadValueObject();
                    if (record.ValueObject is null)
                        Assert.That(valueObject, Is.Null, $"{what}: an empty object reads as absent");
                    else
                        Assert.That(valueObject.value, Is.EqualTo(record.ValueObject.value), $"{what}: object bytes");
                }
            }
            finally
            {
                reader.EndRecord();
            }
        }

        static readonly int[] KeyLengths = [0, 100, 511, 512, 1000, 3000, 9000];
        static readonly int[] ValueLengths = [0, 1, 300, 507, 508, 1016, 1017, 2500, 9000];

        /// <summary>Every combination of key length, value kind, and value length round-trips, read from memory and then from the device.</summary>
        [Test, Category(ObjectLogCategory)]
        public void RecordsRoundTripFromMemoryAndDevice([Values] ValueKind valueKind)
        {
            var records = new System.Collections.Generic.List<TestRecord>();
            foreach (var keyLength in KeyLengths)
            {
                foreach (var valueLength in ValueLengths)
                {
                    if (keyLength == 0 && valueKind == ValueKind.None)
                        continue;
                    if (valueKind == ValueKind.None && valueLength > 0)
                        continue;
                    var record = new TestRecord(keyLength, valueKind, valueLength);
                    Write(record);
                    records.Add(record);
                }
            }

            foreach (var record in records)
                Verify(record, $"memory: key {record.Key?.Length ?? 0}, value {record.SerializedValueLength}");

            ReopenFromDevice();
            foreach (var record in records)
                Verify(record, $"device: key {record.Key?.Length ?? 0}, value {record.SerializedValueLength}");
        }

        /// <summary>The shape the writer chooses: a single extent exactly when one component fits a chunk; exact framing exactly when the
        /// extent's payload is at most 511 bytes.</summary>
        [Test, Category(ObjectLogCategory)]
        public void ShapeAndFramingFollowTheStream(
            [Values(1, 511, 512, 1024)] int valueLength,
            [Values] bool withKey)
        {
            AlignTailToPageStart();
            var record = new TestRecord(withKey ? 200 : 0, ValueKind.Overflow, valueLength);
            Write(record);

            var word = record.Result.PositionWord;
            if (withKey)
            {
                Assert.That(record.Result.HasDirectory, Is.True, "an overflow key with an out-of-line value always has a directory");
                Assert.That((word & ObjectLogPositionInfo.kValueIsExactSizeMask) == 0, "the key slot sizes a directory, not the value slot");
            }
            else
            {
                Assert.That(record.Result.HasDirectory, Is.False, "one component that fits a chunk is a single extent");
                var isExact = (word & ObjectLogPositionInfo.kValueIsExactSizeMask) != 0;
                Assert.That(isExact, Is.EqualTo(valueLength <= ObjectLogExtent.ExactCutoff), "exact framing exactly when the payload is at most 511 bytes");
                Assert.That(record.Result.ValueSizeHint, Is.EqualTo(isExact ? valueLength : 1), "an exact hint is the length; otherwise one 4 KB page here");
            }
            Verify(record, "shape");
        }

        /// <summary>A record whose components fit one allocation gets one combined directory record, and a chunk holding the end of the
        /// key and the start of the value carries two entries at the same address.</summary>
        [Test, Category(ObjectLogCategory)]
        public void MixedChunkHasTwoEntriesAtOneAddress()
        {
            AlignTailToPageStart();
            var record = new TestRecord(300, ValueKind.Object, 300);
            Write(record);
            Assert.That(record.Result.HasDirectory, Is.True);

            var (header, entries, _) = ReadTopDirectory(record);
            Assert.That(header.IsCombined, Is.True, "the final data travels with the directory");
            Assert.That(header.EntryCount, Is.EqualTo(2));
            Assert.That(header.KeyEntryCount, Is.EqualTo(1));
            Assert.That(entries[1].Address, Is.EqualTo(entries[0].Address), "the key and value portions of one chunk share its address");
            Assert.That(entries[0].Length, Is.EqualTo(300));
            Assert.That(entries[1].Length, Is.EqualTo(304));
            Verify(record, "mixed chunk");
        }

        /// <summary>A length-prefixed extent that does not fit the page tail, when both pieces are worth keeping, is cut at the page end:
        /// the tail becomes a data chunk and the record gets a directory. An exact extent is never cut.</summary>
        [Test, Category(ObjectLogCategory)]
        public void SingleExtentIsCutAtThePageEndOnlyWhenNotExact([Values] bool exact)
        {
            AlignTailToPageStart();
            FillPageUntilRemaining(remaining: exact ? 200 : 600);

            var record = new TestRecord(0, ValueKind.Overflow, exact ? 400 : 1000);
            var tailBefore = objectLog.TailAddress;
            Write(record);

            if (exact)
            {
                Assert.That(record.Result.HasDirectory, Is.False, "an exact extent moves to the next page rather than being cut");
                Assert.That(objectLog.GetPage(record.Result.Address), Is.EqualTo(objectLog.GetPage(tailBefore) + 1));
            }
            else
            {
                Assert.That(record.Result.HasDirectory, Is.True, "the page tail took the start of the value");
                var (_, entries, _) = ReadTopDirectory(record);
                Assert.That(entries[0].Address, Is.EqualTo(tailBefore), "the first chunk fills the page tail");
                Assert.That(entries[0].Length, Is.EqualTo(600));
            }
            Verify(record, "page cut");
        }

        /// <summary>A record with more entries than a directory record holds writes spine records, each linked from the next one's first
        /// entry, and still reads back exactly, from memory and from the device.</summary>
        [Test, Category(ObjectLogCategory)]
        public void LongDirectoriesUseABackwardSpine()
        {
            // Room for four entries per record: a 30 KB value in 1 KB chunks needs about thirty.
            var maxDirectoryRecordSize = ObjectLogDirectory.RecordSize(4);
            CloseLog();
            OpenLog(maxDirectoryRecordSize);

            var record = new TestRecord(2000, ValueKind.Object, 30_000);
            Write(record);

            var (header, entries, _) = ReadTopDirectory(record);
            Assert.That(entries[0].IsDirectory, Is.True, "the top record's first entry links back to a spine record");
            Assert.That(header.KeyEntryCount, Is.EqualTo(2), "the key's two chunks are counted over the whole chain");
            Verify(record, "spine, memory");

            ReopenFromDevice(maxDirectoryRecordSize);
            Verify(record, "spine, device");
        }

        /// <summary>An object value that is absent writes nothing and reads back as absent, alone and alongside an overflow key.</summary>
        [Test, Category(ObjectLogCategory)]
        public void AnAbsentObjectWritesNothing([Values] bool withKey)
        {
            var tailBefore = objectLog.TailAddress;
            var record = new TestRecord(withKey ? 600 : 0, ValueKind.Object, 0);
            Write(record);
            if (!withKey)
            {
                Assert.That(objectLog.TailAddress, Is.EqualTo(tailBefore), "a zero-length component allocates nothing");
                Assert.That((record.Result.PositionWord & ObjectLogPositionInfo.kValueIsExactSizeMask) != 0 && record.Result.ValueSizeHint == 0,
                    "an exact hint of zero says there is nothing to read");
            }
            Verify(record, "absent object");
        }

        /// <summary>Copying a record into another log re-chunks it there and yields hints that read it back exactly.</summary>
        [Test, Category(ObjectLogCategory)]
        public void CopyingARecordRechunksItInTheDestination([Values(300, 3000, 20_000)] int valueLength)
        {
            var record = new TestRecord(700, ValueKind.Object, valueLength);
            Write(record);

            using var destinationDevice = Devices.CreateLogDevice(Path.Join(MethodTestDir, "copy.obj.log"), deleteOnClose: false);
            using var destinationLog = new ObjectLog(new ObjectLogSettings { LogDevice = destinationDevice, PageSizeBits = 13, PageCount = 8, SegmentSizeBits = 16 });
            using var destinationWriter = new ObjectLogWriter<ClassStoreFunctions>(destinationLog, storeFunctions,
                new ObjectLogWriterSettings { ChunkSize = 2048, MinPartialBytes = MinPartialBytes });
            using var destinationReader = new ObjectLogReader<ClassStoreFunctions>(destinationLog, storeFunctions);

            reader.BeginRecord(record.Ref);
            ObjectLogWriteResult copied;
            try
            {
                copied = reader.CopyRecordTo(destinationWriter);
            }
            finally
            {
                reader.EndRecord();
            }
            Assert.That(copied.ValueObjectLength, Is.EqualTo((ulong)record.SerializedValueLength));

            destinationReader.BeginRecord(ObjectLogRecordRef.FromWriteResult(copied, keyIsOverflow: true, valueIsOverflow: false, valueIsObject: true));
            try
            {
                Assert.That(destinationReader.ReadKey().ReadOnlySpan.ToArray(), Is.EqualTo(record.Key));
                Assert.That(((TestLargeObjectValue)destinationReader.ReadValueObject()).value, Is.EqualTo(record.ValueObject.value));
            }
            finally
            {
                destinationReader.EndRecord();
            }
        }

        /// <summary>Reads from the device take one IO for a single extent and one for a directory whose final data is combined with it;
        /// a directory with separate chunks takes one more IO per chunk outside the top record.</summary>
        [Test, Category(ObjectLogCategory)]
        public void DeviceReadsTakeOneIOPerExtent()
        {
            AlignTailToPageStart();
            var small = new TestRecord(0, ValueKind.Overflow, 300);
            var medium = new TestRecord(0, ValueKind.Object, 900);
            var mixed = new TestRecord(200, ValueKind.Object, 200);
            var large = new TestRecord(0, ValueKind.Overflow, 2500);
            foreach (var record in new[] { small, medium, mixed, large })
                Write(record);

            // Move the tail to a fresh page, so reopening loads nothing these records share into memory.
            AlignTailToPageStart();
            ReopenFromDevice();

            Assert.That(ReadsFor(small), Is.EqualTo(1), "an exact extent is one IO");
            Assert.That(ReadsFor(medium), Is.EqualTo(1), "a length-prefixed extent sized by its hint is one IO");
            Assert.That(ReadsFor(mixed), Is.EqualTo(1), "a combined directory record carries all of a small record's data");

            var (header, entries, _) = ReadTopDirectory(large);
            var chunksOutsideTop = header.EntryCount - (header.IsCombined ? 1 : 0);
            Assert.That(ReadsFor(large), Is.EqualTo(1 + chunksOutsideTop), "one IO for the top record and one per chunk outside it");
        }

        int ReadsFor(TestRecord record)
        {
            countingDevice.ResetReadCount();
            Verify(record, "counted");
            return countingDevice.ReadCount;
        }

        /// <summary>Records written back to back, alternating exact and length-prefixed single extents.</summary>
        List<TestRecord> WriteSmallRecords(ValueKind valueKind, int count)
        {
            var records = new List<TestRecord>(count);
            for (var ii = 0; ii < count; ++ii)
            {
                var record = new TestRecord(0, valueKind, ii % 2 == 0 ? 100 : 700);
                Write(record);
                records.Add(record);
            }
            return records;
        }

        /// <summary>A read-ahead over the extents of <paramref name="records"/>, as a page walk starts one.</summary>
        ObjectLogReadAhead StartReadAhead(List<TestRecord> records, int maxWindows)
        {
            var last = records[^1].Ref;
            return new ObjectLogReadAhead(objectLog, records[0].Result.Address, last.Address + last.TopExtentInitialRead, PageSize, maxWindows);
        }

        /// <summary>Verify <paramref name="records"/> in order through <paramref name="readAhead"/>, and return the reads that took.</summary>
        int VerifyThrough(ObjectLogReadAhead readAhead, List<TestRecord> records, string what)
        {
            reader.ReadAhead = readAhead;
            try
            {
                foreach (var record in records)
                    Verify(record, what);
            }
            finally
            {
                reader.ReadAhead = null;
            }
            return countingDevice.ReadCount;
        }

        /// <summary>A walk over many small records reads them through the read-ahead's windows: a read per log page, rather than one per
        /// record, with every record intact.</summary>
        [Test, Category(ObjectLogCategory)]
        public void ReadAheadReadsSmallRecordsInAReadPerPage([Values(ValueKind.Overflow, ValueKind.Object)] ValueKind valueKind)
        {
            AlignTailToPageStart();
            var records = WriteSmallRecords(valueKind, 100);
            AlignTailToPageStart();
            ReopenFromDevice();

            countingDevice.ResetReadCount();
            foreach (var record in records)
                Verify(record, "direct");
            var directReads = countingDevice.ReadCount;
            Assert.That(directReads, Is.EqualTo(records.Count), "without the read-ahead, each record is one read");

            var pagesSpanned = objectLog.GetPage(records[^1].Result.Address) - objectLog.GetPage(records[0].Result.Address) + 1;
            countingDevice.ResetReadCount();
            using var readAhead = StartReadAhead(records, maxWindows: 2);
            var readAheadReads = VerifyThrough(readAhead, records, "read-ahead");
            Assert.That(readAheadReads, Is.EqualTo(pagesSpanned), "the read-ahead reads each log page the records span once");
            Assert.That(readAhead.WindowCount, Is.LessThanOrEqualTo(2), "the read-ahead keeps no more windows than it may");
        }

        /// <summary>Records with directories and chunks on several pages read correctly through the read-ahead, which serves their chunks from
        /// its windows; the first record's chunks, below the range, are read directly.</summary>
        [Test, Category(ObjectLogCategory)]
        public void ReadAheadServesDirectoryRecordsAndReadsTheRestDirectly()
        {
            AlignTailToPageStart();
            var records = new List<TestRecord>();
            foreach (var (keyLength, valueKind, valueLength) in new[] { (3000, ValueKind.Object, 9000), (0, ValueKind.Overflow, 300), (200, ValueKind.Object, 200),
                (0, ValueKind.Overflow, 2500), (1000, ValueKind.Overflow, 5000), (0, ValueKind.Object, 900) })
            {
                var record = new TestRecord(keyLength, valueKind, valueLength);
                Write(record);
                records.Add(record);
            }
            AlignTailToPageStart();
            ReopenFromDevice();

            countingDevice.ResetReadCount();
            foreach (var record in records)
                Verify(record, "direct");
            var directReads = countingDevice.ReadCount;

            countingDevice.ResetReadCount();
            using var readAhead = StartReadAhead(records, maxWindows: 2);
            var readAheadReads = VerifyThrough(readAhead, records, "read-ahead");
            Assert.That(readAheadReads, Is.LessThan(directReads), "the read-ahead serves chunks that would each be a read");
        }

        /// <summary>Bytes still in the log's memory are copied into the windows, with no device read.</summary>
        [Test, Category(ObjectLogCategory)]
        public void ReadAheadCopiesResidentBytesWithoutReading([Values(ValueKind.Overflow, ValueKind.Object)] ValueKind valueKind)
        {
            var records = WriteSmallRecords(valueKind, 10);
            Assert.That(records[0].Result.Address, Is.GreaterThanOrEqualTo(objectLog.HeadAddress), "the records must still be in memory");

            countingDevice.ResetReadCount();
            using var readAhead = StartReadAhead(records, maxWindows: 2);
            Assert.That(VerifyThrough(readAhead, records, "resident"), Is.Zero, "resident bytes need no device read");
        }

        /// <summary>A window whose read fails costs reads, not correctness: the bytes it would have served are read directly.</summary>
        [Test, Category(ObjectLogCategory)]
        public void AFailedWindowReadFallsBackToDirectReads()
        {
            AlignTailToPageStart();
            var records = WriteSmallRecords(ValueKind.Object, 40);
            AlignTailToPageStart();
            ReopenFromDevice();

            // The read-ahead issues its first windows' reads when it starts, so these are the ones that fail.
            const int MaxWindows = 2;
            countingDevice.ResetReadCount();
            countingDevice.FailNextReads(MaxWindows);
            using var readAhead = StartReadAhead(records, MaxWindows);
            _ = VerifyThrough(readAhead, records, "after failed windows");
        }

        /// <summary>Disposing a read-ahead waits for its windows' reads, so their buffers never return to the pool while a device may still
        /// write into them.</summary>
        [Test, Category(ObjectLogCategory)]
        public void DisposingAReadAheadWaitsForItsReads()
        {
            AlignTailToPageStart();
            var records = WriteSmallRecords(ValueKind.Overflow, 40);
            AlignTailToPageStart();
            var tail = objectLog.FlushTail(wait: true);
            CloseLog();

            using var fileDevice = Devices.CreateLogDevice(Path.Join(MethodTestDir, "chunks.obj.log"), deleteOnClose: false);
            var gatedDevice = new GatedCompletionDevice(fileDevice);
            using var gatedLog = new ObjectLog(new ObjectLogSettings { LogDevice = gatedDevice, PageSizeBits = PageSizeBits, PageCount = 8, SegmentSizeBits = 16 });
            gatedLog.RestoreTail(LogAddress.FirstValidAddress, tail);

            gatedDevice.Gate = true;
            var last = records[^1].Ref;
            var readAhead = new ObjectLogReadAhead(gatedLog, records[0].Result.Address, last.Address + last.TopExtentInitialRead, PageSize, maxWindows: 2);
            var released = 0;
            try
            {
                gatedDevice.WaitForPending(2, TimeSpan.FromSeconds(30));
                var dispose = Task.Run(readAhead.Dispose);
                Assert.That(dispose.Wait(TimeSpan.FromMilliseconds(500)), Is.False, "Dispose returned while its windows' reads were in flight");

                gatedDevice.Gate = false;
                for (; released < gatedDevice.CapturedCount; released++)
                    gatedDevice.Release(released);
                Assert.That(dispose.Wait(TimeSpan.FromSeconds(30)), Is.True, "Dispose did not return once its windows' reads completed");
            }
            finally
            {
                gatedDevice.Gate = false;
                for (; released < gatedDevice.CapturedCount; released++)
                    gatedDevice.Release(released);
                readAhead.Dispose();
            }
        }

        /// <summary>Corrupt structure is rejected before any data is read or deserialized.</summary>
        [Test, Category(ObjectLogCategory)]
        public void CorruptStructureIsRejected()
        {
            // A length prefix that claims more than the page holds.
            var address = WriteRaw(writer => Unsafe_WriteInt(writer, 0, PageSize));
            AssertRejected(new ObjectLogRecordRef { PositionWord = (ulong)address, ValueIsOverflow = true, ValueSizeHint = 1 }, "oversized prefix");

            // A length prefix at or below the exact cutoff, which a writer never produces. The extent is long enough that only the
            // cutoff, not the available bytes, rejects it.
            address = WriteRaw(writer => Unsafe_WriteInt(writer, 0, 100), length: 200);
            AssertRejected(new ObjectLogRecordRef { PositionWord = (ulong)address, ValueIsOverflow = true, ValueSizeHint = 1 }, "undersized prefix");

            // A directory that claims more entries than it holds.
            address = WriteDirectory(entryCount: 50, keyEntryCount: 0, [new DirectoryEntry(LogAddress.FirstValidAddress, 10)]);
            AssertRejected(DirectoryRef(address), "too many entries");

            // A directory whose key entry count exceeds its entries.
            address = WriteDirectory(entryCount: 1, keyEntryCount: 2, [new DirectoryEntry(LogAddress.FirstValidAddress, 10)]);
            AssertRejected(DirectoryRef(address, keyIsOverflow: true), "impossible key split");

            // A spine link that points forward.
            address = WriteDirectory(entryCount: 2, keyEntryCount: 0,
                [new DirectoryEntry(LogAddress.FirstValidAddress + 100_000, 24, isDirectory: true), new DirectoryEntry(LogAddress.FirstValidAddress, 10)]);
            AssertRejected(DirectoryRef(address), "forward spine link");

            // An entry outside the log.
            address = WriteDirectory(entryCount: 1, keyEntryCount: 0, [new DirectoryEntry(objectLog.TailAddress + PageSize, 10)]);
            AssertRejected(DirectoryRef(address), "entry past the tail");

            // Both components out of line without a directory.
            AssertRejected(new ObjectLogRecordRef { PositionWord = (ulong)address | ObjectLogPositionInfo.kKeyIsExactSizeMask, KeyIsOverflow = true, KeySizeHint = 10, ValueIsObject = true },
                "two components without a directory");
        }

        static void Unsafe_WriteInt(Span<byte> span, int offset, int value) => System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(span.Slice(offset), value);

        long WriteRaw(SpanAction write, int length = 64)
        {
            var scope = objectLog.BeginScope();
            try
            {
                var address = objectLog.Allocate(length);
                var span = objectLog.AsSpan(address, length);
                span.Clear();
                write(span);
                return address;
            }
            finally
            {
                objectLog.EndScope(scope);
            }
        }

        delegate void SpanAction(Span<byte> span);

        /// <summary>Write a length-prefixed directory record with <paramref name="entries"/>, padded past the exact cutoff; the padding
        /// reads as combined data.</summary>
        unsafe long WriteDirectory(int entryCount, int keyEntryCount, DirectoryEntry[] entries)
        {
            const int PayloadLength = 600;
            return WriteRaw(span =>
            {
                fixed (byte* p = span)
                {
                    *(int*)p = PayloadLength;
                    new DirectoryRecordHeader(entryCount, keyEntryCount, DirectoryFlags.DirectoryIsCombined).WriteTo(p + ObjectLogExtent.LengthPrefixSize);
                    for (var ii = 0; ii < entries.Length; ++ii)
                        entries[ii].WriteTo(p + ObjectLogExtent.LengthPrefixSize + ObjectLogDirectory.EntriesOffset + (ii * DirectoryEntry.Size));
                }
            }, length: ObjectLogExtent.LengthPrefixSize + PayloadLength);
        }

        static ObjectLogRecordRef DirectoryRef(long address, bool keyIsOverflow = false)
            => new()
            {
                PositionWord = (ulong)address | ObjectLogPositionInfo.kHasDirectoryMask,
                KeyIsOverflow = keyIsOverflow,
                ValueIsOverflow = !keyIsOverflow,
                KeySizeHint = keyIsOverflow ? 1 : 0,
                KeyRdhLengthBits = 0,
                ValueSizeHint = keyIsOverflow ? 0 : 1,
            };

        void AssertRejected(ObjectLogRecordRef record, string what)
        {
            var exception = Assert.Throws<TsavoriteException>(() =>
            {
                reader.BeginRecord(record);
                if (record.KeyIsOverflow)
                    _ = reader.ReadKey();
                if (record.ValueIsOverflow)
                    _ = reader.ReadValueOverflow();
            }, what);
            reader.EndRecord();
            Assert.That(exception.Message, Does.Contain("object-log").Or.Contain("Object log"), what);
        }

        /// <summary>Read and decode the top directory record a record's position addresses.</summary>
        unsafe (DirectoryRecordHeader header, DirectoryEntry[] entries, int prefix) ReadTopDirectory(TestRecord record)
        {
            Assert.That(record.Result.HasDirectory, Is.True, "the record must have a directory");
            var word = record.Result.PositionWord;
            var isExact = (word & (record.Key is not null ? ObjectLogPositionInfo.kKeyIsExactSizeMask : ObjectLogPositionInfo.kValueIsExactSizeMask)) != 0;
            var buffer = new byte[PageSize];
            var address = record.Result.Address;
            var length = (int)Math.Min(objectLog.RemainingOnPage(address), objectLog.TailAddress - address);
            objectLog.Read(address, length, buffer);
            fixed (byte* p = buffer)
            {
                var prefix = isExact ? 0 : ObjectLogExtent.LengthPrefixSize;
                var header = DirectoryRecordHeader.ReadFrom(p + prefix);
                var entries = new DirectoryEntry[header.EntryCount];
                for (var ii = 0; ii < entries.Length; ++ii)
                    entries[ii] = DirectoryEntry.ReadFrom(p + prefix + ObjectLogDirectory.EntriesOffset + (ii * DirectoryEntry.Size));
                return (header, entries, prefix);
            }
        }

        /// <summary>Move the tail to the start of a page, so a test's layout does not depend on what came before.</summary>
        void AlignTailToPageStart()
        {
            var remaining = objectLog.RemainingOnPage(objectLog.TailAddress);
            if (remaining < PageSize)
                FillPageUntilRemaining(0);
        }

        /// <summary>Allocate filler until the current page has <paramref name="remaining"/> bytes left (zero means the next page start).</summary>
        void FillPageUntilRemaining(int remaining)
        {
            var current = objectLog.RemainingOnPage(objectLog.TailAddress);
            var filler = remaining == 0 ? current : current - remaining;
            if (filler <= 0)
                return;
            var scope = objectLog.BeginScope();
            try
            {
                _ = objectLog.Allocate(filler);
            }
            finally
            {
                objectLog.EndScope(scope);
            }
        }
    }
}