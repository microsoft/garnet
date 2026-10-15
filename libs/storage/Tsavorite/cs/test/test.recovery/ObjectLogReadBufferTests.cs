// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    /// <summary>
    /// Tests for object-log read positioning and buffering, focused on segment boundaries. Uses small segments over a device
    /// filled with an address-dependent byte pattern, so any position error surfaces as a byte mismatch.
    /// </summary>
    [TestFixture]
    internal unsafe class ObjectLogReadBufferTests : TestBase
    {
        const int SegmentSizeBits = 16;
        const ulong SegmentSize = 1UL << SegmentSizeBits;
        const int NumSegments = 6;
        const ulong DeviceEnd = NumSegments * SegmentSize;
        const int DefaultBufferSize = 4096;

        // Record plan marker that pads to the next sector boundary, as the writer does at the end of each flush.
        const int Pad = 0;

        static readonly int[] BufferSizes = [1024, DefaultBufferSize, (int)SegmentSize];
        static readonly int[] BufferCounts = [LogSettings.kMinDeserializationBuffers, 4];

        IDevice device;
        SectorAlignedBufferPool bufferPool;
        int sectorSize;

        [SetUp]
        public void Setup() => DeleteDirectory(MethodTestDir, wait: true);

        [TearDown]
        public void TearDown()
        {
            device?.Dispose();
            device = null;
            bufferPool?.Free();
            bufferPool = null;
            OnTearDown();
        }

        static ObjectLogFilePositionInfo Position(int segmentId, ulong offset) => new(0, SegmentSizeBits) { SegmentId = segmentId, Offset = offset };

        static ObjectLogFilePositionInfo Position(ulong address) => Position((int)(address >> SegmentSizeBits), address & (SegmentSize - 1));

        static byte PatternByte(ulong address) => (byte)(((uint)address * 0x9E3779B1u) >> 24);

        #region ObjectLogFilePositionInfo

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        [TestCase(0, 0UL, 0UL, 0, 0UL)]
        [TestCase(0, 0UL, 100UL, 0, 100UL)]
        [TestCase(0, 100UL, SegmentSize - 101, 0, SegmentSize - 1)]
        [TestCase(0, 100UL, SegmentSize - 100, 1, 0UL)]
        [TestCase(0, 100UL, SegmentSize, 1, 100UL)]
        [TestCase(0, 100UL, 2 * SegmentSize + 50, 2, 150UL)]
        [TestCase(0, SegmentSize - 100, 300UL, 1, 200UL)]
        [TestCase(0, 0UL, SegmentSize, 1, 0UL)]
        [TestCase(0, 0UL, 3 * SegmentSize, 3, 0UL)]
        [TestCase(2, SegmentSize - 1, 1UL, 3, 0UL)]
        [TestCase(5, 10UL, 2 * SegmentSize - 11, 6, SegmentSize - 1)]
        public void PositionAdvance(int startSegmentId, ulong startOffset, ulong size, int expectedSegmentId, ulong expectedOffset)
        {
            var position = Position(startSegmentId, startOffset);
            position.Advance(size);

            Assert.That(position.SegmentId, Is.EqualTo(expectedSegmentId));
            Assert.That(position.Offset, Is.EqualTo(expectedOffset));
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void PositionAdvancePastMaxSegmentThrows()
        {
            const int segmentSizeBits = 30;
            var position = new ObjectLogFilePositionInfo(0, segmentSizeBits) { SegmentId = (1 << (ObjectLogFilePositionInfo.NumSegmentAndOffsetBits - segmentSizeBits)) - 1 };

            Assert.Throws<InvalidDataException>(() => position.Advance(2 * position.SegmentSize));
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        [TestCase(0, 500UL, 0, 100UL, 400UL)]
        [TestCase(1, 0UL, 0, SegmentSize - 100, 100UL)]
        [TestCase(2, 5UL, 0, SegmentSize - 10, SegmentSize + 15)]
        [TestCase(3, 0UL, 1, 0UL, 2 * SegmentSize)]
        public void PositionDistance(int leftSegmentId, ulong leftOffset, int rightSegmentId, ulong rightOffset, ulong expectedDistance)
            => Assert.That(Position(leftSegmentId, leftOffset) - Position(rightSegmentId, rightOffset), Is.EqualTo(expectedDistance));

        #endregion

        #region DiskReadBuffer

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        [TestCase(0, SegmentSize - DefaultBufferSize, 0, 0, SegmentSize - DefaultBufferSize)]
        [TestCase(0, SegmentSize - DefaultBufferSize, DefaultBufferSize - 1, 0, SegmentSize - 1)]
        [TestCase(0, SegmentSize - DefaultBufferSize, DefaultBufferSize, 1, 0UL)]
        [TestCase(2, SegmentSize - 512, 512, 3, 0UL)]
        [TestCase(1, 0UL, DefaultBufferSize, 1, (ulong)DefaultBufferSize)]
        public void DiskReadBufferCurrentFilePosition(int startSegmentId, ulong startOffset, int currentPosition, int expectedSegmentId, ulong expectedOffset)
        {
            bufferPool = new SectorAlignedBufferPool(1, 512);
            using var buffer = new DiskReadBuffer(bufferPool.Get(DefaultBufferSize), device: null, logger: null)
            {
                startFilePosition = Position(startSegmentId, startOffset),
                currentPosition = currentPosition,
                endPosition = DefaultBufferSize
            };

            var position = buffer.GetCurrentFilePosition();

            Assert.That(position.SegmentId, Is.EqualTo(expectedSegmentId));
            Assert.That(position.Offset, Is.EqualTo(expectedOffset));
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void DiskReadBufferWithoutReadHasNoData()
        {
            bufferPool = new SectorAlignedBufferPool(1, 512);
            using var buffer = new DiskReadBuffer(bufferPool.Get(DefaultBufferSize), device: null, logger: null);

            Assert.That(buffer.HasData, Is.False);
            Assert.That(buffer.AvailableLength, Is.EqualTo(0));
            Assert.That(buffer.WaitForDataAvailable(), Is.False);
            Assert.That(buffer.HasInFlightRead, Is.False);
        }

        #endregion

        #region CircularDiskReadBuffer

        static IEnumerable<TestCaseData> ReadScenarios()
        {
            const ulong S = SegmentSize;
            const int B = DefaultBufferSize;
            static TestCaseData Scenario(string name, int bufferSize, int numBuffers, ulong start, params int[] plan)
                => new TestCaseData(bufferSize, numBuffers, start, plan).SetName($"{nameof(CircularDiskReadBufferReadsRecords)}({name})");

            yield return Scenario("ReadShortenedAtSegmentEnd", B, 2, S - 1024, 2048);
            yield return Scenario("UnalignedStartBeforeBoundary", B, 2, S - 1000, 3000);
            yield return Scenario("UnalignedStartInLaterSegment", B, 4, S + 12345, 5000);
            yield return Scenario("StartAtSegmentStart", B, 2, S, 100);
            yield return Scenario("StartInLastSegments", B, 2, 3 * S - 700, 1400);
            yield return Scenario("RecordEndsAtSegmentEndThenNextAtSegmentStart", B, 2, S - B - 300, B + 300, 1000);
            yield return Scenario("PaddingEndsAtSegmentEnd", B, 2, S - 3000, 2900, Pad, 1000);
            yield return Scenario("PaddingEndsAtBufferEndInsideSegment", B, 2, 0, B - 100, Pad, 100);
            yield return Scenario("OneByteRecordsStraddleBoundary", B, 2, S - 5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
            yield return Scenario("OneByteRecordPaddedToSegmentEnd", B, 2, S - 1, 1, Pad, 1);
            yield return Scenario("BufferSizedRecordsAcrossBoundary", B, 2, S - 2 * B, B, B, B, B);
            yield return Scenario("RecordSpansSegmentsTwoBuffers", B, 2, 100, 3 * (int)S);
            yield return Scenario("RecordSpansSegmentsBufferEqualsSegment", (int)S, 2, S / 2 + 512, 2 * (int)S);
            yield return Scenario("RecordSpansSegmentsBufferEqualsSegmentUnaligned", (int)S, 4, S / 2 + 77, 2 * (int)S + 33);
            yield return Scenario("RecordsAlignedToSegmentStarts", (int)S, 2, 0, (int)S, (int)S, (int)S);
            yield return Scenario("SmallBuffersWithPadding", 1024, 4, S - 3000, 700, Pad, 900, Pad, 2500, 10, Pad, 4000);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        [TestCaseSource(nameof(ReadScenarios))]
        public void CircularDiskReadBufferReadsRecords(int bufferSize, int numBuffers, ulong start, int[] plan)
        {
            CreatePatternDevice();
            using var readBuffers = new CircularDiskReadBuffer(bufferPool, bufferSize, numBuffers, device, logger: null);
            ReadRecords(readBuffers, BuildRecords(start, plan), "scenario");
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void CircularDiskReadBufferSweepsStartsAndLengthsAroundBoundaries()
        {
            CreatePatternDevice();
            var deltas = new[] { 0, 1, sectorSize - 1 };
            foreach (var bufferSize in BufferSizes)
            {
                var lengths = new[] { 1, sectorSize - 1, sectorSize, sectorSize + 1, bufferSize - 1, bufferSize, bufferSize + 1, 2 * bufferSize + 3 };
                foreach (var numBuffers in BufferCounts)
                {
                    // Reusing one instance also covers reinitialization between read operations.
                    using var readBuffers = new CircularDiskReadBuffer(bufferPool, bufferSize, numBuffers, device, logger: null);
                    foreach (var boundary in new[] { SegmentSize, 2 * SegmentSize })
                    {
                        for (var sectorsFromBoundary = -9; sectorsFromBoundary <= 1; sectorsFromBoundary++)
                        {
                            foreach (var delta in deltas)
                            {
                                var start = (ulong)((long)boundary + (sectorsFromBoundary * sectorSize) + delta);
                                foreach (var length in lengths)
                                {
                                    foreach (var padded in new[] { false, true })
                                    {
                                        int[] plan = padded ? [length, Pad, sectorSize + 7] : [length, sectorSize + 7];
                                        ReadRecords(readBuffers, BuildRecords(start, plan), $"buffer {bufferSize}x{numBuffers}, start {start}, length {length}, padded {padded}");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void CircularDiskReadBufferReadsRandomRecordLayouts()
        {
            CreatePatternDevice();
            foreach (var bufferSize in BufferSizes)
            {
                foreach (var numBuffers in BufferCounts)
                {
                    using var readBuffers = new CircularDiskReadBuffer(bufferPool, bufferSize, numBuffers, device, logger: null);
                    for (var seed = 0; seed < 40; seed++)
                    {
                        var rng = new Random(seed);
                        var interestingLengths = new[] { 1, 7, sectorSize - 1, sectorSize, sectorSize + 1, bufferSize - 1, bufferSize, bufferSize + 1 };
                        var start = (ulong)rng.Next(0, 2 * (int)SegmentSize);
                        var targetEnd = start + (ulong)rng.Next((int)SegmentSize, 3 * (int)SegmentSize);
                        var plan = new List<int>();
                        var end = start;
                        while (end < targetEnd)
                        {
                            var length = rng.Next(3) == 0 ? rng.Next(1, 3 * bufferSize) : interestingLengths[rng.Next(interestingLengths.Length)];
                            if (end + (ulong)length > DeviceEnd)
                                break;
                            plan.Add(length);
                            end += (ulong)length;
                            if (rng.Next(3) == 0)
                            {
                                plan.Add(Pad);
                                end = RoundUp(end, sectorSize);
                            }
                        }

                        if (plan[^1] == Pad)
                            plan.RemoveAt(plan.Count - 1);
                        ReadRecords(readBuffers, BuildRecords(start, [.. plan]), $"buffer {bufferSize}x{numBuffers}, seed {seed}, start {start}");
                    }
                }
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        [TestCase(SegmentSize - DefaultBufferSize, DefaultBufferSize, TestName = "CircularDiskReadBufferReturnsFalseWhenNoDataRemains(AtSegmentEnd)")]
        [TestCase(SegmentSize - 1000, 900, TestName = "CircularDiskReadBufferReturnsFalseWhenNoDataRemains(PaddingToSegmentEnd)")]
        [TestCase(1000UL, 3096, TestName = "CircularDiskReadBufferReturnsFalseWhenNoDataRemains(AtBufferEnd)")]
        [TestCase(1000UL, 3000, TestName = "CircularDiskReadBufferReturnsFalseWhenNoDataRemains(PaddingToBufferEnd)")]
        [TestCase(100UL, 2 * DefaultBufferSize + 3996, TestName = "CircularDiskReadBufferReturnsFalseWhenNoDataRemains(AfterWrap)")]
        public void CircularDiskReadBufferReturnsFalseWhenNoDataRemains(ulong start, int length)
        {
            CreatePatternDevice();
            using var readBuffers = new CircularDiskReadBuffer(bufferPool, DefaultBufferSize, 2, device, logger: null);
            var records = BuildRecords(start, length);
            ReadRecords(readBuffers, records, "record");

            Assert.That(readBuffers.OnBeginRecord(Position(RoundUp(start + (ulong)length, sectorSize))), Is.False);
        }

        static ulong RoundUp(ulong value, int alignment) => (value + (ulong)alignment - 1) & ~((ulong)alignment - 1);

        /// <summary>Converts a plan of record lengths and <see cref="Pad"/> markers into record start addresses and lengths.</summary>
        List<(ulong start, int length)> BuildRecords(ulong start, params int[] plan)
        {
            var records = new List<(ulong start, int length)>();
            var address = start;
            foreach (var step in plan)
            {
                if (step == Pad)
                {
                    address = RoundUp(address, sectorSize);
                    continue;
                }

                records.Add((address, step));
                address += (ulong)step;
            }

            Assert.That(address, Is.LessThanOrEqualTo(DeviceEnd), "Record plan exceeds the pattern device");
            return records;
        }

        /// <summary>Reads the records the way <see cref="ObjectLogReader{TStoreFunctions}"/> does, checking positions and every byte.</summary>
        static void ReadRecords(CircularDiskReadBuffer readBuffers, List<(ulong start, int length)> records, string context)
        {
            var (firstStart, _) = records[0];
            var (lastStart, lastLength) = records[^1];
            try
            {
                readBuffers.OnBeginReadRecords(Position(firstStart), lastStart + (ulong)lastLength - firstStart);
                foreach (var (start, length) in records)
                    ReadRecord(readBuffers, start, length);
                readBuffers.OnEndReadRecords();
            }
            catch (Exception ex)
            {
                Assert.Fail($"{context}: {ex.Message}");
            }
        }

        static void ReadRecord(CircularDiskReadBuffer readBuffers, ulong start, int length)
        {
            if (!readBuffers.OnBeginRecord(Position(start)))
                throw new InvalidOperationException($"OnBeginRecord returned false for record at {start}");
            var buffer = readBuffers.GetCurrentBuffer();
            AssertAddress(buffer.GetCurrentFilePosition(), start, "record start");

            var data = new byte[length];
            var copied = 0;
            while (copied < length)
            {
                if (buffer.AvailableLength == 0)
                {
                    if (!readBuffers.MoveToNextBuffer(out buffer))
                        throw new InvalidOperationException($"Ran out of data after {copied} of {length} bytes of record at {start}");
                    continue;
                }

                var count = Math.Min(buffer.AvailableLength, length - copied);
                buffer.AvailableSpan[..count].CopyTo(data.AsSpan(copied));
                buffer.currentPosition += count;
                copied += count;
            }

            for (var ii = 0; ii < length; ii++)
            {
                if (data[ii] != PatternByte(start + (ulong)ii))
                    throw new InvalidOperationException($"Byte mismatch at address {start + (ulong)ii} (record at {start}, index {ii})");
            }

            AssertAddress(buffer.GetCurrentFilePosition(), start + (ulong)length, "record end");
        }

        static void AssertAddress(ObjectLogFilePositionInfo position, ulong expectedAddress, string what)
        {
            if (position.CurrentAddress != expectedAddress)
                throw new InvalidOperationException($"Expected {what} at {Position(expectedAddress)}, but buffer is at {position}");
        }

        void CreatePatternDevice()
        {
            device = Devices.CreateLogDevice(Path.Join(MethodTestDir, "pattern.obj.log"), deleteOnClose: true);
            device.Initialize((long)SegmentSize);
            sectorSize = (int)device.SectorSize;
            bufferPool = new SectorAlignedBufferPool(1, sectorSize);

            var memory = bufferPool.Get((int)SegmentSize);
            try
            {
                for (var segmentId = 0; segmentId < NumSegments; segmentId++)
                {
                    var span = new Span<byte>(memory.aligned_pointer, (int)SegmentSize);
                    for (var ii = 0; ii < span.Length; ii++)
                        span[ii] = PatternByte(((ulong)segmentId << SegmentSizeBits) + (ulong)ii);

                    using var done = new ManualResetEventSlim();
                    uint writeError = 0;
                    device.WriteAsync((IntPtr)memory.aligned_pointer, segmentId, 0, (uint)SegmentSize, (errorCode, _, _, _) =>
                    {
                        writeError = errorCode;
                        done.Set();
                    }, context: null);
                    done.Wait();
                    Assert.That(writeError, Is.EqualTo(0u));
                }
            }
            finally
            {
                memory.Return();
            }
        }

        #endregion
    }
}