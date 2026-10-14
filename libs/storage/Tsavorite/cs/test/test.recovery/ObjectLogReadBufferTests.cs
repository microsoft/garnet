// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    [TestFixture]
    internal unsafe class ObjectLogReadBufferTests : TestBase
    {
        const int SegmentSizeBits = 22;
        const ulong SegmentSize = 1UL << SegmentSizeBits;
        const int BufferSize = 4096;
        const byte Segment0Fill = 0xA0;
        const byte Segment1Fill = 0xB1;

        IDevice device;
        SectorAlignedBufferPool bufferPool;

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

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        [TestCase(0UL, 100UL, 0, 100UL)]
        [TestCase(100UL, SegmentSize - 100, 1, 0UL)]
        [TestCase(100UL, SegmentSize, 1, 100UL)]
        [TestCase(100UL, 2 * SegmentSize + 50, 2, 150UL)]
        [TestCase(SegmentSize - 100, 300UL, 1, 200UL)]
        public void PositionAdvance(ulong startOffset, ulong size, int expectedSegmentId, ulong expectedOffset)
        {
            var position = Position(0, startOffset);
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
        [TestCase(BufferSize - 100, 0, SegmentSize - 100)]
        [TestCase(BufferSize, 1, 0UL)]
        public void DiskReadBufferCurrentFilePositionAtSegmentEnd(int currentPosition, int expectedSegmentId, ulong expectedOffset)
        {
            bufferPool = new SectorAlignedBufferPool(1, 512);
            using var buffer = new DiskReadBuffer(bufferPool.Get(BufferSize), device: null, logger: null)
            {
                startFilePosition = Position(0, SegmentSize - BufferSize),
                currentPosition = currentPosition,
                endPosition = BufferSize
            };

            var position = buffer.GetCurrentFilePosition();

            Assert.That(position.SegmentId, Is.EqualTo(expectedSegmentId));
            Assert.That(position.Offset, Is.EqualTo(expectedOffset));
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        [TestCase(BufferSize)]
        [TestCase(BufferSize - 100)]
        public void CircularDiskReadBufferBeginsRecordAtNextSegmentStart(int consumedInFirstBuffer)
        {
            using var readBuffers = CreateReadBuffersAcrossSegmentBoundary(totalLength: 2 * BufferSize);
            Assert.That(readBuffers.OnBeginRecord(Position(0, SegmentSize - BufferSize)), Is.True);
            var buffer = readBuffers.GetCurrentBuffer();
            Assert.That(buffer.AvailableSpan.IndexOfAnyExcept(Segment0Fill), Is.EqualTo(-1));

            // Consume the first record; any remainder is sector-alignment padding before the segment boundary.
            buffer.currentPosition = consumedInFirstBuffer;
            Assert.That(readBuffers.OnBeginRecord(Position(1, 0)), Is.True);

            buffer = readBuffers.GetCurrentBuffer();
            Assert.That(buffer.AvailableLength, Is.EqualTo(BufferSize));
            Assert.That(buffer.AvailableSpan.IndexOfAnyExcept(Segment1Fill), Is.EqualTo(-1));
            readBuffers.OnEndReadRecords();
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void CircularDiskReadBufferReturnsFalseWhenNoDataRemains()
        {
            using var readBuffers = CreateReadBuffersAcrossSegmentBoundary(totalLength: BufferSize);
            Assert.That(readBuffers.OnBeginRecord(Position(0, SegmentSize - BufferSize)), Is.True);

            readBuffers.GetCurrentBuffer().currentPosition = BufferSize;
            Assert.That(readBuffers.OnBeginRecord(Position(1, 0)), Is.False);
        }

        /// <summary>Writes the last <see cref="BufferSize"/> bytes of segment 0 and the first of segment 1, then starts reading at the former.</summary>
        CircularDiskReadBuffer CreateReadBuffersAcrossSegmentBoundary(ulong totalLength)
        {
            device = Devices.CreateLogDevice(Path.Join(MethodTestDir, "readbuffer.obj.log"), deleteOnClose: true);
            device.Initialize((long)SegmentSize);
            bufferPool = new SectorAlignedBufferPool(1, (int)device.SectorSize);

            WriteFilled(0, SegmentSize - BufferSize, Segment0Fill);
            WriteFilled(1, 0, Segment1Fill);

            var readBuffers = new CircularDiskReadBuffer(bufferPool, BufferSize, numBuffers: 2, device, logger: null);
            readBuffers.OnBeginReadRecords(Position(0, SegmentSize - BufferSize), totalLength);
            return readBuffers;
        }

        void WriteFilled(int segmentId, ulong offset, byte fill)
        {
            var memory = bufferPool.Get(BufferSize);
            try
            {
                new Span<byte>(memory.aligned_pointer, BufferSize).Fill(fill);
                using var done = new ManualResetEventSlim();
                uint writeError = 0;
                device.WriteAsync((IntPtr)memory.aligned_pointer, segmentId, offset, BufferSize, (errorCode, _, _, _) =>
                {
                    writeError = errorCode;
                    done.Set();
                }, context: null);
                done.Wait();
                Assert.That(writeError, Is.EqualTo(0u));
            }
            finally
            {
                memory.Return();
            }
        }
    }
}