// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.Objects
{
    /// <summary>
    /// Unit tests for the FLUSH (v2.2) non-inline ValueLength encoding on <see cref="RecordDataHeader"/>:
    /// chunked object (buffer count + final-page count), overflow-with-header (read hint), and headerless (exact length).
    /// See website/docs/dev/objectlog-serialization.md.
    /// </summary>
    [TestFixture]
    internal class RecordDataHeaderFlushEncodingTests
    {
        // 4095 = kFlushMaxBufferCount, 1023 = kFlushMaxFinalPages, 4194303 = kFlushMaxReadHint (< 4 MB).

        [Test]
        [Category("Smoke")]
        public void FlushChunkedObjectRoundTrips(
            [Values(0, 1, 2, 100, 4095)] int bufferCount,
            [Values(0, 1, 512, 1023)] int finalPages)
        {
            var encoded = RecordDataHeader.EncodeFlushChunkedObject(bufferCount, finalPages);
            Assert.That(encoded, Is.LessThanOrEqualTo((uint)RecordDataHeader.kValueLengthLowBitsMask), "must fit the 24-bit ValueLength field");
            Assert.That(RecordDataHeader.FlushValueIsChunkedObject(encoded), Is.True);
            Assert.That(RecordDataHeader.FlushChunkedBufferCount(encoded), Is.EqualTo(bufferCount));
            Assert.That(RecordDataHeader.FlushChunkedFinalPages(encoded), Is.EqualTo(finalPages));
        }

        [Test]
        [Category("Smoke")]
        public void FlushOverflowHeaderRoundTrips([Values(0, 1, 4096, 4194303)] int readHint)
        {
            var encoded = RecordDataHeader.EncodeFlushOverflowHeader(readHint);
            Assert.That(encoded, Is.LessThanOrEqualTo((uint)RecordDataHeader.kValueLengthLowBitsMask));
            Assert.That(RecordDataHeader.FlushValueIsChunkedObject(encoded), Is.False);
            Assert.That(RecordDataHeader.FlushValueHasOverflowHeader(encoded), Is.True);
            Assert.That(RecordDataHeader.FlushValueReadHintOrExact(encoded), Is.EqualTo(readHint));
        }

        [Test]
        [Category("Smoke")]
        public void FlushHeaderlessRoundTrips([Values(0, 1, 30, 131072, 4194303)] int exact)
        {
            var encoded = RecordDataHeader.EncodeFlushHeaderless(exact);
            Assert.That(encoded, Is.LessThanOrEqualTo((uint)RecordDataHeader.kValueLengthLowBitsMask));
            Assert.That(RecordDataHeader.FlushValueIsChunkedObject(encoded), Is.False);
            Assert.That(RecordDataHeader.FlushValueHasOverflowHeader(encoded), Is.False);
            Assert.That(RecordDataHeader.FlushValueReadHintOrExact(encoded), Is.EqualTo(exact));
        }

        [Test]
        [Category("Smoke")]
        public void FlushEncodingsAreDistinguishable()
        {
            // Same low payload, different kind -> distinguished by the flag bits (23/22).
            const int payload = 12345;
            var chunked = RecordDataHeader.EncodeFlushChunkedObject(payload & 0xFFF, 0); // buffer count is 12-bit
            var ovf = RecordDataHeader.EncodeFlushOverflowHeader(payload);
            var hdrless = RecordDataHeader.EncodeFlushHeaderless(payload);

            Assert.That(RecordDataHeader.FlushValueIsChunkedObject(chunked), Is.True);

            Assert.That(RecordDataHeader.FlushValueIsChunkedObject(ovf), Is.False);
            Assert.That(RecordDataHeader.FlushValueHasOverflowHeader(ovf), Is.True);

            Assert.That(RecordDataHeader.FlushValueIsChunkedObject(hdrless), Is.False);
            Assert.That(RecordDataHeader.FlushValueHasOverflowHeader(hdrless), Is.False);
        }

        // ── v2.2 out-of-line VALUE encoding (12-bit: bits 0-9 payload, bit 10 hasHeader, bit 11 isExactSize) ──

        [Test]
        [Category("Smoke")]
        public void FlushExactSizeValueRoundTrips([Values(0, 1, 128, 510, 511)] int exactLength)
        {
            var encoded = RecordDataHeader.EncodeFlushOutOfLineValue(exactLength, totalOnDiskExtent: exactLength);
            Assert.That(encoded, Is.LessThanOrEqualTo((uint)RecordDataHeader.kValueLengthLowBitsMask), "must fit the ValueLength field");
            Assert.That(RecordDataHeader.FlushValueIsExactSize(encoded), Is.True);
            Assert.That(RecordDataHeader.FlushValueHasHeader(encoded), Is.False, "exact-size values are headerless");
            Assert.That(RecordDataHeader.FlushValueExactByteSize(encoded), Is.EqualTo(exactLength));
            Assert.That(RecordDataHeader.DecodeFlushValueInitialReadExtent(encoded), Is.EqualTo((ulong)exactLength));
        }

        [Test]
        [Category("Smoke")]
        public void FlushHeaderedValueBelowSentinelRoundTrips([Values(1024, 4096, 100000, 4000000)] int dataLength)
        {
            // Extent = header + data (no DMA padding in this unit test); page count = ceil(extent / 4 KB).
            var extent = ChunkHeader.TotalSize + (long)dataLength;
            var expectedPages = (int)((extent + RecordDataHeader.kFlushPageSize - 1) / RecordDataHeader.kFlushPageSize);
            Assume.That(expectedPages, Is.LessThan(RecordDataHeader.kOutOfLinePageSentinel), "test data must stay below the sentinel");

            var encoded = RecordDataHeader.EncodeFlushOutOfLineValue(dataLength, extent);
            Assert.That(RecordDataHeader.FlushValueIsExactSize(encoded), Is.False);
            Assert.That(RecordDataHeader.FlushValueHasHeader(encoded), Is.True);
            Assert.That(RecordDataHeader.FlushValuePageCountIsSentinel(encoded), Is.False);
            Assert.That(RecordDataHeader.FlushValuePageCount(encoded), Is.EqualTo(expectedPages));
            Assert.That(RecordDataHeader.DecodeFlushValueInitialReadExtent(encoded), Is.EqualTo((ulong)expectedPages * RecordDataHeader.kFlushPageSize));
        }

        [Test]
        [Category("Smoke")]
        public void FlushHeaderedValueAtOrAboveSentinelClampsToSentinel([Values(4186113, 4190208, 5000000, 16 * 1024 * 1024)] long extent)
        {
            // Any extent whose 4 KB-page count reaches 1023 clamps to the sentinel -> 4 MB-block reads.
            var encoded = RecordDataHeader.EncodeFlushOutOfLineValue(dataLength: extent, totalOnDiskExtent: extent);
            Assert.That(RecordDataHeader.FlushValueIsExactSize(encoded), Is.False);
            Assert.That(RecordDataHeader.FlushValueHasHeader(encoded), Is.True);
            Assert.That(RecordDataHeader.FlushValuePageCountIsSentinel(encoded), Is.True);
            Assert.That(RecordDataHeader.DecodeFlushValueInitialReadExtent(encoded), Is.EqualTo((ulong)IStreamBuffer.BufferSize));
        }

        [Test]
        [Category("Smoke")]
        public void FlushValueCutoffBoundary()
        {
            // The exact-size cutoff is kOutOfLineExactSizeCutoff (511): 511 bytes -> exact/headerless; 512 bytes -> headered page-count.
            // (The 10-bit exact-size field could hold up to 1023, but the writer caps exact at 511 because the exact length is being
            // relocated into the objectId slot's 9 top bits, whose max is 511.)
            var atCutoff = RecordDataHeader.EncodeFlushOutOfLineValue(RecordDataHeader.kOutOfLineExactSizeCutoff, RecordDataHeader.kOutOfLineExactSizeCutoff);
            Assert.That(RecordDataHeader.FlushValueIsExactSize(atCutoff), Is.True);
            Assert.That(RecordDataHeader.FlushValueExactByteSize(atCutoff), Is.EqualTo(RecordDataHeader.kOutOfLineExactSizeCutoff));

            var aboveCutoff = RecordDataHeader.EncodeFlushOutOfLineValue(RecordDataHeader.kOutOfLineExactSizeCutoff + 1, ChunkHeader.TotalSize + RecordDataHeader.kOutOfLineExactSizeCutoff + 1);
            Assert.That(RecordDataHeader.FlushValueIsExactSize(aboveCutoff), Is.False);
            Assert.That(RecordDataHeader.FlushValueHasHeader(aboveCutoff), Is.True);
            Assert.That(RecordDataHeader.FlushValuePageCount(aboveCutoff), Is.EqualTo(1));
        }

        [Test]
        [Category("Smoke")]
        public void FlushExactAndHeaderedAreDistinguishable()
        {
            // Same low-10-bit payload (100), different kind -> distinguished by the isExactSize/hasHeader flag bits.
            var exact = RecordDataHeader.EncodeFlushOutOfLineValue(100, 100);                                             // exact size 100
            var headered = RecordDataHeader.EncodeFlushOutOfLineValue(1024, 100L * RecordDataHeader.kFlushPageSize);      // 100 pages
            Assert.That(RecordDataHeader.FlushValueIsExactSize(exact), Is.True);
            Assert.That(RecordDataHeader.FlushValueIsExactSize(headered), Is.False);
            Assert.That(RecordDataHeader.FlushValueHasHeader(exact), Is.False);
            Assert.That(RecordDataHeader.FlushValueHasHeader(headered), Is.True);
            Assert.That(RecordDataHeader.FlushValueExactByteSize(exact), Is.EqualTo(100));
            Assert.That(RecordDataHeader.FlushValuePageCount(headered), Is.EqualTo(100));
        }
    }
}