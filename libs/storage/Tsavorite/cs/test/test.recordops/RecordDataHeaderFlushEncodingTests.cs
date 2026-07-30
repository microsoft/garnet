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
    }
}