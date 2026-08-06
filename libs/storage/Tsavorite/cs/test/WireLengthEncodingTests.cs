// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// Unit tests for the migration/replication WIRE encoding of an out-of-line length into a record's 4-byte objectId slot
    /// (<see cref="LogRecord.EncodeWireValueObjectSlot"/>, written by <see cref="LogRecord.SetWireOutOfLineLengths"/> /
    /// <see cref="LogRecord.SetWireValueObjectLength"/>). The wire format carries out-of-line lengths in the objectId slot
    /// instead of the RDH KeyLength/ValueLength fields, which stay untouched. An object VALUE's slot uses
    /// <see cref="ChunkedRecordConstants.ContinuationFlag"/> as its high bit: clear when the whole object fits the low 31 bits
    /// (the receiver uses the value directly), set when the object streams as multiple chunks (the slot then holds the
    /// first-chunk length and the receiver derives the total from the record stream). Overflow key/value lengths ride in the
    /// same slot as a plain <see cref="int"/>; a .NET <c>byte[]</c> can never reach the continuation bit, so those always read
    /// back cleanly.
    /// </summary>
    [TestFixture]
    internal class WireLengthEncodingTests
    {
        const int ContinuationFlag = ChunkedRecordConstants.ContinuationFlag; // 0x80000000
        const int LengthMask = ~ContinuationFlag;                            // 0x7FFFFFFF

        static bool Continues(int slot) => (slot & ContinuationFlag) != 0;
        static int LengthBits(int slot) => slot & LengthMask;

        [Test]
        [Category("TsavoriteLog")]
        public void WireObjectSlot_WholeObject_NoContinuation([Values(1, 1024, 96 * 1024, int.MaxValue)] int totalLength)
        {
            // Object length fits the slot's 31 bits => exact length stored, continuation clear, first-chunk hint ignored.
            var slot = LogRecord.EncodeWireValueObjectSlot(totalLength, firstChunkLength: 7);
            ClassicAssert.IsFalse(Continues(slot), "whole object that fits 31 bits must not set the continuation flag");
            ClassicAssert.AreEqual(totalLength, slot, "slot must hold the exact object length");
            ClassicAssert.AreEqual(totalLength, LengthBits(slot));
        }

        [Test]
        [Category("TsavoriteLog")]
        public void WireObjectSlot_StreamedObject_SetsContinuationAndFirstChunk(
            [Values(int.MaxValue + 1L, 3L * 1024 * 1024 * 1024, 6L * 1024 * 1024 * 1024)] long totalLength)
        {
            // Object exceeds int.MaxValue => streams as chunks: slot holds the first-chunk length with continuation set, and the
            // receiver derives the total object length from the summed chunks (not from this slot).
            const int firstChunkLength = 1 * 1024 * 1024;
            var slot = LogRecord.EncodeWireValueObjectSlot(totalLength, firstChunkLength);
            ClassicAssert.IsTrue(Continues(slot), "object larger than int.MaxValue must set the continuation flag");
            ClassicAssert.AreEqual(firstChunkLength, LengthBits(slot), "the low 31 bits must hold the first-chunk length");
        }

        [Test]
        [Category("TsavoriteLog")]
        public void WireOverflowLength_NeverReachesContinuationBit()
        {
            // Overflow key/value lengths ride in the slot as a plain int. A .NET byte[] tops out below the continuation bit, so
            // an overflow length always reads back as itself (the receiver reads it as a plain int, no accidental continuation).
            ClassicAssert.IsTrue((uint)Array.MaxLength < unchecked((uint)ContinuationFlag),
                "byte[] max length must stay below the continuation flag so overflow lengths read back cleanly");
            ClassicAssert.IsFalse(Continues(Array.MaxLength));
            ClassicAssert.AreEqual(Array.MaxLength, LengthBits(Array.MaxLength));
        }
    }
}