// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.Objects
{
    /// <summary>
    /// Unit tests for the object-log out-of-line size-hint primitives: the <see cref="ObjectLogPositionInfo"/> position-word
    /// Key/ValueIsExactSize flag bits, and the <see cref="ObjectIdMap"/> objectId-slot index/size-hint bit layout. An out-of-line
    /// component whose byte length is &lt;= <see cref="ObjectIdMap.MaxObjectIdSizeHint"/> stores its exact length in the top bits of
    /// its objectId slot (no leading ChunkHeader), flagged in the record's position word. Larger keys combine those bits with raw
    /// RDH KeyLength to encode an exact page count; values retain the objectId-only sentinel scheme. See
    /// website/docs/dev/tsavorite/objectlog-serialization.md.
    /// </summary>
    [TestFixture]
    internal class ObjectIdSlotAndPositionFlagsTests
    {
        // ── ObjectIdMap slot bit layout ──────────────────────────────────────────────────────────────────

        [Test]
        [Category("Smoke")]
        public void SlotLayoutConstantsAreConsistent()
        {
            Assert.That(ObjectIdMap.ObjectIdSizeHintShift, Is.EqualTo(ObjectIdMap.ObjectIdIndexBits));
            Assert.That(ObjectIdMap.ObjectIdIndexBits + ObjectIdMap.ObjectIdSizeHintBits, Is.EqualTo(sizeof(int) * 8));
            Assert.That(ObjectIdMap.ObjectIdIndexMask, Is.EqualTo((1 << ObjectIdMap.ObjectIdIndexBits) - 1));
            Assert.That(ObjectIdMap.MaxObjectIdSizeHint, Is.EqualTo(511));
            // Index and size-hint fields are disjoint and cover all 32 bits.
            Assert.That(ObjectIdMap.ObjectIdIndexMask & (ObjectIdMap.ObjectIdSizeHintMask << ObjectIdMap.ObjectIdSizeHintShift), Is.EqualTo(0));
        }

        [Test]
        [Category("Smoke")]
        public void StampSizeHintRoundTrips(
            [Values(0, 1, 1000, 0x3FFFFF, 0x7FFFFE)] int index,
            [Values(0, 1, 255, 256, 510, 511)] int sizeHint)
        {
            var stamped = ObjectIdMap.StampSizeHint(index, sizeHint);
            Assert.That(ObjectIdMap.GetIndex(stamped), Is.EqualTo(index), "index must survive the stamp");
            Assert.That(ObjectIdMap.GetSizeHint(stamped), Is.EqualTo(sizeHint), "size hint must survive the stamp");
            Assert.That(stamped, Is.Not.EqualTo(ObjectIdMap.InvalidObjectId), "a stamped in-range slot never collides with InvalidObjectId");
        }

        [Test]
        [Category("Smoke")]
        public void GetIndexPassesInvalidObjectIdThrough()
        {
            Assert.That(ObjectIdMap.GetIndex(ObjectIdMap.InvalidObjectId), Is.EqualTo(ObjectIdMap.InvalidObjectId));
        }

        [Test]
        [Category("Smoke")]
        public void GetIndexOnUnstampedSlotIsIdentity([Values(0, 1, 42, 0x3FFFFF, 0x7FFFFF)] int index)
        {
            // An objectId slot that has not been size-hint-stamped (top bits clear) reads back its own index.
            Assert.That(ObjectIdMap.GetIndex(index), Is.EqualTo(index));
            Assert.That(ObjectIdMap.GetSizeHint(index), Is.EqualTo(0));
        }

        [Test]
        [Category("Smoke")]
        public void StampSizeHintIsNonDestructiveToIndexAtBoundary()
        {
            // Max size hint on a mid-range index sets high bits but keeps the index recoverable and stays != -1.
            const int index = 0x123456;
            var stamped = ObjectIdMap.StampSizeHint(index, ObjectIdMap.MaxObjectIdSizeHint);
            Assert.That(stamped, Is.LessThan(0), "top-bit stamp makes the slot read as a negative int");
            Assert.That(ObjectIdMap.GetIndex(stamped), Is.EqualTo(index));
            Assert.That(ObjectIdMap.GetSizeHint(stamped), Is.EqualTo(ObjectIdMap.MaxObjectIdSizeHint));
        }

        [Test]
        [Category("Smoke")]
        public void DecodeExactSizeReturnsExactBytes([Values(0, 1, 128, 510, 511)] int sizeHint)
            => Assert.That(RecordDataHeader.DecodeObjectIdValueInitialReadExtent(sizeHint, isExactSize: true), Is.EqualTo((ulong)sizeHint));

        [Test]
        [Category("Smoke")]
        public void DecodePageCountReturnsPageExtent([Values(1, 2, 64, 128, 510)] int pageCount)
            => Assert.That(RecordDataHeader.DecodeObjectIdValueInitialReadExtent(pageCount, isExactSize: false),
                Is.EqualTo((ulong)(uint)pageCount * RecordDataHeader.kFlushPageSize));

        [Test]
        [Category("Smoke")]
        public void DecodeSentinelReturnsOneDiscoveryWindow()
            => Assert.That(RecordDataHeader.DecodeObjectIdValueInitialReadExtent(ObjectIdMap.MaxObjectIdSizeHint, isExactSize: false),
                Is.EqualTo((ulong)IStreamBuffer.BufferSize));

        [TestCase(0, 0, true, 0UL)]
        [TestCase(4, 511, true, 511UL)]
        [TestCase(0, 1, false, 1UL * RecordDataHeader.kFlushPageSize)]
        [TestCase(0, 511, false, 511UL * RecordDataHeader.kFlushPageSize)]
        [TestCase(1, 0, false, 512UL * RecordDataHeader.kFlushPageSize)]
        [TestCase(1, 1, false, 513UL * RecordDataHeader.kFlushPageSize)]
        [TestCase(1023, 511, false, (ulong)RecordDataHeader.kMaxOverflowKeyPageCount * RecordDataHeader.kFlushPageSize)]
        public void DecodeOverflowKeyPageCountUsesRdhHighBits(int rdhKeyLengthBits, int objectIdHint, bool isExact, ulong expected)
            => Assert.That(RecordDataHeader.DecodeOverflowKeyInitialReadExtent(rdhKeyLengthBits, objectIdHint, isExact), Is.EqualTo(expected));

        [TestCase(0, 0, 0, 4, true)]
        [TestCase(511, 511, 511, 4, true)]
        [TestCase(512, 520, 1, 0, false)]
        [TestCase((510 * RecordDataHeader.kFlushPageSize) - ChunkHeader.TotalSize, 510 * RecordDataHeader.kFlushPageSize, 510, 0, false)]
        [TestCase((511 * RecordDataHeader.kFlushPageSize) - ChunkHeader.TotalSize, 511 * RecordDataHeader.kFlushPageSize, 511, 0, false)]
        [TestCase((512 * RecordDataHeader.kFlushPageSize) - ChunkHeader.TotalSize, 512 * RecordDataHeader.kFlushPageSize, 0, 1, false)]
        [TestCase((513 * RecordDataHeader.kFlushPageSize) - ChunkHeader.TotalSize, 513 * RecordDataHeader.kFlushPageSize, 1, 1, false)]
        public void ComputeOverflowKeyPageCountRoundTrips(int dataLength, int extent, int expectedObjectIdHint, int expectedRdhBits, bool expectedExact)
        {
            var hint = RecordDataHeader.ComputeOverflowKeySizeHint(dataLength, extent, out var rdhBits, out var isExact);

            Assert.That(hint, Is.EqualTo(expectedObjectIdHint));
            Assert.That(rdhBits, Is.EqualTo(expectedRdhBits));
            Assert.That(isExact, Is.EqualTo(expectedExact));
            Assert.That(RecordDataHeader.DecodeOverflowKeyInitialReadExtent(rdhBits, hint, isExact),
                Is.EqualTo(isExact ? (ulong)dataLength : (ulong)RoundUpToPage(extent)));
        }

        [Test]
        public unsafe void DeserializationErrorNormalizesOverflowKeyLengthBeforeMakingItInline()
        {
            var storage = stackalloc byte[64];
            new Span<byte>(storage, 64).Clear();
            var record = new LogRecord(storage, new ObjectIdMap());
            RecordDataHeader dataHeader = default;
            dataHeader.SetKeyIsOverflow();
            dataHeader.KeyLength = 1; // Simulate the high page-count bits of a key above the 9-bit range.
            dataHeader.SetValueIsInline();
            dataHeader.ValueLength = 0;
            record.SetDataHeader(dataHeader);

            record.OnDeserializationError(keyWasSet: false);

            Assert.That(record.DataHeader.KeyIsInline, Is.True);
            Assert.That(record.DataHeader.GetKeyLengthRaw(), Is.EqualTo(ObjectIdMap.ObjectIdSize));
            Assert.That(record.DataHeader.KeyLength, Is.EqualTo(ObjectIdMap.ObjectIdSize));
        }

        [Test]
        public void OverflowKeyPageCountCoversMaximumConfiguredKeyAndAlignment()
        {
            const long maxConfiguredKeyLength = 1L << LogSettings.kMaxStringSizeBits;
            const int maxSectorAlignmentPadding = (1 << 16) - 1;
            var extent = maxConfiguredKeyLength + ChunkHeader.TotalSize + maxSectorAlignmentPadding;

            var hint = RecordDataHeader.ComputeOverflowKeySizeHint(maxConfiguredKeyLength, extent, out var rdhBits, out var isExact);
            var decodedExtent = RecordDataHeader.DecodeOverflowKeyInitialReadExtent(rdhBits, hint, isExact);

            Assert.That(isExact, Is.False);
            Assert.That(decodedExtent, Is.GreaterThanOrEqualTo((ulong)extent));
            Assert.That(decodedExtent - (ulong)extent, Is.LessThan((ulong)RecordDataHeader.kFlushPageSize));
            Assert.That(rdhBits, Is.LessThanOrEqualTo((1 << RecordDataHeader.kKeyLengthBits) - 1));
        }

        static int RoundUpToPage(int length)
            => ((length + RecordDataHeader.kFlushPageSize - 1) / RecordDataHeader.kFlushPageSize) * RecordDataHeader.kFlushPageSize;

        // ── ObjectLogPositionInfo exact-size flag bits ───────────────────────────────────────────────

        [Test]
        [Category("Smoke")]
        public unsafe void KeyIsExactSizeFlagSetsAndClears()
        {
            ulong word = 0;
            Assert.That(ObjectLogPositionInfo.GetKeyIsExactSize(&word), Is.False);
            ObjectLogPositionInfo.SetKeyIsExactSize(&word);
            Assert.That(ObjectLogPositionInfo.GetKeyIsExactSize(&word), Is.True);
            Assert.That(word, Is.EqualTo(ObjectLogPositionInfo.kKeyIsExactSizeMask));
        }

        [Test]
        [Category("Smoke")]
        public unsafe void ValueIsExactSizeFlagSetsAndClears()
        {
            ulong word = 0;
            Assert.That(ObjectLogPositionInfo.GetValueIsExactSize(&word), Is.False);
            ObjectLogPositionInfo.SetValueIsExactSize(&word);
            Assert.That(ObjectLogPositionInfo.GetValueIsExactSize(&word), Is.True);
            Assert.That(word, Is.EqualTo(ObjectLogPositionInfo.kValueIsExactSizeMask));
        }

        [Test]
        [Category("Smoke")]
        public unsafe void ExactSizeFlagsAreIndependentAndDoNotDisturbOtherBits()
        {
            // Start with a realistic segment+offset payload plus the (bit-63) unused flag set.
            ulong segmentAndOffset = 0x0ABCDEF012345UL & ObjectLogPositionInfo.AddressMask;
            ulong word = segmentAndOffset | ObjectLogPositionInfo.kUnusedBit63Mask;

            ObjectLogPositionInfo.SetKeyIsExactSize(&word);
            ObjectLogPositionInfo.SetValueIsExactSize(&word);

            Assert.That(ObjectLogPositionInfo.GetKeyIsExactSize(&word), Is.True);
            Assert.That(ObjectLogPositionInfo.GetValueIsExactSize(&word), Is.True);
            Assert.That(word & ObjectLogPositionInfo.kUnusedBit63Mask, Is.Not.EqualTo(0UL), "existing flag preserved");
            // The segment+offset payload is untouched by the flag bits.
            Assert.That(word & ObjectLogPositionInfo.AddressMask, Is.EqualTo(segmentAndOffset));
        }

        [Test]
        [Category("Smoke")]
        public void ExactSizeFlagBitsAreDistinctAndAboveTheAddressRange()
        {
            Assert.That(ObjectLogPositionInfo.kKeyIsExactSizeMask, Is.Not.EqualTo(ObjectLogPositionInfo.kValueIsExactSizeMask));
            // Both flags live above the 60-bit address range and below the unused bit 63.
            Assert.That(ObjectLogPositionInfo.kKeyIsExactSizeMask & ObjectLogPositionInfo.AddressMask, Is.EqualTo(0UL));
            Assert.That(ObjectLogPositionInfo.kValueIsExactSizeMask & ObjectLogPositionInfo.AddressMask, Is.EqualTo(0UL));
            Assert.That(ObjectLogPositionInfo.kKeyIsExactSizeMask & ObjectLogPositionInfo.kUnusedBit63Mask, Is.EqualTo(0UL));
            Assert.That(ObjectLogPositionInfo.kValueIsExactSizeMask & ObjectLogPositionInfo.kUnusedBit63Mask, Is.EqualTo(0UL));
        }

        /// <summary>
        /// <c>HasDirectory</c> must be readable without consulting any other part of the word, because the reader uses it to learn an
        /// object's shape before issuing any IO. It therefore cannot collide with the address, the exact-size flags, or the remaining
        /// spare bit, and it must survive unstamping, which rewrites only the address bits.
        /// </summary>
        [Test]
        [Category("Smoke")]
        public unsafe void HasDirectoryIsIndependentOfEveryOtherFieldOfThePositionWord()
        {
            foreach (var otherMask in new[] { ObjectLogPositionInfo.AddressMask, ObjectLogPositionInfo.kKeyIsExactSizeMask,
                                              ObjectLogPositionInfo.kValueIsExactSizeMask, ObjectLogPositionInfo.kUnusedBit63Mask })
                Assert.That(ObjectLogPositionInfo.kHasDirectoryMask & otherMask, Is.EqualTo(0UL));

            // Setting it leaves a realistic address and the other flags alone.
            var address = 0x0ABCDEF012345UL & ObjectLogPositionInfo.AddressMask;
            var word = address | ObjectLogPositionInfo.kUnusedBit63Mask;
            ObjectLogPositionInfo.SetKeyIsExactSize(&word);
            ObjectLogPositionInfo.SetHasDirectory(&word);

            Assert.That(ObjectLogPositionInfo.GetHasDirectory(&word), Is.True);
            Assert.That(ObjectLogPositionInfo.WordHasDirectory(word), Is.True);
            Assert.That(ObjectLogPositionInfo.GetKeyIsExactSize(&word), Is.True);
            Assert.That(ObjectLogPositionInfo.GetValueIsExactSize(&word), Is.False);
            Assert.That(word & ObjectLogPositionInfo.AddressMask, Is.EqualTo(address));
            Assert.That(word & ObjectLogPositionInfo.kUnusedBit63Mask, Is.Not.EqualTo(0UL));

            // Unstamping rewrites only the address bits, so a stamped directory flag survives it.
            ObjectLogPositionInfo.Unstamp(&word);
            Assert.That(ObjectLogPositionInfo.IsUnstamped(&word), Is.True);
            Assert.That(ObjectLogPositionInfo.GetHasDirectory(&word), Is.True);
        }

        [Test]
        [Category("Smoke")]
        public unsafe void Bit63RemainsAvailableAndDistinct()
        {
            // The remaining spare flag bit must not collide with any in-use field, so it stays available for a format discriminator.
            Assert.That(ObjectLogPositionInfo.kUnusedBit63Mask & ObjectLogPositionInfo.AddressMask, Is.EqualTo(0UL));
            Assert.That(ObjectLogPositionInfo.kUnusedBit63Mask & ObjectLogPositionInfo.kKeyIsExactSizeMask, Is.EqualTo(0UL));
            Assert.That(ObjectLogPositionInfo.kUnusedBit63Mask & ObjectLogPositionInfo.kValueIsExactSizeMask, Is.EqualTo(0UL));
            Assert.That(ObjectLogPositionInfo.kUnusedBit63Mask & ObjectLogPositionInfo.kHasDirectoryMask, Is.EqualTo(0UL));

            // An unstamped word leaves the spare bit untouched, so stamping it survives unstamping.
            ulong word = 0;
            ObjectLogPositionInfo.Unstamp(&word);
            Assert.That(word & ObjectLogPositionInfo.kUnusedBit63Mask, Is.EqualTo(0UL));
            Assert.That(word & ObjectLogPositionInfo.kHasDirectoryMask, Is.EqualTo(0UL));
        }

        [TestCase(50UL, 3, 4050UL)]
        [TestCase(96UL, 4, 0UL)]
        [TestCase(100UL, 4, 4UL)]
        [TestCase(8296UL, 6, 8UL)]
        public void PositionAdvanceHandlesSegmentBoundaries(ulong distance, int expectedSegment, ulong expectedOffset)
        {
            const int segmentBits = 12;
            ObjectLogPositionInfo position = new(((ulong)3 << segmentBits) | 4000, segmentBits);

            position.Advance(distance);

            Assert.That(position.SegmentId, Is.EqualTo(expectedSegment));
            Assert.That(position.Offset, Is.EqualTo(expectedOffset));
        }

        [Test]
        public void PositionSubtractionRejectsReversedPositions()
        {
            ObjectLogPositionInfo earlier = new(100, 12);
            ObjectLogPositionInfo later = new(200, 12);

            Assert.Throws<System.IO.InvalidDataException>(() => _ = earlier - later);
        }

        [Test]
        public void PositionSubtractionRejectsDifferentSegmentSizes()
        {
            ObjectLogPositionInfo left = new(200, 12);
            ObjectLogPositionInfo right = new(100, 13);

            Assert.Throws<System.IO.InvalidDataException>(() => _ = left - right);
        }
    }
}