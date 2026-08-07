// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.Objects
{
    /// <summary>
    /// Unit tests for the object-log out-of-line EXACT-SIZE primitives: the <see cref="ObjectLogFilePositionInfo"/> position-word
    /// Key/ValueIsExactSize flag bits, and the <see cref="ObjectIdMap"/> objectId-slot index/exact-size bit layout. An out-of-line
    /// component whose byte length is &lt;= <see cref="ObjectIdMap.MaxObjectIdExactSize"/> stores its exact length in the top bits of
    /// its objectId slot (no leading ChunkHeader), flagged in the record's position word. See
    /// website/docs/dev/objectlog-serialization.md.
    /// </summary>
    [TestFixture]
    internal class ObjectIdSlotAndPositionFlagsTests
    {
        // ── ObjectIdMap slot bit layout ──────────────────────────────────────────────────────────────────

        [Test]
        [Category("Smoke")]
        public void SlotLayoutConstantsAreConsistent()
        {
            Assert.That(ObjectIdMap.ObjectIdExactSizeShift, Is.EqualTo(ObjectIdMap.ObjectIdIndexBits));
            Assert.That(ObjectIdMap.ObjectIdIndexBits + ObjectIdMap.ObjectIdExactSizeBits, Is.EqualTo(sizeof(int) * 8));
            Assert.That(ObjectIdMap.ObjectIdIndexMask, Is.EqualTo((1 << ObjectIdMap.ObjectIdIndexBits) - 1));
            Assert.That(ObjectIdMap.MaxObjectIdExactSize, Is.EqualTo(511));
            // Index and exact-size fields are disjoint and cover all 32 bits.
            Assert.That(ObjectIdMap.ObjectIdIndexMask & (ObjectIdMap.ObjectIdExactSizeMask << ObjectIdMap.ObjectIdExactSizeShift), Is.EqualTo(0));
        }

        [Test]
        [Category("Smoke")]
        public void StampExactSizeRoundTrips(
            [Values(0, 1, 1000, 0x3FFFFF, 0x7FFFFE)] int index,
            [Values(0, 1, 255, 256, 510, 511)] int exactSize)
        {
            var stamped = ObjectIdMap.StampExactSize(index, exactSize);
            Assert.That(ObjectIdMap.GetIndex(stamped), Is.EqualTo(index), "index must survive the stamp");
            Assert.That(ObjectIdMap.GetExactSize(stamped), Is.EqualTo(exactSize), "exact size must survive the stamp");
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
            // An objectId slot that has not been exact-size-stamped (top bits clear) reads back its own index.
            Assert.That(ObjectIdMap.GetIndex(index), Is.EqualTo(index));
            Assert.That(ObjectIdMap.GetExactSize(index), Is.EqualTo(0));
        }

        [Test]
        [Category("Smoke")]
        public void StampExactSizeIsNonDestructiveToIndexAtBoundary()
        {
            // Max exact size on a mid-range index sets high bits but keeps the index recoverable and stays != -1.
            const int index = 0x123456;
            var stamped = ObjectIdMap.StampExactSize(index, ObjectIdMap.MaxObjectIdExactSize);
            Assert.That(stamped, Is.LessThan(0), "top-bit stamp makes the slot read as a negative int");
            Assert.That(ObjectIdMap.GetIndex(stamped), Is.EqualTo(index));
            Assert.That(ObjectIdMap.GetExactSize(stamped), Is.EqualTo(ObjectIdMap.MaxObjectIdExactSize));
        }

        // ── ObjectLogFilePositionInfo exact-size flag bits ───────────────────────────────────────────────

        [Test]
        [Category("Smoke")]
        public unsafe void KeyIsExactSizeFlagSetsAndClears()
        {
            ulong word = 0;
            Assert.That(ObjectLogFilePositionInfo.GetKeyIsExactSize(&word), Is.False);
            ObjectLogFilePositionInfo.SetKeyIsExactSize(&word);
            Assert.That(ObjectLogFilePositionInfo.GetKeyIsExactSize(&word), Is.True);
            Assert.That(word, Is.EqualTo(ObjectLogFilePositionInfo.kKeyIsExactSizeMask));
        }

        [Test]
        [Category("Smoke")]
        public unsafe void ValueIsExactSizeFlagSetsAndClears()
        {
            ulong word = 0;
            Assert.That(ObjectLogFilePositionInfo.GetValueIsExactSize(&word), Is.False);
            ObjectLogFilePositionInfo.SetValueIsExactSize(&word);
            Assert.That(ObjectLogFilePositionInfo.GetValueIsExactSize(&word), Is.True);
            Assert.That(word, Is.EqualTo(ObjectLogFilePositionInfo.kValueIsExactSizeMask));
        }

        [Test]
        [Category("Smoke")]
        public unsafe void ExactSizeFlagsAreIndependentAndDoNotDisturbOtherBits()
        {
            // Start with a realistic segment+offset payload plus the (bit-63) ReuseObjectIdForSize flag set.
            ulong segmentAndOffset = 0x0ABCDEF012345UL & ObjectLogFilePositionInfo.SegmentAndOffsetMask;
            ulong word = segmentAndOffset | ObjectLogFilePositionInfo.kReuseObjectIdForSizeMask;

            ObjectLogFilePositionInfo.SetKeyIsExactSize(&word);
            ObjectLogFilePositionInfo.SetValueIsExactSize(&word);

            Assert.That(ObjectLogFilePositionInfo.GetKeyIsExactSize(&word), Is.True);
            Assert.That(ObjectLogFilePositionInfo.GetValueIsExactSize(&word), Is.True);
            Assert.That(ObjectLogFilePositionInfo.GetReuseObjectIdForSize(&word), Is.True, "existing flag preserved");
            // The segment+offset payload is untouched by the flag bits.
            Assert.That(word & ObjectLogFilePositionInfo.SegmentAndOffsetMask, Is.EqualTo(segmentAndOffset));
        }

        [Test]
        [Category("Smoke")]
        public void ExactSizeFlagBitsAreDistinctAndAboveTheSegmentOffsetRange()
        {
            Assert.That(ObjectLogFilePositionInfo.kKeyIsExactSizeMask, Is.Not.EqualTo(ObjectLogFilePositionInfo.kValueIsExactSizeMask));
            // Both flags live above the 60-bit segment+offset range and below the bit-63 ReuseObjectIdForSize flag.
            Assert.That(ObjectLogFilePositionInfo.kKeyIsExactSizeMask & ObjectLogFilePositionInfo.SegmentAndOffsetMask, Is.EqualTo(0UL));
            Assert.That(ObjectLogFilePositionInfo.kValueIsExactSizeMask & ObjectLogFilePositionInfo.SegmentAndOffsetMask, Is.EqualTo(0UL));
            Assert.That(ObjectLogFilePositionInfo.kKeyIsExactSizeMask & ObjectLogFilePositionInfo.kReuseObjectIdForSizeMask, Is.EqualTo(0UL));
            Assert.That(ObjectLogFilePositionInfo.kValueIsExactSizeMask & ObjectLogFilePositionInfo.kReuseObjectIdForSizeMask, Is.EqualTo(0UL));
            // And distinct from the remaining reserved bit.
            Assert.That(ObjectLogFilePositionInfo.kKeyIsExactSizeMask & ObjectLogFilePositionInfo.kReservedFlagsMask, Is.EqualTo(0UL));
            Assert.That(ObjectLogFilePositionInfo.kValueIsExactSizeMask & ObjectLogFilePositionInfo.kReservedFlagsMask, Is.EqualTo(0UL));
        }
    }
}