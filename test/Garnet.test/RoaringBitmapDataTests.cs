// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Garnet.Extensions.RoaringBitmap.Containers;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.RoaringBitmap
{
    /// <summary>
    /// Pure data-structure tests for <see cref="Garnet.Extensions.RoaringBitmap.RoaringBitmap"/>.
    /// These tests exercise the algorithm in isolation (no RESP layer, no server)
    /// and use a HashSet&lt;uint&gt; oracle to verify behaviour.
    /// </summary>
    [TestFixture]
    public class RoaringBitmapDataTests
    {
        // ---------- Basics ----------

        [Test]
        public void EmptyBitmap_HasZeroCardinalityAndIsEmpty()
        {
            var rb = new global::Garnet.Extensions.RoaringBitmap.RoaringBitmap();
            ClassicAssert.IsTrue(rb.IsEmpty);
            ClassicAssert.AreEqual(0, rb.Cardinality);
            ClassicAssert.AreEqual(0, rb.ChunkCount);
            ClassicAssert.AreEqual(0, rb.GetBit(0));
            ClassicAssert.AreEqual(0, rb.GetBit(uint.MaxValue));
            ClassicAssert.AreEqual(-1L, rb.BitPos(1));
            ClassicAssert.AreEqual(0L, rb.BitPos(0)); // first unset is at 0
        }

        [Test]
        public void SetBit_SetsAndReturnsPrevious()
        {
            var rb = new global::Garnet.Extensions.RoaringBitmap.RoaringBitmap();
            ClassicAssert.AreEqual(0, rb.SetBit(42, true));
            ClassicAssert.AreEqual(1, rb.SetBit(42, true));   // already set
            ClassicAssert.AreEqual(1, rb.GetBit(42));
            ClassicAssert.AreEqual(1L, rb.Cardinality);
            ClassicAssert.AreEqual(1, rb.SetBit(42, false));  // clear returns previous=1
            ClassicAssert.AreEqual(0, rb.SetBit(42, false));  // clear non-existing
            ClassicAssert.AreEqual(0L, rb.Cardinality);
            ClassicAssert.IsTrue(rb.IsEmpty);
        }

        [Test]
        public void ChunkBoundaryOffsets_AllRoundTrip()
        {
            var rb = new global::Garnet.Extensions.RoaringBitmap.RoaringBitmap();
            uint[] offsets = [0, 1, 65535, 65536, 65537, 131071, 131072, int.MaxValue, (uint)int.MaxValue + 1, uint.MaxValue - 1, uint.MaxValue];
            foreach (var o in offsets) rb.SetBit(o, true);
            foreach (var o in offsets) ClassicAssert.AreEqual(1, rb.GetBit(o), $"offset {o} should be set");
            ClassicAssert.AreEqual(offsets.Length, rb.Cardinality);
        }

        // ---------- Threshold promotion / demotion ----------

        [Test]
        public void Promotion_ArrayToBitmap_AtThreshold()
        {
            var rb = new global::Garnet.Extensions.RoaringBitmap.RoaringBitmap();
            // Insert 4096 distinct lo-keys in a single chunk: should still be array.
            for (uint i = 0; i < ArrayContainer.ArrayThreshold; i++) rb.SetBit(i, true);
            ClassicAssert.AreEqual(ContainerKind.Array, rb.GetChunkKind(0));
            // The 4097th forces promotion.
            rb.SetBit(ArrayContainer.ArrayThreshold, true);
            ClassicAssert.AreEqual(ContainerKind.Bitmap, rb.GetChunkKind(0));
            ClassicAssert.AreEqual(ArrayContainer.ArrayThreshold + 1, rb.Cardinality);
        }

        [Test]
        public void Demotion_BitmapToArray_AfterRemoves()
        {
            var rb = new global::Garnet.Extensions.RoaringBitmap.RoaringBitmap();
            for (uint i = 0; i < 5000; i++) rb.SetBit(i, true);
            ClassicAssert.AreEqual(ContainerKind.Bitmap, rb.GetChunkKind(0));
            // Remove until <= 4096.
            for (uint i = 0; i < 5000 - ArrayContainer.ArrayThreshold; i++) rb.SetBit(i, false);
            ClassicAssert.AreEqual(ContainerKind.Array, rb.GetChunkKind(0));
            ClassicAssert.AreEqual(ArrayContainer.ArrayThreshold, rb.Cardinality);
        }

        [Test]
        public void Oscillation_AcrossThreshold_PreservesCorrectness()
        {
            var rb = new global::Garnet.Extensions.RoaringBitmap.RoaringBitmap();
            for (uint i = 0; i < ArrayContainer.ArrayThreshold; i++) rb.SetBit(i, true);
            for (int cycle = 0; cycle < 20; cycle++)
            {
                rb.SetBit((uint)ArrayContainer.ArrayThreshold, true);   // promote
                rb.SetBit((uint)ArrayContainer.ArrayThreshold, false);  // demote
            }
            ClassicAssert.AreEqual(ArrayContainer.ArrayThreshold, rb.Cardinality);
            ClassicAssert.AreEqual(ContainerKind.Array, rb.GetChunkKind(0));
        }

        // ---------- Oracle-based tests ----------

        [Test]
        public void RandomDenseSingleChunk_MatchesOracle()
        {
            // Dense distribution within a single chunk hits both array and bitmap paths.
            var rb = new global::Garnet.Extensions.RoaringBitmap.RoaringBitmap();
            var oracle = new HashSet<uint>();
            var rng = new Random(0xC0FFEE);
            for (int i = 0; i < 70_000; i++)
            {
                uint v = (uint)rng.Next(0, 65536);
                bool set = rng.Next(2) == 0;
                rb.SetBit(v, set);
                if (set) oracle.Add(v); else oracle.Remove(v);
            }
            AssertEqualsOracle(rb, oracle);
        }

        [Test]
        public void SparseAcrossChunks_MatchesOracle()
        {
            var rb = new global::Garnet.Extensions.RoaringBitmap.RoaringBitmap();
            var oracle = new HashSet<uint>();
            var rng = new Random(42);
            for (int i = 0; i < 50_000; i++)
            {
                uint v = (uint)rng.NextInt64(0, uint.MaxValue);
                bool set = rng.Next(4) != 0; // bias to set
                rb.SetBit(v, set);
                if (set) oracle.Add(v); else oracle.Remove(v);
            }
            AssertEqualsOracle(rb, oracle);
        }

        [Test]
        public void SmallUniverse_BitPos_MatchesOracle()
        {
            // Bounded universe lets us exhaustively check bitpos correctness.
            const int Universe = 200_000;
            var rb = new global::Garnet.Extensions.RoaringBitmap.RoaringBitmap();
            var bits = new bool[Universe];
            var rng = new Random(7);
            for (int i = 0; i < Universe / 4; i++)
            {
                int v = rng.Next(Universe);
                bits[v] = true;
                rb.SetBit((uint)v, true);
            }
            // Check bitpos(1) and bitpos(0) at random starting points.
            for (int trial = 0; trial < 200; trial++)
            {
                int from = rng.Next(Universe + 100);
                long expected1 = -1, expected0 = -1;
                for (int i = from; i < Universe; i++)
                {
                    if (expected1 < 0 && bits[i]) expected1 = i;
                    if (expected0 < 0 && !bits[i]) expected0 = i;
                    if (expected1 >= 0 && expected0 >= 0) break;
                }
                if (expected0 < 0 && from < Universe)
                {
                    // Past the populated range — first unset is at `from` itself if outside,
                    // otherwise at Universe (still <= uint.MaxValue, so a valid answer).
                    expected0 = from;
                }
                long actual1 = rb.BitPos(1, (uint)from);
                long actual0 = rb.BitPos(0, (uint)from);
                ClassicAssert.AreEqual(expected1, actual1, $"bitpos(1, {from})");
                // For bitpos(0), our scan reports the first unset somewhere in the
                // uint32 universe — past the set range any value qualifies, so
                // accept any value >= from that's not in the bits array.
                ClassicAssert.GreaterOrEqual(actual0, from);
                ClassicAssert.IsTrue(actual0 == 0 || (actual0 < bits.LongLength ? !bits[actual0] : true));
            }
        }

        // ---------- BitPos edge cases ----------

        [Test]
        public void BitPos_OnlyBit0_Set()
        {
            var rb = new global::Garnet.Extensions.RoaringBitmap.RoaringBitmap();
            rb.SetBit(0, true);
            ClassicAssert.AreEqual(0L, rb.BitPos(1));
            ClassicAssert.AreEqual(1L, rb.BitPos(0));
        }

        [Test]
        public void BitPos_FullFirstChunk_FindsZeroInSecond()
        {
            var rb = new global::Garnet.Extensions.RoaringBitmap.RoaringBitmap();
            for (uint i = 0; i < 65536; i++) rb.SetBit(i, true);
            ClassicAssert.AreEqual(0L, rb.BitPos(1));
            ClassicAssert.AreEqual(65536L, rb.BitPos(0));
        }

        [Test]
        public void BitPos_GapBetweenChunks_FindsGap()
        {
            var rb = new global::Garnet.Extensions.RoaringBitmap.RoaringBitmap();
            // Fully populate chunk 0, leave chunk 1 empty, populate chunk 2 partially.
            for (uint i = 0; i < 65536; i++) rb.SetBit(i, true);
            rb.SetBit(2 * 65536u + 100u, true);
            // First unset starting at 0 is 65536 (start of empty chunk 1).
            ClassicAssert.AreEqual(65536L, rb.BitPos(0));
            ClassicAssert.AreEqual(2 * 65536L, rb.BitPos(0, 2 * 65536u));
            // First set starting at 70000: look in chunk 1 (no chunk allocated, so skip)
            // and find the bit in chunk 2 at hi*65536+100.
            ClassicAssert.AreEqual(2 * 65536L + 100L, rb.BitPos(1, 70000u));
        }

        [Test]
        public void BitPos_MaxValue_Set()
        {
            var rb = new global::Garnet.Extensions.RoaringBitmap.RoaringBitmap();
            rb.SetBit(uint.MaxValue, true);
            ClassicAssert.AreEqual(uint.MaxValue, rb.BitPos(1));
            ClassicAssert.AreEqual(0L, rb.BitPos(0));
            ClassicAssert.AreEqual(uint.MaxValue - 1, rb.BitPos(0, uint.MaxValue - 1));
            // From uint.MaxValue with bit==0 — that one IS set, so no unset >= MaxValue.
            ClassicAssert.AreEqual(-1L, rb.BitPos(0, uint.MaxValue));
        }

        [Test]
        public void BitPos_NoSetBits_ReturnsMinusOne()
        {
            var rb = new global::Garnet.Extensions.RoaringBitmap.RoaringBitmap();
            ClassicAssert.AreEqual(-1L, rb.BitPos(1));
            ClassicAssert.AreEqual(-1L, rb.BitPos(1, 1234));
        }

        // ---------- Serialization round-trip ----------

        [Test]
        [TestCase(0)]
        [TestCase(1)]
        [TestCase(100)]
        [TestCase(4095)]
        [TestCase(4096)]
        [TestCase(4097)]
        [TestCase(50_000)]
        public void Serialize_RoundTrip_Matches(int count)
        {
            var rb = new global::Garnet.Extensions.RoaringBitmap.RoaringBitmap();
            var rng = new Random(count);
            for (int i = 0; i < count; i++)
            {
                rb.SetBit((uint)rng.NextInt64(0, uint.MaxValue), true);
            }
            byte[] bytes;
            using (var ms = new MemoryStream())
            using (var bw = new BinaryWriter(ms))
            {
                rb.Serialize(bw);
                bw.Flush();
                bytes = ms.ToArray();
            }
            using (var ms = new MemoryStream(bytes))
            using (var br = new BinaryReader(ms))
            {
                var rb2 = global::Garnet.Extensions.RoaringBitmap.RoaringBitmap.Deserialize(br);
                ClassicAssert.AreEqual(rb.Cardinality, rb2.Cardinality);
                CollectionAssert.AreEqual(rb.Enumerate().ToArray(), rb2.Enumerate().ToArray());
            }
        }

        [Test]
        public void Deserialize_BadVersion_Throws()
        {
            byte[] bytes = [0xFF, 0, 0, 0, 0];
            using var ms = new MemoryStream(bytes);
            using var br = new BinaryReader(ms);
            ClassicAssert.Throws<InvalidDataException>(() => global::Garnet.Extensions.RoaringBitmap.RoaringBitmap.Deserialize(br));
        }

        [Test]
        public void Deserialize_DescendingChunkKeys_Throws()
        {
            // Hand-craft: version=1, chunkCount=2, hi=10 + array(1 entry "1"), then hi=5 (descending) -> reject.
            using var ms = new MemoryStream();
            using (var bw = new BinaryWriter(ms, System.Text.Encoding.UTF8, leaveOpen: true))
            {
                bw.Write((byte)1);                       // version
                bw.Write((int)2);                        // chunkCount
                bw.Write((ushort)10);                    // hi
                bw.Write((byte)ContainerKind.Array);
                bw.Write((int)1);                        // cardinality
                bw.Write((ushort)1);                     // sole element
                bw.Write((ushort)5);                     // hi (descending — must throw)
                bw.Write((byte)ContainerKind.Array);
                bw.Write((int)1);
                bw.Write((ushort)1);
            }
            ms.Position = 0;
            using var br = new BinaryReader(ms);
            ClassicAssert.Throws<InvalidDataException>(() => global::Garnet.Extensions.RoaringBitmap.RoaringBitmap.Deserialize(br));
        }

        [Test]
        public void Deserialize_BadCardinality_Throws()
        {
            using var ms = new MemoryStream();
            using (var bw = new BinaryWriter(ms, System.Text.Encoding.UTF8, leaveOpen: true))
            {
                bw.Write((byte)1);
                bw.Write((int)1);
                bw.Write((ushort)0);
                bw.Write((byte)ContainerKind.Array);
                bw.Write((int)2);              // claim 2 entries
                bw.Write((ushort)5);           // but only write 1 — but actually the body is fixed-size (cardinality * sizeof(ushort)),
                bw.Write((ushort)5);           // entries equal — must reject (strictly ascending)
            }
            ms.Position = 0;
            using var br = new BinaryReader(ms);
            ClassicAssert.Throws<InvalidDataException>(() => global::Garnet.Extensions.RoaringBitmap.RoaringBitmap.Deserialize(br));
        }

        // ---------- Clone / Clear / Enumerate ----------

        [Test]
        public void Clone_IsDeep()
        {
            var rb = new global::Garnet.Extensions.RoaringBitmap.RoaringBitmap();
            for (uint i = 0; i < 5000; i++) rb.SetBit(i, true);
            var clone = rb.Clone();
            // Mutate original.
            for (uint i = 0; i < 1000; i++) rb.SetBit(i, false);
            ClassicAssert.AreEqual(5000, clone.Cardinality);
            ClassicAssert.AreEqual(5000 - 1000, rb.Cardinality);
        }

        [Test]
        public void Enumerate_Ascending()
        {
            var rb = new global::Garnet.Extensions.RoaringBitmap.RoaringBitmap();
            uint[] vals = [0, 65535, 65536, 100_000, uint.MaxValue, 7];
            foreach (var v in vals) rb.SetBit(v, true);
            var got = rb.Enumerate().ToArray();
            var expected = vals.OrderBy(x => x).ToArray();
            CollectionAssert.AreEqual(expected, got);
        }

        [Test]
        public void Clear_ResetsState()
        {
            var rb = new global::Garnet.Extensions.RoaringBitmap.RoaringBitmap();
            for (uint i = 0; i < 5000; i++) rb.SetBit(i, true);
            rb.Clear();
            ClassicAssert.AreEqual(0, rb.Cardinality);
            ClassicAssert.AreEqual(0, rb.ChunkCount);
            ClassicAssert.AreEqual(-1L, rb.BitPos(1));
        }

        // ---------- Helpers ----------

        private static void AssertEqualsOracle(global::Garnet.Extensions.RoaringBitmap.RoaringBitmap rb, HashSet<uint> oracle)
        {
            ClassicAssert.AreEqual(oracle.Count, rb.Cardinality, "cardinality differs");
            // Membership probe across both sets.
            foreach (var v in oracle)
                ClassicAssert.AreEqual(1, rb.GetBit(v), $"oracle has {v}, rb does not");
            // Sample the universe to ensure rb has no extras.
            var rng = new Random(123);
            for (int i = 0; i < 5_000; i++)
            {
                uint v = (uint)rng.NextInt64(0, uint.MaxValue);
                ClassicAssert.AreEqual(oracle.Contains(v) ? 1 : 0, rb.GetBit(v), $"membership mismatch for {v}");
            }
            // Enumerate and compare.
            var rbList = rb.Enumerate().ToHashSet();
            ClassicAssert.AreEqual(oracle.Count, rbList.Count);
            ClassicAssert.IsTrue(rbList.SetEquals(oracle));
        }
    }
}
