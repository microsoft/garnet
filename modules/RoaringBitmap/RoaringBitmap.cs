// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using GarnetRoaringBitmap.Containers;

namespace GarnetRoaringBitmap
{
    /// <summary>
    /// Compressed bitmap of <see cref="uint"/> elements based on the Roaring Bitmap
    /// algorithm (Chambi, Lemire, Kaser, Godin, 2014).
    ///
    /// The 32-bit element space is partitioned into 65,536 chunks of 65,536 elements
    /// each, addressed by the high 16 bits of the element. Each chunk is stored as
    /// either a sorted-array container (sparse, &lt;= 4096 elements) or a 8 KiB bitmap
    /// container (dense). Empty chunks are not stored. Chunks are kept in
    /// ascending order by high-key for deterministic serialization and efficient
    /// scans (e.g., bit-position queries).
    ///
    /// This class is NOT thread-safe; the parent RoaringBitmapObject (added in a
    /// follow-up commit) relies on Garnet's per-key locking for concurrency.
    ///
    /// Serialization format (stable, versioned):
    ///   byte    version          = 0x01
    ///   int32   chunkCount       (LE)
    ///   foreach chunk in ascending high-key order:
    ///     ushort highKey
    ///     byte   containerKind   (1 = array, 2 = bitmap)
    ///     int32  cardinality
    ///     bytes  body            (kind-dependent)
    /// </summary>
    public sealed class RoaringBitmap
    {
        /// <summary>Wire format version. Bump and add a branch in <see cref="Deserialize"/> for any breaking change.</summary>
        public const byte FormatVersion = 0x01;

        /// <summary>
        /// Hysteresis applied at the parent layer when demoting. A bitmap container whose
        /// cardinality would drop to <see cref="Containers.ArrayContainer.ArrayThreshold"/>
        /// will demote; one whose cardinality immediately re-grows past that threshold
        /// will repromote on the next add. We accept this oscillation cost in exchange
        /// for memory tightness — a more aggressive hysteresis could be added later.
        /// </summary>
        private readonly SortedDictionary<ushort, IContainer> chunks;

        /// <summary>Cached running total. Maintained incrementally on every mutation. Matches sum-of-chunk-cardinalities exactly.</summary>
        private long totalCardinality;

        public RoaringBitmap()
        {
            chunks = new SortedDictionary<ushort, IContainer>();
            totalCardinality = 0;
        }

        private RoaringBitmap(SortedDictionary<ushort, IContainer> chunks, long totalCardinality)
        {
            this.chunks = chunks;
            this.totalCardinality = totalCardinality;
        }

        /// <summary>True iff no bits are set.</summary>
        public bool IsEmpty => totalCardinality == 0;

        /// <summary>Number of elements present (population count). Always &gt;= 0; can exceed int.MaxValue if the universe is fully populated.</summary>
        public long Cardinality => totalCardinality;

        /// <summary>Number of chunks currently allocated. Useful for test assertions and metrics.</summary>
        public int ChunkCount => chunks.Count;

        /// <summary>True iff <paramref name="value"/> is set.</summary>
        public bool Contains(uint value)
        {
            ushort hi = (ushort)(value >> 16);
            ushort lo = (ushort)(value & 0xFFFF);
            return chunks.TryGetValue(hi, out var c) && c.Contains(lo);
        }

        /// <summary>
        /// Sets bit <paramref name="value"/> to 1. Returns the previous value (0 or 1).
        /// </summary>
        public int Add(uint value)
        {
            ushort hi = (ushort)(value >> 16);
            ushort lo = (ushort)(value & 0xFFFF);
            if (!chunks.TryGetValue(hi, out var c))
            {
                var arr = new ArrayContainer();
                var newC = arr.Add(lo, out _); // always added on a fresh container
                chunks[hi] = newC;
                totalCardinality++;
                return 0;
            }
            var updated = c.Add(lo, out bool added);
            if (!ReferenceEquals(updated, c)) chunks[hi] = updated;
            if (added) { totalCardinality++; return 0; }
            return 1;
        }

        /// <summary>
        /// Clears bit <paramref name="value"/>. Returns the previous value (0 or 1).
        /// </summary>
        public int Remove(uint value)
        {
            ushort hi = (ushort)(value >> 16);
            ushort lo = (ushort)(value & 0xFFFF);
            if (!chunks.TryGetValue(hi, out var c)) return 0;
            var updated = c.Remove(lo, out bool removed);
            if (updated == null) { chunks.Remove(hi); }
            else if (!ReferenceEquals(updated, c)) { chunks[hi] = updated; }
            if (removed) { totalCardinality--; return 1; }
            return 0;
        }

        /// <summary>Convenience wrapper used by RESP SETBIT: dispatches to <see cref="Add"/> or <see cref="Remove"/>.</summary>
        public int SetBit(uint offset, bool set) => set ? Add(offset) : Remove(offset);

        /// <summary>0 or 1 — the bit value at <paramref name="offset"/>.</summary>
        public int GetBit(uint offset) => Contains(offset) ? 1 : 0;

        /// <summary>
        /// Position of the first bit equal to <paramref name="bit"/> at or after <paramref name="from"/>.
        /// Returns -1 if no such bit exists in the uint32 universe at or after <paramref name="from"/>.
        ///
        /// For <c>bit==0</c>: scans chunks plus the gaps between/around them. Any unallocated
        /// high-key represents a fully-unset 65,536-bit run, so the first unset bit may be
        /// the first position in a missing high-key.
        /// </summary>
        public long BitPos(int bit, uint from = 0)
        {
            if (bit != 0 && bit != 1) throw new ArgumentOutOfRangeException(nameof(bit), "bit must be 0 or 1");
            ushort fromHi = (ushort)(from >> 16);
            ushort fromLo = (ushort)(from & 0xFFFF);

            if (bit == 1)
            {
                // Find the first chunk whose high-key >= fromHi that has a set bit at or after the low-key.
                foreach (var kv in chunks)
                {
                    if (kv.Key < fromHi) continue;
                    int startLo = kv.Key == fromHi ? fromLo : 0;
                    int found = kv.Value.NextSetBit(startLo);
                    if (found >= 0)
                        return ((long)kv.Key << 16) | (uint)found;
                }
                return -1;
            }
            else
            {
                // bit == 0: any missing high-key in [fromHi..65535] gives an instant answer.
                // We walk in ascending high-key order. For each present chunk, check if it
                // has an unset bit at/after the relevant low-key offset (mind chunk being full).
                // For each *gap* (missing high-key), the first unset is at high << 16.
                int expectedHi = fromHi;
                foreach (var kv in chunks)
                {
                    if (kv.Key < fromHi) continue;
                    if (kv.Key > expectedHi)
                    {
                        // Gap: high-keys [expectedHi .. kv.Key - 1] are entirely unset.
                        int startLo = expectedHi == fromHi ? fromLo : 0;
                        return ((long)expectedHi << 16) | (uint)startLo;
                    }
                    // Same chunk — try to find an unset within it.
                    int startLo2 = kv.Key == fromHi ? fromLo : 0;
                    int found = kv.Value.NextUnsetBit(startLo2);
                    if (found >= 0)
                        return ((long)kv.Key << 16) | (uint)found;
                    // Chunk full from startLo2 to 65535: advance expectedHi.
                    if (kv.Key == 65535) return -1; // exhausted universe
                    expectedHi = (ushort)(kv.Key + 1);
                }
                // No more chunks — the next gap starts at expectedHi.
                if (expectedHi <= 65535)
                {
                    int startLo = expectedHi == fromHi ? fromLo : 0;
                    return ((long)expectedHi << 16) | (uint)startLo;
                }
                return -1;
            }
        }

        /// <summary>
        /// Estimated heap byte cost, including an approximate base-object cost and
        /// approximate per-entry <see cref="SortedDictionary{TKey, TValue}"/> node
        /// overhead. Excludes the .NET object header overhead.
        /// </summary>
        public long ByteSize
        {
            get
            {
                long sum = 24; // approximate RoaringBitmap instance/base-object cost
                foreach (var kv in chunks)
                {
                    // Per-entry estimate: key (2B), reference (8B), red-black tree
                    // node overhead (~40B), and the container's own footprint.
                    sum += 50 + kv.Value.ByteSize;
                }
                return sum;
            }
        }

        /// <summary>Deep clone — used for copy-semantics on transactions and tests.</summary>
        public RoaringBitmap Clone()
        {
            var newChunks = new SortedDictionary<ushort, IContainer>();
            foreach (var kv in chunks)
                newChunks[kv.Key] = kv.Value.Clone();
            return new RoaringBitmap(newChunks, totalCardinality);
        }

        /// <summary>Removes all bits.</summary>
        public void Clear()
        {
            chunks.Clear();
            totalCardinality = 0;
        }

        /// <summary>
        /// Enumerates all set bits in ascending order. O(N) in cardinality.
        /// Allocation-free per element after the initial enumerator.
        /// </summary>
        public IEnumerable<uint> Enumerate()
        {
            foreach (var kv in chunks)
            {
                ushort hi = kv.Key;
                int next = kv.Value.NextSetBit(0);
                while (next >= 0)
                {
                    yield return ((uint)hi << 16) | (uint)next;
                    if (next == 65535) break;
                    next = kv.Value.NextSetBit(next + 1);
                }
            }
        }

        public void Serialize(BinaryWriter writer)
        {
            if (writer == null) throw new ArgumentNullException(nameof(writer));
            writer.Write(FormatVersion);
            writer.Write(chunks.Count);
            foreach (var kv in chunks)
            {
                writer.Write(kv.Key);
                writer.Write((byte)kv.Value.Kind);
                writer.Write(kv.Value.Cardinality);
                kv.Value.SerializeBody(writer);
            }
        }

        public static RoaringBitmap Deserialize(BinaryReader reader)
        {
            if (reader == null) throw new ArgumentNullException(nameof(reader));
            byte version = reader.ReadByte();
            if (version != FormatVersion)
                throw new InvalidDataException($"Unsupported RoaringBitmap format version: 0x{version:X2}");
            int chunkCount = reader.ReadInt32();
            if (chunkCount < 0 || chunkCount > 65536)
                throw new InvalidDataException($"Invalid chunk count: {chunkCount}");

            var dict = new SortedDictionary<ushort, IContainer>();
            long total = 0;
            int prevHi = -1;
            for (int i = 0; i < chunkCount; i++)
            {
                ushort hi = reader.ReadUInt16();
                if (hi <= prevHi)
                    throw new InvalidDataException($"Chunk high-keys must be strictly ascending; got {hi} after {prevHi}");
                prevHi = hi;
                ContainerKind kind = (ContainerKind)reader.ReadByte();
                int card = reader.ReadInt32();
                IContainer container = kind switch
                {
                    ContainerKind.Array => ArrayContainer.DeserializeBody(reader, card),
                    ContainerKind.Bitmap => BitmapContainer.DeserializeBody(reader, card),
                    _ => throw new InvalidDataException($"Unknown container kind: {(byte)kind}"),
                };
                if (container.Cardinality != card)
                    throw new InvalidDataException($"Container cardinality mismatch: stored={card} actual={container.Cardinality}");
                dict[hi] = container;
                total += card;
            }
            return new RoaringBitmap(dict, total);
        }

        // ---- Test/diagnostic helpers ----
        // These are internal so tests in the same assembly (or InternalsVisibleTo) can probe layout.

        internal IReadOnlyDictionary<ushort, IContainer> InternalChunks => chunks;

        internal ContainerKind GetChunkKind(ushort highKey)
        {
            if (!chunks.TryGetValue(highKey, out var c))
                throw new KeyNotFoundException($"No chunk for highKey={highKey}");
            return c.Kind;
        }
    }
}