// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections;
using System.Numerics;
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
    /// This class is NOT thread-safe; the parent RoaringBitmapObject
    /// relies on Garnet's per-key locking for concurrency.
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
    public sealed class RoaringBitmap : IEnumerable<uint>
    {
        /// <summary>Wire format version. Bump and add a branch in <see cref="Deserialize"/> for any breaking change.</summary>
        public const byte FormatVersion = 0x01;

        private const int InitialCapacity = 4;

        private ushort[] highKeys;        // sorted ascending, unique, chunkCount entries
        private IContainer[] containers;  // parallel to highKeys
        private int chunkCount;

        /// <summary>Cached running total. Maintained incrementally on every mutation. Matches sum-of-chunk-cardinalities exactly.</summary>
        private long totalCardinality;

        public RoaringBitmap()
            : this(new ushort[InitialCapacity], new IContainer[InitialCapacity], 0, 0)
        {
        }

        private RoaringBitmap(ushort[] highKeys, IContainer[] containers, int chunkCount, long totalCardinality)
        {
            this.highKeys = highKeys;
            this.containers = containers;
            this.chunkCount = chunkCount;
            this.totalCardinality = totalCardinality;
        }

        /// <summary>True iff no bits are set.</summary>
        public bool IsEmpty => totalCardinality == 0;

        /// <summary>Number of elements present (population count). Always &gt;= 0; can exceed int.MaxValue if the universe is fully populated.</summary>
        public long Cardinality => totalCardinality;

        /// <summary>Number of chunks currently allocated. Useful for test assertions and metrics.</summary>
        public int ChunkCount => chunkCount;

        /// <summary>True iff <paramref name="value"/> is set.</summary>
        public bool Contains(uint value)
        {
            var hi = (ushort)(value >> 16);
            var lo = (ushort)(value & 0xFFFF);
            var idx = highKeys.AsSpan(0, chunkCount).BinarySearch(hi);
            return idx >= 0 && containers[idx].Contains(lo);
        }

        /// <summary>
        /// Sets bit <paramref name="value"/> to 1. Returns true if the bit was newly added.
        /// </summary>
        public bool Add(uint value)
        {
            var hi = (ushort)(value >> 16);
            var lo = (ushort)(value & 0xFFFF);
            var idx = highKeys.AsSpan(0, chunkCount).BinarySearch(hi);
            if (idx < 0)
            {
                var insert = ~idx;
                var arr = new ArrayContainer();
                var newC = arr.Add(lo, out _); // always added on a fresh container
                InsertChunk(insert, hi, newC);
                totalCardinality++;
                return true;
            }
            var updated = containers[idx].Add(lo, out var added);
            if (!ReferenceEquals(updated, containers[idx])) containers[idx] = updated;
            if (added) totalCardinality++;
            return added;
        }

        /// <summary>
        /// Clears bit <paramref name="value"/>. Returns true if the bit was previously set.
        /// </summary>
        public bool Remove(uint value)
        {
            var hi = (ushort)(value >> 16);
            var lo = (ushort)(value & 0xFFFF);
            var idx = highKeys.AsSpan(0, chunkCount).BinarySearch(hi);
            if (idx < 0) return false;
            var updated = containers[idx].Remove(lo, out var removed);
            if (updated == null)
                RemoveChunk(idx);
            else if (!ReferenceEquals(updated, containers[idx]))
                containers[idx] = updated;
            if (removed) totalCardinality--;
            return removed;
        }

        /// <summary>Convenience wrapper used by RESP SETBIT: dispatches to <see cref="Add"/> or <see cref="Remove"/>. Returns the previous bit value.</summary>
        public bool SetBit(uint offset, bool set)
        {
            if (set)
                return !Add(offset); // Add=true means newly added → previous was false
            else
                return Remove(offset); // Remove=true means was present → previous was true
        }

        /// <summary>True if the bit at <paramref name="offset"/> is set.</summary>
        public bool GetBit(uint offset) => Contains(offset);

        /// <summary>
        /// Position of the first bit equal to <paramref name="bit"/> at or after <paramref name="from"/>.
        /// Returns -1 if no such bit exists in the uint32 universe at or after <paramref name="from"/>.
        ///
        /// For <c>bit==false</c>: scans chunks plus the gaps between/around them. Any unallocated
        /// high-key represents a fully-unset 65,536-bit run, so the first unset bit may be
        /// the first position in a missing high-key.
        /// </summary>
        public long BitPos(bool bit, uint from = 0)
        {
            var fromHi = (ushort)(from >> 16);
            var fromLo = (ushort)(from & 0xFFFF);

            // Find the first chunk with highKey >= fromHi.
            var startIdx = highKeys.AsSpan(0, chunkCount).BinarySearch(fromHi);
            if (startIdx < 0) startIdx = ~startIdx;

            if (bit)
            {
                for (var i = startIdx; i < chunkCount; i++)
                {
                    var startLo = highKeys[i] == fromHi ? fromLo : 0;
                    var found = containers[i].NextSetBit(startLo);
                    if (found >= 0)
                        return ((long)highKeys[i] << 16) | (uint)found;
                }
                return -1;
            }
            else
            {
                var expectedHi = (int)fromHi;
                for (var i = startIdx; i < chunkCount; i++)
                {
                    if (highKeys[i] > expectedHi)
                    {
                        // Gap: high-keys [expectedHi .. highKeys[i] - 1] are entirely unset.
                        var startLo = expectedHi == fromHi ? fromLo : 0;
                        return ((long)expectedHi << 16) | (uint)startLo;
                    }
                    // Same chunk — try to find an unset within it.
                    var startLo2 = highKeys[i] == fromHi ? fromLo : 0;
                    var found = containers[i].NextUnsetBit(startLo2);
                    if (found >= 0)
                        return ((long)highKeys[i] << 16) | (uint)found;
                    // Chunk full from startLo2 to 65535: advance expectedHi.
                    if (highKeys[i] == 65535) return -1; // exhausted universe
                    expectedHi = highKeys[i] + 1;
                }
                // No more chunks — the next gap starts at expectedHi.
                if (expectedHi <= 65535)
                {
                    var startLo = expectedHi == fromHi ? fromLo : 0;
                    return ((long)expectedHi << 16) | (uint)startLo;
                }
                return -1;
            }
        }

        /// <summary>
        /// Estimated heap byte cost, including an approximate base-object cost and
        /// approximate per-entry overhead. Excludes the .NET object header overhead.
        /// </summary>
        public long ByteSize
        {
            get
            {
                // Base: 32B (object + fields). Per-chunk: 2B key + 8B IContainer ref.
                var sum = 32L + 10L * chunkCount;
                for (var i = 0; i < chunkCount; i++)
                    sum += containers[i].ByteSize;
                return sum;
            }
        }

        /// <summary>Deep clone — used for copy-semantics on transactions and tests.</summary>
        public RoaringBitmap Clone()
        {
            var newKeys = new ushort[Math.Max(InitialCapacity, chunkCount)];
            var newContainers = new IContainer[newKeys.Length];
            Array.Copy(highKeys, newKeys, chunkCount);
            for (var i = 0; i < chunkCount; i++)
                newContainers[i] = containers[i].Clone();
            return new RoaringBitmap(newKeys, newContainers, chunkCount, totalCardinality);
        }

        /// <summary>Removes all bits.</summary>
        public void Clear()
        {
            Array.Clear(containers, 0, chunkCount);
            chunkCount = 0;
            totalCardinality = 0;
        }

        /// <summary>
        /// Enumerates all set bits in ascending order. O(N) in cardinality.
        /// </summary>
        public IEnumerable<uint> Enumerate() => this;

        /// <inheritdoc/>
        public IEnumerator<uint> GetEnumerator()
        {
            for (var i = 0; i < chunkCount; i++)
            {
                var hi = highKeys[i];
                var next = containers[i].NextSetBit(0);
                while (next >= 0)
                {
                    yield return ((uint)hi << 16) | (uint)next;
                    if (next == 65535) break;
                    next = containers[i].NextSetBit(next + 1);
                }
            }
        }

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public void Serialize(BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            writer.Write(FormatVersion);
            writer.Write(chunkCount);
            for (var i = 0; i < chunkCount; i++)
            {
                writer.Write(highKeys[i]);
                writer.Write((byte)containers[i].Kind);
                writer.Write(containers[i].Cardinality);
                containers[i].SerializeBody(writer);
            }
        }

        public static RoaringBitmap Deserialize(BinaryReader reader)
        {
            ArgumentNullException.ThrowIfNull(reader);
            var version = reader.ReadByte();
            if (version != FormatVersion)
                throw new InvalidDataException($"Unsupported RoaringBitmap format version: 0x{version:X2}");
            var count = reader.ReadInt32();
            if (count < 0 || count > 65536)
                throw new InvalidDataException($"Invalid chunk count: {count}");

            var keys = new ushort[Math.Max(InitialCapacity, count)];
            var ctrs = new IContainer[keys.Length];
            long total = 0;
            var prevHi = -1;
            for (var i = 0; i < count; i++)
            {
                var hi = reader.ReadUInt16();
                if (hi <= prevHi)
                    throw new InvalidDataException($"Chunk high-keys must be strictly ascending; got {hi} after {prevHi}");
                prevHi = hi;
                var kind = (ContainerKind)reader.ReadByte();
                var card = reader.ReadInt32();
                IContainer container = kind switch
                {
                    ContainerKind.Array => ArrayContainer.DeserializeBody(reader, card),
                    ContainerKind.Bitmap => BitmapContainer.DeserializeBody(reader, card),
                    _ => throw new InvalidDataException($"Unknown container kind: {(byte)kind}"),
                };
                if (container.Cardinality != card)
                    throw new InvalidDataException($"Container cardinality mismatch: stored={card} actual={container.Cardinality}");
                keys[i] = hi;
                ctrs[i] = container;
                total += card;
            }
            return new RoaringBitmap(keys, ctrs, count, total);
        }

        // ---- Internal helpers ----

        private void InsertChunk(int index, ushort hi, IContainer container)
        {
            EnsureChunkCapacity(chunkCount + 1);
            if (index < chunkCount)
            {
                Array.Copy(highKeys, index, highKeys, index + 1, chunkCount - index);
                Array.Copy(containers, index, containers, index + 1, chunkCount - index);
            }
            highKeys[index] = hi;
            containers[index] = container;
            chunkCount++;
        }

        private void RemoveChunk(int index)
        {
            chunkCount--;
            if (index < chunkCount)
            {
                Array.Copy(highKeys, index + 1, highKeys, index, chunkCount - index);
                Array.Copy(containers, index + 1, containers, index, chunkCount - index);
            }
            containers[chunkCount] = null; // release reference
        }

        private void EnsureChunkCapacity(int required)
        {
            if (required <= highKeys.Length) return;
            var newCap = (int)Math.Min(65536, BitOperations.RoundUpToPowerOf2((uint)required));
            Array.Resize(ref highKeys, newCap);
            Array.Resize(ref containers, newCap);
        }

        // ---- Test/diagnostic helpers ----
        // These are internal so tests in the same assembly (or InternalsVisibleTo) can probe layout.

        internal IReadOnlyList<(ushort hi, IContainer c)> InternalChunks
        {
            get
            {
                var list = new List<(ushort, IContainer)>(chunkCount);
                for (var i = 0; i < chunkCount; i++)
                    list.Add((highKeys[i], containers[i]));
                return list;
            }
        }

        internal ContainerKind GetChunkKind(ushort highKey)
        {
            var idx = highKeys.AsSpan(0, chunkCount).BinarySearch(highKey);
            if (idx < 0)
                throw new KeyNotFoundException($"No chunk for highKey={highKey}");
            return containers[idx].Kind;
        }
    }
}