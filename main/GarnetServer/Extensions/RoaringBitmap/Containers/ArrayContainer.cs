// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.Extensions.RoaringBitmap.Containers
{
    /// <summary>
    /// Sorted-array container: stores up to <see cref="ArrayThreshold"/> 16-bit values in
    /// ascending order. Uses ~2 bytes per element; preferred representation when the
    /// container's cardinality is below the threshold because membership is O(log n)
    /// and the memory footprint scales with cardinality. Above the threshold the parent
    /// promotes the container to a <see cref="BitmapContainer"/> (constant 8 KiB).
    ///
    /// Serialization layout (body only — caller writes kind+cardinality):
    ///   ushort[cardinality] values  (little-endian per BinaryWriter contract)
    /// </summary>
    internal sealed class ArrayContainer : IContainer
    {
        /// <summary>
        /// Crossover point at which an array-encoded container should be promoted to a bitmap.
        /// 4096 is the canonical Roaring threshold: at 4096 elements an array uses 8 KiB
        /// (same as a bitmap container) but membership is still O(log n); past 4096 the
        /// bitmap becomes strictly cheaper per element.
        /// </summary>
        public const int ArrayThreshold = 4096;

        // Initial capacity is intentionally small to keep tiny containers cheap;
        // capacity grows geometrically up to ArrayThreshold + 1 (the +1 lets us detect
        // overflow before promoting in Add).
        private const int InitialCapacity = 4;

        private ushort[] values;
        private int count;

        public ArrayContainer()
        {
            values = new ushort[InitialCapacity];
            count = 0;
        }

        public ArrayContainer(ushort[] sortedUnique, int count)
        {
            if (sortedUnique == null) throw new ArgumentNullException(nameof(sortedUnique));
            if ((uint)count > (uint)sortedUnique.Length) throw new ArgumentOutOfRangeException(nameof(count));
            this.values = sortedUnique;
            this.count = count;
        }

        public ContainerKind Kind => ContainerKind.Array;
        public int Cardinality => count;
        public long ByteSize => 16 + 2L * values.Length; // array header ~16B + 2B per slot

        public bool Contains(ushort value)
        {
            return BinarySearch(value) >= 0;
        }

        /// <summary>Returns the index of <paramref name="value"/>, or the bitwise complement of the insertion position.</summary>
        private int BinarySearch(ushort value)
        {
            int lo = 0;
            int hi = count - 1;
            while (lo <= hi)
            {
                int mid = (lo + hi) >>> 1;
                int cmp = values[mid] - value;
                if (cmp == 0) return mid;
                if (cmp < 0) lo = mid + 1; else hi = mid - 1;
            }
            return ~lo;
        }

        public IContainer Add(ushort value, out bool added)
        {
            int idx = BinarySearch(value);
            if (idx >= 0) { added = false; return this; }

            int insert = ~idx;
            // Promote to bitmap before exceeding the threshold to avoid one wasteful array grow.
            if (count >= ArrayThreshold)
            {
                var bitmap = ToBitmap();
                bitmap.SetUnchecked(value);
                added = true;
                return bitmap;
            }

            EnsureCapacity(count + 1);
            if (insert < count)
            {
                Array.Copy(values, insert, values, insert + 1, count - insert);
            }
            values[insert] = value;
            count++;
            added = true;
            return this;
        }

        public IContainer Remove(ushort value, out bool removed)
        {
            int idx = BinarySearch(value);
            if (idx < 0) { removed = false; return this; }

            if (idx < count - 1)
            {
                Array.Copy(values, idx + 1, values, idx, count - idx - 1);
            }
            count--;
            removed = true;
            return count == 0 ? null : this;
        }

        public ushort First()
        {
            if (count == 0) throw new InvalidOperationException("empty container");
            return values[0];
        }

        public ushort Last()
        {
            if (count == 0) throw new InvalidOperationException("empty container");
            return values[count - 1];
        }

        public int NextSetBit(int from)
        {
            // Find first values[i] >= from
            int lo = 0, hi = count;
            while (lo < hi)
            {
                int mid = (lo + hi) >>> 1;
                if (values[mid] < from) lo = mid + 1; else hi = mid;
            }
            return lo == count ? -1 : values[lo];
        }

        public int NextUnsetBit(int from)
        {
            if (from < 0 || from > 65535) return -1;
            // Find first values[i] >= from. Then walk forward looking for a gap.
            int lo = 0, hi = count;
            while (lo < hi)
            {
                int mid = (lo + hi) >>> 1;
                if (values[mid] < from) lo = mid + 1; else hi = mid;
            }
            // Walk from `from`. If values[lo] != from, then `from` itself is unset.
            int candidate = from;
            while (lo < count && values[lo] == candidate)
            {
                if (candidate == 65535) return -1;
                candidate++;
                lo++;
            }
            return candidate <= 65535 ? candidate : -1;
        }

        public IContainer Clone()
        {
            var copy = new ushort[count]; // tight copy — clones don't preallocate growth
            Array.Copy(values, copy, count);
            return new ArrayContainer(copy, count);
        }

        public void SerializeBody(BinaryWriter writer)
        {
            for (int i = 0; i < count; i++)
                writer.Write(values[i]);
        }

        public static ArrayContainer DeserializeBody(BinaryReader reader, int cardinality)
        {
            if (cardinality < 1 || cardinality > ArrayThreshold)
                throw new InvalidDataException($"ArrayContainer cardinality out of range: {cardinality}");
            var arr = new ushort[cardinality];
            ushort prev = 0;
            for (int i = 0; i < cardinality; i++)
            {
                ushort v = reader.ReadUInt16();
                if (i > 0 && v <= prev)
                    throw new InvalidDataException("ArrayContainer values must be strictly ascending");
                arr[i] = v;
                prev = v;
            }
            return new ArrayContainer(arr, cardinality);
        }

        private void EnsureCapacity(int required)
        {
            if (required <= values.Length) return;
            int newCap = values.Length;
            while (newCap < required) newCap = checked(newCap * 2);
            if (newCap > ArrayThreshold + 1) newCap = ArrayThreshold + 1;
            Array.Resize(ref values, newCap);
        }

        public BitmapContainer ToBitmap()
        {
            var bitmap = new BitmapContainer();
            for (int i = 0; i < count; i++)
                bitmap.SetUnchecked(values[i]);
            return bitmap;
        }

        // Accessor for tests and RunContainer conversion.
        internal ushort GetAt(int index) => values[index];
    }
}
