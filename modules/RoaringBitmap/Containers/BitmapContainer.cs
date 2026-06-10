// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Numerics;
using System.Runtime.InteropServices;

namespace GarnetRoaringBitmap.Containers
{
    /// <summary>
    /// Dense bitmap container: fixed-size 1024-element <see cref="ulong"/> array
    /// (8,192 bytes — exactly 65,536 bits). Membership, set, clear are O(1).
    /// Cardinality is maintained incrementally for O(1) reads.
    ///
    /// The parent demotes back to an <see cref="ArrayContainer"/> when cardinality drops
    /// to <see cref="ArrayContainer.ArrayThreshold"/> with hysteresis applied at the
    /// parent layer.
    ///
    /// Serialization layout (body only):
    ///   ulong[1024] words
    /// </summary>
    internal sealed class BitmapContainer : IContainer
    {
        public const int WordCount = 1024; // 1024 * 64 bits = 65536
        public const int BitsPerWord = 64;

        private readonly ulong[] words;
        private int cardinality;

        public BitmapContainer()
        {
            words = new ulong[WordCount];
            cardinality = 0;
        }

        public BitmapContainer(ulong[] preExistingWords, int cardinality)
        {
            ArgumentNullException.ThrowIfNull(preExistingWords);
            if (preExistingWords.Length != WordCount)
                throw new ArgumentException($"BitmapContainer requires exactly {WordCount} words");
            this.words = preExistingWords;
            this.cardinality = cardinality;
        }

        public ContainerKind Kind => ContainerKind.Bitmap;
        public int Cardinality => cardinality;
        public long ByteSize => 16 + 8L * WordCount; // header + 1024 ulongs

        public bool Contains(ushort value)
        {
            var wordIdx = value >> 6;
            var bitIdx = value & 0x3F;
            return (words[wordIdx] & (1UL << bitIdx)) != 0;
        }

        public IContainer Add(ushort value, out bool added)
        {
            var wordIdx = value >> 6;
            var bitIdx = value & 0x3F;
            var mask = 1UL << bitIdx;
            var before = words[wordIdx];
            var after = before | mask;
            if (after == before)
            {
                added = false;
                return this;
            }
            words[wordIdx] = after;
            cardinality++;
            added = true;
            return this;
        }

        /// <summary>Set without changing cardinality bookkeeping. Used during ToBitmap rebuilds where caller knows the count.</summary>
        internal void SetUnchecked(ushort value)
        {
            var wordIdx = value >> 6;
            var bitIdx = value & 0x3F;
            var mask = 1UL << bitIdx;
            var before = words[wordIdx];
            var after = before | mask;
            if (after != before)
            {
                words[wordIdx] = after;
                cardinality++;
            }
        }

        public IContainer Remove(ushort value, out bool removed)
        {
            var wordIdx = value >> 6;
            var bitIdx = value & 0x3F;
            var mask = 1UL << bitIdx;
            var before = words[wordIdx];
            if ((before & mask) == 0)
            {
                removed = false;
                return this;
            }
            words[wordIdx] = before & ~mask;
            cardinality--;
            removed = true;

            // Demote to array container if we've crossed below the threshold.
            // The threshold is reused but with NO hysteresis at this layer because the
            // parent decides hysteresis when bouncing around the boundary.
            if (cardinality <= ArrayContainer.ArrayThreshold)
                return ToArrayContainer();
            return this;
        }

        public ushort First()
        {
            var span = words.AsSpan();
            var idx = span.IndexOfAnyExcept(0UL);
            return (ushort)((idx << 6) + BitOperations.TrailingZeroCount(span[idx]));
        }

        public ushort Last()
        {
            var span = words.AsSpan();
            var idx = span.LastIndexOfAnyExcept(0UL);
            return (ushort)((idx << 6) + (BitsPerWord - 1 - BitOperations.LeadingZeroCount(span[idx])));
        }

        public int NextSetBit(int from)
        {
            var wordIdx = from >> 6;
            var bitIdx = from & 0x3F;
            var word = words[wordIdx] & (~0UL << bitIdx);
            while (true)
            {
                if (word != 0)
                    return (wordIdx << 6) + BitOperations.TrailingZeroCount(word);
                wordIdx++;
                if (wordIdx >= WordCount) return -1;
                word = words[wordIdx];
            }
        }

        public int NextUnsetBit(int from)
        {
            var wordIdx = from >> 6;
            var bitIdx = from & 0x3F;
            // Invert the word so an unset bit appears as 1, then find first set in it.
            var word = ~words[wordIdx] & (~0UL << bitIdx);
            while (true)
            {
                if (word != 0)
                {
                    var pos = (wordIdx << 6) + BitOperations.TrailingZeroCount(word);
                    return pos > 65535 ? -1 : pos;
                }
                wordIdx++;
                if (wordIdx >= WordCount) return -1;
                word = ~words[wordIdx];
            }
        }

        public IContainer Clone() => new BitmapContainer(words.ToArray(), cardinality);

        public void SerializeBody(BinaryWriter writer)
        {
            writer.Write(MemoryMarshal.AsBytes<ulong>(words.AsSpan()));
        }

        public static BitmapContainer DeserializeBody(BinaryReader reader, int cardinality)
        {
            if (cardinality < 1 || cardinality > 65536)
                throw new InvalidDataException($"BitmapContainer cardinality out of range: {cardinality}");
            var w = new ulong[WordCount];
            reader.BaseStream.ReadExactly(MemoryMarshal.AsBytes(w.AsSpan()));
            var actualCount = 0;
            foreach (var v in w) actualCount += BitOperations.PopCount(v);
            if (actualCount != cardinality)
                throw new InvalidDataException($"BitmapContainer popcount {actualCount} != stored cardinality {cardinality}");
            return new BitmapContainer(w, cardinality);
        }

        public ArrayContainer ToArrayContainer()
        {
            var arr = new ushort[cardinality];
            var outIdx = 0;
            for (var wi = 0; wi < WordCount; wi++)
            {
                var w = words[wi];
                while (w != 0)
                {
                    var bit = BitOperations.TrailingZeroCount(w);
                    arr[outIdx++] = (ushort)((wi << 6) + bit);
                    w &= w - 1;
                }
            }
            return new ArrayContainer(arr, cardinality);
        }

        /// <summary>Direct word access for tests and bulk ops. Do not mutate without updating cardinality.</summary>
        internal ulong[] WordsUnsafe => words;
    }
}