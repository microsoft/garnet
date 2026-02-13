// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace Garnet.server.Vector.QueryEngine
{
    /// <summary>
    /// Dense bitmap for filtering vectors during DiskANN graph traversal.
    /// Each bit represents whether a vector ID matches the filter predicate.
    /// 
    /// This is a simple dense bitmap (not Roaring) for initial implementation.
    /// For production with large datasets, consider upgrading to Roaring bitmaps
    /// which provide 10-100x compression for sparse filters.
    /// </summary>
    internal sealed class FilterBitmap
    {
        private readonly ulong[] _bits;
        private readonly long _capacity;

        /// <summary>
        /// Number of bits set to 1 (matching vectors).
        /// </summary>
        public long PopCount { get; private set; }

        /// <summary>
        /// Total capacity of the bitmap.
        /// </summary>
        public long Capacity => _capacity;

        /// <summary>
        /// Selectivity: fraction of bits set (0.0 to 1.0).
        /// </summary>
        public double Selectivity => _capacity > 0 ? (double)PopCount / _capacity : 0.0;

        /// <summary>
        /// Create a new bitmap with the specified capacity.
        /// </summary>
        /// <param name="capacity">Maximum vector ID + 1.</param>
        public FilterBitmap(long capacity)
        {
            if (capacity < 0) throw new ArgumentOutOfRangeException(nameof(capacity));
            _capacity = capacity;
            _bits = new ulong[(capacity + 63) / 64];
        }

        /// <summary>
        /// Set a bit (mark vector as matching filter).
        /// </summary>
        public void Set(long vectorId)
        {
            if (vectorId < 0 || vectorId >= _capacity) return;
            var wordIndex = vectorId / 64;
            var bitIndex = (int)(vectorId % 64);
            var mask = 1UL << bitIndex;
            if ((_bits[wordIndex] & mask) == 0)
            {
                _bits[wordIndex] |= mask;
                PopCount++;
            }
        }

        /// <summary>
        /// Check if a bit is set (vector matches filter).
        /// </summary>
        public bool IsSet(long vectorId)
        {
            if (vectorId < 0 || vectorId >= _capacity) return false;
            var wordIndex = vectorId / 64;
            var bitIndex = (int)(vectorId % 64);
            return (_bits[wordIndex] & (1UL << bitIndex)) != 0;
        }

        /// <summary>
        /// Clear a bit (mark vector as not matching).
        /// </summary>
        public void Clear(long vectorId)
        {
            if (vectorId < 0 || vectorId >= _capacity) return;
            var wordIndex = vectorId / 64;
            var bitIndex = (int)(vectorId % 64);
            var mask = 1UL << bitIndex;
            if ((_bits[wordIndex] & mask) != 0)
            {
                _bits[wordIndex] &= ~mask;
                PopCount--;
            }
        }

        /// <summary>
        /// Get the underlying bits array for FFI / DiskANN integration.
        /// </summary>
        public ReadOnlySpan<ulong> GetBits() => _bits;

        /// <summary>
        /// Get a byte span view for passing to native code.
        /// </summary>
        public ReadOnlySpan<byte> GetBytes()
            => System.Runtime.InteropServices.MemoryMarshal.AsBytes(_bits.AsSpan());

        /// <summary>
        /// Build a bitmap from a set of matching vector IDs.
        /// </summary>
        public static FilterBitmap FromIds(IReadOnlySet<long> ids, long capacity)
        {
            var bitmap = new FilterBitmap(capacity);
            foreach (var id in ids)
            {
                bitmap.Set(id);
            }
            return bitmap;
        }

        /// <summary>
        /// AND operation: intersection of two bitmaps.
        /// </summary>
        public static FilterBitmap And(FilterBitmap a, FilterBitmap b)
        {
            var capacity = Math.Max(a._capacity, b._capacity);
            var result = new FilterBitmap(capacity);
            var minLen = Math.Min(a._bits.Length, b._bits.Length);

            for (var i = 0; i < minLen; i++)
            {
                result._bits[i] = a._bits[i] & b._bits[i];
            }
            // Bits beyond minLen are implicitly 0 for the shorter bitmap, so AND = 0

            result.RecalculatePopCount();
            return result;
        }

        /// <summary>
        /// OR operation: union of two bitmaps.
        /// </summary>
        public static FilterBitmap Or(FilterBitmap a, FilterBitmap b)
        {
            var capacity = Math.Max(a._capacity, b._capacity);
            var result = new FilterBitmap(capacity);
            var maxLen = Math.Max(a._bits.Length, b._bits.Length);

            for (var i = 0; i < maxLen; i++)
            {
                var aVal = i < a._bits.Length ? a._bits[i] : 0UL;
                var bVal = i < b._bits.Length ? b._bits[i] : 0UL;
                result._bits[i] = aVal | bVal;
            }

            result.RecalculatePopCount();
            return result;
        }

        /// <summary>
        /// NOT operation: complement of a bitmap.
        /// </summary>
        public static FilterBitmap Not(FilterBitmap a, long totalVectors)
        {
            var result = new FilterBitmap(totalVectors);
            var len = Math.Min(a._bits.Length, result._bits.Length);

            for (var i = 0; i < len; i++)
            {
                result._bits[i] = ~a._bits[i];
            }
            // Fill remaining with all 1s
            for (var i = len; i < result._bits.Length; i++)
            {
                result._bits[i] = ulong.MaxValue;
            }

            // Mask the last word to not set bits beyond totalVectors
            var lastBitIndex = (int)(totalVectors % 64);
            if (lastBitIndex > 0 && result._bits.Length > 0)
            {
                result._bits[^1] &= (1UL << lastBitIndex) - 1;
            }

            result.RecalculatePopCount();
            return result;
        }

        /// <summary>
        /// Enumerate all set bit positions (matching vector IDs).
        /// Useful for pre-filter strategy with small candidate sets.
        /// </summary>
        public IEnumerable<long> EnumerateSetBits()
        {
            for (var wordIndex = 0; wordIndex < _bits.Length; wordIndex++)
            {
                var word = _bits[wordIndex];
                while (word != 0)
                {
                    var bitIndex = System.Numerics.BitOperations.TrailingZeroCount(word);
                    yield return (long)wordIndex * 64 + bitIndex;
                    word &= word - 1; // Clear lowest set bit
                }
            }
        }

        private void RecalculatePopCount()
        {
            long count = 0;
            for (var i = 0; i < _bits.Length; i++)
            {
                count += System.Numerics.BitOperations.PopCount(_bits[i]);
            }
            PopCount = count;
        }
    }
}
