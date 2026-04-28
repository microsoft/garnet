// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Numerics;
using System.Runtime.InteropServices;

namespace Garnet.common
{
    public struct BitVector(int bytes)
    {
        readonly byte[] vector = new byte[bytes];

        void GetOffsets(int index, out int byteIndex, out int bitIndex)
        {
            byteIndex = index >> 3;
            bitIndex = index & 7;
        }

        /// <summary>
        /// Check if bit at index in the bit vector is set
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public bool IsSet(int index)
        {
            GetOffsets(index, out var byteIndex, out var bitIndex);
            return (vector[byteIndex] & (byte)(1 << bitIndex)) > 0;
        }

        /// <summary>
        /// Set bit at index
        /// </summary>
        /// <param name="index"></param>
        /// <returns>True if bit was previously not set, false otherwise</returns>
        public bool SetBit(int index)
        {
            GetOffsets(index, out var byteIndex, out var bitIndex);
            var wasClear = (vector[byteIndex] & (byte)(1 << bitIndex)) == 0;
            vector[byteIndex] |= (byte)(1 << bitIndex);
            return wasClear;
        }

        /// <summary>
        /// Clear all bits set in this bit vector
        /// </summary>
        public void Clear()
            => Array.Clear(vector);

        /// <summary>
        /// Copy span to this BitVector
        /// </summary>
        /// <param name="span"></param>
        public readonly void CopyTo(Span<byte> span)
            => vector.CopyTo(span);

        /// <summary>
        /// Copy from span
        /// </summary>
        /// <param name="span"></param>
        /// <returns></returns>
        public static BitVector CopyFrom(Span<byte> span)
        {
            var bitVector = new BitVector(span.Length);
            span.CopyTo(bitVector.vector);
            return bitVector;
        }

        /// <summary>
        /// Count bits set in this BitVector
        /// </summary>
        /// <returns></returns>
        public readonly int PopCount()
        {
            var count = 0;
            ReadOnlySpan<ulong> ulongs = MemoryMarshal.Cast<byte, ulong>(vector);
            foreach (var value in ulongs)
                count += BitOperations.PopCount(value);

            // Handle remaining bytes
            var remainder = vector.Length % 8;
            for (var i = vector.Length - remainder; i < vector.Length; i++)
                count += BitOperations.PopCount(vector[i]);

            return count;
        }
    }
}