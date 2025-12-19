// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Numerics;
using System.Runtime.InteropServices;

namespace Garnet.common
{
    public struct BitVector(int bitCount)
    {
        readonly byte[] vector = new byte[((bitCount - 1) / 8) + 1];

        /// <summary>
        /// Check if bit at index in the bit vector is set
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public bool IsSet(int index)
        {
            var byteIndex = index >> 3;
            var bitIndex = index & 7;
            return (vector[byteIndex] & (byte)(1 << bitIndex)) > 0;
        }

        /// <summary>
        /// Set bit at index
        /// </summary>
        /// <param name="index"></param>
        public void SetBit(int index)
        {
            var byteIndex = index >> 3;
            var bitIndex = index & 7;
            vector[byteIndex] |= (byte)(1 << bitIndex);
        }

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
            var bitVector = new BitVector(span.Length * 8);
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