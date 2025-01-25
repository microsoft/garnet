// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.common;

/// <summary>
/// Port of redis crc64 from https://github.com/redis/redis/blob/7.2/src/crc64.c
/// </summary>
public static class Crc64
{
    /// <summary>
    /// Polynomial (same as redis)
    /// </summary>
    private const ulong POLY = 0xad93d23594c935a9UL;

    /// <summary>
    /// Reverse all bits in a 64-bit value (bit reflection).
    /// Only used for data_len == 64 in this code.
    /// </summary>
    private static ulong Reflect64(ulong data)
    {
        // swap odd/even bits
        data = ((data >> 1) & 0x5555555555555555UL) | ((data & 0x5555555555555555UL) << 1);
        // swap consecutive pairs
        data = ((data >> 2) & 0x3333333333333333UL) | ((data & 0x3333333333333333UL) << 2);
        // swap nibbles
        data = ((data >> 4) & 0x0F0F0F0F0F0F0F0FUL) | ((data & 0x0F0F0F0F0F0F0F0FUL) << 4);
        // swap bytes, then 2-byte pairs, then 4-byte pairs
        data = System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(data);
        return data;
    }

    /// <summary>
    /// A direct bit-by-bit CRC64 calculation (like _crc64 in C).
    /// </summary>
    private static ulong Crc64Bitwise(ReadOnlySpan<byte> data)
    {
        ulong crc = 0;

        foreach (var c in data)
        {
            for (byte i = 1; i != 0; i <<= 1)
            {
                // interpret the top bit of 'crc' and current bit of 'c'
                var bitSet = (crc & 0x8000000000000000UL) != 0;
                var cbit = (c & i) != 0;

                // if cbit flips the sense, invert bitSet
                if (cbit)
                    bitSet = !bitSet;

                // shift
                crc <<= 1;

                // apply polynomial if needed
                if (bitSet)
                    crc ^= POLY;
            }

            // ensure it stays in 64 bits
            crc &= 0xffffffffffffffffUL;
        }

        // reflect and XOR, per standard
        crc &= 0xffffffffffffffffUL;
        crc = Reflect64(crc) ^ 0x0000000000000000UL;
        return crc;
    }

    /// <summary>
    /// Computes crc64
    /// </summary>
    /// <param name="data"></param>
    /// <returns></returns>
    public static byte[] Hash(ReadOnlySpan<byte> data)
    {
        var bitwiseCrc = Crc64Bitwise(data);
        return BitConverter.GetBytes(bitwiseCrc);
    }
}