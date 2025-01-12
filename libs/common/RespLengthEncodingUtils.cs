// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;

namespace Garnet.common;

/// <summary>
/// Utils for working with redis length encoding
/// </summary>
public static class RespLengthEncodingUtils
{
    /// <summary>
    /// Decodes the redis length encoded length and returns payload start
    /// </summary>
    /// <param name="buff"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public static (long length, byte payloadStart) DecodeLength(ref ReadOnlySpan<byte> buff)
    {
        // remove the value type byte
        var encoded = buff.Slice(1);

        if (encoded.Length == 0)
        {
            throw new ArgumentException("Encoded length cannot be empty.", nameof(encoded));
        }

        var firstByte = encoded[0];
        return (firstByte >> 6) switch
        {
            // 6-bit encoding
            0 => (firstByte & 0x3F, 1),
            // 14-bit encoding
            1 when encoded.Length < 2 => throw new ArgumentException("Not enough bytes for 14-bit encoding."),
            1 => (((firstByte & 0x3F) << 8) | encoded[1], 2),
            // 32-bit encoding
            2 when encoded.Length < 5 => throw new ArgumentException("Not enough bytes for 32-bit encoding."),
            2 => ((long)((encoded[1] << 24) | (encoded[2] << 16) | (encoded[3] << 8) | encoded[4]), 5),
            _ => throw new ArgumentException("Invalid encoding type.", nameof(encoded))
        };
    }

    /// <summary>
    /// Encoded payload length to redis encoded payload length
    /// </summary>
    /// <param name="length"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    public static byte[] EncodeLength(long length)
    {
        switch (length)
        {
            // 6-bit encoding (length ≤ 63)
            case < 1 << 6:
                return [(byte)(length & 0x3F)]; // 00xxxxxx
            // 14-bit encoding (64 ≤ length ≤ 16,383)
            case < 1 << 14:
                {
                    var firstByte = (byte)(((length >> 8) & 0x3F) | (1 << 6)); // 01xxxxxx
                    var secondByte = (byte)(length & 0xFF);
                    return [firstByte, secondByte];
                }
            // 32-bit encoding (length ≤ 4,294,967,295)
            case <= 0xFFFFFFFF:
                {
                    var firstByte = (byte)(2 << 6); // 10xxxxxx
                    var lengthBytes = BitConverter.GetBytes((uint)length); // Ensure unsigned
                    if (BitConverter.IsLittleEndian)
                    {
                        Array.Reverse(lengthBytes); // Convert to big-endian
                    }

                    return new[] {firstByte}.Concat(lengthBytes).ToArray();
                }
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(length), "Length exceeds maximum allowed for Redis encoding (4,294,967,295).");
        }
    }
}