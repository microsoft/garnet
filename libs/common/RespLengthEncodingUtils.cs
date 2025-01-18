// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;

namespace Garnet.common;

/// <summary>
/// Utils for working with RESP length encoding
/// </summary>
public static class RespLengthEncodingUtils
{
    /// <summary>
    /// Maximum length that can be encoded
    /// </summary>
    private const int MaxLength = 0xFFFFFF;

    /// <summary>
    /// Try read RESP-encoded length
    /// </summary>
    /// <param name="input"></param>
    /// <param name="length"></param>
    /// <param name="bytesRead"></param>
    /// <returns></returns>
    public static bool TryReadLength(ReadOnlySpan<byte> input, out int length, out int bytesRead)
    {
        length = 0;
        bytesRead = 0;
        if (input.Length < 1)
        {
            return false;
        }

        var firstByte = input[0];
        switch (firstByte >> 6)
        {
            case 0:
                bytesRead = 1;
                length = firstByte & 0x3F;
                return true;
            case 1 when input.Length > 1:
                bytesRead = 2;
                length = ((firstByte & 0x3F) << 8) | input[1];
                return true;
            case 2:
                bytesRead = 5;
                return BinaryPrimitives.TryReadInt32BigEndian(input, out length);
            default:
                return false;
        }
    }

    /// <summary>
    /// Try to write RESP-encoded length
    /// </summary>
    /// <param name="length"></param>
    /// <param name="output"></param>
    /// <param name="bytesWritten"></param>
    /// <returns></returns>
    public static bool TryWriteLength(int length, Span<byte> output, out int bytesWritten)
    {
        bytesWritten = 0;

        if (length > MaxLength)
        {
            return false;
        }

        // 6-bit encoding (length ≤ 63)
        if (length < 1 << 6)
        {
            if (output.Length < 1)
            {
                return false;
            }

            output[0] = (byte)(length & 0x3F);

            bytesWritten = 1;
            return true;
        }

        // 14-bit encoding (64 ≤ length ≤ 16,383)
        if (length < 1 << 14)
        {
            if (output.Length < 2)
            {
                return false;
            }

            output[0] = (byte)(((length >> 8) & 0x3F) | (1 << 6));
            output[1] = (byte)(length & 0xFF);

            bytesWritten = 2;
            return true;
        }

        // 32-bit encoding (length ≤ 4,294,967,295)
        if (output.Length < 5)
        {
            return false;
        }

        output[0] = 2 << 6;
        BinaryPrimitives.WriteUInt32BigEndian(output.Slice(1), (uint)length);

        bytesWritten = 5;
        return true;
    }
}