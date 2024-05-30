// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Text;

namespace Garnet.common;

/// <summary>
/// Utilites for ASCII parsing and manipulation.
/// </summary>
public static class AsciiUtils
{
    public static byte ToLower(byte value)
    {
        if ((uint)(value - 'A') <= (uint)('Z' - 'A')) // Is in [A-Z]
            value = (byte)(value | 0x20);
        return value;
    }

    /// <summary>
    /// Convert ASCII Span to upper case
    /// </summary>
    public static void ToUpperInPlace(Span<byte> command)
    {
#if NET8_0_OR_GREATER
        Ascii.ToUpperInPlace(command, out _);
#else
        foreach (ref var c in command)
            if (c > 96 && c < 123)
                c -= 32;
#endif
    }

    /// <summary>
    /// Check if two byte spans are equal, where right is an all-upper-case span, ignoring case if there are ASCII bytes.
    /// </summary>
    public static bool EqualsIgnoreCase(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
#if NET8_0_OR_GREATER
        return Ascii.EqualsIgnoreCase(left, right);
#else
        if (left.SequenceEqual(right))
            return true;
        if (left.Length != right.Length)
            return false;
        for (int i = 0; i < left.Length; i++)
        {
            var b1 = left[i];
            var b2 = right[i];

            // Debug assert that b2 is an upper case letter 'A'-'Z'
            Debug.Assert(b2 is >= 65 and <= 90);

            if (b1 == b2 || b1 - 32 == b2)
                continue;
            return false;
        }
        return true;
#endif
    }
}