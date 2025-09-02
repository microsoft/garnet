// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Text;

namespace Garnet.common;

/// <summary>
/// Utilites for ASCII parsing and manipulation.
/// </summary>
/// <remarks>
/// This class polyfills various <see cref="char"/> and <c>Ascii</c> methods for .NET 6.
/// </remarks>
public static class AsciiUtils
{
    /// <summary>Indicates whether a character is within the specified inclusive range.</summary>
    /// <param name="c">The character to evaluate.</param>
    /// <param name="minInclusive">The lower bound, inclusive.</param>
    /// <param name="maxInclusive">The upper bound, inclusive.</param>
    /// <returns>true if <paramref name="c"/> is within the specified range; otherwise, false.</returns>
    /// <remarks>
    /// The method does not validate that <paramref name="maxInclusive"/> is greater than or equal
    /// to <paramref name="minInclusive"/>.  If <paramref name="maxInclusive"/> is less than
    /// <paramref name="minInclusive"/>, the behavior is undefined.
    /// </remarks>
    public static bool IsBetween(byte c, char minInclusive, char maxInclusive)
    {
        return (uint)(c - minInclusive) <= (uint)(maxInclusive - minInclusive);
    }

    public static byte ToLower(byte c)
    {
        if (IsBetween(c, 'A', 'Z'))
            c = (byte)(c | 0x20);
        return c;
    }
    public static byte ToUpper(byte c)
    {
        if (IsBetween(c, 'a', 'z'))
            c = (byte)(c & ~0x20);
        return c;
    }

    /// <summary>
    /// Convert ASCII Span to upper case
    /// </summary>
    public static void ToUpperInPlace(Span<byte> command)
    {
        Ascii.ToUpperInPlace(command, out _);
    }

    /// <summary>
    /// Convert ASCII Span to lower case
    /// </summary>
    public static void ToLowerInPlace(Span<byte> command)
    {
        Ascii.ToLowerInPlace(command, out _);
    }

    /// <inheritdoc cref="EqualsUpperCaseSpanIgnoringCase(ReadOnlySpan{byte}, ReadOnlySpan{byte})"/>
    public static bool EqualsUpperCaseSpanIgnoringCase(this Span<byte> left, ReadOnlySpan<byte> right)
        => EqualsUpperCaseSpanIgnoringCase((ReadOnlySpan<byte>)left, right);

    /// <summary>
    /// Check if two byte spans are equal, where right is an all-upper-case span, ignoring case if there are ASCII bytes.
    /// </summary>
    public static bool EqualsUpperCaseSpanIgnoringCase(this ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        if (left.SequenceEqual(right))
            return true;
        if (left.Length != right.Length)
            return false;

        for (var i = 0; i < left.Length; i++)
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
    }

    /// <summary>
    /// Check if two byte spans are equal, where right is an all-upper-case span, ignoring case if there are ASCII bytes.
    /// </summary>
    public static bool EqualsUpperCaseSpanIgnoringCase(this ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, bool allowNonAlphabeticChars)
    {
        if (left.SequenceEqual(right))
            return true;
        if (left.Length != right.Length)
            return false;

        for (var i = 0; i < left.Length; i++)
        {
            var b1 = left[i];
            var b2 = right[i];

            // Alphabetic characters
            if (b2 is >= 65 and <= 90)
            {
                if (b1 != b2 && b1 - 32 != b2)
                    return false;
            }
            // Non-alphabetic characters
            else
            {
                if (!allowNonAlphabeticChars || b1 != b2)
                    return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Check if two byte spans are equal, where right is an all-lower-case span, ignoring case if there are ASCII bytes.
    /// </summary>
    public static bool EqualsLowerCaseSpanIgnoringCase(this ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, bool allowNonAlphabeticChars)
    {
        if (left.SequenceEqual(right))
            return true;
        if (left.Length != right.Length)
            return false;

        for (var i = 0; i < left.Length; i++)
        {
            var b1 = left[i];
            var b2 = right[i];

            // Alphabetic characters
            if (b2 is >= 97 and <= 122)
            {
                if (b1 != b2 && b1 + 32 != b2)
                    return false;
            }
            // Non-alphabetic characters
            else
            {
                if (!allowNonAlphabeticChars || b1 != b2)
                    return false;
            }
        }

        return true;
    }
}