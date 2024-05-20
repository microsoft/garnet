// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;

namespace Garnet.common
{
    /// <summary>
    /// Convert data primitives.
    /// </summary>
    public static class ConvertUtils
    {
        /// <summary>
        /// Convert diff ticks - utcNow.ticks to seconds.
        /// </summary>
        /// <param name="ticks"></param>
        /// <returns></returns>
        public static long SecondsFromDiffUtcNowTicks(long ticks)
        {
            long seconds = -1;
            if (ticks > 0)
            {
                ticks -= DateTimeOffset.UtcNow.Ticks;
                seconds = ticks > 0 ? (long)TimeSpan.FromTicks(ticks).TotalSeconds : -1;
            }
            return seconds;
        }


        /// <summary>
        /// Convert diff ticks - utcNow.ticks to milliseconds.
        /// </summary>
        /// <param name="ticks"></param>
        /// <returns></returns>
        public static long MillisecondsFromDiffUtcNowTicks(long ticks)
        {
            long milliseconds = -1;
            if (ticks > 0)
            {
                ticks -= DateTimeOffset.UtcNow.Ticks;
                milliseconds = ticks > 0 ? (long)TimeSpan.FromTicks(ticks).TotalMilliseconds : -1;
            }
            return milliseconds;
        }

        /// <summary>
        /// Convert ASCII Span to upper case
        /// </summary>
        /// <param name="command"></param>
        public static void MakeUpperCase(Span<byte> command)
        {
            foreach (ref var c in command)
                if (c > 96 && c < 123)
                    c -= 32;
        }

        /// <summary>
        /// Check if two byte spans are equal, where right is an all-upper-case span, ignoring case if there are ASCII bytes.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <returns></returns>
        public static bool EqualsUpperCaseSpanIgnoringCase(this ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
        {
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
        }

        /// <summary>
        /// Check if two byte spans are equal, where right is an all-upper-case span, ignoring case if there are ASCII bytes.
        /// </summary>
        public static bool EqualsUpperCaseSpanIgnoringCase(this Span<byte> left, ReadOnlySpan<byte> right)
            => EqualsUpperCaseSpanIgnoringCase((ReadOnlySpan<byte>)left, right);
    }
}