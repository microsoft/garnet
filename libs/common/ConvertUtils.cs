// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

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
        /// Check if two byte spans are equal, ignoring case if there are ASCII bytes.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <returns></returns>
        public static bool EqualsIgnoreCase(this ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
        {
            if (left.SequenceEqual(right))
                return true;
            if (left.Length != right.Length)
                return false;
            for (int i = 0; i < left.Length; i++)
            {
                byte b1 = left[i];
                byte b2 = right[i];
                if (b1 == b2)
                    continue;
                if (b1 >= 65 && b1 <= 90)
                {
                    if (b1 + 32 == b2)
                        continue;
                }
                else if (b1 >= 97 && b1 <= 122)
                {
                    if (b1 - 32 == b2)
                        continue;
                }
                return false;
            }
            return true;
        }

        /// <summary>
        /// Check if two byte spans are equal, ignoring case if there are ASCII bytes.
        /// </summary>
        public static bool EqualsIgnoreCase(this Span<byte> left, ReadOnlySpan<byte> right)
            => EqualsIgnoreCase((ReadOnlySpan<byte>)left, right);
    }
}