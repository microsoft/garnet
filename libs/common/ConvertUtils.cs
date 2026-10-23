// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Garnet.common
{
    /// <summary>
    /// Convert data primitives.
    /// </summary>
    public static class ConvertUtils
    {
        /// <summary>
        /// Contains the number of ticks representing 1970/1/1. Value is equal to new DateTime(1970, 1, 1).Ticks
        /// </summary>
        private static readonly long _unixEpochTicks = DateTimeOffset.UnixEpoch.Ticks;

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
                seconds = ticks > 0 ? (ticks + TimeSpan.TicksPerSecond / 2) / TimeSpan.TicksPerSecond : -1;
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
                milliseconds = ticks > 0 ? ticks / TimeSpan.TicksPerMillisecond : -1;
            }
            return milliseconds;
        }

        /// <summary>
        /// Attempts to convert a Unix timestamp in seconds to ticks, failing instead of throwing or
        /// silently overflowing if the result would not be representable as a DateTime/DateTimeOffset.
        /// </summary>
        /// <param name="unixTimestamp">The Unix timestamp in seconds.</param>
        /// <param name="ticks">The equivalent number of ticks, when conversion succeeds.</param>
        /// <returns>True if the conversion succeeded; false if it would overflow the representable date range.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryUnixTimestampInSecondsToTicks(long unixTimestamp, out long ticks)
            => TryScaleAndAddTicks(_unixEpochTicks, unixTimestamp, TimeSpan.TicksPerSecond, out ticks);

        /// <summary>
        /// Attempts to convert a Unix timestamp in milliseconds to ticks, failing instead of throwing or
        /// silently overflowing if the result would not be representable as a DateTime/DateTimeOffset.
        /// </summary>
        /// <param name="unixTimestamp">The Unix timestamp in milliseconds.</param>
        /// <param name="ticks">The equivalent number of ticks, when conversion succeeds.</param>
        /// <returns>True if the conversion succeeded; false if it would overflow the representable date range.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryUnixTimestampInMillisecondsToTicks(long unixTimestamp, out long ticks)
            => TryScaleAndAddTicks(_unixEpochTicks, unixTimestamp, TimeSpan.TicksPerMillisecond, out ticks);

        /// <summary>
        /// Attempts to add a relative number of seconds, from the current UTC time, to an absolute number of
        /// ticks, failing instead of throwing or silently overflowing if the result would not be representable
        /// as a DateTime/DateTimeOffset.
        /// </summary>
        /// <param name="seconds">The number of seconds from now.</param>
        /// <param name="ticks">The resulting absolute number of ticks, when conversion succeeds.</param>
        /// <returns>True if the conversion succeeded; false if it would overflow the representable date range.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryRelativeSecondsToTicks(long seconds, out long ticks)
            => TryScaleAndAddTicks(DateTimeOffset.UtcNow.Ticks, seconds, TimeSpan.TicksPerSecond, out ticks);

        /// <summary>
        /// Attempts to add a relative number of milliseconds, from the current UTC time, to an absolute number
        /// of ticks, failing instead of throwing or silently overflowing if the result would not be
        /// representable as a DateTime/DateTimeOffset.
        /// </summary>
        /// <param name="milliseconds">The number of milliseconds from now.</param>
        /// <param name="ticks">The resulting absolute number of ticks, when conversion succeeds.</param>
        /// <returns>True if the conversion succeeded; false if it would overflow the representable date range.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryRelativeMillisecondsToTicks(long milliseconds, out long ticks)
            => TryScaleAndAddTicks(DateTimeOffset.UtcNow.Ticks, milliseconds, TimeSpan.TicksPerMillisecond, out ticks);

        /// <summary>
        /// Attempts to add a relative number of ticks, from the current UTC time, to an absolute number of ticks,
        /// failing instead of throwing or silently overflowing if the result would not be representable as a
        /// DateTime/DateTimeOffset.
        /// </summary>
        /// <param name="relativeTicks">The number of ticks from now.</param>
        /// <param name="ticks">The resulting absolute number of ticks, when conversion succeeds.</param>
        /// <returns>True if the conversion succeeded; false if it would overflow the representable date range.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryRelativeTicksToTicks(long relativeTicks, out long ticks)
            => TryScaleAndAddTicks(DateTimeOffset.UtcNow.Ticks, relativeTicks, 1, out ticks);

        /// <summary>
        /// Scales <paramref name="value"/> by <paramref name="ticksPerUnit"/> and adds it to
        /// <paramref name="baseTicks"/>, failing instead of throwing or silently wrapping if the result would
        /// not be representable as a DateTime/DateTimeOffset tick count.
        /// </summary>
        private static bool TryScaleAndAddTicks(long baseTicks, long value, long ticksPerUnit, out long ticks)
        {
            try
            {
                checked
                {
                    ticks = baseTicks + (value * ticksPerUnit);
                }
            }
            catch (OverflowException)
            {
                ticks = default;
                return false;
            }

            return ticks >= 0 && ticks <= DateTime.MaxValue.Ticks;
        }

        /// <summary>
        /// Convert ticks to Unix time in seconds.
        /// </summary>
        /// <param name="ticks">The ticks to convert.</param>
        /// <returns>The Unix time in seconds.</returns>
        public static long UnixTimeInSecondsFromTicks(long ticks)
        {
            return ticks > 0 ? (ticks - _unixEpochTicks) / TimeSpan.TicksPerSecond : -1;
        }

        /// <summary>
        /// Convert ticks to Unix time in milliseconds.
        /// </summary>
        /// <param name="ticks">The ticks to convert.</param>
        /// <returns>The Unix time in milliseconds.</returns>
        public static long UnixTimeInMillisecondsFromTicks(long ticks)
        {
            return ticks > 0 ? (ticks - _unixEpochTicks) / TimeSpan.TicksPerMillisecond : -1;
        }
    }
}