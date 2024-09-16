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
        private const long _unixTillStartTimeTicks = 621355968000000000;

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
        /// Converts a Unix timestamp in seconds to ticks.
        /// </summary>
        /// <param name="unixTimestamp">The Unix timestamp in seconds.</param>
        /// <returns>The equivalent number of ticks.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long UnixTimestampInSecondsToTicks(long unixTimestamp)
        {
            return unixTimestamp * TimeSpan.TicksPerSecond + _unixTillStartTimeTicks;
        }

        /// <summary>
        /// Converts a Unix timestamp in milliseconds to ticks.
        /// </summary>
        /// <param name="unixTimestamp">The Unix timestamp in milliseconds.</param>
        /// <returns>The equivalent number of ticks.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long UnixTimestampInMillisecondsToTicks(long unixTimestamp)
        {
            return unixTimestamp * TimeSpan.TicksPerMillisecond + _unixTillStartTimeTicks;
        }
    }
}