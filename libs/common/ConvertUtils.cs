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