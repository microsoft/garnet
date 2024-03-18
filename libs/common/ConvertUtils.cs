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
    }
}