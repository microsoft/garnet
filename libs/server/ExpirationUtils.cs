// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Utilities for handling expiration timestamps in .NET ticks
    /// </summary>
    public static class ExpirationUtils
    {
        /// <summary>
        /// Encode a .NET ticks expiration timestamps and an ExpireOption enum into a single Int64
        /// </summary>
        /// <param name="expirationTimeInTicks">.NET ticks expiration timestamp</param>
        /// <param name="expirationOption">Expiration option</param>
        /// <returns>Encoded expiration time and expiration option</returns>
        internal static long EncodeExpirationToInt64(long expirationTimeInTicks, ExpireOption expirationOption)
        {
            // Encode the expiration time and expiration option into a single int64 value.
            var coarseTicks = expirationTimeInTicks >> 8;
            return (coarseTicks << 4) | ((long)expirationOption & 0xF);
        }

        /// <summary>
        /// Decode a .NET ticks expiration timestamp and an ExpireOption enum from a single Int64
        /// </summary>
        /// <param name="encodedExpiration">Encoded expiration time and expiration option</param>
        /// <returns>.NET ticks expiration timestamp and ExpireOption enum</returns>
        internal static (long expirationTimeInTicks, ExpireOption expirationOption) DecodeExpirationFromInt64(long encodedExpiration)
        {
            // Extract the expiration option and expiration time from the encoded value.
            var coarseTicks = encodedExpiration >> 4;
            var expirationTimeInTicks = coarseTicks << 8;
            return (expirationTimeInTicks, (ExpireOption)(encodedExpiration & 0xF));
        }

        /// <summary>
        /// Encode a .NET ticks expiration timestamp and an ExpireOption enum into two integers
        /// </summary>
        /// <param name="expirationTimeInTicks">.NET ticks expiration timestamp</param>
        /// <param name="expirationOption">Expiration option</param>
        /// <returns>Encoded tail and head of expiration time and expiration option</returns>
        internal static (int encodedTail, int encodedHead) EncodeExpirationToTwoInt32(long expirationTimeInTicks, ExpireOption expirationOption)
        {
            // Encode the expiration time and expiration option into two int32 values.
            // Split expiration into head and tail parts.
            var coarseTicks = expirationTimeInTicks >> 8;

            var tail = (uint)(coarseTicks & 0xFFFFFFFF);
            var head = (uint)((coarseTicks >> 32) & 0x0FFFFFFF);
            head = (head << 4) | ((uint)expirationOption & 0xF);

            return ((int)tail, (int)head);
        }

        /// <summary>
        /// Decode a .NET ticks expiration timestamp and an ExpireOption enum from two integers
        /// </summary>
        /// <param name="tail">Encoded tail</param>
        /// <param name="head">Encoded head</param>
        /// <returns>.NET ticks expiration timestamp and ExpireOption enum</returns>
        internal static (long expirationInTicks, ExpireOption expirationOption) DecodeExpirationFromTwoInt32(int tail, int head)
        {
            // Decode the expiration time and expiration option from two int32 values.
            var uTail = (uint)tail;
            var uHead = (uint)head;

            var option = (ExpireOption)(uHead & 0xF);
            var coarseTicks = ((long)(uHead >> 4) << 32) | uTail;
            var ticks = coarseTicks << 8;

            return (ticks, option);
        }
    }
}