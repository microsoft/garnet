// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Utilities for handling unix expiration timestamps
    /// </summary>
    public static class ExpirationUtils
    {
        /// <summary>
        /// Encode a unix expiration timestamps in milliseconds and an ExpireOption enum into a single Int64
        /// </summary>
        /// <param name="expirationTimeInMs">Unix expiration timestamp in milliseconds</param>
        /// <param name="expirationOption">Expiration option</param>
        /// <returns>Encoded expiration time and expiration option</returns>
        internal static long EncodeExpirationToInt64(long expirationTimeInMs, ExpireOption expirationOption)
        {
            // Encode the expiration time and expiration option into a single int64 value.
            return ((long)expirationOption << 56) | (expirationTimeInMs & 0x00FFFFFFFFFFFFFF);
        }

        /// <summary>
        /// Decode a unix expiration timestamp in milliseconds and an ExpireOption enum from a single Int64
        /// </summary>
        /// <param name="encodedExpiration">Encoded expiration time and expiration option</param>
        /// <returns>Unix expiration timestamp in milliseconds and ExpireOption enum</returns>
        internal static (long expirationTimeInMs, ExpireOption expirationOption) DecodeExpirationFromInt64(long encodedExpiration)
        {
            // Extract the expiration option and expiration time from the encoded value.
            var expirationOption = (ExpireOption)(encodedExpiration >> 56);
            var expiration = (encodedExpiration & 0x00FFFFFFFFFFFFFF);
            return (expiration, expirationOption);
        }

        /// <summary>
        /// Encode a unix expiration timestamp in milliseconds and an ExpireOption enum into two integers
        /// </summary>
        /// <param name="expirationTimeInMs">Unix expiration timestamp in milliseconds</param>
        /// <param name="expirationOption">Expiration option</param>
        /// <returns>Encoded tail & head of expiration time and expiration option</returns>
        internal static (int encodedTail, int encodedHead) EncodeExpirationToTwoInt32(long expirationTimeInMs, ExpireOption expirationOption)
        {
            // Encode the expiration time and expiration option into two int32 values.
            // Split expiration into head and tail parts.
            var expTail = (uint)(expirationTimeInMs & 0xFFFFFFFF);
            var expHead = (uint)((expirationTimeInMs >> 32) & 0x1FF); // Only need top 9 bits (2^9 = 512)

            return ((int)expTail, (int)(expHead | ((uint)expirationOption << 9)));
        }

        /// <summary>
        /// Decode a unix expiration timestamp in milliseconds and an ExpireOption enum from two integers
        /// </summary>
        /// <param name="tail">Encoded tail</param>
        /// <param name="head">Encoded head</param>
        /// <returns>Unix expiration timestamp in milliseconds and ExpireOption enum</returns>
        internal static (long expirationInMs, ExpireOption expirationOption) DecodeExpirationFromTwoInt32(int tail, int head)
        {
            // Decode the expiration time and expiration option from two int32 values.
            var expirationOption = (ExpireOption)(head >> 9);
            var expirationInMs = ((long)(head & 0x1FF) << 32) | (uint)(tail & 0xFFFFFFFF);
            return (expirationInMs, expirationOption);
        }
    }
}