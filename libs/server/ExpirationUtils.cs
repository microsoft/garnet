// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    public static class ExpirationUtils
    {
        internal static long EncodeExpirationToInt64(long expirationInMs, ExpireOption expirationOption)
        {
            // Encode the expiration time and expiration option into a single int64 value.
            return ((long)expirationOption << 56) | (expirationInMs & 0x00FFFFFFFFFFFFFF);
        }

        internal static (long expiration, ExpireOption expirationOption) DecodeExpirationFromInt64(long encodedExpiration)
        {
            // Extract the expiration option and expiration time from the encoded value.
            var expirationOption = (ExpireOption)(encodedExpiration >> 56);
            var expiration = (encodedExpiration & 0x00FFFFFFFFFFFFFF);
            return (expiration, expirationOption);
        }

        internal static (int tail, int head) EncodeExpirationToTwoInt32(long expirationInMs, ExpireOption expirationOption)
        {
            // Encode the expiration time and expiration option into two int32 values.
            // Split expiration into head and tail parts.
            var expTail = (uint)(expirationInMs & 0xFFFFFFFF);
            var expHead = (uint)((expirationInMs >> 32) & 0x1FF); // Only need top 9 bits (2^9 = 512)

            return ((int)expTail, (int)(expHead | ((uint)expirationOption << 9)));
        }

        internal static (long expirationInMs, ExpireOption expirationOption) DecodeExpirationFromTwoInt32(int tail, int head)
        {
            // Decode the expiration time and expiration option from two int32 values.
            var expirationOption = (ExpireOption)(head >> 9);
            var expirationInMs = ((long)(head & 0x1FF) << 32) | (uint)(tail & 0xFFFFFFFF);
            return (expirationInMs, expirationOption);
        }
    }
}